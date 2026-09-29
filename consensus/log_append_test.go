package consensus

import "testing"

// 直接驱动 AppendEntries 入口，验证各种异常追加都有可区分的拒绝原因，
// 而不会被静默接受。
func TestAppendEntriesRejectReasons(t *testing.T) {
	c := NewCluster(3, nil) // 静默日志，聚焦判定
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.nodes["n3"]
	f.currentTerm = 1
	// 手工铺设 [1@1, 2@1, 3@2] 三条日志。
	f.log.Append(Entry{Index: 1, Term: 1})
	f.log.Append(Entry{Index: 2, Term: 1})
	f.log.Append(Entry{Index: 3, Term: 2})

	leader := "n1"

	// 1) 前向跳跃（prevLogIndex=5 超过本地 lastIndex=3）：log_gap
	r := f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 5, PrevLogTerm: 2,
		Entries: []Entry{{Index: 6, Term: 1}},
	})
	if r.Success || r.Reason != ReasonLogGap {
		t.Fatalf("gap: success=%v reason=%v want log_gap", r.Success, r.Reason)
	}

	// 2) prevLog 任期冲突（2@2，本地 index2 的任期是 1）：log_conflict
	r = f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []Entry{{Index: 3, Term: 2}},
	})
	if r.Success || r.Reason != ReasonLogConflict {
		t.Fatalf("conflict: success=%v reason=%v want log_conflict", r.Success, r.Reason)
	}

	// 3) 冲突条目若会改写"已提交"位置：replayed_append
	f.commitIndex = 2
	f.lastApplied = 2
	r = f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 2, PrevLogTerm: 2,
		Entries: []Entry{{Index: 3, Term: 2}},
	})
	if r.Success || r.Reason != ReasonReplayedAppend {
		t.Fatalf("committed overwrite: success=%v reason=%v want replayed_append", r.Success, r.Reason)
	}

	// 4) 更小任期：stale_term
	r = f.onAppendEntries(&AppendArgs{Term: 0, LeaderID: "old", PrevLogIndex: 3, PrevLogTerm: 2})
	if r.Success || r.Reason != ReasonStaleTerm {
		t.Fatalf("stale: success=%v reason=%v want stale_term", r.Success, r.Reason)
	}

	// 5) 正常追加新任期条目：成功，并裁剪冲突尾部
	before := f.log.LastIndex()
	r = f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []Entry{{Index: 4, Term: 1}},
	})
	if !r.Success {
		t.Fatalf("valid append rejected: reason=%v", r.Reason)
	}
	if f.log.LastIndex() != before+1 {
		t.Fatalf("log length = %d want %d", f.log.LastIndex(), before+1)
	}

	// 6) 重复投递同一条目：幂等成功，长度不变
	r = f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []Entry{{Index: 4, Term: 1}},
	})
	if !r.Success || f.log.LastIndex() != 4 {
		t.Fatalf("duplicate append not idempotent: success=%v last=%d", r.Success, f.log.LastIndex())
	}

	// 7) 同位置不同任期（未提交）：冲突裁剪后用新条目覆盖
	r = f.onAppendEntries(&AppendArgs{
		Term: 1, LeaderID: leader, PrevLogIndex: 3, PrevLogTerm: 2,
		Entries: []Entry{{Index: 4, Term: 3}},
	})
	if !r.Success {
		t.Fatalf("tail conflict overwrite rejected: reason=%v", r.Reason)
	}
	t4, _ := f.log.TermAt(4)
	if t4 != 3 {
		t.Fatalf("conflicting tail not truncated/replaced, term4=%d want 3", t4)
	}
}

func TestVoteRejectReasons(t *testing.T) {
	c := NewCluster(3, nil)
	c.mu.Lock()
	defer c.mu.Unlock()
	f := c.nodes["n3"]
	f.currentTerm = 2
	f.votedFor = "n1"
	f.log.Append(Entry{Index: 1, Term: 2})

	// 更小任期
	r := f.onRequestVote(&VoteArgs{Term: 1, CandidateID: "n2", LastLogIndex: 9, LastLogTerm: 9})
	if r.Granted || r.Reason != ReasonStaleTerm {
		t.Fatalf("stale vote: granted=%v reason=%v", r.Granted, r.Reason)
	}
	// 同任期已投他人
	r = f.onRequestVote(&VoteArgs{Term: 2, CandidateID: "n2", LastLogIndex: 9, LastLogTerm: 9})
	if r.Granted || r.Reason != ReasonAlreadyVoted {
		t.Fatalf("already voted: granted=%v reason=%v", r.Granted, r.Reason)
	}
	// 更高任期但日志落后（候选者只有 1@1，本地是 1@2）
	r = f.onRequestVote(&VoteArgs{Term: 3, CandidateID: "n2", LastLogIndex: 1, LastLogTerm: 1})
	if r.Granted || r.Reason != ReasonLogNotUpToDate {
		t.Fatalf("behind log: granted=%v reason=%v", r.Granted, r.Reason)
	}
	if f.currentTerm != 3 || f.votedFor != "" {
		t.Fatalf("stale election must reset candidate vote, term=%d voted=%q", f.currentTerm, f.votedFor)
	}
	// 更高任期且日志够新 -> 授权
	r = f.onRequestVote(&VoteArgs{Term: 3, CandidateID: "n2", LastLogIndex: 2, LastLogTerm: 3})
	if !r.Granted {
		t.Fatalf("valid vote denied: reason=%v", r.Reason)
	}
}

func TestUnreachableReason(t *testing.T) {
	c := NewCluster(3, nil)
	c.Isolate("n1")
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.RequestVote("n1", "n2", &VoteArgs{Term: 1}); ReasonOf(err) != ReasonUnreachable {
		t.Fatalf("partitioned rpc reason=%v want unreachable", ReasonOf(err))
	}
	c.nodes["n2"].down = true
	if _, err := c.AppendEntries("n1", "n2", &AppendArgs{Term: 1}); ReasonOf(err) != ReasonUnreachable {
		t.Fatalf("down peer rpc reason=%v want unreachable", ReasonOf(err))
	}
}
