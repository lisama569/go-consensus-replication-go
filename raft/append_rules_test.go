package raft

import (
	"context"
	"testing"
)

// TestAppendConflictRejections: 直接对 follower 构造各种异常 AppendEntries，
// 验证拒绝原因彼此可区分，且任何异常请求都不会静默改变日志/提交位置。
func TestAppendConflictRejections(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	ctx := context.Background()

	// 先建立 3 条客户端日志（索引 2..4）。
	for i := 1; i <= 3; i++ {
		r := leader.ClientWrite(ctx, writeReq("app", uint64(i), "k", "v"))
		if !r.OK {
			t.Fatalf("预置写失败: %s", r.Reject)
		}
	}
	waitConverge(t, c, map[string]string{"k": "v"})

	follower := c.Node(c.IDs()[1])
	if follower.Status().ID == leader.Status().ID {
		follower = c.Node(c.IDs()[2])
	}
	st := follower.Status()
	term := st.Term
	before := follower.Status()

	// 1) 乱序追加：prevLogIndex 远大于本地末尾 -> log-conflict + hint。
	resp := follower.HandleAppend(AppendRequest{
		Term: term, LeaderID: leader.Status().ID,
		PrevLogIndex: before.LastLogIndex + 10, PrevLogTerm: term,
		Entries: []Entry{{Term: term, Op: OpPut, Key: "x"}},
	})
	if resp.Success || resp.Reject != RejectLogConflict {
		t.Fatalf("乱序追加应返回 log-conflict, got success=%v reject=%s",
			resp.Success, resp.Reject)
	}
	if resp.ConflictIndex == 0 {
		t.Fatal("日志缺失冲突应回传 ConflictIndex 快速回退提示")
	}
	t.Logf("乱序追加被拒 reason=%s conflictHint=(%d,%d)",
		resp.Reject, resp.ConflictIndex, resp.ConflictTerm)

	// 2) 任期不匹配：同索引 prevLogTerm 错误 -> log-conflict + conflictTerm。
	resp = follower.HandleAppend(AppendRequest{
		Term: term, LeaderID: leader.Status().ID,
		PrevLogIndex: before.LastLogIndex, PrevLogTerm: term + 99,
		Entries: []Entry{{Term: term, Op: OpPut, Key: "y"}},
	})
	if resp.Success || resp.Reject != RejectLogConflict || resp.ConflictTerm == 0 {
		t.Fatalf("任期不匹配应返回带 conflictTerm 的 log-conflict, got %+v", resp)
	}
	t.Logf("任期不匹配被拒 reason=%s conflictTerm=%d", resp.Reject, resp.ConflictTerm)

	// 3) 旧任期请求 -> stale-term，必须与日志类拒绝区分开。
	resp = follower.HandleAppend(AppendRequest{
		Term: 0, LeaderID: "ghost", PrevLogIndex: 1, PrevLogTerm: 0,
	})
	if resp.Success || resp.Reject != RejectStaleTerm {
		t.Fatalf("旧任期追加应返回 stale-term, got %+v", resp)
	}

	// 4) 试图回退已提交日志：构造一条与已提交区间同索引、不同任期的覆盖请求。
	//    必须返回 committed-rollback，绝不能静默截断。
	resp = follower.HandleAppend(AppendRequest{
		Term: term, LeaderID: leader.Status().ID,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []Entry{
			{Term: term + 5, Op: OpPut, Key: "forged", Value: "1"},
		},
	})
	if resp.Success {
		t.Fatal("覆盖已提交区间的请求绝不能成功")
	}
	if resp.Reject != RejectCommittedRollback {
		t.Fatalf("回退已提交日志应返回 committed-rollback, 实际 %s", resp.Reject)
	}
	t.Logf("回退已提交日志被拒 reason=%s commit=%d", resp.Reject, before.CommitIndex)

	// 经过全部恶意请求，follower 状态必须原样未动。
	after := follower.Status()
	if after.CommitIndex != before.CommitIndex ||
		after.LastLogIndex != before.LastLogIndex ||
		after.LastApplied != before.LastApplied {
		t.Fatalf("被拒绝的请求改变了节点状态: before=(%d/%d/%d) after=(%d/%d/%d)",
			before.CommitIndex, before.LastLogIndex, before.LastApplied,
			after.CommitIndex, after.LastLogIndex, after.LastApplied)
	}
	if v, ok := after.SMData["forged"]; ok {
		t.Fatalf("伪造条目污染了状态机: %q", v)
	}
}

// TestSnapshotCatchUp: 手工安装快照 RPC，落后节点按快照位置对齐，
// 且重复安装是幂等的。
func TestSnapshotInstallIdempotent(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	data := map[string]string{"s1": "a", "s2": "b"}

	req := SnapshotRequest{
		Term: leader.Status().Term, LeaderID: leader.Status().ID,
		LastInc: 10, LastTerm: leader.Status().Term, Data: data,
	}
	follower := c.Node(c.IDs()[1])
	if follower.Status().ID == leader.Status().ID {
		follower = c.Node(c.IDs()[2])
	}
	resp1 := follower.HandleSnapshot(req)
	resp2 := follower.HandleSnapshot(req) // 重复投递
	if !resp1.OK || !resp2.OK {
		t.Fatalf("快照安装/重复安装都应成功: %+v %+v", resp1, resp2)
	}
	st := follower.Status()
	if st.CommitIndex < 10 || st.LastApplied < 10 {
		t.Fatalf("安装快照后提交/应用位置应推进: commit=%d applied=%d",
			st.CommitIndex, st.LastApplied)
	}
	if st.SMData["s1"] != "a" || st.SMData["s2"] != "b" {
		t.Fatalf("快照数据未加载: %+v", st.SMData)
	}
	// 更旧任期的快照必须拒绝。
	old := follower.HandleSnapshot(SnapshotRequest{
		Term: 0, LeaderID: "ghost", LastInc: 99, Data: map[string]string{},
	})
	if old.OK || old.Reject != RejectStaleTerm {
		t.Fatalf("旧任期快照应 stale-term 拒绝, got %+v", old)
	}
	t.Logf("快照安装幂等, commit=%d applied=%d baseIndex=%d",
		st.CommitIndex, st.LastApplied, st.LastLogIndex)
}
