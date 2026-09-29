package raft

import (
	"errors"
	"testing"
	"time"
)

func newTestCluster(t *testing.T, n int, fast bool) *Cluster {
	t.Helper()
	ids := make([]string, n)
	for i := range ids {
		ids[i] = nodeName(i)
	}
	cfg := ClusterConfig{IDs: ids, Logger: NewTestLogger(t, "cluster")}
	if fast {
		cfg.ElectionTimeoutMin = 200 * time.Millisecond
		cfg.ElectionTimeoutMax = 400 * time.Millisecond
		cfg.HeartbeatInterval = 40 * time.Millisecond
	}
	c := NewCluster(cfg)
	c.Start()
	t.Cleanup(c.Stop)
	return c
}

func nodeName(i int) string {
	return "n" + string(rune('1'+i))
}

func TestElectSingleLeader(t *testing.T) {
	c := newTestCluster(t, 5, true)

	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("期望选出唯一 leader: %v", err)
	}
	st := leader.Status()
	t.Logf("选举完成: leader=%s term=%d commit=%d lastIndex=%d",
		st.ID, st.Term, st.CommitIndex, st.LastLogIndex)

	// 再等几个心跳周期，确认没有抖动出第二个 leader 或任期乱跳。
	time.Sleep(300 * time.Millisecond)
	l2, err := c.Leader(nil)
	if err != nil {
		t.Fatalf("稳定期后查询 leader 失败: %v", err)
	}
	if l2.Status().ID != st.ID {
		t.Fatalf("无故障情况下 leader 发生切换: %s -> %s", st.ID, l2.Status().ID)
	}
	if l2.Status().Term != st.Term {
		t.Fatalf("无故障情况下任期变化: %d -> %d", st.Term, l2.Status().Term)
	}
}

func TestStaleTermRejected(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("未选出 leader: %v", err)
	}
	term := leader.Status().Term

	// 等待所有节点都已收到当前任期心跳（term 同步），避免节点尚在 term 0
	// 时把 term-0 请求视为合法拉票。
	deadline := timeNow().Add(2 * time.Second)
	for timeNow().Before(deadline) {
		synced := true
		for _, id := range c.IDs() {
			if c.Node(id).Status().Term < term {
				synced = false
			}
		}
		if synced {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range c.IDs() {
		if c.Node(id).Status().Term < term {
			t.Fatalf("节点 %s 任期尚未同步到 %d", id, term)
		}
	}

	// 用一个更小的任期向各节点发 RequestVote / AppendEntries，
	// 必须全部拒绝，且原因可区分为 stale-term。
	staleVote := VoteRequest{Term: 0, CandidateID: "ghost", LastLogIndex: 100}
	for _, id := range c.IDs() {
		resp := c.Node(id).HandleVote(staleVote)
		if resp.VoteGranted {
			t.Fatalf("%s 不应给旧任期投票", id)
		}
		if !errors.Is(&RaftError{Reason: resp.Reject}, RejectStaleTerm) {
			t.Fatalf("%s 拒绝原因应为 stale-term, 实际 %s", id, resp.Reject)
		}
	}
	staleAppend := AppendRequest{
		Term: 0, LeaderID: "ghost", PrevLogIndex: 100, PrevLogTerm: 0,
	}
	for _, id := range c.IDs() {
		resp := c.Node(id).HandleAppend(staleAppend)
		if resp.Success {
			t.Fatalf("%s 不应接受旧任期 Append", id)
		}
		if resp.Reject != RejectStaleTerm {
			t.Fatalf("%s Append 拒绝原因应为 stale-term, 实际 %s", id, resp.Reject)
		}
	}
	t.Logf("旧任期请求均被拒绝, 当前 term=%d", term)
}
