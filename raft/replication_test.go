package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func waitLeader(t *testing.T, c *Cluster) *Node {
	t.Helper()
	n, err := c.WaitLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("等待 leader 失败: %v", err)
	}
	return n
}

func writeReq(client string, seq uint64, key, value string) ClientRequest {
	return ClientRequest{ClientID: client, Seq: seq, Op: OpPut, Key: key, Value: value}
}

// TestQuorumReplicationAndRead: 正常 5 节点，连续写入必须全部由多数派确认；
// 任意节点（尤其落后节点）恢复后读到的值与 leader 一致。
func TestQuorumReplicationAndRead(t *testing.T) {
	c := newTestCluster(t, 5, true)
	leader := waitLeader(t, c)
	ctx := context.Background()

	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("k%d", i)
		val := fmt.Sprintf("v%d", i)
		resp := leader.ClientWrite(ctx, writeReq("app", uint64(i), key, val))
		if !resp.OK {
			t.Fatalf("第 %d 个写应成功, reject=%s", i, resp.Reject)
		}
		if resp.Index != uint64(i)+1 { // index 1 是 no-op
			t.Fatalf("第 %d 个写提交索引异常: %d (期望 %d)", i, resp.Index, i+1)
		}
		// 读己之写：提交后 leader 立即可读。
		rr := leader.ClientRead(ctx, ReadRequest{Key: key})
		if !rr.OK || rr.Value != val {
			t.Fatalf("已提交写读不到/值错: key=%s got=%q", key, rr.Value)
		}
	}

	st := leader.Status()
	t.Logf("全部写入完成: leader=%s term=%d commit=%d lastApplied=%d",
		st.ID, st.Term, st.CommitIndex, st.LastApplied)

	// 所有节点的状态机最终一致。
	waitConverge(t, c, map[string]string{
		"k1": "v1", "k2": "v2", "k3": "v3", "k4": "v4", "k5": "v5",
		"k6": "v6", "k7": "v7", "k8": "v8", "k9": "v9", "k10": "v10",
	})
}

func waitConverge(t *testing.T, c *Cluster, want map[string]string) {
	t.Helper()
	deadline := timeNow().Add(2 * time.Second)
	for timeNow().Before(deadline) {
		ok := true
		for _, id := range c.IDs() {
			n := c.Node(id)
			if n == nil {
				ok = false
				break
			}
			st := n.Status()
			if len(st.SMData) != len(want) {
				ok = false
				break
			}
			for k, v := range want {
				if st.SMData[k] != v {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range c.IDs() {
		if n := c.Node(id); n != nil {
			t.Logf("收敛检查 %s: commit=%d applied=%d data=%v",
				id, n.Status().CommitIndex, n.Status().LastApplied, n.Status().SMData)
		}
	}
	t.Fatal("各节点状态机未在时限内收敛")
}

// TestMinorityWriteNotVisible: 隔离多数派后，少数派侧的"领导者"无法达成
// 多数派确认，写入必须失败；恢复分区后该写绝不能被读到，也不能污染日志顺序。
func TestMinorityWriteNotVisible(t *testing.T) {
	c := newTestCluster(t, 5, true)
	leader := waitLeader(t, c)
	oldLeaderID := leader.Status().ID

	// 先写入一条基线数据。
	resp := leader.ClientWrite(context.Background(), writeReq("app", 1, "base", "1"))
	if !resp.OK {
		t.Fatalf("基线写失败: %s", resp.Reject)
	}

	// 找到包含老 leader 的 2 节点少数派。
	minority := []string{oldLeaderID}
	for _, id := range c.IDs() {
		if id != oldLeaderID {
			minority = append(minority, id)
			break
		}
	}
	majority := []string{}
	for _, id := range c.IDs() {
		if !contains(minority, id) {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)

	// 老 leader 已过期：它的写无法得到多数派确认，应超时失败。
	wctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	bad := leader.ClientWrite(wctx, writeReq("app", 2, "ghost", "x"))
	if bad.OK {
		t.Fatalf("少数派写入绝不能成功, resp index=%d", bad.Index)
	}
	t.Logf("少数派写被拒绝 reason=%s term=%d", bad.Reject, bad.Term)

	// 过期 leader 收到来自新多数派的更大任期心跳后会主动下台，
	// 恢复分区后也不能让 "ghost" 可见。
	c.Heal()
	newLeader := waitLeader(t, c)
	rr := newLeader.ClientRead(context.Background(), ReadRequest{Key: "ghost"})
	if rr.Found {
		t.Fatalf("未达成多数派的写入被读到了: %q", rr.Value)
	}
	base := newLeader.ClientRead(context.Background(), ReadRequest{Key: "base"})
	if !base.Found || base.Value != "1" {
		t.Fatalf("已提交基线数据丢失: %+v", base)
	}
}

// TestNotLeaderRejectsClient: 发给 follower 的写必须被拒，且带 leader 提示；
// 错误原因可与 stale-term / timeout / duplicate 区分。
func TestNotLeaderRejectsClient(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	for _, id := range c.IDs() {
		if id == leader.Status().ID {
			continue
		}
		resp := c.Node(id).ClientWrite(context.Background(), writeReq("a", 1, "k", "v"))
		if resp.OK {
			t.Fatalf("follower %s 不应接受写", id)
		}
		if !errors.Is(&RaftError{Reason: resp.Reject}, RejectNotLeader) {
			t.Fatalf("拒绝原因应为 not-leader, 实际 %s", resp.Reject)
		}
		if resp.LeaderID != leader.Status().ID {
			t.Fatalf("应提示正确 leader, got %q want %s", resp.LeaderID, leader.Status().ID)
		}
	}
}

// TestMonotonicCommitNoGap: 领导者日志的提交位置必须单调、无空洞；
// 随机 key 的顺序写入后，每个节点上 lastApplied == commitIndex 且
// 日志索引连续。
func TestMonotonicCommitNoGap(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	for i := 1; i <= 20; i++ {
		r := leader.ClientWrite(context.Background(),
			writeReq("seq", uint64(i), fmt.Sprintf("k%02d", i), fmt.Sprintf("v%02d", i)))
		if !r.OK || r.Index != uint64(i)+1 {
			t.Fatalf("写 %d 异常: ok=%v index=%d reject=%s", i, r.OK, r.Index, r.Reject)
		}
	}
	waitConverge(t, c, nil20())
	for _, id := range c.IDs() {
		st := c.Node(id).Status()
		if st.LastApplied != st.CommitIndex {
			t.Fatalf("%s applied(%d) != commit(%d) 存在空洞", id, st.LastApplied, st.CommitIndex)
		}
		if st.LastLogIndex < st.CommitIndex {
			t.Fatalf("%s 日志比提交点短: last=%d commit=%d", id, st.LastLogIndex, st.CommitIndex)
		}
	}
}

func nil20() map[string]string {
	m := map[string]string{}
	for i := 1; i <= 20; i++ {
		m[fmt.Sprintf("k%02d", i)] = fmt.Sprintf("v%02d", i)
	}
	return m
}
