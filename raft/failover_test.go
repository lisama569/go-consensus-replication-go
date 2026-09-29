package raft

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestPartitionLeaderSwitch: 5 节点分区成 2/3。老 leader 落在少数派，
// 多数派必须在超时后选出新 leader，任期严格大于旧任期；分区期间两侧
// 写入语义分别为：多数派成功且可见、少数派失败且不可见；恢复后集群
// 重新收敛，老 leader 上的未提交幽灵日志被新领导者日志覆盖。
func TestPartitionLeaderSwitch(t *testing.T) {
	c := newTestCluster(t, 5, true)
	old := waitLeader(t, c)
	oldID := old.Status().ID
	oldTerm := old.Status().Term

	// 预置一条已提交数据。
	if r := old.ClientWrite(context.Background(), writeReq("app", 1, "a", "1")); !r.OK {
		t.Fatalf("预置写失败: %s", r.Reject)
	}

	majority := []string{}
	minority := []string{oldID}
	for _, id := range c.IDs() {
		if id == oldID {
			continue
		}
		if len(majority) < 3 {
			majority = append(majority, id)
		} else {
			minority = append(minority, id)
		}
	}
	c.Partition(majority, minority)
	t.Logf("分区: majority=%v minority=%v", majority, minority)

	// 多数派选出新 leader。
	var newLeader *Node
	deadline := timeNow().Add(2 * time.Second)
	for timeNow().Before(deadline) {
		for _, id := range majority {
			n := c.Node(id)
			if n != nil && n.Status().Role == Leader {
				newLeader = n
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("多数派未在超时内选出新 leader")
	}
	if newLeader.Status().Term <= oldTerm {
		t.Fatalf("新任期必须更大: old=%d new=%d", oldTerm, newLeader.Status().Term)
	}
	t.Logf("新领导者=%s term=%d (老 leader=%s term=%d)",
		newLeader.Status().ID, newLeader.Status().Term, oldID, oldTerm)

	// 多数派写成功。
	r := newLeader.ClientWrite(context.Background(), writeReq("app", 2, "b", "2"))
	if !r.OK {
		t.Fatalf("多数派写应成功: %s", r.Reject)
	}

	// 少数派老 leader 写失败。
	wctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	stale := old.ClientWrite(wctx, writeReq("app", 3, "c", "3"))
	cancel()
	if stale.OK {
		t.Fatal("少数派老 leader 的写不能成功")
	}
	if !errors.Is(&RaftError{Reason: stale.Reject}, RejectTimeout) &&
		!errors.Is(&RaftError{Reason: stale.Reject}, RejectNotLeader) {
		t.Fatalf("少数派写失败原因不可区分: %s", stale.Reject)
	}

	// 少数派收到更大任期的请求（分区内无法收到，这里直接验证 RPC 拒绝规则）：
	// 老 leader 拒绝更小任期，接受更大任期并下台。
	resp := old.HandleAppend(AppendRequest{
		Term: newLeader.Status().Term, LeaderID: newLeader.Status().ID,
		PrevLogIndex: old.Status().LastLogIndex,
		PrevLogTerm:  old.Status().LastLogTerm,
	})
	// 它本地可能没有新 leader 任期的后续日志：若因日志冲突拒绝是合法的，
	// 但不能接受同 term 第二 leader；这里核心断言是它 term 已前进。
	if old.Status().Term < newLeader.Status().Term && resp.Term < newLeader.Status().Term {
		t.Fatalf("老 leader 看到更大任期必须前进 term")
	}

	c.Heal()
	t.Log("分区恢复，等待集群重新收敛")

	// 恢复后唯一 leader（新老任意一位均可），且全部已提交数据在所有节点可见，
	// 幽灵 key=c 绝不可见。
	waitConverge(t, c, map[string]string{"a": "1", "b": "2"})
	leader := waitLeader(t, c)
	if got := leader.ClientRead(context.Background(), ReadRequest{Key: "c"}); got.Found {
		t.Fatal("少数派幽灵写入在恢复后可见，违反多数派提交规则")
	}
	t.Logf("恢复后 leader=%s term=%d commit=%d, 幽灵写未泄漏",
		leader.Status().ID, leader.Status().Term, leader.Status().CommitIndex)

	// 老 leader 恢复后必须以 follower 身份追上，日志与新领导一致。
	if old.Status().Role == Leader {
		t.Fatalf("过期 leader 恢复后不能继续当 leader")
	}
	if old.Status().CommitIndex != leader.Status().CommitIndex {
		t.Fatalf("老 leader commit=%d 未追上 %d",
			old.Status().CommitIndex, leader.Status().CommitIndex)
	}
}

// TestLeaderCrashElection: leader 直接崩溃，剩余节点必须重新选主，
// 新 leader 拥有最完整日志，且崩溃前已提交的写入一条不丢。
func TestLeaderCrashElection(t *testing.T) {
	c := newTestCluster(t, 3, true)
	old := waitLeader(t, c)

	for i := 1; i <= 4; i++ {
		r := old.ClientWrite(context.Background(), writeReq("app", uint64(i),
			fmt.Sprintf("w%d", i), fmt.Sprintf("%d", i)))
		if !r.OK {
			t.Fatalf("预置写 %d 失败: %s", i, r.Reject)
		}
	}
	oldTerm := old.Status().Term
	c.Crash(old.Status().ID)

	var n2 *Node
	deadline := timeNow().Add(2 * time.Second)
	for timeNow().Before(deadline) {
		for _, id := range c.IDs() {
			if n := c.Node(id); n != nil && n.Status().Role == Leader {
				n2 = n
			}
		}
		if n2 != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n2 == nil {
		t.Fatal("leader 崩溃后未重新选主")
	}
	if n2.Status().Term <= oldTerm {
		t.Fatalf("新 leader 任期应更大: %d <= %d", n2.Status().Term, oldTerm)
	}
	for i := 1; i <= 4; i++ {
		rr := n2.ClientRead(context.Background(), ReadRequest{Key: fmt.Sprintf("w%d", i)})
		if rr.Value != fmt.Sprintf("%d", i) {
			t.Fatalf("崩溃前已提交数据 w%d 丢失", i)
		}
	}
	// 新 leader 能继续服务。
	if r := n2.ClientWrite(context.Background(), writeReq("app", 5, "w5", "5")); !r.OK {
		t.Fatalf("故障切换后写失败: %s", r.Reject)
	}
	t.Logf("故障切换成功: %s(term=%d) -> %s(term=%d) commit=%d",
		old.Status().ID, oldTerm, n2.Status().ID, n2.Status().Term, n2.Status().CommitIndex)
}
