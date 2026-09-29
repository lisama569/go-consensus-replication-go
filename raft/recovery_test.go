package raft

import (
	"context"
	"fmt"
	"testing"
)

// TestCrashRestartCatchUp: follower 崩溃期间集群继续提交写入；该节点重启
// 后必须自动追上，状态机与日志提交位置与领导者一致，且期间已提交数据
// 一条都不能丢。
func TestCrashRestartCatchUp(t *testing.T) {
	c := newTestCluster(t, 5, true)
	leader := waitLeader(t, c)
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		r := leader.ClientWrite(ctx, writeReq("app", uint64(i),
			fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		if !r.OK {
			t.Fatalf("崩溃前写 %d 失败: %s", i, r.Reject)
		}
	}

	victim := ""
	for _, id := range c.IDs() {
		if id != leader.Status().ID {
			victim = id
			break
		}
	}
	c.Crash(victim)
	t.Logf("节点 %s 崩溃, 集群继续服务", victim)

	// 崩溃期间再写 7 条（仍有 4 个存活节点，多数派=3 满足）。
	for i := 4; i <= 10; i++ {
		r := leader.ClientWrite(ctx, writeReq("app", uint64(i),
			fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		if !r.OK {
			t.Fatalf("崩溃期间写 %d 失败: %s", i, r.Reject)
		}
	}

	restarted := c.Restart(victim)
	if restarted == nil {
		t.Fatal("重启返回空节点")
	}
	t.Logf("节点 %s 重启, 等待追赶", victim)

	want := map[string]string{}
	for i := 1; i <= 10; i++ {
		want[fmt.Sprintf("k%d", i)] = fmt.Sprintf("v%d", i)
	}
	waitConverge(t, c, want)

	st := restarted.Status()
	lst := leader.Status()
	if st.CommitIndex != lst.CommitIndex {
		t.Fatalf("重启节点 commit=%d 与 leader=%d 不一致", st.CommitIndex, lst.CommitIndex)
	}
	t.Logf("追赶完成: %s commit=%d lastApplied=%d 与 leader=%d 一致",
		victim, st.CommitIndex, st.LastApplied, lst.CommitIndex)
}

// TestRestartLostDataCatchUp: 节点连持久日志一起丢失后以空日志归队，
// 必须通过领导者的日志复制（冲突回退 + 重传）完整追上，不能静默接受
// 乱序追加，也不能少数据。
func TestRestartLostDataCatchUp(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		r := leader.ClientWrite(ctx, writeReq("app", uint64(i),
			fmt.Sprintf("d%d", i), fmt.Sprintf("x%d", i)))
		if !r.OK {
			t.Fatalf("预置写 %d 失败: %s", i, r.Reject)
		}
	}

	victim := ""
	for _, id := range c.IDs() {
		if id != leader.Status().ID {
			victim = id
			break
		}
	}
	c.Crash(victim)
	empty := c.RestartLostData(victim)
	if empty == nil {
		t.Fatal("丢盘重启返回空节点")
	}
	st0 := empty.Status()
	if st0.LastLogIndex != 0 {
		t.Fatalf("丢盘节点日志应为空, lastIndex=%d", st0.LastLogIndex)
	}
	t.Logf("节点 %s 丢盘以空日志归队, leader=%s commit=%d",
		victim, leader.Status().ID, leader.Status().CommitIndex)

	want := map[string]string{}
	for i := 1; i <= 5; i++ {
		want[fmt.Sprintf("d%d", i)] = fmt.Sprintf("x%d", i)
	}
	waitConverge(t, c, want)
	t.Logf("丢盘节点已通过复制追上: commit=%d lastApplied=%d",
		empty.Status().CommitIndex, empty.Status().LastApplied)
}
