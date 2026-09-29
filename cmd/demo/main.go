// Command demo 在单进程内启动 5 个内存 Raft 节点，演示：
//  1. 自动选主与唯一领导者；
//  2. 多数派提交的写入与线性一致读；
//  3. 网络分区后少数派写失败、多数派改选、分区恢复后重新收敛；
//  4. 重复请求幂等（同 ClientID+Seq 只应用一次）。
//
// 运行：go run ./cmd/demo
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lisama569/go-consensus-replication-go/raft"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	fmt.Printf("    "+format+"\n", args...)
}

func must(err error, msg string) {
	if err != nil {
		fmt.Printf("!! %s: %v\n", msg, err)
		os.Exit(1)
	}
}

func main() {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	c := raft.NewCluster(raft.ClusterConfig{
		IDs:                ids,
		Logger:             stdLogger{},
		ElectionTimeoutMin: 200 * time.Millisecond,
		ElectionTimeoutMax: 400 * time.Millisecond,
		HeartbeatInterval:  40 * time.Millisecond,
	})
	c.Start()
	defer c.Stop()

	ctx := context.Background()

	fmt.Println("== 1. 等待选主 ==")
	leader, err := c.WaitLeader(2 * time.Second)
	must(err, "选主失败")
	fmt.Printf("    当前 leader=%s term=%d\n", leader.Status().ID, leader.Status().Term)

	fmt.Println("== 2. 正常写入并读取（多数派确认后可见） ==")
	for i := 1; i <= 3; i++ {
		r := leader.ClientWrite(ctx, raft.ClientRequest{
			ClientID: "demo", Seq: uint64(i),
			Op: raft.OpPut, Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("val%d", i),
		})
		if !r.OK {
			must(fmt.Errorf("%s", r.Reject), "写入被拒")
		}
		fmt.Printf("    写入 seq=%d -> index=%d term=%d\n", i, r.Index, r.Term)
	}
	r := leader.ClientRead(ctx, raft.ReadRequest{Key: "key2"})
	fmt.Printf("    读 key2 => %q (found=%v, commit=%d)\n", r.Value, r.Found, leader.Status().CommitIndex)

	fmt.Println("== 3. 重复请求幂等 ==")
	dup := leader.ClientWrite(ctx, raft.ClientRequest{
		ClientID: "demo", Seq: 2, Op: raft.OpPut, Key: "key2", Value: "TAMPERED",
	})
	fmt.Printf("    重复 seq=2: index=%d result=%q reason=%s（dup 标记表示复用旧结果）\n",
		dup.Index, dup.Result, dup.Reject)
	again := leader.ClientRead(ctx, raft.ReadRequest{Key: "key2"})
	fmt.Printf("    再读 key2 => %q（值没有被重复投递污染）\n", again.Value)

	fmt.Println("== 4. 网络分区：现任 leader 落入 2 节点少数派 ==")
	leaderID := leader.Status().ID
	var other string
	for _, id := range ids {
		if id != leaderID {
			other = id
			break
		}
	}
	minority := []string{leaderID, other}
	var majority []string
	for _, id := range ids {
		if id != leaderID && id != other {
			majority = append(majority, id)
		}
	}
	c.Partition(minority, majority)

	wctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	stale := leader.ClientWrite(wctx, raft.ClientRequest{
		ClientID: "demo", Seq: 99, Op: raft.OpPut, Key: "ghost", Value: "x",
	})
	cancel()
	fmt.Printf("    少数派旧 leader 写入: ok=%v reason=%s（无法凑齐多数派）\n",
		stale.OK, stale.Reject)

	var n2 *raft.Node
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range majority {
			if n := c.Node(id); n != nil && n.Status().Role == raft.Leader {
				n2 = n
			}
		}
		if n2 != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	must(func() error {
		if n2 == nil {
			return fmt.Errorf("多数派未选出新 leader")
		}
		return nil
	}(), "多数派改选")
	fmt.Printf("    多数派新 leader=%s term=%d\n", n2.Status().ID, n2.Status().Term)
	ok := n2.ClientWrite(ctx, raft.ClientRequest{
		ClientID: "demo", Seq: 4, Op: raft.OpPut, Key: "key4", Value: "val4",
	})
	fmt.Printf("    多数派写入 seq=4: ok=%v index=%d reason=%s\n", ok.OK, ok.Index, ok.Reject)

	fmt.Println("== 5. 分区恢复，集群重新收敛 ==")
	c.Heal()
	time.Sleep(600 * time.Millisecond)
	final, err := c.Leader(ctx)
	must(err, "恢复后无唯一 leader")
	ghost := final.ClientRead(ctx, raft.ReadRequest{Key: "ghost"})
	k4 := final.ClientRead(ctx, raft.ReadRequest{Key: "key4"})
	fmt.Printf("    恢复后 leader=%s term=%d: ghost found=%v（少数派未提交写不可见）, key4=%q\n",
		final.Status().ID, final.Status().Term, ghost.Found, k4.Value)

	fmt.Println("== 6. 节点崩溃重启后追赶 ==")
	victim := ""
	for _, id := range ids {
		if id != final.Status().ID {
			victim = id
			break
		}
	}
	c.Crash(victim)
	for i := 5; i <= 7; i++ {
		w := final.ClientWrite(ctx, raft.ClientRequest{
			ClientID: "demo", Seq: uint64(i),
			Op: raft.OpPut, Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("val%d", i),
		})
		if !w.OK {
			must(fmt.Errorf("%s", w.Reject), "崩溃期间写入")
		}
	}
	c.Restart(victim)
	time.Sleep(400 * time.Millisecond)
	vn := c.Node(victim)
	fmt.Printf("    %s 重启后 commit=%d applied=%d（与 leader commit=%d 一致）\n",
		victim, vn.Status().CommitIndex, vn.Status().LastApplied, final.Status().CommitIndex)

	fmt.Println("demo 完成")
}
