// 本地可运行演示：3 节点内存集群，展示选主、多数派提交、
// 少数派不可提交/不可读、分区恢复后过期领导者下台与日志追赶。
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lisama569/go-consensus-replication-go/consensus"
)

func main() {
	logger := log.New(os.Stdout, "raft ", log.Lmicroseconds)
	c := consensus.NewCluster(3, logger)

	// 用真实时间后台驱动 tick（也可以用 c.TickAll() 手动驱动，测试即如此）。
	c.Start()
	defer c.StopLoop()
	time.Sleep(300 * time.Millisecond)

	l := c.Leader()
	if l == nil {
		log.Fatal("no leader elected")
	}
	fmt.Printf(">>> 领导者当选: id=%s term=%d\n\n", l.ID(), l.Term())

	put := func(key, val, client string, seq uint64) {
		res, err := l.Propose(consensus.Command{Op: "set", Key: key, Value: val, ClientID: client, SeqNum: seq})
		if err != nil {
			log.Fatalf("propose: %v (reason=%s)", err, consensus.ReasonOf(err))
		}
		fmt.Printf(">>> 写入并提交: %s=%s (client=%s seq=%d) commitIndex=%d\n",
			key, res.Value, client, seq, l.CommitIndex())
	}
	put("color", "blue", "writer", 1)

	v, ok, _ := l.Read("color")
	fmt.Printf(">>> 线性一致读: color=%s found=%v\n\n", v, ok)

	// 重复投递同一请求：幂等，返回首次结果，不重复应用。
	_, _ = l.Propose(consensus.Command{Op: "set", Key: "color", Value: "blue", ClientID: "writer", SeqNum: 1})
	fmt.Printf(">>> 重复投递 seq=1 后，集群应用位置 lastApplied=%d（无重复应用）\n\n", l.LastApplied())

	// 隔离领导者 -> 只剩 1/3 少数派。
	fmt.Println(">>> 网络分区：隔离当前领导者")
	c.Isolate(l.ID())
	time.Sleep(400 * time.Millisecond)

	_, err := l.Propose(consensus.Command{Op: "set", Key: "ghost", Value: "x", ClientID: "writer", SeqNum: 2})
	fmt.Printf(">>> 少数派写入被拒: reason=%s\n", consensus.ReasonOf(err))
	_, _, err = l.Read("color")
	fmt.Printf(">>> 少数派线性一致读被拒: reason=%s\n\n", consensus.ReasonOf(err))

	// 多数派选出新领导者并继续提交。
	nl := c.Leader()
	fmt.Printf(">>> 多数派新领导者: id=%s term=%d\n", nl.ID(), nl.Term())
	_, err = nl.Propose(consensus.Command{Op: "set", Key: "shape", Value: "circle", ClientID: "writer2", SeqNum: 1})
	if err != nil {
		log.Fatalf("new leader propose: %v", err)
	}
	fmt.Println()

	// 恢复网络：过期领导者下台并追赶日志。
	fmt.Println(">>> 恢复网络")
	c.Heal()
	time.Sleep(400 * time.Millisecond)

	for _, id := range c.IDs() {
		n := c.Node(id)
		color, _ := n.Data().Get("color")
		shape, _ := n.Data().Get("shape")
		fmt.Printf(">>> 节点 %s state=%s term=%d commitIndex=%d lastApplied=%d color=%q shape=%q\n",
			id, n.State(), n.Term(), n.CommitIndex(), n.LastApplied(), color, shape)
	}
	fmt.Println("\n所有节点状态一致，演示结束。")
}
