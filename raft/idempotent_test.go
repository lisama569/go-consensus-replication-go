package raft

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// TestDuplicateWriteIdempotent: 同一个 (ClientID, Seq) 即使内容不同或被
// 投递多次，也只能产生一次应用：日志中只有一条该 seq 的记录，状态机
// 值是第一次的，重复响应复用旧结果而不是新参数。
func TestDuplicateWriteIdempotent(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	ctx := context.Background()

	first := writeReq("client-A", 1, "k", "first")
	r1 := leader.ClientWrite(ctx, first)
	if !r1.OK || r1.Result != "ok:first" {
		t.Fatalf("首次写异常: %+v", r1)
	}
	idx := r1.Index

	// 同样的 client+seq，哪怕 value 改成 second，也必须被识别为重复。
	dup := writeReq("client-A", 1, "k", "second")
	r2 := leader.ClientWrite(ctx, dup)
	if !r2.OK {
		t.Fatalf("重复请求应返回成功（复用旧结果）: %s", r2.Reject)
	}
	if r2.Reject != RejectDuplicate {
		t.Fatalf("重复请求应标记 RejectDuplicate, 实际 %s", r2.Reject)
	}
	if r2.Index != idx {
		t.Fatalf("重复请求应复用原索引 %d, 实际 %d", idx, r2.Index)
	}
	if r2.Result != "ok:first" {
		t.Fatalf("重复请求不能把新参数当结果, got %q", r2.Result)
	}

	// 再来 N 次，结果与索引仍然完全一致。
	for i := 0; i < 5; i++ {
		rr := leader.ClientWrite(ctx, dup)
		if rr.Index != idx || rr.Result != "ok:first" || rr.Reject != RejectDuplicate {
			t.Fatalf("第 %d 次重复未幂等: %+v", i, rr)
		}
	}

	// 状态机里的值没有被 "second" 污染。
	read := leader.ClientRead(ctx, ReadRequest{Key: "k"})
	if read.Value != "first" {
		t.Fatalf("重复投递污染了状态机: %q", read.Value)
	}

	// 所有节点日志中 client-A seq=1 的条目恰好出现一次。
	waitConverge(t, c, map[string]string{"k": "first"})
	for _, id := range c.IDs() {
		st := c.Node(id).Status()
		seqs := st.AppliedSeqs["client-A"]
		if len(seqs) != 1 || seqs[0] != 1 {
			t.Fatalf("%s 去重记录异常: %+v", id, st.AppliedSeqs)
		}
	}
	t.Logf("重复写复用 index=%d result=%q, 所有节点去重水位=1", idx, r2.Result)
}

// TestOutOfOrderDuplicateSeq: 关键回归——seq=1..5 全部提交后，重投一个
// 早于最新 seq 的历史请求（seq=2），也必须被识别为重复。只记录"最新
// seq 水位"的错误实现会把它当成新写，此用例专门卡住这个问题。
func TestOutOfOrderDuplicateSeq(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		r := leader.ClientWrite(ctx, writeReq("cli", uint64(i),
			fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		if !r.OK {
			t.Fatalf("预置写 %d 失败: %s", i, r.Reject)
		}
	}
	// 重投 seq=2，即便 key/value 改成别的。
	r := leader.ClientWrite(ctx, writeReq("cli", 2, "k2", "FORGED"))
	if r.Reject != RejectDuplicate || r.Result != "ok:v2" {
		t.Fatalf("历史 seq=2 重投应幂等返回旧结果, got %+v", r)
	}
	// 重投 seq=5。
	r = leader.ClientWrite(ctx, writeReq("cli", 5, "k5", "FORGED"))
	if r.Reject != RejectDuplicate || r.Result != "ok:v5" {
		t.Fatalf("历史 seq=5 重投应幂等返回旧结果, got %+v", r)
	}
	if got := leader.ClientRead(ctx, ReadRequest{Key: "k2"}); got.Value != "v2" {
		t.Fatalf("历史 seq 重投污染状态机: %q", got.Value)
	}
	t.Logf("乱序历史 seq 重投均被幂等拦截")
}

// TestConcurrentDuplicates: 同一个请求并发投递，只允许追加一条日志、
// 应用一次；不同 seq 的并发写各自独立成功且顺序连续。
func TestConcurrentDuplicates(t *testing.T) {
	c := newTestCluster(t, 3, true)
	leader := waitLeader(t, c)

	const dupWorkers = 20
	var wg sync.WaitGroup
	results := make([]ClientResponse, dupWorkers)
	for i := 0; i < dupWorkers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = leader.ClientWrite(context.Background(),
				writeReq("dup-client", 1, "hot", "x"))
		}(i)
	}
	wg.Wait()

	var idx uint64
	for i, r := range results {
		if !r.OK {
			t.Fatalf("并发重复写 %d 失败: %s", i, r.Reject)
		}
		if idx == 0 {
			idx = r.Index
		} else if r.Index != idx {
			t.Fatalf("并发重复写映射到多个索引: %d vs %d", idx, r.Index)
		}
	}

	// 不同 seq 的并发写也必须全部成功（各占一个连续日志位置）。
	const distinct = 15
	dresps := make([]ClientResponse, distinct)
	for i := 0; i < distinct; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dresps[i] = leader.ClientWrite(context.Background(),
				writeReq("multi", uint64(i+1), "dk", "v"))
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for i, r := range dresps {
		if !r.OK {
			t.Fatalf("并发写 %d 失败: %s", i, r.Reject)
		}
		if seen[r.Index] {
			t.Fatalf("不同请求占用同一索引 %d", r.Index)
		}
		seen[r.Index] = true
	}
	if len(seen) != distinct {
		t.Fatalf("应有 %d 个不同提交索引, 实际 %d", distinct, len(seen))
	}
	st := leader.Status()
	t.Logf("并发完成: commit=%d lastApplied=%d 去重表=%v",
		st.CommitIndex, st.LastApplied, st.AppliedSeqs)
}
