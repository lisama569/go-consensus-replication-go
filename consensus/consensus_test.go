package consensus

import (
	"log"
	"os"
	"sync"
	"testing"
)

// 协议日志在 -v 下直接打印：每条都带 term / lastIndex / commitIndex / lastApplied。
func testLogger() Logger {
	return log.New(os.Stdout, "raft ", log.Lmicroseconds)
}

func waitForLeader(t *testing.T, c *Cluster, maxTicks int) *Node {
	t.Helper()
	for i := 0; i < maxTicks; i++ {
		c.TickAll()
		if l := c.Leader(); l != nil {
			return l
		}
	}
	t.Fatalf("no leader after %d ticks", maxTicks)
	return nil
}

func tickN(c *Cluster, n int) {
	for i := 0; i < n; i++ {
		c.TickAll()
	}
}

func TestElectLeaderAndMajorityWrite(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	t.Logf("leader=%s term=%d commitIndex=%d", l.id, l.Term(), l.CommitIndex())

	res, err := l.Propose(Command{Op: "set", Key: "k1", Value: "v1", ClientID: "cl", SeqNum: 1})
	if err != nil {
		t.Fatalf("majority propose failed: %v (reason=%s)", err, ReasonOf(err))
	}
	if res.Value != "v1" {
		t.Fatalf("unexpected result %+v", res)
	}
	// 多数派确认后必须可线性一致读到。
	v, ok, err := l.Read("k1")
	if err != nil || !ok || v != "v1" {
		t.Fatalf("read k1 = %q,%v,%v", v, ok, err)
	}
	// 所有节点日志与提交位置最终一致。
	tickN(c, 10)
	for _, id := range c.IDs() {
		n := c.Node(id)
		if n.CommitIndex() != l.CommitIndex() {
			t.Fatalf("node %s commitIndex=%d want %d", id, n.CommitIndex(), l.CommitIndex())
		}
	}
}

func TestFollowerRejectsClientRequest(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	for _, id := range c.IDs() {
		if id == l.id {
			continue
		}
		f := c.Node(id)
		if _, err := f.Propose(Command{Op: "set", Key: "x", Value: "y"}); ReasonOf(err) != ReasonNotLeader {
			t.Fatalf("follower propose reason=%v want not_leader", ReasonOf(err))
		}
		if _, _, err := f.Read("x"); ReasonOf(err) != ReasonNotLeader {
			t.Fatalf("follower read reason=%v want not_leader", ReasonOf(err))
		}
	}
}

func TestStaleTermRPCRejected(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	var follower *Node
	for _, id := range c.IDs() {
		if id != l.id {
			follower = c.Node(id)
			break
		}
	}
	term := l.Term()
	c.mu.Lock()
	vr := c.nodes[follower.id].onRequestVote(&VoteArgs{
		Term: term - 1, CandidateID: "ghost", LastLogIndex: 100, LastLogTerm: 100,
	})
	ar := c.nodes[follower.id].onAppendEntries(&AppendArgs{
		Term: term - 1, LeaderID: "ghost",
	})
	c.mu.Unlock()
	if vr.Granted || vr.Reason != ReasonStaleTerm {
		t.Fatalf("stale vote granted=%v reason=%v", vr.Granted, vr.Reason)
	}
	if ar.Success || ar.Reason != ReasonStaleTerm {
		t.Fatalf("stale append success=%v reason=%v", ar.Success, ar.Reason)
	}
}

func TestIdempotentDuplicatePropose(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	cmd := Command{Op: "set", Key: "k", Value: "a", ClientID: "cl", SeqNum: 1}
	r1, err := l.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := l.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatalf("duplicate returned different result: %+v vs %+v", r1, r2)
	}
	tickN(c, 6)
	// 1 条 noop + 1 条真实写入；重复投递不能产生第二次应用。
	hist := l.Data().History()
	if len(hist) != 2 {
		t.Fatalf("applied history len=%d want 2", len(hist))
	}
	// 旧请求的重投必须返回旧结果，不能串成后续新请求的结果。
	cmd2 := Command{Op: "set", Key: "k2", Value: "b", ClientID: "cl", SeqNum: 2}
	if _, err := l.Propose(cmd2); err != nil {
		t.Fatal(err)
	}
	r3, err := l.Propose(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if r3.Key != "k" || r3.Value != "a" || r3.SeqNum != 1 {
		t.Fatalf("old request returned new result: %+v", r3)
	}
}

func TestMinorityCannotCommitOrRead(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	if _, err := l.Propose(Command{Op: "set", Key: "committed", Value: "yes", ClientID: "c", SeqNum: 1}); err != nil {
		t.Fatal(err)
	}
	// 把领导者单独隔离到少数派分区。
	c.Isolate(l.id)
	tickN(c, 30)

	// 旧领导者上的新写无法获得多数派确认，且不能被读到。
	_, err := l.Propose(Command{Op: "set", Key: "ghost", Value: "no", ClientID: "c", SeqNum: 2})
	if ReasonOf(err) != ReasonNoQuorum {
		t.Fatalf("minority propose reason=%v want no_quorum", ReasonOf(err))
	}
	if _, _, err := l.Read("committed"); ReasonOf(err) != ReasonNoQuorum {
		t.Fatalf("minority read reason=%v want no_quorum", ReasonOf(err))
	}
	if _, ok := l.Data().Get("ghost"); ok {
		t.Fatal("uncommitted write must not be visible")
	}

	// 恢复网络后，过期领导者必须被更高任期拉下台，拒绝写请求。
	c.Heal()
	tickN(c, 30)
	_, err = l.Propose(Command{Op: "set", Key: "x", Value: "y", ClientID: "c", SeqNum: 3})
	if ReasonOf(err) != ReasonNotLeader {
		t.Fatalf("stale leader after heal reason=%v want not_leader", ReasonOf(err))
	}
}

func TestLeaderSwitchAfterStopAndDataPreserved(t *testing.T) {
	c := NewCluster(5, testLogger())
	l := waitForLeader(t, c, 120)
	for i := 1; i <= 5; i++ {
		cmd := Command{Op: "set", Key: "k" + itoa(uint64(i)), Value: "v" + itoa(uint64(i)), ClientID: "w", SeqNum: uint64(i)}
		if _, err := l.Propose(cmd); err != nil {
			t.Fatal(err)
		}
	}
	oldTerm := l.Term()
	l.Stop()
	tickN(c, 60)
	nl := c.Leader()
	if nl == nil || nl.id == l.id || nl.Term() <= oldTerm {
		t.Fatalf("new leader not elected: %+v", nl)
	}
	if _, err := nl.Propose(Command{Op: "set", Key: "k6", Value: "v6", ClientID: "w", SeqNum: 6}); err != nil {
		t.Fatalf("write after leader switch failed: %v", err)
	}
	for i := 1; i <= 6; i++ {
		v, ok, err := nl.Read("k" + itoa(uint64(i)))
		if err != nil || !ok || v != "v"+itoa(uint64(i)) {
			t.Fatalf("committed write k%d lost: v=%q ok=%v err=%v", i, v, ok, err)
		}
	}
	// 旧领导者重启后自动追赶，状态与新领导者一致。
	l.Restart()
	tickN(c, 30)
	if l.LogSnapshot()[len(l.LogSnapshot())-1].Index != nl.CommitIndex() {
		t.Fatalf("restarted follower not caught up: last=%d leaderCommit=%d",
			len(l.LogSnapshot()), nl.CommitIndex())
	}
	if v, _ := l.Data().Get("k6"); v != "v6" {
		t.Fatalf("restarted node missing k6, got %q", v)
	}
}

func TestWipedNodeCatchesUp(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	for i := 1; i <= 4; i++ {
		if _, err := l.Propose(Command{Op: "set", Key: "k", Value: itoa(uint64(i)), ClientID: "c", SeqNum: uint64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	var f *Node
	for _, id := range c.IDs() {
		if id != l.id {
			f = c.Node(id)
			break
		}
	}
	f.Stop()
	if _, err := l.Propose(Command{Op: "set", Key: "late", Value: "1", ClientID: "c", SeqNum: 5}); err != nil {
		t.Fatal(err)
	}
	f.WipeAndRestart() // 空日志节点
	tickN(c, 40)
	if f.CommitIndex() != l.CommitIndex() {
		t.Fatalf("wiped node commit=%d want %d", f.CommitIndex(), l.CommitIndex())
	}
	if v, _ := f.Data().Get("late"); v != "1" {
		t.Fatalf("wiped node failed catch-up, late=%q", v)
	}
}

func TestDivergentUncommittedTailTruncated(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	if _, err := l.Propose(Command{Op: "set", Key: "a", Value: "1", ClientID: "c", SeqNum: 1}); err != nil {
		t.Fatal(err)
	}
	c.Isolate(l.id)
	tickN(c, 40) // 多数派选出新领导者
	nl := c.Leader()
	if nl == nil || nl.id == l.id {
		t.Fatalf("majority did not elect new leader, got %v", nl)
	}
	// 旧领导者在隔离期产生一条只有它自己有的分叉未提交日志。
	ghostIdx := l.LogSnapshot()[len(l.LogSnapshot())-1].Index + 1
	if _, err := l.Propose(Command{Op: "set", Key: "ghost", Value: "zzz", ClientID: "c", SeqNum: 2}); err == nil {
		t.Fatal("minority propose unexpectedly succeeded")
	}
	if l.LogSnapshot()[len(l.LogSnapshot())-1].Index != ghostIdx {
		t.Fatal("ghost entry should have been appended even without commit")
	}
	// 新领导者继续提交写入。
	if _, err := nl.Propose(Command{Op: "set", Key: "b", Value: "2", ClientID: "c2", SeqNum: 1}); err != nil {
		t.Fatal(err)
	}
	c.Heal()
	tickN(c, 60)
	final := c.Leader()
	if final == nil {
		t.Fatal("no leader after heal")
	}
	tickN(c, 20)
	// 旧领导者分叉尾部必须被裁剪并追赶一致。
	if len(l.LogSnapshot()) != len(final.LogSnapshot()) {
		t.Fatalf("logs diverged after heal: old=%d leader=%d", len(l.LogSnapshot()), len(final.LogSnapshot()))
	}
	for i, e := range l.LogSnapshot() {
		g := final.LogSnapshot()[i]
		if e.Index != g.Index || e.Term != g.Term {
			t.Fatalf("conflicting tail not truncated at %d: %+v vs %+v", i, e, g)
		}
	}
	if _, applied := l.Data().Get("ghost"); applied {
		t.Fatal("uncommitted divergent entry must never apply")
	}
	if v, _ := l.Data().Get("b"); v != "2" {
		t.Fatalf("committed value lost after conflict resolution: %q", v)
	}
}

func TestConcurrentProposesNoLossNoDoubleApply(t *testing.T) {
	c := NewCluster(3, testLogger())
	l := waitForLeader(t, c, 100)
	const n = 40
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := Command{
				Op: "set", Key: "k" + itoa(uint64(i)), Value: "v" + itoa(uint64(i)),
				ClientID: "client-" + itoa(uint64(i)), SeqNum: 1,
			}
			if _, err := l.Propose(cmd); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent propose failed: %v", err)
	}
	tickN(c, 10)
	for i := 1; i <= n; i++ {
		v, ok, err := l.Read("k" + itoa(uint64(i)))
		if err != nil || !ok || v != "v"+itoa(uint64(i)) {
			t.Fatalf("lost concurrent write k%d: v=%q ok=%v err=%v", i, v, ok, err)
		}
	}
	// 1 条 noop + n 条真实写入，且每个 follower 应用次数相同（无重复应用）。
	want := uint64(n) + 1
	for _, id := range c.IDs() {
		node := c.Node(id)
		if node.LastApplied() != want {
			t.Fatalf("node %s lastApplied=%d want %d", id, node.LastApplied(), want)
		}
		if len(node.Data().History()) != int(want) {
			t.Fatalf("node %s history=%d want %d", id, len(node.Data().History()), want)
		}
	}
}
