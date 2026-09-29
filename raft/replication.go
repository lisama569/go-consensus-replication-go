package raft

import (
	"context"
	"sort"
	"strconv"
	"time"
)

// sendHeartbeat 由 leader 的节拍循环调用：唤醒所有复制器立即发一轮
// AppendEntries（空 Entries 即心跳）。
func (n *Node) sendHeartbeat(ctx context.Context) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return
	}
	n.signalAll()
	n.mu.Unlock()
}

// replicator 是每个跟随者一个的后台复制协程。它被唤醒时向该 follower
// 推一次日志；leader 心跳节拍通过 notifyCh 驱动，因此空闲时也会持续心跳。
// replicator 是每个跟随者一个的后台复制协程。它在三种事件下动作：
// 心跳节拍到达（空 Entries 也发）、被显式唤醒（有新日志、nextIndex
// 回退需要立刻重试），或 wake 被关闭（本节点失去领导权 / 停机）。
// 任期结束时 wake 一定会被关闭，旧任期的复制器必然退出，因此不会出现
// 过期 leader 的 goroutine 在新任期继续冒充发送的情况。
func (n *Node) replicator(peer string, wake, exitCh chan struct{}) {
	defer close(exitCh)
	ticker := time.NewTicker(n.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-wake:
			// 通道可能是被唤醒也可能是被关闭，下面持锁检查角色区分。
		case <-ticker.C:
		}
		n.mu.Lock()
		stopped := n.stopped
		isLeader := n.role == Leader
		term := n.p.currentTerm
		if stopped || !isLeader {
			n.mu.Unlock()
			return // 失去领导权或停机：本任期复制器终结
		}
		req, needSnapshot := n.buildAppend(peer)
		n.mu.Unlock()

		if needSnapshot {
			n.pushSnapshot(peer, req)
			// pushSnapshot 期间可能已经卸任，快速自检。
			n.mu.Lock()
			stillLeader := n.role == Leader && n.p.currentTerm == term && !n.stopped
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			continue
		}
		resp, err := n.trans.AppendEntries(context.Background(), n.id, peer, req)
		if err != nil {
			continue // 不可达：等下一次心跳重试
		}
		// 卸任后响应处理会自行忽略；返回后下一轮 select 也会立刻退出。
		n.onAppendResponse(peer, req, resp)
	}
}

// stopReplicators 关闭当前任期所有复制器的唤醒通道。调用方持锁，
// 因此只 close 不等待（复制器退出路径可能还要抢锁）；需要等待时用
// stopReplicatorsAndWait（Stop 路径，不持锁）。
func (n *Node) stopReplicators() {
	for peer, ch := range n.notifyCh {
		close(ch)
		delete(n.notifyCh, peer)
	}
}

// stopReplicatorsAndWait 在节点停机时调用：关闭唤醒通道并等待复制器退出，
// 保证测试结束后没有残留 goroutine 写日志/发 RPC。
func (n *Node) stopReplicatorsAndWait() {
	n.mu.Lock()
	wakers := make([]chan struct{}, 0, len(n.notifyCh))
	exits := make([]chan struct{}, 0, len(n.repDone))
	for peer, ch := range n.notifyCh {
		wakers = append(wakers, ch)
		exits = append(exits, n.repDone[peer])
	}
	n.notifyCh = map[string]chan struct{}{}
	n.repDone = map[string]chan struct{}{}
	n.mu.Unlock()
	for _, ch := range wakers {
		close(ch)
	}
	for _, ex := range exits {
		if ex != nil {
			<-ex
		}
	}
}

// buildAppend 调用方持锁。按 nextIndex 组装一条 AppendEntries 请求；
// 若 follower 的 nextIndex 已经被本地快照压缩，置 needSnapshot。
func (n *Node) buildAppend(peer string) (req AppendRequest, needSnapshot bool) {
	ni := n.nextIndex[peer]
	if ni <= n.p.log.baseIndex {
		return AppendRequest{
			Term:         n.p.currentTerm,
			LeaderID:     n.id,
			PrevLogIndex: n.p.log.baseIndex,
			PrevLogTerm:  n.p.log.baseTerm,
			LeaderCommit: n.commitIndex,
		}, true
	}
	prev := ni - 1
	prevTerm, _ := n.p.log.termAt(prev)
	return AppendRequest{
		Term:         n.p.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.p.log.sliceFrom(ni),
		LeaderCommit: n.commitIndex,
	}, false
}

// pushSnapshot 发送快照并处理响应（本实现里 leader 自身不截断日志，
// 该分支主要服务于"曾经安装过快照"后又被重启/落后的节点）。
func (n *Node) pushSnapshot(peer string, _ AppendRequest) {
	n.mu.Lock()
	sreq := SnapshotRequest{
		Term:     n.p.currentTerm,
		LeaderID: n.id,
		LastInc:  n.p.log.baseIndex,
		LastTerm: n.p.log.baseTerm,
		Data:     n.sm.clone(),
	}
	n.mu.Unlock()
	resp, err := n.trans.InstallSnapshot(context.Background(), n.id, peer, sreq)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.p.currentTerm {
		n.stepDown(resp.Term)
		return
	}
	if n.role != Leader || !resp.OK {
		return
	}
	n.matchIndex[peer] = sreq.LastInc
	if n.nextIndex[peer] < sreq.LastInc+1 {
		n.nextIndex[peer] = sreq.LastInc + 1
	}
	n.advanceCommit()
}

// onAppendResponse 处理 follower 的 AppendEntries 应答。
func (n *Node) onAppendResponse(peer string, req AppendRequest, resp AppendResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.p.currentTerm {
		n.stepDown(resp.Term)
		return
	}
	if n.role != Leader || resp.Term < n.p.currentTerm {
		return // 过期 leader / 过期响应，忽略
	}

	if resp.Success {
		matched := resp.MatchIndex
		if matched < req.PrevLogIndex {
			matched = req.PrevLogIndex
		}
		if matched > n.matchIndex[peer] {
			n.matchIndex[peer] = matched
		}
		if n.nextIndex[peer] < matched+1 {
			n.nextIndex[peer] = matched + 1
		}
		n.advanceCommit()
		return
	}

	// ---- 被拒绝：只可能是日志冲突（任期拒绝会在上面的 term 判断处理） ----
	switch resp.Reject {
	case RejectCommittedRollback:
		// 理论上不应该发生：leader 永远不会要求删除已提交日志。
		n.logg.Printf("node=%s leader 收到 committed-rollback peer=%s，忽略", n.id, peer)
	case RejectLogConflict:
		// 快速回退优化（论文 §5.3）。
		switch {
		case resp.ConflictTerm != 0:
			newNext := uint64(0)
			for i := n.p.log.lastIndex(); i > n.p.log.baseIndex; i-- {
				t, _ := n.p.log.termAt(i)
				if t == resp.ConflictTerm {
					newNext = i + 1
					break
				}
			}
			if newNext == 0 {
				newNext = resp.ConflictIndex
			}
			n.nextIndex[peer] = newNext
		case resp.ConflictIndex != 0:
			n.nextIndex[peer] = resp.ConflictIndex
		default:
			if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}
		n.logg.Printf("node=%s leader term=%d peer=%s 日志冲突，nextIndex 回退到 %d",
			n.id, n.p.currentTerm, peer, n.nextIndex[peer])
		n.signalPeer(peer)
	}
}

// advanceCommit 调用方持锁。按论文规则：存在 N 满足多数派 matchIndex>=N、
// log[N].term == currentTerm，且 N>commitIndex，则提交到 N。
// 要求"提交条目来自当前任期"是论文 §5.4 的关键约束，防止通过复制旧任期
// 条目间接提交一个并未真正得到多数派确认的旧任期。
func (n *Node) advanceCommit() {
	total := len(n.peers) + 1
	matched := make([]uint64, 0, total)
	matched = append(matched, n.p.log.lastIndex()) // leader 自己
	for _, p := range n.peers {
		matched = append(matched, n.matchIndex[p])
	}
	sort.Slice(matched, func(i, j int) bool { return matched[i] > matched[j] })
	candidate := matched[quorum(total)-1] // 多数派位置上的最高索引

	for candidate > n.commitIndex {
		t, ok := n.p.log.termAt(candidate)
		if !ok {
			return
		}
		if t != n.p.currentTerm {
			candidate-- // 不能跨旧任期计数提交流水线
			continue
		}
		n.commitIndex = candidate
		n.logg.Printf("node=%s leader term=%d commit=%d lastIndex=%d 多数派确认，推进提交位置",
			n.id, n.p.currentTerm, n.commitIndex, n.p.log.lastIndex())
		n.applyCommitted()
		return
	}
}

// applyCommitted 调用方持锁。把 (lastApplied, commitIndex] 区间按顺序
// 应用到状态机，保证单调、无空洞。幂等：同 (ClientID,Seq) 只应用一次，
// 重复投递复用旧结果。
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		idx := n.lastApplied + 1
		e, ok := n.p.log.entryAt(idx)
		if !ok {
			// 已被快照包含：直接跳到 commitIndex（快照状态机已加载）。
			if idx <= n.p.log.baseIndex {
				n.lastApplied = n.commitIndex
				if n.lastApplied < n.p.log.baseIndex {
					n.lastApplied = n.p.log.baseIndex
				}
				return
			}
			return // 本地居然没有这段日志：防御性等待，不能跳空洞
		}

		var result string
		if !e.isNoOp() {
			key := dedupKey(e.ClientID, e.Seq)
			if rec, dup := n.p.applied[key]; dup {
				// 同一 (ClientID,Seq) 的日志条目重复（去重表已有记录），
				// 绝不二次应用状态机，直接复用首次结果。
				result = rec.result
				n.logg.Printf("node=%s term=%d index=%d commit=%d 重复条目，跳过应用 client=%s seq=%d",
					n.id, e.Term, e.Index, n.commitIndex, e.ClientID, e.Seq)
			} else {
				result = n.sm.Apply(e)
				n.p.applied[key] = appliedRecord{
					client: e.ClientID, seq: e.Seq, index: idx, result: result,
				}
			}
		}
		e.applied = true
		e.Result = result
		// 回写 Result 到日志，便于重复请求复用。
		n.p.log.entries[idx-n.p.log.baseIndex] = e

		n.lastApplied = idx
		n.logg.Printf("node=%s term=%d index=%d commit=%d applied op=%s key=%s client=%s seq=%d result=%q",
			n.id, e.Term, e.Index, n.commitIndex, e.Op, e.Key, e.ClientID, e.Seq, result)

		// 唤醒在该索引上等待的客户端写。
		if pw, ok := n.pending[idx]; ok {
			resp := ClientResponse{
				OK:       true,
				Result:   result,
				LeaderID: n.id,
				Term:     n.p.currentTerm,
				Index:    idx,
			}
			for _, w := range pw.waiters {
				select {
				case w <- resp:
				default:
				}
			}
			delete(n.pending, idx)
		}
	}
}

// ---- HandleAppend（follower / candidate RPC 入口） ----

// HandleAppend 实现论文 §5.3 的接收者规则，并显式区分各类拒绝原因：
//   - 任期更旧：RejectStaleTerm（过期领导者）；
//   - 日志缺失/任期不一致：RejectLogConflict + 快速回退提示；
//   - 试图回退已提交日志：RejectCommittedRollback，绝不静默截断；
//   - 提交位置倒退或产生空洞：RejectCommitGap。
func (n *Node) HandleAppend(req AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.p.currentTerm {
		n.logg.Printf("node=%s 拒绝 Append: term=%d 小于本地 term=%d (%s) leader=%s",
			n.id, req.Term, n.p.currentTerm, RejectStaleTerm, req.LeaderID)
		return AppendResponse{Term: n.p.currentTerm, Reject: RejectStaleTerm}
	}
	if req.Term > n.p.currentTerm {
		n.stepDown(req.Term)
	}
	// 同任期内承认请求方为唯一领导者（论文：同一 term 最多一个 leader，
	// 因为同一 term 最多一张有效选票组合）。
	n.role = Follower
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	// ---- 一致性检查 ----
	status, hintIndex, hintTerm := n.p.log.checkPrev(req.PrevLogIndex, req.PrevLogTerm)
	if status == conflictCompacted {
		// 领导者落后于本地快照：让它发快照（本实现 leader 不会主动截断，
		// 走到这里说明拓扑极端；明确拒绝而不是乱覆盖）。
		return AppendResponse{
			Term: n.p.currentTerm, Reject: RejectLogConflict,
			ConflictIndex: n.p.log.baseIndex + 1, ConflictTerm: n.p.log.baseTerm,
		}
	}
	if status != conflictOK {
		n.logg.Printf("node=%s 拒绝 Append: 日志不一致 prev=(%d,%d) lastIndex=%d (%s) leader=%s term=%d",
			n.id, req.PrevLogIndex, req.PrevLogTerm, n.p.log.lastIndex(),
			RejectLogConflict, req.LeaderID, req.Term)
		return AppendResponse{
			Term: n.p.currentTerm, Reject: RejectLogConflict,
			ConflictIndex: hintIndex, ConflictTerm: hintTerm,
		}
	}

	// ---- 回退保护：不能静默覆盖已提交日志 ----
	if first := n.p.log.firstConflict(req.PrevLogIndex, req.Entries); first != 0 {
		if first <= n.commitIndex {
			n.logg.Printf("node=%s 拒绝 Append: 试图回退已提交日志 index=%d commit=%d (%s)",
				n.id, first, n.commitIndex, RejectCommittedRollback)
			return AppendResponse{Term: n.p.currentTerm, Reject: RejectCommittedRollback}
		}
	}

	n.p.log.storeEntries(req.PrevLogIndex, req.Entries)
	matched := req.PrevLogIndex + uint64(len(req.Entries))

	// ---- 推进 commitIndex：只能单调前进，且不能越过本地日志 ----
	if req.LeaderCommit < n.commitIndex {
		n.logg.Printf("node=%s 拒绝提交倒退: leaderCommit=%d localCommit=%d (%s)",
			n.id, req.LeaderCommit, n.commitIndex, RejectCommitGap)
	} else {
		target := req.LeaderCommit
		if target > n.p.log.lastIndex() {
			target = n.p.log.lastIndex()
		}
		if target < n.commitIndex {
			// 与上面分支等价的防御：绝不允许空洞/回退。
			return AppendResponse{Term: n.p.currentTerm, Reject: RejectCommitGap}
		}
		n.commitIndex = target
		n.applyCommitted()
	}

	n.logg.Printf("node=%s follower term=%d 追加成功 prev=%d +%d matched=%d commit=%d lastIndex=%d leader=%s",
		n.id, req.Term, req.PrevLogIndex, len(req.Entries), matched, n.commitIndex,
		n.p.log.lastIndex(), req.LeaderID)
	return AppendResponse{Term: n.p.currentTerm, Success: true, MatchIndex: matched}
}

func dedupKey(clientID string, seq uint64) string {
	return clientID + "#" + strconv.FormatUint(seq, 10)
}
