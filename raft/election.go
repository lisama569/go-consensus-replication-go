package raft

import "context"

// tickElection 在选举超时到期时由后台循环调用：转为候选者并发起一轮投票。
func (n *Node) tickElection() {
	n.mu.Lock()
	if n.role == Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	n.startElection()
	req := n.makeVoteRequest()
	n.mu.Unlock()

	n.broadcastVote(req)
}

// startElection 调用方持锁。推进任期、投自己一票、重置计时器。
func (n *Node) startElection() {
	n.p.currentTerm++
	n.role = Candidate
	n.leaderID = ""
	n.p.votedFor = n.id
	n.votes = map[string]bool{n.id: true}
	n.resetElectionTimer()
	n.logg.Printf("node=%s role=candidate term=%d lastIndex=%d commit=%d 选举超时，开始竞选",
		n.id, n.p.currentTerm, n.p.log.lastIndex(), n.commitIndex)
}

func (n *Node) makeVoteRequest() VoteRequest {
	lastIdx := n.p.log.lastIndex()
	lastTerm, _ := n.p.log.termAt(lastIdx)
	return VoteRequest{
		Term:         n.p.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
}

// broadcastVote 向所有其它节点拉票，达成多数派立即转主。
func (n *Node) broadcastVote(req VoteRequest) {
	for _, peer := range n.peers {
		peer := peer
		go func() {
			resp, err := n.trans.RequestVote(context.Background(), n.id, peer, req)
			if err != nil {
				return // 分区 / 崩溃：忽略这一票
			}
			n.onVoteResponse(peer, req, resp)
		}()
	}
}

func (n *Node) onVoteResponse(from string, req VoteRequest, resp VoteResponse) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 通用任期规则：响应任期更大则退回 follower。
	if resp.Term > n.p.currentTerm {
		n.stepDown(resp.Term)
		return
	}
	// 过期响应：任期对不上或者我已经不是这一任期的候选者。
	if resp.Term < n.p.currentTerm || n.role != Candidate {
		return
	}
	if !resp.VoteGranted {
		return
	}
	n.votes[from] = true
	if len(n.votes) >= quorum(len(n.peers)+1) {
		n.becomeLeader()
	}
}

// becomeLeader 调用方持锁。初始化所有 nextIndex/matchIndex，并立刻追加
// 一条 no-op 条目（论文 §8：no-op 用于尽快提交前任任期的条目，避免
// 新领导者直接返回旧数据）。
func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.id
	last := n.p.log.lastIndex()
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.notifyCh = map[string]chan struct{}{}
	n.repDone = map[string]chan struct{}{}
	for _, p := range n.peers {
		n.nextIndex[p] = last + 1
		n.matchIndex[p] = 0
		// wake 每次任期新建；stepDown 会 close 它来终止旧任期的复制器，
		// 防止旧 leader 的 goroutine 在新任期继续冒充发心跳。
		ch := make(chan struct{}, 1)
		done := make(chan struct{})
		n.notifyCh[p] = ch
		n.repDone[p] = done
		go n.replicator(p, ch, done)
	}
	// no-op 占位条目，标记本任期的提交位置。
	e := n.p.log.appendNew(n.p.currentTerm, Entry{Op: OpNoOp})
	n.registerPending(e)
	n.logg.Printf("node=%s role=leader term=%d lastIndex=%d commit=%d 赢得多数派，追加 no-op index=%d",
		n.id, n.p.currentTerm, e.Index, n.commitIndex, e.Index)
	n.signalAll()
}

// ---- HandleVote（RPC 入口） ----

// HandleVote 处理其它节点的拉票请求。投票规则（论文 §5.2）：
//  1. 请求 term 更旧 -> 拒绝（RejectStaleTerm），并回传当前 term；
//  2. term 一样但本票已投给别人 -> 拒绝；
//  3. 候选者日志不能比本地旧（先比最后任期，再比最后索引）-> 否则拒绝；
//  4. 其余情况：投票、认对方为合法候选者、重置选举计时器。
func (n *Node) HandleVote(req VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.p.currentTerm {
		n.logg.Printf("node=%s 拒绝投票: term=%d 小于本地 term=%d (%s) candidate=%s",
			n.id, req.Term, n.p.currentTerm, RejectStaleTerm, req.CandidateID)
		return VoteResponse{Term: n.p.currentTerm, Reject: RejectStaleTerm}
	}
	if req.Term > n.p.currentTerm {
		n.stepDown(req.Term)
	}

	if n.p.votedFor != "" && n.p.votedFor != req.CandidateID {
		n.logg.Printf("node=%s 拒绝投票: term=%d 已投给 %s candidate=%s",
			n.id, req.Term, n.p.votedFor, req.CandidateID)
		return VoteResponse{Term: n.p.currentTerm, Reject: RejectStaleTerm}
	}

	lastIdx := n.p.log.lastIndex()
	lastTerm, _ := n.p.log.termAt(lastIdx)
	upToDate := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if !upToDate {
		n.logg.Printf("node=%s 拒绝投票: candidate=%s 日志过旧 (c=(%d,%d) local=(%d,%d))",
			n.id, req.CandidateID, req.LastLogTerm, req.LastLogIndex, lastTerm, lastIdx)
		return VoteResponse{Term: n.p.currentTerm, Reject: RejectLogConflict}
	}

	n.p.votedFor = req.CandidateID
	if n.role != Leader {
		n.role = Follower
	}
	n.resetElectionTimer()
	n.logg.Printf("node=%s role=follower term=%d 投票给 candidate=%s lastIndex=%d commit=%d",
		n.id, req.Term, req.CandidateID, lastIdx, n.commitIndex)
	return VoteResponse{Term: n.p.currentTerm, VoteGranted: true}
}

// stepDown 调用方持锁。发现更大任期时退回 follower：term 推进到
// newTerm，同时本任期投票记录复位（论文：votedFor 每个任期开始时为空），
// 清领导者身份与复制状态，但日志、commitIndex 等已提交内容全部保留。
func (n *Node) stepDown(newTerm uint64) {
	oldTerm := n.p.currentTerm
	n.p.currentTerm = newTerm
	n.role = Follower
	n.leaderID = ""
	n.p.votedFor = ""
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	// 关闭并清空本任期所有复制器，等待它们退出，避免旧任期 goroutine
	// 在新任期继续以过期领导者身份发 AppendEntries。
	n.stopReplicators()
	n.failPending()
	n.resetElectionTimer()
	if newTerm != oldTerm {
		n.logg.Printf("node=%s role=follower term=%d 发现更高任期，退回 follower commit=%d lastIndex=%d",
			n.id, newTerm, n.commitIndex, n.p.log.lastIndex())
	}
}
