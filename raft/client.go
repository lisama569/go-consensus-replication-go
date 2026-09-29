package raft

import "context"

// ClientWrite 是客户端写入口。规则：
//  1. 非 leader 一律拒绝（RejectNotLeader），并在响应中带上 leader 提示；
//     过期任期写入会在复制阶段被对端用 RejectStaleTerm 顶回来；
//  2. (ClientID, Seq) 幂等：已应用的重复请求直接复用旧结果，不写新日志，
//     也绝不会把旧结果伪装成新的提交；
//  3. 已追加但未提交的同一请求：挂到同一个等待者上，只等一次复制；
//  4. 新请求追加到本地日志并唤醒复制，只有多数派确认提交后才返回成功；
//     ctx 超时 / leader 被更高任期顶掉时返回可区分的失败原因。
func (n *Node) ClientWrite(ctx context.Context, req ClientRequest) ClientResponse {
	n.mu.Lock()
	selfID, term := n.id, n.p.currentTerm

	if n.stopped {
		n.mu.Unlock()
		return ClientResponse{Reject: RejectUnreachable, Term: term}
	}
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		reason := RejectNotLeader
		if term == 0 && leader == "" {
			reason = RejectNoLeader
		}
		return ClientResponse{Reject: reason, Term: term, LeaderID: leader}
	}

	// ---- 幂等检查 1：该 (ClientID, Seq) 已经应用过 ----
	if rec, ok := n.p.applied[dedupKey(req.ClientID, req.Seq)]; ok {
		n.logg.Printf("node=%s leader term=%d 重复写请求 client=%s seq=%d 复用结果 index=%d result=%q",
			selfID, term, req.ClientID, req.Seq, rec.index, rec.result)
		n.mu.Unlock()
		return ClientResponse{
			OK: true, Result: rec.result, LeaderID: selfID,
			Term: term, Index: rec.index, Reject: RejectDuplicate,
		}
	}

	// ---- 幂等检查 2：同一请求正在复制中，合并等待者 ----
	for idx, pw := range n.pending {
		if !pw.entry.isNoOp() && pw.entry.ClientID == req.ClientID && pw.entry.Seq == req.Seq {
			ch := make(chan ClientResponse, 1)
			pw.waiters = append(pw.waiters, ch)
			n.logg.Printf("node=%s leader term=%d 重复写请求 client=%s seq=%d 合并到在途提交 index=%d",
				selfID, term, req.ClientID, req.Seq, idx)
			n.mu.Unlock()
			return n.waitWrite(ctx, ch)
		}
	}

	entry := Entry{
		Op:       req.Op,
		Key:      req.Key,
		Value:    req.Value,
		ClientID: req.ClientID,
		Seq:      req.Seq,
	}
	e := n.p.log.appendNew(term, entry)
	n.registerPending(e)
	ch := make(chan ClientResponse, 1)
	n.addWaiter(e.Index, ch)
	n.signalAll()
	commit := n.commitIndex
	n.mu.Unlock()

	n.logg.Printf("node=%s leader term=%d 新写请求 client=%s seq=%d 追加 index=%d commit=%d",
		selfID, term, req.ClientID, req.Seq, e.Index, commit)
	return n.waitWrite(ctx, ch)
}

// waitWrite 等待多数派提交回调，或者上下文取消 / 领导层变更。
// 注意所有节点字段只在锁内读取，避免与 stepDown 竞争。
func (n *Node) waitWrite(ctx context.Context, ch chan ClientResponse) ClientResponse {
	select {
	case resp := <-ch:
		return resp
	case <-ctx.Done():
		n.mu.Lock()
		term, leader, role := n.p.currentTerm, n.leaderID, n.role
		n.mu.Unlock()
		if role != Leader {
			return ClientResponse{Reject: RejectNotLeader, Term: term, LeaderID: leader}
		}
		return ClientResponse{Reject: RejectTimeout, Term: term, LeaderID: leader}
	}
}

// ClientRead 提供线性一致读。简化实现采用"leader 只读已提交状态 +
// 当选后先提交 no-op 确认领导权"：新 leader 的 no-op 提交意味着它已经
// 掌握所有前任任期的已提交条目，因此读状态机不会陈旧；非 leader 直接
// 拒绝并给出 leader 提示。未达到多数派的写入只存在于未提交日志中，
// 永远不会出现在状态机里，所以不可能被读到。
func (n *Node) ClientRead(ctx context.Context, req ReadRequest) ReadResponse {
	n.mu.Lock()
	selfID, term := n.id, n.p.currentTerm
	if n.stopped {
		n.mu.Unlock()
		return ReadResponse{Reject: RejectUnreachable, Term: term}
	}
	if n.role != Leader {
		leader := n.leaderID
		n.mu.Unlock()
		reason := RejectNotLeader
		if term == 0 && leader == "" {
			reason = RejectNoLeader
		}
		return ReadResponse{Reject: reason, Term: term, LeaderID: leader}
	}
	// 只有已经 apply 到状态机的内容可读：未复制到多数派的写不可见。
	v, found := n.sm.Read(req.Key)
	commit := n.commitIndex
	n.mu.Unlock()
	n.logg.Printf("node=%s leader term=%d 读 key=%s found=%v commit=%d",
		selfID, term, req.Key, found, commit)
	return ReadResponse{OK: true, Value: v, Found: found, LeaderID: selfID, Term: term}
}
