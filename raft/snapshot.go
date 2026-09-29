package raft

// HandleSnapshot 处理领导者推送的快照（论文 §5.7），用于让
// 落后太多 / 数据丢失重启的节点追上集群：
//   - 快照 term 更旧 -> RejectStaleTerm；
//   - 快照不新于本地基点 -> 幂等忽略，返回 OK（重复投递安全）；
//   - 否则压缩日志、整体加载状态机数据，提交位置/应用位置至少推进到
//     快照包含的最后索引，且只能单调前进。
func (n *Node) HandleSnapshot(req SnapshotRequest) SnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.p.currentTerm {
		return SnapshotResponse{Term: n.p.currentTerm, Reject: RejectStaleTerm}
	}
	if req.Term > n.p.currentTerm {
		n.stepDown(req.Term)
	}
	n.role = Follower
	n.leaderID = req.LeaderID
	n.resetElectionTimer()

	if req.LastInc <= n.p.log.baseIndex {
		// 幂等：已经装过相同/更新的快照。
		return SnapshotResponse{Term: n.p.currentTerm, OK: true}
	}

	n.p.log.installSnapshot(req.LastInc, req.LastTerm)
	n.sm.load(req.Data)
	n.p.snapshotData = n.sm.clone()

	if req.LastInc > n.commitIndex {
		n.commitIndex = req.LastInc
	}
	if n.lastApplied < req.LastInc {
		n.lastApplied = req.LastInc
	}
	n.logg.Printf("node=%s follower term=%d 安装快照 through=%d commit=%d lastIndex=%d",
		n.id, req.Term, req.LastInc, n.commitIndex, n.p.log.lastIndex())
	return SnapshotResponse{Term: n.p.currentTerm, OK: true}
}
