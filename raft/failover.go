package raft

// failPending 在领导者失去身份时调用：所有尚未提交的在途写立刻收到
// RejectNotLeader，客户端可以带着 leader 提示去重试，而不必干等超时。
// 调用方持锁。
func (n *Node) failPending() {
	for idx, pw := range n.pending {
		resp := ClientResponse{
			OK:       false,
			Reject:   RejectNotLeader,
			Term:     n.p.currentTerm,
			LeaderID: n.leaderID,
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
