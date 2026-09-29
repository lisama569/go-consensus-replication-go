package raft

// registerPending 为新追加的日志条目登记一个完成通道。no-op 条目也登记，
// 但没有外部等待者，只用于在它提交时触发 apply/推进。
// 调用方持锁。
func (n *Node) registerPending(e Entry) {
	n.pending[e.Index] = &pendingWrite{
		entry:   e,
		waiters: nil,
	}
}

// addWaiter 把客户端等待通道挂到指定索引上（重复请求会挂到同一个索引）。
// 调用方持锁。
func (n *Node) addWaiter(index uint64, ch chan ClientResponse) {
	if pw, ok := n.pending[index]; ok {
		pw.waiters = append(pw.waiters, ch)
	}
}

// signalAll / signalPeer 非阻塞地唤醒复制协程，让它立刻发一次 AppendEntries。
// 调用方持 n.mu 也没关系：channel 发送是有缓冲的非阻塞操作。
func (n *Node) signalAll() {
	for peer, ch := range n.notifyCh {
		select {
		case ch <- struct{}{}:
		default:
			_ = peer
		}
	}
}

func (n *Node) signalPeer(peer string) {
	if ch, ok := n.notifyCh[peer]; ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
