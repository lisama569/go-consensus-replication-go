package raft

import "context"

// memTransport 是进程内直连的 Transporter：调用直接转发给目标 Node 的
// HandleXxx。通过 alive/partition 集合可以模拟节点崩溃与网络分区，
// 不可达时返回 RejectUnreachable，与真实网络故障的可观察行为一致。
type memTransport struct {
	c *Cluster
}

func (t *memTransport) deliver(from, to string) error {
	if !t.c.canDeliver(from, to) {
		return &RaftError{Reason: RejectUnreachable, Message: to + " unreachable"}
	}
	return nil
}

func (t *memTransport) RequestVote(ctx context.Context, from, to string, req VoteRequest) (VoteResponse, error) {
	if err := t.deliver(from, to); err != nil {
		return VoteResponse{}, err
	}
	n := t.c.Node(to)
	if n == nil {
		return VoteResponse{}, &RaftError{Reason: RejectUnreachable, Message: to}
	}
	return n.HandleVote(req), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, from, to string, req AppendRequest) (AppendResponse, error) {
	if err := t.deliver(from, to); err != nil {
		return AppendResponse{}, err
	}
	n := t.c.Node(to)
	if n == nil {
		return AppendResponse{}, &RaftError{Reason: RejectUnreachable, Message: to}
	}
	return n.HandleAppend(req), nil
}

func (t *memTransport) InstallSnapshot(ctx context.Context, from, to string, req SnapshotRequest) (SnapshotResponse, error) {
	if err := t.deliver(from, to); err != nil {
		return SnapshotResponse{}, err
	}
	n := t.c.Node(to)
	if n == nil {
		return SnapshotResponse{}, &RaftError{Reason: RejectUnreachable, Message: to}
	}
	return n.HandleSnapshot(req), nil
}
