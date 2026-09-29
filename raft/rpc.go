package raft

import "context"

// ---------- RequestVote ----------

type VoteRequest struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
	Reject      RejectReason
}

// ---------- AppendEntries（同时承载心跳） ----------

type AppendRequest struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendResponse struct {
	Term    uint64
	Success bool
	Reject  RejectReason
	// ConflictIndex / ConflictTerm 用于冲突快速回退（论文 §5.3 优化）。
	ConflictIndex uint64
	ConflictTerm  uint64
	// MatchIndex 跟随者追加成功后与领导者一致的最高日志索引。
	MatchIndex uint64
}

// ---------- InstallSnapshot（追赶落后节点） ----------

type SnapshotRequest struct {
	Term     uint64
	LeaderID string
	LastInc  uint64 // 快照包含到的最后日志索引
	LastTerm uint64
	Data     map[string]string
}

type SnapshotResponse struct {
	Term   uint64
	OK     bool
	Reject RejectReason
}

// ---------- 客户端读写 ----------

type ClientRequest struct {
	ClientID string
	Seq      uint64
	Op       string
	Key      string
	Value    string
}

type ClientResponse struct {
	OK       bool
	Result   string
	LeaderID string
	Reject   RejectReason
	Term     uint64
	// Index 该请求最终提交的日志索引（便于测试断言与日志打印）。
	Index uint64
}

type ReadRequest struct {
	Key string
}

type ReadResponse struct {
	OK       bool
	Value    string
	Found    bool
	LeaderID string
	Reject   RejectReason
	Term     uint64
}

// Transporter 抽象节点之间的网络。内存实现可以注入分区、丢包与不可达，
// 这样"网络分区"场景不需要真的起 socket。
type Transporter interface {
	RequestVote(ctx context.Context, from, to string, req VoteRequest) (VoteResponse, error)
	AppendEntries(ctx context.Context, from, to string, req AppendRequest) (AppendResponse, error)
	InstallSnapshot(ctx context.Context, from, to string, req SnapshotRequest) (SnapshotResponse, error)
}
