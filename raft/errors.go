package raft

// RejectReason 是 RPC 请求被拒绝时的可机读原因，调用方可以据此区分
// "任期过旧"、"不是领导者"、"日志不一致" 等不同失败场景。
type RejectReason int

const (
	RejectNone RejectReason = iota
	// RejectStaleTerm 请求来自更小的任期，接收方当前任期更大。
	RejectStaleTerm
	// RejectNotLeader 接收方不是领导者（例如它是跟随者，或已不知主）。
	RejectNotLeader
	// RejectStaleLeader 接收方是领导者，但请求方携带的任期更旧。
	RejectStaleLeader
	// RejectLogConflict AppendEntries 的 prevLog 与本地日志不一致。
	RejectLogConflict
	// RejectCommittedRollback 请求试图删除/覆盖一条已提交的日志。
	RejectCommittedRollback
	// RejectCommitGap 新的 leaderCommit 与本地提交位置之间存在空洞。
	RejectCommitGap
	// RejectDuplicate 重复的客户端请求（同一 ClientID + Seq 已处理）。
	RejectDuplicate
	// RejectUnreachable 目标节点当前不可达（已崩溃或处于网络分区另一侧）。
	RejectUnreachable
	// RejectTimeout 多数派确认未在超时时间内收集到。
	RejectTimeout
	// RejectNoLeader 当前集群暂时没有已知的有效领导者。
	RejectNoLeader
	// RejectGap 写入尚未达到多数派提交，读取必须拒绝陈旧数据。
	RejectGap
)

// String 返回拒绝原因的人类可读描述。
func (r RejectReason) String() string {
	switch r {
	case RejectStaleTerm:
		return "stale-term"
	case RejectNotLeader:
		return "not-leader"
	case RejectStaleLeader:
		return "stale-leader"
	case RejectLogConflict:
		return "log-conflict"
	case RejectCommittedRollback:
		return "committed-rollback"
	case RejectCommitGap:
		return "commit-gap"
	case RejectDuplicate:
		return "duplicate-request"
	case RejectUnreachable:
		return "node-unreachable"
	case RejectTimeout:
		return "quorum-timeout"
	case RejectNoLeader:
		return "no-known-leader"
	case RejectGap:
		return "not-committed-gap"
	default:
		return "unknown"
	}
}

// Error 让 RejectReason 可以直接作为 error 使用。
func (r RejectReason) Error() string { return r.String() }

// RaftError 携带一次失败的结构化信息：拒绝原因、接收方观察到的最新任期，
// 以及可选的当前领导者提示。
type RaftError struct {
	Reason  RejectReason
	Term    uint64
	Hint    string
	Leader  string
	Message string
}

func (e *RaftError) Error() string {
	if e.Message != "" {
		return e.Reason.String() + ": " + e.Message
	}
	return e.Reason.String()
}

// Is 支持 errors.Is(err, RejectXxx) 判定。
func (e *RaftError) Is(target error) bool {
	if r, ok := target.(RejectReason); ok {
		return e.Reason == r
	}
	return false
}
