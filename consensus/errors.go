package consensus

import "errors"

// RejectReason 以可区分的原因码描述所有拒绝场景，
// 调用方可以通过 ReasonOf(err) 精确判断失败原因，而不是解析错误文本。
type RejectReason string

const (
	// ReasonStaleTerm 来自更小任期的请求（过期领导者 / 旧候选者）。
	ReasonStaleTerm RejectReason = "stale_term"
	// ReasonNotLeader 当前节点不是领导者，无法处理写/读请求。
	ReasonNotLeader RejectReason = "not_leader"
	// ReasonNoQuorum 提案在超时时间内未得到多数派确认。
	ReasonNoQuorum RejectReason = "no_quorum"
	// ReasonAlreadyVoted 当前任期已经把票投给了其他候选者。
	ReasonAlreadyVoted RejectReason = "already_voted"
	// ReasonLogNotUpToDate 候选者日志不如节点本地日志新，不能投票。
	ReasonLogNotUpToDate RejectReason = "log_not_up_to_date"
	// ReasonLogGap 追加请求前向跳跃，prevLogIndex 之前存在空洞。
	ReasonLogGap RejectReason = "log_gap"
	// ReasonLogConflict 追加位置的日志任期/索引与本地冲突，需要回退裁剪。
	ReasonLogConflict RejectReason = "log_conflict"
	// ReasonReplayedAppend 追加请求回退到已冲突裁剪点之前，属于乱序/重放。
	ReasonReplayedAppend RejectReason = "replayed_append"
	// ReasonUnreachable 对端不可达（分区、停机、丢包），这不是协议拒绝。
	ReasonUnreachable RejectReason = "unreachable"
	// ReasonNodeDown 节点已停止，不处理任何请求。
	ReasonNodeDown RejectReason = "node_down"
	// ReasonInvalidIndex 客户端或日志参数非法。
	ReasonInvalidIndex RejectReason = "invalid_index"
)

// RejectError 携带结构化的拒绝原因。
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Detail
}

func newReject(reason RejectReason, detail string) *RejectError {
	return &RejectError{Reason: reason, Detail: detail}
}

// ReasonOf 提取错误中的 RejectReason；普通错误返回空串。
func ReasonOf(err error) RejectReason {
	if err == nil {
		return ""
	}
	var re *RejectError
	if errors.As(err, &re) {
		return re.Reason
	}
	return ""
}
