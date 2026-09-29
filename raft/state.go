package raft

// KVStateMachine 是一个极简 KV 状态机，顺序应用已提交日志。
// 只有 committed 之后的条目才会进入 Apply；重复条目必须在去重表拦截，
// 绝不允许同一条客户端请求被 Apply 两次。
type KVStateMachine struct {
	data map[string]string
}

func newKVStateMachine() *KVStateMachine {
	return &KVStateMachine{data: map[string]string{}}
}

// Apply 顺序应用一条已提交条目，返回对客户端可见的结果字符串。
// 同一个 (ClientID, Seq) 的第二次 Apply 必须由节点的去重表拦截，
// 状态机本身不再重复防护——单调、无空洞的提交顺序由 Raft 层保证。
func (s *KVStateMachine) Apply(e Entry) string {
	switch e.Op {
	case OpPut:
		s.data[e.Key] = e.Value
		return "ok:" + e.Value
	case OpDelete:
		delete(s.data, e.Key)
		return "deleted"
	case OpNoOp:
		return ""
	default:
		return "unknown-op"
	}
}

func (s *KVStateMachine) Read(key string) (string, bool) {
	v, ok := s.data[key]
	return v, ok
}

// load 用快照内容整体替换状态机（InstallSnapshot 恢复时使用）。
func (s *KVStateMachine) load(data map[string]string) {
	s.data = make(map[string]string, len(data))
	for k, v := range data {
		s.data[k] = v
	}
}

func (s *KVStateMachine) clone() map[string]string {
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}
