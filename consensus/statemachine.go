package consensus

// ApplyResult 是某条命令被应用后的结果。
type ApplyResult struct {
	ClientID string
	SeqNum   uint64
	Op       string
	Key      string
	Value    string // 写：被写入的值；读：读取到的值
	Found    bool
}

// KVStateMachine 是一个极简顺序 KV，按日志顺序 apply。
// 已提交命令按 (ClientID, SeqNum) 幂等：重复命令不会再次执行，
// 但会返回与首次相同的结果，避免旧结果被误当成新请求的结果。
type KVStateMachine struct {
	data    map[string]string
	lastSeq map[string]uint64
	// 保存每个客户端每个序号首次应用时的结果，供重试返回相同结果。
	results map[string]map[uint64]ApplyResult
	// 按日志索引保存应用结果（含判定为重复的投递），供 Propose 取回结果，
	// 避免对同一条目二次执行副作用。
	byIndex map[uint64]ApplyResult
	history []ApplyResult
}

func newKVStateMachine() *KVStateMachine {
	return &KVStateMachine{
		data:    map[string]string{},
		lastSeq: map[string]uint64{},
		results: map[string]map[uint64]ApplyResult{},
		byIndex: map[uint64]ApplyResult{},
	}
}

// Apply 应用一条日志条目。
// applied=true 表示该命令首次执行；applied=false 表示重复投递，
// 此时返回的是首次应用时记录的结果，不会重复修改状态。
func (m *KVStateMachine) Apply(e Entry) (result ApplyResult, applied bool) {
	cmd := e.Cmd
	defer func() { m.byIndex[e.Index] = result }()
	if cmd.ClientID != "" {
		if r, ok := m.results[cmd.ClientID]; ok {
			if old, ok2 := r[cmd.SeqNum]; ok2 {
				return old, false
			}
		}
		// 序号必须单调递增：乱序序号拒绝应用（正常 Raft 流程不会发生）。
		if cmd.SeqNum <= m.lastSeq[cmd.ClientID] {
			// 已被更晚序号覆盖又查不到结果，按重复的已见请求处理。
			return ApplyResult{ClientID: cmd.ClientID, SeqNum: cmd.SeqNum}, false
		}
	}

	res := ApplyResult{
		ClientID: cmd.ClientID,
		SeqNum:   cmd.SeqNum,
		Op:       cmd.Op,
		Key:      cmd.Key,
	}
	switch cmd.Op {
	case "set":
		m.data[cmd.Key] = cmd.Value
		res.Value = cmd.Value
	case "delete":
		delete(m.data, cmd.Key)
		res.Value = cmd.Value
	default: // 未知操作按 no-op 记录，保证应用位置单调
		res.Value = cmd.Value
	}
	m.record(cmd, res)
	return res, true
}

func (m *KVStateMachine) record(cmd Command, res ApplyResult) {
	if cmd.ClientID == "" {
		m.history = append(m.history, res)
		return
	}
	if m.results[cmd.ClientID] == nil {
		m.results[cmd.ClientID] = map[uint64]ApplyResult{}
	}
	m.results[cmd.ClientID][cmd.SeqNum] = res
	if cmd.SeqNum > m.lastSeq[cmd.ClientID] {
		m.lastSeq[cmd.ClientID] = cmd.SeqNum
	}
	m.history = append(m.history, res)
}

// ResultAt 返回某日志索引首次/重复应用时记录的结果。
func (m *KVStateMachine) ResultAt(index uint64) (ApplyResult, bool) {
	r, ok := m.byIndex[index]
	return r, ok
}

// Get 返回已应用的键值。
func (m *KVStateMachine) Get(key string) (string, bool) {
	v, ok := m.data[key]
	return v, ok
}

// LastSeq 返回某客户端已应用的最大请求序号。
func (m *KVStateMachine) LastSeq(clientID string) uint64 {
	return m.lastSeq[clientID]
}

// History 返回已应用结果的拷贝（测试用）。
func (m *KVStateMachine) History() []ApplyResult {
	out := make([]ApplyResult, len(m.history))
	copy(out, m.history)
	return out
}
