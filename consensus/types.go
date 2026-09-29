package consensus

// State 是节点的角色状态。
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Command 是客户端写请求。ClientID + SeqNum 构成幂等键。
type Command struct {
	Op       string
	Key      string
	Value    string
	ClientID string
	SeqNum   uint64
}

// Entry 是日志条目，Index 从 1 开始。
type Entry struct {
	Index uint64
	Term  uint64
	Cmd   Command
}

// VoteArgs / VoteReply 是 RequestVote RPC 的参数与响应。
type VoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type VoteReply struct {
	Term    uint64
	Granted bool
	Reason  RejectReason
	PeerID  string
}

// AppendArgs / AppendReply 是 AppendEntries RPC（同时承担心跳）。
type AppendArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendReply struct {
	Term    uint64
	Success bool
	Reason  RejectReason
	PeerID  string
	// 冲突提示：帮助领导者快速回退 nextIndex。
	ConflictIndex uint64
	ConflictTerm  uint64
}

// Transport 抽象节点之间的 RPC，由内存集群实现。
type Transport interface {
	RequestVote(from, to string, args *VoteArgs) (*VoteReply, error)
	AppendEntries(from, to string, args *AppendArgs) (*AppendReply, error)
}
