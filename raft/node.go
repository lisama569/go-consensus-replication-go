package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "follower"
	}
}

// pendingWrite 记录一个等待多数派确认的客户端写（或去重命中的同 seq 等待者）。
type pendingWrite struct {
	entry   Entry
	waiters []chan ClientResponse
}

// persisted 打包"落盘"的状态，模拟崩溃重启时它可以被保留或丢弃。
type persisted struct {
	currentTerm uint64
	votedFor    string
	log         *PersistentLog
	// 去重表也视为持久状态的一部分：重启后重复投递仍然要幂等。
	// 快照安装时它同样随之更新。按 (ClientID, Seq) 逐个保存，而不是只
	// 保存最新 seq 水位——重投任意历史 seq 都必须能识别为重复。
	applied      map[string]appliedRecord
	snapshotData map[string]string // 非空表示曾通过快照压缩
}

// appliedRecord 记录一次客户端写被应用后的结果，供重复投递幂等返回。
type appliedRecord struct {
	client string
	seq    uint64
	index  uint64
	result string
}

// Node 是单个 Raft 节点。持久字段与易失字段在 persisted 中分开存放，
// 模拟崩溃重启时的差异。
type Node struct {
	mu sync.Mutex

	id    string
	peers []string
	trans Transporter
	sm    *KVStateMachine
	logg  Logger

	p persisted

	// ---- 所有服务器上的易失状态 ----
	commitIndex uint64
	lastApplied uint64
	role        Role
	leaderID    string

	// 客户端正在等待结果的写（追加但尚未提交），索引 -> 等待者集合。
	pending map[uint64]*pendingWrite

	// ---- 领导者易失状态 ----
	nextIndex  map[string]uint64
	matchIndex map[string]uint64
	notifyCh   map[string]chan struct{} // 当前任期每个跟随者一个唤醒信号
	repDone    map[string]chan struct{} // 当前任期每个复制器的退出信号
	resetSig   chan struct{}            // 心跳/新投票事件到达时打断旧的选举等待

	// ---- 候选者 ----
	votes map[string]bool

	// ---- 计时与生命周期 ----
	electionMin time.Duration
	electionMax time.Duration
	heartbeat   time.Duration
	rng         *rand.Rand
	electionDue time.Time
	cancel      context.CancelFunc
	done        chan struct{}
	stopped     bool
}

// Config 描述一个节点的初始配置。
type Config struct {
	ID        string
	Peers     []string
	Transport Transporter
	Logger    Logger

	// 计时参数留空时使用默认值；测试会传入较短时长加速收敛。
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RandSeed           int64
}

var defaultElectionMin = 150 * time.Millisecond
var defaultElectionMax = 300 * time.Millisecond
var defaultHeartbeat = 60 * time.Millisecond

// NewNode 创建一个处于 follower 状态、任期 0 的空节点。
func NewNode(cfg Config) *Node {
	n := &Node{
		id:          cfg.ID,
		peers:       cfg.Peers,
		trans:       cfg.Transport,
		sm:          newKVStateMachine(),
		logg:        cfg.Logger,
		p:           persisted{log: newPersistentLog(), applied: map[string]appliedRecord{}},
		pending:     map[uint64]*pendingWrite{},
		nextIndex:   map[string]uint64{},
		matchIndex:  map[string]uint64{},
		notifyCh:    map[string]chan struct{}{},
		repDone:     map[string]chan struct{}{},
		votes:       map[string]bool{},
		resetSig:    make(chan struct{}, 1),
		electionMin: cfg.ElectionTimeoutMin,
		electionMax: cfg.ElectionTimeoutMax,
		heartbeat:   cfg.HeartbeatInterval,
		rng:         rand.New(rand.NewSource(cfg.RandSeed)),
	}
	if n.logg == nil {
		n.logg = nopLogger{}
	}
	if n.electionMin <= 0 {
		n.electionMin = defaultElectionMin
	}
	if n.electionMax < n.electionMin {
		n.electionMax = n.electionMin * 2
	}
	if n.heartbeat <= 0 {
		n.heartbeat = defaultHeartbeat
	}
	n.resetElectionTimer()
	n.logg.Printf("node=%s role=follower term=0 lastIndex=0 commit=0 初始化完成 peers=%v",
		n.id, n.peers)
	return n
}

// reviveFrom 用一份持久状态"重启"节点：易失状态清零，持久状态保留。
func reviveFrom(cfg Config, p persisted) *Node {
	n := NewNode(cfg)
	n.p = p
	n.commitIndex = p.log.baseIndex
	n.lastApplied = p.log.baseIndex
	if p.snapshotData != nil {
		n.sm.load(p.snapshotData)
	}
	n.logg.Printf("node=%s 重启恢复 term=%d lastIndex=%d baseIndex=%d commit=%d",
		n.id, p.currentTerm, p.log.lastIndex(), p.log.baseIndex, n.commitIndex)
	return n
}

// freshPersisted 供"磁盘损坏"重启使用。
func freshPersisted() persisted {
	return persisted{
		log:     newPersistentLog(),
		applied: map[string]appliedRecord{},
	}
}

func (n *Node) resetElectionTimer() {
	span := int64(n.electionMax - n.electionMin)
	d := n.electionMin
	if span > 0 {
		d += time.Duration(n.rng.Int63n(span))
	}
	n.electionDue = timeNow().Add(d)
	// 打断 runLoop 中可能正在进行的旧等待，让它按新的 due 重新计时。
	select {
	case n.resetSig <- struct{}{}:
	default:
	}
}

// ---- 生命周期 ----

func (n *Node) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.done = make(chan struct{})
	go n.runLoop(ctx)
}

func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	cancel := n.cancel
	n.mu.Unlock()
	if cancel != nil {
		cancel()
		<-n.done
	}
	// runLoop 退出后再等待本任期复制器全部收尾，避免残留 goroutine。
	n.stopReplicatorsAndWait()
}

// runLoop 是节点的后台节拍：follower/candidate 等选举超时，
// leader 按心跳间隔广播并推进复制。所有真实时间只在这里出现，
// 测试也可以直接调用 tickElection / sendHeartbeat 做确定性驱动。
func (n *Node) runLoop(ctx context.Context) {
	for {
		n.mu.Lock()
		role := n.role
		var wait time.Duration
		if role == Leader {
			wait = n.heartbeat
		} else {
			wait = time.Until(n.electionDue)
			if wait < 0 {
				wait = 0
			}
		}
		n.mu.Unlock()

		// 排掉"进入本伦等待之前"产生的残留重置信号，只监听本轮内的新事件。
		select {
		case <-n.resetSig:
		default:
		}

		select {
		case <-ctx.Done():
			close(n.done)
			return
		case <-timeAfter(wait):
		case <-n.resetSig:
			// 选举计时器在本轮等待期间被心跳/投票事件重置，重新计算等待。
			continue
		}

		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			continue
		}
		nowLeader := n.role
		n.mu.Unlock()
		switch nowLeader {
		case Leader:
			n.sendHeartbeat(ctx)
		default:
			n.tickElection()
		}
	}
}

// ---- 只读快照（测试与日志打印用） ----

type NodeStatus struct {
	ID           string
	Role         Role
	Term         uint64
	Leader       string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
	VotedFor     string
	LogLen       int
	SMData       map[string]string
	AppliedSeqs  map[string][]uint64
}

func (n *Node) Status() NodeStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	lt, _ := n.p.log.termAt(n.p.log.lastIndex())
	return NodeStatus{
		ID:           n.id,
		Role:         n.role,
		Term:         n.p.currentTerm,
		Leader:       n.leaderID,
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
		LastLogIndex: n.p.log.lastIndex(),
		LastLogTerm:  lt,
		VotedFor:     n.p.votedFor,
		LogLen:       int(n.p.log.lastIndex()-n.p.log.baseIndex) + 1,
		SMData:       n.sm.clone(),
		AppliedSeqs:  n.snapshotAppliedSeqs(),
	}
}

// snapshotAppliedSeqs 把去重表按 client 聚合成 seq 列表，仅用于测试观察。
func (n *Node) snapshotAppliedSeqs() map[string][]uint64 {
	out := map[string][]uint64{}
	for _, rec := range n.p.applied {
		out[rec.client] = append(out[rec.client], rec.seq)
	}
	return out
}
