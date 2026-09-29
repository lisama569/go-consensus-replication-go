package consensus

import (
	"hash/fnv"
	"time"
)

// Logger 用于输出带任期/索引/提交位置的协议日志。
type Logger interface {
	Printf(format string, args ...any)
}

const (
	heartbeatEveryTicks = 3
	minElectionTicks    = 15
	electionJitterTicks = 10
	// proposeRounds 限制单条提案内部的同步轮次（每轮至少推进一个 nextIndex）。
	proposeRounds = 128
)

// Node 是一个内存 Raft 节点。所有节点共享 Cluster.mu：
// 公开方法（Tick/Propose/Read/生命周期）加锁，RPC 与内部方法假定已持锁，
// 从而把整个小集群模拟成串行执行，彻底排除数据竞争，又能验证并发入口的语义。
type Node struct {
	id      string
	cluster *Cluster

	state       State
	currentTerm uint64
	votedFor    string // 节点 ID；"" 表示本任期尚未投票
	leaderID    string

	log         *LogStore
	commitIndex uint64
	lastApplied uint64
	sm          *KVStateMachine

	// 领导者复制进度
	nextIndex  map[string]uint64
	matchIndex map[string]uint64

	// 选举 / 心跳计时（单位：tick）
	electionTimeout int
	electionElapsed int
	heartbeatTick   int
	randState       uint64

	down bool

	logf Logger
}

func newNode(c *Cluster, id string, logger Logger) *Node {
	n := &Node{
		id:         id,
		cluster:    c,
		state:      Follower,
		log:        newLogStore(),
		sm:         newKVStateMachine(),
		nextIndex:  map[string]uint64{},
		matchIndex: map[string]uint64{},
		logf:       logger,
	}
	h := fnv.New64a()
	h.Write([]byte(id))
	n.randState = h.Sum64() ^ uint64(time.Now().UnixNano())
	n.electionTimeout = n.randomElectionTimeout()
	return n
}

// ---- 观察接口（加锁）----

func (n *Node) ID() string   { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.id }
func (n *Node) State() State { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.state }
func (n *Node) Term() uint64 { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.currentTerm }
func (n *Node) CommitIndex() uint64 {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	return n.commitIndex
}
func (n *Node) LastApplied() uint64 {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	return n.lastApplied
}
func (n *Node) LeaderID() string { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.leaderID }
func (n *Node) Down() bool       { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.down }
func (n *Node) LogSnapshot() []Entry {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	return n.log.Snapshot()
}
func (n *Node) Data() *KVStateMachine { n.cluster.mu.Lock(); defer n.cluster.mu.Unlock(); return n.sm }
func (n *Node) ElectionElapsed() int {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	return n.electionElapsed
}

// ---- 生命周期（加锁）----

// Stop 模拟节点崩溃：不再响应任何 RPC 与时钟，但内存状态保留。
func (n *Node) Stop() {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	n.down = true
	n.protoLog("stop(crash)", "")
}

// Restart 模拟崩溃后重启：保留日志、任期与投票硬状态，以跟随者身份回归。
func (n *Node) Restart() {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	n.down = false
	n.state = Follower
	n.leaderID = ""
	n.electionElapsed = 0
	n.electionTimeout = n.randomElectionTimeout()
	n.protoLog("restart(keep-state)", "")
}

// WipeAndRestart 模拟内存数据完全丢失后的空节点重启，需从领导者完整追赶。
func (n *Node) WipeAndRestart() {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	n.down = false
	n.state = Follower
	n.currentTerm = 0
	n.votedFor = ""
	n.leaderID = ""
	n.log = newLogStore()
	n.commitIndex = 0
	n.lastApplied = 0
	n.sm = newKVStateMachine()
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.electionElapsed = 0
	n.electionTimeout = n.randomElectionTimeout()
	n.protoLog("restart(wiped)", "")
}

// ---- 定时驱动（加锁）----

// Tick 推进一个逻辑时钟：跟随者/候选者计时选举，领导者周期发心跳。
func (n *Node) Tick() {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	n.tickLocked()
}

func (n *Node) tickLocked() {
	if n.down {
		return
	}
	switch n.state {
	case Follower, Candidate:
		n.electionElapsed++
		if n.electionElapsed >= n.electionTimeout {
			n.electionElapsed = 0
			n.startElection()
		}
	case Leader:
		n.heartbeatTick++
		if n.heartbeatTick >= heartbeatEveryTicks {
			n.heartbeatTick = 0
			n.syncRound()
			n.advanceCommit()
		}
	}
}

// ---- 选举（调用方持锁）----

func (n *Node) randomElectionTimeout() int {
	x := n.randState
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	n.randState = x
	return minElectionTicks + int(x%uint64(electionJitterTicks))
}

func (n *Node) startElection() {
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	n.leaderID = ""
	n.electionTimeout = n.randomElectionTimeout()
	n.protoLog("election-start", "")

	args := &VoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.id,
		LastLogIndex: n.log.LastIndex(),
		LastLogTerm:  n.log.LastTerm(),
	}
	votes := 1 // 自己投自己
	for _, peer := range n.cluster.order {
		if peer == n.id {
			continue
		}
		reply, err := n.cluster.RequestVote(n.id, peer, args)
		if err != nil {
			n.protoLog("vote-rpc-unreachable", "peer="+peer+" err="+err.Error())
			continue
		}
		if reply.Term > n.currentTerm {
			// 集群中已存在更高任期，旧候选者立即退回跟随者。
			n.stepDown(reply.Term)
			n.protoLog("election-abort-stale-term", "peer="+peer)
			return
		}
		if reply.Granted {
			votes++
			n.protoLog("vote-granted", "peer="+peer)
			if votes >= n.cluster.majority() {
				n.becomeLeader()
				return
			}
		} else {
			n.protoLog("vote-denied", "peer="+peer+" reason="+string(reply.Reason))
		}
	}
	n.protoLog("election-no-quorum", "votes="+itoa(uint64(votes)))
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.heartbeatTick = 0
	for _, peer := range n.cluster.order {
		if peer == n.id {
			continue
		}
		n.nextIndex[peer] = n.log.LastIndex() + 1
		n.matchIndex[peer] = 0
	}
	// 新领导者先写一条当前任期 no-op：它一旦被多数派复制，
	// 领导者就可以安全提交之前任期遗留的日志（Raft 提交规则）。
	n.log.Append(Entry{
		Index: n.log.LastIndex() + 1,
		Term:  n.currentTerm,
		Cmd:   Command{Op: "noop"},
	})
	n.protoLog("become-leader", "")
	n.syncRound()
	n.advanceCommit()
}

// stepDown 见到更高任期时退回跟随者，并清掉本任期投票。
func (n *Node) stepDown(term uint64) {
	if term < n.currentTerm {
		return
	}
	if term > n.currentTerm {
		n.currentTerm = term
		n.votedFor = ""
	}
	n.state = Follower
	n.leaderID = ""
	n.electionElapsed = 0
	n.electionTimeout = n.randomElectionTimeout()
}

// ---- RequestVote RPC（调用方持锁）----

func (n *Node) onRequestVote(args *VoteArgs) *VoteReply {
	reply := &VoteReply{Term: n.currentTerm, PeerID: n.id}

	if args.Term < n.currentTerm {
		reply.Reason = ReasonStaleTerm
		n.protoLog("vote-reject-stale-term", "candidate="+args.CandidateID)
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm

	// 日志新旧比较：最后一条日志任期更大者更新；任期相同则索引更大者更新。
	myLastTerm := n.log.LastTerm()
	myLastIndex := n.log.LastIndex()
	upToDate := args.LastLogTerm > myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)
	if !upToDate {
		reply.Reason = ReasonLogNotUpToDate
		n.protoLog("vote-reject-log-behind", "candidate="+args.CandidateID)
		return reply
	}

	if n.votedFor != "" && n.votedFor != args.CandidateID {
		reply.Reason = ReasonAlreadyVoted
		n.protoLog("vote-reject-already-voted", "candidate="+args.CandidateID)
		return reply
	}

	n.votedFor = args.CandidateID
	n.electionElapsed = 0
	reply.Granted = true
	n.protoLog("vote-grant", "candidate="+args.CandidateID)
	return reply
}

// ---- AppendEntries RPC（调用方持锁）----

func (n *Node) onAppendEntries(args *AppendArgs) *AppendReply {
	reply := &AppendReply{Term: n.currentTerm, PeerID: n.id}

	if args.Term < n.currentTerm {
		reply.Reason = ReasonStaleTerm
		n.protoLog("append-reject-stale-term", "leader="+args.LeaderID)
		return reply
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm
	// 同任期见到合法领导者心跳：候选者让位，最多一个有效领导者。
	if n.state != Follower {
		n.state = Follower
		n.leaderID = args.LeaderID
		n.votedFor = ""
	}
	n.leaderID = args.LeaderID
	n.electionElapsed = 0

	last := n.log.LastIndex()

	// 1) prevLog 一致性检查。
	if args.PrevLogIndex > last {
		// 前向跳跃：本地在 prevLogIndex 之前存在空洞。
		reply.Reason = ReasonLogGap
		reply.ConflictIndex = last + 1
		if last > 0 {
			reply.ConflictTerm, _ = n.log.TermAt(last)
		}
		n.protoLog("append-reject-gap", "prevIndex="+itoa(args.PrevLogIndex))
		return reply
	}
	if !n.log.Contains(args.PrevLogIndex, args.PrevLogTerm) {
		localTerm, _ := n.log.TermAt(args.PrevLogIndex)
		if args.PrevLogIndex <= n.commitIndex {
			// 分歧点落在已提交位置：过期/回退领导者想改写已提交日志，明确拒绝。
			reply.Reason = ReasonReplayedAppend
			n.protoLog("append-reject-committed-overwrite", "prevIndex="+itoa(args.PrevLogIndex))
			return reply
		}
		reply.Reason = ReasonLogConflict
		reply.ConflictTerm = localTerm
		reply.ConflictIndex = n.firstIndexOfTerm(localTerm)
		n.protoLog("append-reject-conflict",
			"prevIndex="+itoa(args.PrevLogIndex)+" conflictTerm="+itoa(localTerm))
		return reply
	}

	// 2) 合并 entries：匹配前缀幂等跳过，冲突后缀裁剪，尾部空缺追加。
	truncated := false
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx != e.Index {
			reply.Reason = ReasonInvalidIndex
			return reply
		}
		local, ok := n.log.EntryAt(idx)
		switch {
		case ok && local.Term == e.Term:
			// 相同位置相同任期：重复/重放投递，幂等跳过，绝不重复应用。
			continue
		case ok:
			if idx <= n.commitIndex {
				reply.Reason = ReasonReplayedAppend
				n.protoLog("append-reject-committed-overwrite", "index="+itoa(idx))
				return reply
			}
			n.log.TruncateSuffix(idx) // 冲突裁剪
			truncated = true
		}
		n.log.Append(e)
	}
	if truncated {
		n.protoLog("append-truncated-conflict-tail", "")
	}

	// 3) 推进提交位置并按顺序应用，无空洞。
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > n.log.LastIndex() {
			newCommit = n.log.LastIndex()
		}
		n.commitIndex = newCommit
		n.applyCommitted()
	}
	reply.Success = true
	n.protoLog("append-ok", "entries="+itoa(uint64(len(args.Entries))))
	return reply
}

// firstIndexOfTerm 返回本地日志中指定任期第一条日志的索引；找不到返回 1。
func (n *Node) firstIndexOfTerm(term uint64) uint64 {
	for _, e := range n.log.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return 1
}

// applyCommitted 按索引顺序应用 (lastApplied, commitIndex]，保证单调无空洞。
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e, ok := n.log.EntryAt(n.lastApplied)
		if !ok {
			// 理论不可达：提交位置永远不超过日志长度。
			n.lastApplied--
			return
		}
		res, applied := n.sm.Apply(e)
		if applied {
			n.protoLog("applied",
				"index="+itoa(e.Index)+" client="+res.ClientID+" seq="+itoa(res.SeqNum)+
					" key="+res.Key+" op="+res.Op)
		} else {
			n.protoLog("apply-dedup", "index="+itoa(e.Index)+" client="+res.ClientID+
				" seq="+itoa(res.SeqNum))
		}
	}
}

// ---- 领导者复制（调用方持锁）----

type appendOutcome struct {
	ok           bool
	unreachable  bool
	termOutdated bool
}

// replicateAppend 向单个跟随者发送一轮 AppendEntries，按冲突提示回退 nextIndex。
func (n *Node) replicateAppend(peer string) appendOutcome {
	prev := n.nextIndex[peer] - 1
	prevTerm := uint64(0)
	if t, ok := n.log.TermAt(prev); ok {
		prevTerm = t
	}
	var entries []Entry
	if n.nextIndex[peer] <= n.log.LastIndex() {
		got, err := n.log.Get(n.nextIndex[peer], n.log.LastIndex())
		if err == nil {
			entries = got
		}
	}
	args := &AppendArgs{
		Term:         n.currentTerm,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	reply, err := n.cluster.AppendEntries(n.id, peer, args)
	if err != nil {
		return appendOutcome{unreachable: true}
	}
	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return appendOutcome{termOutdated: true}
	}
	if reply.Success {
		matched := args.PrevLogIndex + uint64(len(args.Entries))
		if matched > n.matchIndex[peer] {
			n.matchIndex[peer] = matched
		}
		n.nextIndex[peer] = matched + 1
		return appendOutcome{ok: true}
	}
	// 被拒绝：用冲突提示快速回退；否则保守回退一格。
	switch reply.Reason {
	case ReasonLogGap:
		n.nextIndex[peer] = reply.ConflictIndex
	case ReasonLogConflict:
		last := n.lastIndexOfTerm(reply.ConflictTerm)
		if last > 0 {
			n.nextIndex[peer] = last + 1
		} else {
			n.nextIndex[peer] = reply.ConflictIndex
		}
	default:
		// ReplayedAppend / StaleTerm（同任期不应出现）：保守回退。
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
	return appendOutcome{}
}

func (n *Node) lastIndexOfTerm(term uint64) uint64 {
	for i := len(n.log.entries) - 1; i >= 0; i-- {
		if n.log.entries[i].Term == term {
			return n.log.entries[i].Index
		}
	}
	return 0
}

// syncRound 向所有可达跟随者各推进复制，必要时多轮回退，直到无进展。
// 返回当前任期下确认存活的跟随者数量（不含自己）。
func (n *Node) syncRound() int {
	acked := 0
	for _, peer := range n.cluster.order {
		if peer == n.id {
			continue
		}
		for round := 0; round < proposeRounds; round++ {
			out := n.replicateAppend(peer)
			if out.termOutdated {
				return acked
			}
			if out.unreachable {
				break
			}
			if out.ok {
				acked++
				break
			}
			if n.nextIndex[peer] <= 1 {
				// 已回退到起点，下一轮一定能发完整日志；再试一次。
				out2 := n.replicateAppend(peer)
				if out2.ok {
					acked++
				}
				break
			}
		}
	}
	return acked
}

// advanceCommit 仅按 Raft 规则提交当前任期的日志，再借由日志连续性带动旧条目提交。
func (n *Node) advanceCommit() {
	for idx := n.log.LastIndex(); idx > n.commitIndex; idx-- {
		t, _ := n.log.TermAt(idx)
		if t != n.currentTerm {
			continue
		}
		count := 1 // 领导者自己
		for _, peer := range n.cluster.order {
			if peer == n.id {
				continue
			}
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.cluster.majority() {
			n.commitIndex = idx
			n.applyCommitted()
			return
		}
	}
}

// ---- 客户端入口（加锁）----

// Propose 在领导者上复制一条写命令；重复 (ClientID, SeqNum) 幂等。
func (n *Node) Propose(cmd Command) (ApplyResult, error) {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	if n.down {
		return ApplyResult{}, newReject(ReasonNodeDown, n.id+" is down")
	}
	if n.state != Leader {
		return ApplyResult{}, newReject(ReasonNotLeader, n.id+" is "+n.state.String())
	}

	// 幂等快路径：该请求序号已在本机应用，直接返回首次结果，不写新日志。
	if cmd.ClientID != "" {
		if old, ok := n.sm.results[cmd.ClientID][cmd.SeqNum]; ok {
			n.protoLog("propose-duplicate-return-old", "client="+cmd.ClientID+" seq="+itoa(cmd.SeqNum))
			return old, nil
		}
	}

	// 复用未提交的重复条目（提案超时后客户端用同一请求号重试）。
	var idx uint64
	reuse := n.findUncommitted(cmd)
	if reuse > 0 {
		idx = reuse
		n.protoLog("propose-reuse-uncommitted", "index="+itoa(idx))
	} else {
		idx = n.log.LastIndex() + 1
		n.log.Append(Entry{Index: idx, Term: n.currentTerm, Cmd: cmd})
		n.protoLog("propose-append", "index="+itoa(idx))
	}

	// 在锁内串行完成复制裁决：网络状态不会在本次调用中途变化，
	// 一轮"每节点推进到匹配/不可达"即可得到确定的多数派结论。
	for round := 0; round < proposeRounds; round++ {
		n.syncRound()
		if n.state != Leader {
			return ApplyResult{}, newReject(ReasonNotLeader, "leadership lost during propose")
		}
		prevCommit := n.commitIndex
		n.advanceCommit()
		if n.lastApplied >= idx {
			if cmd.ClientID != "" {
				if r, ok := n.sm.results[cmd.ClientID][cmd.SeqNum]; ok {
					return r, nil
				}
			}
			// 条目已被 applyCommitted 应用，直接取回首次结果，绝不二次执行。
			if r, ok := n.sm.ResultAt(idx); ok {
				return r, nil
			}
			return ApplyResult{}, newReject(ReasonInvalidIndex, "committed entry result missing")
		}
		if n.commitIndex == prevCommit {
			break // 没有新进展，再发也无法改变多数派结论
		}
	}
	return ApplyResult{}, newReject(ReasonNoQuorum,
		"entry index="+itoa(idx)+" commitIndex="+itoa(n.commitIndex))
}

func (n *Node) findUncommitted(cmd Command) uint64 {
	if cmd.ClientID == "" {
		return 0
	}
	for i := n.commitIndex; i < n.log.LastIndex(); i++ {
		e := n.log.entries[i]
		if e.Cmd.ClientID == cmd.ClientID && e.Cmd.SeqNum == cmd.SeqNum {
			return e.Index
		}
	}
	return 0
}

// Read 提供线性一致读：先以一轮心跳获得多数派存活确认，再读已应用状态。
func (n *Node) Read(key string) (string, bool, error) {
	n.cluster.mu.Lock()
	defer n.cluster.mu.Unlock()
	if n.down {
		return "", false, newReject(ReasonNodeDown, n.id+" is down")
	}
	if n.state != Leader {
		return "", false, newReject(ReasonNotLeader, n.id+" is "+n.state.String())
	}
	term := n.currentTerm
	acked := n.syncRound() + 1 // 含自己
	if n.currentTerm != term || n.state != Leader {
		return "", false, newReject(ReasonNotLeader, "leadership lost during read")
	}
	if acked < n.cluster.majority() {
		return "", false, newReject(ReasonNoQuorum, "read without majority ack")
	}
	v, ok := n.sm.Get(key)
	n.protoLog("read-linearizable", "key="+key+" found="+boolStr(ok))
	return v, ok, nil
}

// ---- 协议日志 ----

func (n *Node) protoLog(action, detail string) {
	if n.logf == nil {
		return
	}
	msg := "[node " + n.id + "] " + action +
		" term=" + itoa(n.currentTerm) +
		" state=" + n.state.String() +
		" lastIndex=" + itoa(n.log.LastIndex()) +
		" commitIndex=" + itoa(n.commitIndex) +
		" lastApplied=" + itoa(n.lastApplied)
	if detail != "" {
		msg += " " + detail
	}
	n.logf.Printf("%s", msg)
}

func itoa(x uint64) string {
	if x == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for x > 0 {
		i--
		b[i] = byte('0' + x%10)
		x /= 10
	}
	return string(b[i:])
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
