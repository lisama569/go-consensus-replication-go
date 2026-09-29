package raft

import (
	"context"
	"sync"
	"time"
)

// Cluster 把多个内存 Node 组成一个"小集群"，并集中控制故障注入：
//   - Partition / Heal：双向网络分区；
//   - Crash / Restart：节点进程崩溃与重启（可选择保留或丢弃持久状态）。
type Cluster struct {
	mu sync.Mutex

	ids       []string
	nodes     map[string]*Node
	saved     map[string]persisted // 崩溃节点的持久状态（保留重启用）
	alive     map[string]bool
	reachable map[string]map[string]bool
	trans     *memTransport
	logg      Logger
	timing    Config
}

type ClusterConfig struct {
	IDs    []string
	Logger Logger

	// 可选：统一注入节点计时参数与随机种子，方便测试。
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

func NewCluster(cfg ClusterConfig) *Cluster {
	if cfg.Logger == nil {
		cfg.Logger = nopLogger{}
	}
	c := &Cluster{
		ids:   append([]string{}, cfg.IDs...),
		nodes: map[string]*Node{},
		saved: map[string]persisted{},
		logg:  cfg.Logger,
		timing: Config{
			ElectionTimeoutMin: cfg.ElectionTimeoutMin,
			ElectionTimeoutMax: cfg.ElectionTimeoutMax,
			HeartbeatInterval:  cfg.HeartbeatInterval,
		},
	}
	c.trans = &memTransport{c: c}

	alive := map[string]bool{}
	reachable := map[string]map[string]bool{}
	for _, id := range cfg.IDs {
		alive[id] = true
		reachable[id] = map[string]bool{}
		for _, other := range cfg.IDs {
			reachable[id][other] = true
		}
	}
	c.alive = alive
	c.reachable = reachable
	return c
}

func (c *Cluster) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, id := range c.ids {
		n := NewNode(Config{
			ID:                 id,
			Peers:              c.otherIDs(id),
			Transport:          c.trans,
			Logger:             c.logg,
			ElectionTimeoutMin: c.timing.ElectionTimeoutMin,
			ElectionTimeoutMax: c.timing.ElectionTimeoutMax,
			HeartbeatInterval:  c.timing.HeartbeatInterval,
			RandSeed:           int64(i + 1),
		})
		n.Start()
		c.nodes[id] = n
	}
}

func (c *Cluster) Stop() {
	c.mu.Lock()
	ns := make([]*Node, 0, len(c.nodes))
	for _, n := range c.nodes {
		ns = append(ns, n)
	}
	c.mu.Unlock()
	for _, n := range ns {
		n.Stop()
	}
}

func (c *Cluster) IDs() []string { return append([]string{}, c.ids...) }

func (c *Cluster) Node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

func (c *Cluster) otherIDs(id string) []string {
	out := make([]string, 0, len(c.ids)-1)
	for _, x := range c.ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

// Leader 返回当前 role=leader 的节点；同一时刻有 0 或 1 个。
// 出现多于一个（理论上不可能）时返回错误，便于测试强断言。
func (c *Cluster) Leader(ctx context.Context) (*Node, error) {
	c.mu.Lock()
	var leaders []*Node
	for _, id := range c.ids {
		n := c.nodes[id]
		if n != nil && n.Status().Role == Leader {
			leaders = append(leaders, n)
		}
	}
	c.mu.Unlock()
	if len(leaders) == 1 {
		return leaders[0], nil
	}
	if len(leaders) > 1 {
		return nil, &RaftError{Reason: RejectStaleLeader,
			Message: "检测到多个 leader，违反同一时刻唯一领导者约束"}
	}
	return nil, &RaftError{Reason: RejectNoLeader, Message: "当前无领导者"}
}

// WaitLeader 轮询等待最多 d，直到出现唯一 leader。
func (c *Cluster) WaitLeader(d time.Duration) (*Node, error) {
	deadline := timeNow().Add(d)
	for timeNow().Before(deadline) {
		if n, err := c.Leader(context.Background()); err == nil {
			return n, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return nil, &RaftError{Reason: RejectTimeout, Message: "等待领导者超时"}
}

// ---- 故障注入 ----

// Partition 把节点切成两组，组间双向不可达，组内保持互通。
func (c *Cluster) Partition(a, b []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	set := map[string]bool{}
	for _, id := range append(append([]string{}, a...), b...) {
		set[id] = true
	}
	// 先恢复组内互通。
	for _, x := range a {
		for _, y := range a {
			c.setReachable(x, y, true)
		}
	}
	for _, x := range b {
		for _, y := range b {
			c.setReachable(x, y, true)
		}
	}
	// 组间双向断开。
	for _, x := range a {
		for _, y := range b {
			c.setReachable(x, y, false)
			c.setReachable(y, x, false)
		}
	}
	c.logg.Printf("cluster 注入网络分区: A=%v B=%v", a, b)
}

// Isolate 让单个节点与集群其余节点双向隔离。
func (c *Cluster) Isolate(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, other := range c.ids {
		if other == id {
			continue
		}
		c.setReachable(id, other, false)
		c.setReachable(other, id, false)
	}
	c.logg.Printf("cluster 隔离节点 %s", id)
}

// Heal 清除全部分区规则。
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, x := range c.ids {
		for _, y := range c.ids {
			c.setReachable(x, y, true)
		}
	}
	c.logg.Printf("cluster 分区恢复")
}

func (c *Cluster) setReachable(from, to string, v bool) {
	if c.reachable[from] == nil {
		c.reachable[from] = map[string]bool{}
	}
	c.reachable[from][to] = v
}

// Crash 让节点停止响应：易失状态全部丢失；持久状态被保存下来，
// Restart 时按"持久状态保留、易失状态清空"的语义恢复。
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[id]
	if n == nil {
		return
	}
	n.mu.Lock()
	n.p.snapshotData = n.sm.clone()
	c.saved[id] = n.p
	n.mu.Unlock()
	n.Stop()
	delete(c.nodes, id)
	c.alive[id] = false
	c.logg.Printf("cluster 节点 %s 崩溃（持久状态已保存）", id)
}

// Restart 按"持久状态保留、易失状态清空"的语义重启节点。
func (c *Cluster) Restart(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.restartLocked(id, c.saved[id])
}

// RestartLostData 模拟磁盘损坏：连持久日志/去重表一起丢弃，空日志加入，
// 之后必须靠领导者复制/快照追赶。
func (c *Cluster) RestartLostData(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.restartLocked(id, freshPersisted())
}

func (c *Cluster) restartLocked(id string, p persisted) *Node {
	if _, ok := c.alive[id]; !ok {
		return nil
	}
	// 崩溃期间可能发生过分区恢复操作，重置该节点对外可达性为全网可达，
	// 再由现存分区规则重新裁剪（保持简单：重启即按当前 Heal/Partition 视图）。
	for _, other := range c.ids {
		c.setReachable(id, other, true)
		c.setReachable(other, id, true)
	}
	n := reviveFrom(Config{
		ID:                 id,
		Peers:              c.otherIDs(id),
		Transport:          c.trans,
		Logger:             c.logg,
		ElectionTimeoutMin: c.timing.ElectionTimeoutMin,
		ElectionTimeoutMax: c.timing.ElectionTimeoutMax,
		HeartbeatInterval:  c.timing.HeartbeatInterval,
		RandSeed:           timeNow().UnixNano(),
	}, p)
	n.Start()
	c.nodes[id] = n
	c.alive[id] = true
	return n
}

// Alive 报告节点进程是否存活（未崩溃）。
func (c *Cluster) Alive(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alive[id]
}

// canDeliver 判断 from -> to 的消息当前是否可达。
func (c *Cluster) canDeliver(from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if from == to {
		return true
	}
	if !c.alive[from] || !c.alive[to] {
		return false
	}
	if m, ok := c.reachable[from]; ok {
		if v, exists := m[to]; exists && !v {
			return false
		}
	}
	return true
}
