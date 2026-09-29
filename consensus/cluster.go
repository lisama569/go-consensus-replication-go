package consensus

import (
	"context"
	"sync"
	"time"
)

// Cluster 用内存节点模拟一个小集群，同时充当 Transport：
// 支持停机、网络分区与恢复，并可后台驱动所有节点的逻辑时钟。
//
// 并发模型：整个集群共用一把互斥锁 mu。节点的公开入口（Tick/Propose/
// Read/生命周期）各自加锁，RPC 在持锁状态下直接调用对端方法，
// 因此并发客户端写入会被串行化裁决，结论确定且可复现，没有数据竞争。
type Cluster struct {
	mu    sync.Mutex
	nodes map[string]*Node
	order []string

	// group[id] 为分区编号；0 表示主连通组。
	// 只有同组（或都在 0 组）的节点互相可达。
	group     map[string]int
	nextGroup int

	tickEvery time.Duration
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	running   bool

	logf Logger
}

// NewCluster 创建 size 个节点（ID 为 n1,n2,...）。
func NewCluster(size int, logger Logger) *Cluster {
	ids := make([]string, 0, size)
	for i := 1; i <= size; i++ {
		ids = append(ids, "n"+itoa(uint64(i)))
	}
	return NewClusterWithIDs(ids, logger)
}

// NewClusterWithIDs 使用指定 ID 创建集群。
func NewClusterWithIDs(ids []string, logger Logger) *Cluster {
	c := &Cluster{
		nodes:     map[string]*Node{},
		order:     append([]string(nil), ids...),
		group:     map[string]int{},
		tickEvery: 5 * time.Millisecond,
		logf:      logger,
	}
	for _, id := range ids {
		c.nodes[id] = newNode(c, id, logger)
	}
	return c
}

func (c *Cluster) Node(id string) *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[id]
}

// Leader 返回当前认为自己是领导者、且任期不低于任何其他节点的节点。
func (c *Cluster) Leader() *Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	var best *Node
	for _, id := range c.order {
		n := c.nodes[id]
		if n.down || n.state != Leader {
			continue
		}
		if best == nil || n.currentTerm > best.currentTerm {
			best = n
		}
	}
	return best
}

func (c *Cluster) IDs() []string { return append([]string(nil), c.order...) }

func (c *Cluster) majority() int { return len(c.order)/2 + 1 }

// Start 启动后台 tick 循环；测试也可以只用 TickAll 手动驱动。
func (c *Cluster) Start() {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.running = true
	c.mu.Unlock()

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		t := time.NewTicker(c.tickEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.TickAll()
			}
		}
	}()
}

// StopLoop 停止后台 tick 循环。
func (c *Cluster) StopLoop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	cancel := c.cancel
	c.running = false
	c.mu.Unlock()
	cancel()
	c.wg.Wait()
}

// TickAll 让所有节点按固定顺序各推进一个逻辑时钟。
func (c *Cluster) TickAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.order {
		c.nodes[id].tickLocked()
	}
}

// Partition 将 ids 放入一个彼此连通、与外部隔离的新分区。
func (c *Cluster) Partition(ids ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextGroup++
	g := c.nextGroup
	for _, id := range ids {
		if _, ok := c.nodes[id]; ok {
			c.group[id] = g
		}
	}
}

// Isolate 将单个节点隔离成只含它自己的分区。
func (c *Cluster) Isolate(id string) { c.Partition(id) }

// Heal 恢复全网连通。
func (c *Cluster) Heal() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.group = map[string]int{}
}

// Reachable 判断两个节点当前是否网络可达。
func (c *Cluster) Reachable(from, to string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reachableLocked(from, to)
}

func (c *Cluster) reachableLocked(from, to string) bool {
	if from == to {
		return true
	}
	a, oka := c.nodes[from]
	b, okb := c.nodes[to]
	if !oka || !okb || a.down || b.down {
		return false
	}
	ga, gb := c.group[from], c.group[to]
	if ga == 0 || gb == 0 {
		return ga == gb // 任一在主组时，只有都在主组才连通
	}
	return ga == gb
}

// ---- Transport：调用方必须持有 c.mu ----

func (c *Cluster) RequestVote(from, to string, args *VoteArgs) (*VoteReply, error) {
	if !c.reachableLocked(from, to) {
		return nil, newReject(ReasonUnreachable, from+" -> "+to)
	}
	return c.nodes[to].onRequestVote(args), nil
}

func (c *Cluster) AppendEntries(from, to string, args *AppendArgs) (*AppendReply, error) {
	if !c.reachableLocked(from, to) {
		return nil, newReject(ReasonUnreachable, from+" -> "+to)
	}
	return c.nodes[to].onAppendEntries(args), nil
}
