# go-consensus-replication-go

一个**本地可运行**的一致性协调组件：用多个内存节点模拟一个小集群，实现了
Raft 风格的**多数派选主、日志复制、提交与故障恢复**。所有结论都是确定、可复现、
可解释的——每一次协议事件都打印**任期（term）、日志索引（lastIndex）、
提交位置（commitIndex）、应用位置（lastApplied）**与结构化拒绝原因。

- 纯内存、零外部依赖（仅 Go 标准库），无需网络即可运行。
- 支持节点崩溃/重启、丢内存后的空节点追赶、网络分区与恢复。
- 所有拒绝都返回带 `RejectReason` 的错误，可用 `consensus.ReasonOf(err)` 精确区分。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `consensus/types.go` | 角色状态、命令、日志条目、RequestVote/AppendEntries 报文、Transport 抽象 |
| `consensus/errors.go` | 结构化拒绝原因 `RejectReason` / `RejectError` / `ReasonOf` |
| `consensus/log.go` | 单节点内存日志：追加、按区间读取、冲突裁剪、任期/索引查询 |
| `consensus/statemachine.go` | 顺序 KV 状态机：按 `(ClientID, SeqNum)` 幂等应用，按索引取回结果 |
| `consensus/node.go` | Raft 节点：选举计时、投票、复制、冲突回退、提交推进、线性一致读 |
| `consensus/cluster.go` | 内存集群/Transport：逻辑时钟、停机、分区、可达性判定 |
| `consensus/*_test.go` | 选举/提交/幂等/分区/切换/追赶/分叉裁剪/并发 测试 |
| `cmd/demo/main.go` | 端到端可运行演示 |

## 一致性协议

实现的是 Raft 的核心安全性质：

1. **选举安全（Election Safety）**：同一任期内多数派只能投出一个领导者，
   因此同一时刻最多只有一个有效领导者。候选者必须获得多数派投票才晋升。
2. **领导者只追加（Leader Append-Only）**：领导者从不改写或删除自己的日志，
   只追加条目；跟随者日志以领导者日志为准做一致性检查与裁剪。
3. **日志匹配（Log Matching）**：每个 AppendEntries 都带 `PrevLogIndex/PrevLogTerm`。
   - 前向跳跃（prevLog 超出本地）→ 拒绝 `log_gap`；
   - prevLog 任期不匹配 → 拒绝 `log_conflict`，并返回冲突任期/索引提示，
     领导者据此快速回退 `nextIndex`（不逐条试探）；
   - 已有相同位置相同任期 → 幂等跳过；
   - 相同位置不同任期的未提交尾部 → 先裁剪再覆盖；
   - 若追加请求会改写**已提交**位置 → 拒绝 `replayed_append`，绝不静默接受。
4. **领导者完整（Leader Completeness）**：候选者日志必须“至少和多数派一样新”
   （比较最后日志任期，再比较索引），否则拒绝投票 `log_not_up_to_date`。
   已提交的条目因此永远存在于未来领导者的日志中，**不会丢失**。
5. **状态机安全（State Machine Safety）**：所有节点按相同索引顺序应用相同命令。

### 任期与过期信息

- 任何 RPC/回复若携带比本节点更小的任期，一律拒绝 `stale_term`：
  过期领导者的 AppendEntries、旧候选者的 RequestVote 都会被挡下。
- 节点见到更高任期立即退回跟随者（`stepDown`），清掉本任期投票。
- 同任期内一个正在竞选的候选者收到合法领导者心跳，会立即让位给领导者。

## 提交规则

- 一条日志只有被**多数派**节点复制（`matchIndex >= idx`）后，领导者才推进
  `commitIndex`；只复制到少数派的条目永远不提交、不对客户端可见、不能被读到。
- 遵循 Raft 的“**只直接提交当前任期条目**”规则：通过提交当前任期 no-op
  （新领导者上任时写入），借由日志连续性把之前任期的条目一起安全提交。
- 跟随者按 `min(leaderCommit, 本地最后索引)` 推进提交位置，
  再以 `lastApplied+1 … commitIndex` 的顺序应用，**单调、无空洞**。

## 幂等规则

- 客户端命令携带 `ClientID + SeqNum`。状态机为每个客户端记录已应用的最大序号，
  并保存每个序号**首次应用的结果**。
- 同一 `(ClientID, SeqNum)` 重复投递：
  1. 若结果已在领导者状态机中：直接返回**首次结果**，不写新日志、不重复执行副作用；
  2. 若条目已追加但尚未提交（提案超时后重试）：复用未提交的同一条目，不产生重复条目；
  3. 跟随者重复收到相同位置相同任期的 AppendEntries：幂等跳过，不重复应用。
- 旧请求的重投只会拿到它自己的首次结果，**不会把新请求的结果误当作旧请求结果返回**。
- 状态机还按日志索引保存应用结果，Propose 在条目提交后按索引取回结果，
  从实现上杜绝“再 Apply 一次”。

## 分区与恢复语义

- `Cluster.Partition(ids...)` / `Isolate(id)` 把节点放入与外部隔离的分区；
  只有同分区（或都在主组）的节点互相可达，跨分区 RPC 返回 `unreachable`。
- 被隔离到**少数派**的领导者：
  - 无法获得多数派确认 → 新写返回 `no_quorum`；
  - 线性一致读需要多数派存活确认 → 也返回 `no_quorum`（不提供可能过期的读）；
  - 期间本地追加但未提交的“分叉”日志不会被应用。
- 多数派分区会在选举超时后选出**更高任期**的新领导者继续服务。
- `Heal()` 恢复网络后：
  - 旧领导者收到更高任期的 RPC 立即退回跟随者（之后客户端写得到 `not_leader`）；
  - 其分叉的未提交尾部在 AppendEntries 冲突检查处被**裁剪**，并按领导者日志补齐；
  - 所有节点的日志、`commitIndex`、`lastApplied`、状态机最终一致。

### 节点故障

- `node.Stop()`：模拟崩溃，停止响应 RPC 与时钟，**保留**内存日志/硬状态。
- `node.Restart()`：以跟随者身份回归，任期/投票/日志保留，由领导者心跳追赶。
- `node.WipeAndRestart()`：模拟内存数据全丢后的空节点，任期清零、日志清空，
  从索引 1 开始由领导者完整复制，最终与领导者一致。

## 可区分的失败原因

通过 `consensus.ReasonOf(err)` 获取（均为 `RejectReason`）：

| 原因 | 含义 |
| --- | --- |
| `stale_term` | 请求来自更小任期（过期领导者/旧候选者） |
| `not_leader` | 该节点不是领导者，不能处理写/读 |
| `no_quorum` | 提案或读确认未获得多数派 |
| `already_voted` | 本任期已投票给其他候选者 |
| `log_not_up_to_date` | 候选者日志不够新，不具备当选资格 |
| `log_gap` | AppendEntries 前向跳跃，prevLog 之前有空洞 |
| `log_conflict` | prevLog 或追加位置任期冲突，需要回退/裁剪 |
| `replayed_append` | 追加请求试图改写已提交日志（乱序/回退重放） |
| `unreachable` | 对端分区/停机/不可达（传输层失败，非协议拒绝） |
| `node_down` | 本节点已停止 |
| `invalid_index` | 非法日志参数 |

## 本地验证

```bash
# 单元测试（确定性逻辑时钟，可重复）
go test ./...

# 带协议日志的详细测试：观察 term / lastIndex / commitIndex / lastApplied
go test -v ./...

# 竞态检测（覆盖并发写入入口）
go test -race ./...

# 端到端演示：选主 -> 多数派提交 -> 幂等重投 -> 分区少数派被拒
#           -> 多数派换主 -> 恢复后旧主下台与日志追赶
go run ./cmd/demo
```

### 最小用法

```go
c := consensus.NewCluster(3, logger)
c.Start()                 // 或用 c.TickAll() 手动驱动逻辑时钟
defer c.StopLoop()

leader := c.Leader()      // 等待选举完成
res, err := leader.Propose(consensus.Command{
    Op: "set", Key: "color", Value: "blue",
    ClientID: "writer-1", SeqNum: 1,
})
v, ok, err := leader.Read("color")

c.Isolate(leader.ID())    // 制造网络分区
c.Heal()                  // 恢复
```

### 并发模型说明

整个内存集群共用一把互斥锁：节点的公开入口（`Tick/Propose/Read/生命周期`）
各自加锁，RPC 在持锁状态下串行调用对端方法。这样并发客户端写入会被**串行化裁决**，
既保留“多客户端并发入口”的语义，又使每个测试结论确定、可复现，且无数据竞争
（`go test -race` 干净）。真实分布式实现中这里是跨网络的异步 RPC，但观察到的
安全性质（多数派、任期、提交顺序、幂等）完全一致。
