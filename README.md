# 内存一致性协调组件（Raft 教学/可解释实现）

单进程内用多个内存节点模拟一个小集群，完整实现 Raft 的**选主、日志复制、
提交与故障恢复**，并对每一类失败给出**可区分的拒绝原因**。无外部依赖、
无需起 socket，适合本地直接运行、打断点、读日志来理解一致性协议。

- 语言：Go（module: `github.com/lisama569/go-consensus-replication-go`）
- 代码：`raft/`（库）、`cmd/demo/`（可运行示例）、`raft/*_test.go`（测试）

---

## 1. 一致性协议概览

实现遵循 Raft 论文（Ongaro & Ousterhout, 2014）的核心规则。

### 1.1 角色与任期

节点在 `follower / candidate / leader` 三种角色间切换，用一个单调递增的
**任期（term）** 划分逻辑时间：

- follower 选举超时未收到合法领导者心跳 → 任期 +1，成为 candidate 并拉票；
- candidate 拿到多数派选票 → 成为该任期唯一 leader；
- 任意节点从 RPC 的响应/请求中看到**更大任期** → 立即退回 follower
  （`stepDown`），更新任期、复位本任期投票、终止本任期全部复制协程。

同一任期最多一个 leader：一个节点一个任期只投一张票，且选票要求候选者
日志"至少和自己一样新"（先比最后一条日志的任期，再比索引）。

### 1.2 投票的日志新鲜度检查

`RequestVote` 处理（`raft/election.go`）在以下情况拒绝：

1. `req.Term < currentTerm` → `stale-term`；
2. 本任期票已投给别人 → 拒绝；
3. 候选者日志比本地旧（`(lastLogTerm, lastLogIndex)` 字典序更小）
   → `log-conflict`；
4. 其余情况投票并重置选举计时器。

### 1.3 日志复制与多数派提交

- leader 把客户端写追加到本地日志，通过 `AppendEntries` 并行复制；
- 每个 follower 一个后台复制协程，**心跳节拍**与**新数据唤醒**两条触发
  路径独立，避免"响应慢时心跳信号被合并丢弃"；
- follower 收到追加先做一致性检查（见 §4），通过后写入并按
  `leaderCommit` 推进自己的 `commitIndex`；
- leader 在"某个索引 N 被多数派复制 **且 `log[N].term == currentTerm`**"
  时才把 `commitIndex` 推进到 N。要求提交条目来自当前任期，是论文 §5.4
  防止间接提交旧任期未确认条目的关键约束；
- 新 leader 当选后立即追加一条 **no-op 占位条目**，等它被多数派提交后，
  本节点才安全地对外提供读——这保证了新 leader 已掌握所有前任任期的
  已提交日志。

### 1.4 线性一致读

读只在 leader 上进行，且读的是**已 apply 到状态机**的数据。未达到多数派
的写入只停留在未提交日志里，永远不会进入状态机，因此**不可能被读到**。
配合新 leader 的 no-op 提交，读到的数据不会是过期视图。

---

## 2. 提交与幂等规则

### 2.1 提交顺序：单调、无空洞

- `commitIndex` 只能单调前进；任何让它倒退或越过本地日志的请求都会被
  拒绝（`commit-gap`），不会静默接受；
- 状态机严格按 `lastApplied+1, lastApplied+2, …` 的顺序 apply，
  `lastApplied` 永远 ≤ `commitIndex`，中间不允许跳号。

### 2.2 客户端写幂等

每个客户端请求带 `(ClientID, Seq)`。组件按 **(ClientID, Seq) 逐个**保存
已应用请求的结果（而不是只记一个"最新 seq 水位"——后者会让重投一个较早
的历史 seq 被误判为新请求）：

- 该请求**已应用**：直接复用首次的结果与日志索引返回，标记
  `duplicate-request`，**不写新日志、不二次 apply**，即使重投时携带的
  key/value 已被改成别的内容；
- 该请求**已追加但未提交**：并发的重复投递合并到同一个等待者，只进行
  一次复制，所有调用方拿到同一个结果；
- 否则作为新请求追加日志。

因此：重复投递不会重复应用、不会污染状态机、也不会把旧结果当作新请求
的结果返回。去重表属于持久状态，节点重启后仍然幂等。

---

## 3. 失败原因一览（可区分、可机读）

所有失败都通过 `raft.RejectReason` 返回，可用 `errors.Is(err, raft.RejectXxx)`
判定：

| 原因 | 触发场景 |
| --- | --- |
| `stale-term` | 请求来自更小任期（过期 leader / 过期拉票） |
| `not-leader` | 向 follower 写/读，或写在途时 leader 被更高任期顶掉 |
| `no-known-leader` | 集群启动/改选期间尚无已知领导者 |
| `log-conflict` | AppendEntries 的 prevLogIndex/prevLogTerm 与本地不一致，附带快速回退提示 |
| `committed-rollback` | 追加请求试图覆盖/删除一条**已提交**日志 |
| `commit-gap` | leaderCommit 倒退或会造成提交空洞 |
| `duplicate-request` | 同一 (ClientID, Seq) 重复投递（响应仍为 OK，复用旧结果） |
| `node-unreachable` | 目标节点已崩溃或位于网络分区另一侧 |
| `quorum-timeout` | 写在超时内未凑齐多数派确认（典型：少数派 leader） |

---

## 4. 日志冲突、乱序与回退处理

follower 处理 `AppendEntries` 时（`raft/replication.go` 的 `HandleAppend`、
`raft/log.go`）：

1. **任期更旧** → `stale-term`，回传当前任期，让过期 leader 下台；
2. **prevLog 缺失 / 任期不符** → `log-conflict`，并回传冲突快速回退提示
   （`conflictTerm` 的第一条索引，或 `lastIndex+1`），leader 据此一次性
   回退 `nextIndex`，而不是一条条试探；
3. **覆盖区间命中已提交日志** → `committed-rollback`，**绝不静默截断**；
4. 同索引同任期的重叠部分按 Raft 日志不变量直接幂等跳过，只有任期分叉
   才截断未提交尾部再重写；
5. `leaderCommit` 只能把本地提交点向前推进到 `min(leaderCommit, lastIndex)`。

落后过多 / 丢盘重启的节点通过持续的冲突回退 + 重传追上；也支持
`InstallSnapshot`（`raft/snapshot.go`）按快照基点对齐，重复安装幂等，
旧任期快照返回 `stale-term`。

---

## 5. 分区与恢复语义

`raft.Cluster` 在内存中集中做故障注入：

- `Partition(a, b)` / `Isolate(id)` / `Heal()`：双向网络分区与恢复；
- `Crash(id)`：节点停服，**易失状态全丢**（commitIndex/角色/复制进度），
  持久状态（currentTerm、votedFor、日志、去重表）被保存；
- `Restart(id)`：按"持久状态保留、易失状态清空"重启，自动从 leader 追赶；
- `RestartLostData(id)`：模拟磁盘损坏，连持久日志一起丢弃，空日志归队，
  靠冲突回退/快照完整追上。

语义保证：

- 分区期间，只有包含多数派的一侧能提交；少数派的旧 leader 无法凑齐多数派，
  其写入返回 `quorum-timeout`（或在收到更大任期后返回 `not-leader`）；
- 多数派会选出更大任期的新 leader；恢复分区后旧 leader 见到更大任期立即
  下台，以 follower 身份追上；
- 少数派上**未提交的"幽灵日志"会被新 leader 的日志覆盖**，永远不可见，
  已提交数据一条不丢；
- 节点重启 / 落后追赶后，状态机、日志提交位置与领导者一致。

---

## 6. 目录结构

```
raft/
  errors.go       可区分的拒绝原因 RejectReason / RaftError
  log.go          持久日志：一致性检查、冲突定位、截断保护、快照基点
  rpc.go          RequestVote / AppendEntries / InstallSnapshot / 客户端消息
  state.go        KV 状态机（顺序 apply 已提交条目）
  node.go         节点：持久/易失状态、生命周期、节拍循环、状态快照
  election.go     选主、拉票、投票规则、stepDown
  replication.go  心跳/复制协程、追加处理、多数派提交、顺序 apply
  snapshot.go     InstallSnapshot 处理
  client.go       客户端写（幂等去重）与线性一致读
  failover.go     失去领导权时快速失败在途写
  pending.go      在途写等待者与复制器唤醒
  transport.go    进程内直连传输（注入不可达）
  cluster.go      集群装配、分区、崩溃/重启、领导者观察
  *_test.go       15 个测试用例
cmd/demo/main.go  端到端可运行示例
```

---

## 7. 本地验证方法

要求 Go 1.25+。

```bash
# 1. 运行全部单元测试（-v 可看到带 term/index/commit 的协议日志）
go test ./... -v

# 2. 开启竞态检测并多轮重复（消除时序类 flaky）
go test -race -count=10 ./...

# 3. 只跑某类场景
go test ./... -run 'TestPartitionLeaderSwitch|TestLeaderCrashElection' -v
go test ./... -run 'TestDuplicate|TestConcurrent|TestOutOfOrder' -v
go test ./... -run 'TestCrashRestart|TestRestartLostData' -v
go test ./... -run 'TestAppendConflict|TestSnapshotInstall' -v

# 4. 端到端演示（选主 → 读写 → 幂等 → 分区改选 → 恢复 → 崩溃追赶）
go run ./cmd/demo
```

### 测试覆盖的场景

- **多数派 / 少数派**：正常 5 节点连续写全部提交；隔离多数派后少数派写
  失败、恢复后该写不可见；
- **领导者切换**：分区多数派改选、leader 直接崩溃后重选，新任期严格更大、
  同一时刻最多一个 leader；
- **重复请求**：同 (ClientID, Seq) 多次/并发投递只应用一次；重投较早的
  历史 seq 也能识别（专门的回归用例）；结果不被新参数污染；
- **并发写入**：重复请求合并为同一索引，不同请求各占连续索引、全部成功；
- **故障恢复**：崩溃保留持久状态重启后追上、丢盘空节点靠复制追上，
  已提交数据不丢；
- **异常追加**：乱序、任期不符、回退已提交日志分别被明确拒绝，且被拒绝
  的请求不改变节点任何状态；
- **旧任期拒绝**：过期拉票 / 过期 AppendEntries 一律 `stale-term`。

测试日志通过 `raft.TestLogger` 打印，每行都带节点 id、角色、**term、
日志 index、commit 位置**，例如：

```
node=n3 follower term=2 追加成功 prev=2 +1 matched=3 commit=2 lastIndex=3 leader=n4
node=n4 leader term=2 commit=3 lastIndex=3 多数派确认，推进提交位置
node=n4 term=2 index=3 commit=3 applied op=put key=b client=app seq=2 result="ok:2"
node=n1 拒绝 Append: term=1 小于本地 term=2 (stale-term) leader=n1
```
