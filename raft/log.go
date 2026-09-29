package raft

// Entry 是一条日志条目。
//
// ClientID + Seq 共同标识一次客户端请求，用于去重：同一对 (ClientID, Seq)
// 在整个日志中最多出现一次且只应用一次。Result 在该条目被应用到状态机后
// 写入，重复请求会复用同一结果，而不会被当成新请求重新执行。
type Entry struct {
	Term     uint64
	Index    uint64
	ClientID string
	Seq      uint64
	Op       string
	Key      string
	Value    string
	Result   string
	applied  bool
}

// isNoOp 表示该条目是新领导者上台时追加的占位条目（不携带客户端写）。
func (e *Entry) isNoOp() bool { return e.Op == OpNoOp }

const (
	OpPut    = "put"
	OpDelete = "delete"
	OpNoOp   = "noop"
)

// PersistentLog 是节点的持久化日志（内存模拟）。
//
// entries[0] 始终是"哨兵"：未压缩时是 {Index:0,Term:0}，安装快照后变成
// {Index:baseIndex,Term:baseTerm}，表示 index<=baseIndex 的日志已经被压缩，
// 但其任期信息仍然可查（AppendEntries 的一致性检查需要它）。
type PersistentLog struct {
	baseIndex uint64
	baseTerm  uint64
	entries   []Entry // entries[0] 为哨兵，真实条目从 entries[1] 开始
}

func newPersistentLog() *PersistentLog {
	return &PersistentLog{entries: []Entry{{}}}
}

// firstIndex 返回哨兵位置（压缩前为 0，压缩后为快照最后包含的索引）。
func (l *PersistentLog) firstIndex() uint64 { return l.baseIndex }

func (l *PersistentLog) lastIndex() uint64 {
	return l.baseIndex + uint64(len(l.entries)-1)
}

// termAt 查询 index 处的任期。index 等于 baseIndex 时返回快照基点任期；
// 小于基点或大于末尾都视为不存在。
func (l *PersistentLog) termAt(index uint64) (uint64, bool) {
	if index < l.baseIndex || index > l.lastIndex() {
		return 0, false
	}
	return l.entries[index-l.baseIndex].Term, true
}

// hasAt 精确判断日志在 index 处的任期是否等于 term（论文的一致性检查）。
func (l *PersistentLog) hasAt(index, term uint64) bool {
	t, ok := l.termAt(index)
	return ok && t == term
}

func (l *PersistentLog) entryAt(index uint64) (Entry, bool) {
	if index <= l.baseIndex || index > l.lastIndex() {
		return Entry{}, false
	}
	return l.entries[index-l.baseIndex], true
}

// sliceFrom 返回 index 开始（含）的条目拷贝，返回的条目携带正确的 Index。
func (l *PersistentLog) sliceFrom(index uint64) []Entry {
	if index <= l.baseIndex {
		index = l.baseIndex + 1
	}
	if index > l.lastIndex() {
		return nil
	}
	out := make([]Entry, 0, l.lastIndex()-index+1)
	for i := index; i <= l.lastIndex(); i++ {
		out = append(out, l.entries[i-l.baseIndex])
	}
	return out
}

// appendNew 追加一条由当前领导者产生的条目，返回分配到索引后的完整条目。
func (l *PersistentLog) appendNew(term uint64, e Entry) Entry {
	e.Term = term
	e.Index = l.lastIndex() + 1
	l.entries = append(l.entries, e)
	return e
}

// conflictStatus 描述 AppendEntries 前置一致性检查的结果。
type conflictStatus int

const (
	conflictOK        conflictStatus = iota // prevLogIndex/prevLogTerm 匹配
	conflictMissing                         // 本地日志还没有 prevLogIndex
	conflictTerm                            // 同索引任期不同
	conflictCompacted                       // prevLogIndex 已被快照压缩，需要先发快照
)

// checkPrev 实现论文 §5.3 的接收者检查第 1、2 步，并附带快速回退提示：
// 返回的 hintIndex/hintTerm 供 XTran 的冲突优化使用。
func (l *PersistentLog) checkPrev(prevLogIndex, prevLogTerm uint64) (status conflictStatus, hintIndex, hintTerm uint64) {
	switch {
	case prevLogIndex < l.baseIndex:
		// 领导者以为的位置比本地快照基点还旧，直接用快照覆盖即可。
		return conflictCompacted, l.baseIndex, l.baseTerm
	case prevLogIndex == l.baseIndex:
		if l.baseTerm != prevLogTerm {
			// 基点任期不同：极端情况下只能整段快照对齐。
			return conflictCompacted, l.baseIndex, l.baseTerm
		}
		return conflictOK, 0, 0
	case prevLogIndex > l.lastIndex():
		// 论文优化：hintIndex = lastIndex+1, hintTerm = 0。
		return conflictMissing, l.lastIndex() + 1, 0
	default:
		t := l.entries[prevLogIndex-l.baseIndex].Term
		if t != prevLogTerm {
			// 找到该冲突任期的第一条索引，让领导者一次性回退到那里。
			hintTerm = t
			hintIndex = prevLogIndex
			for hintIndex > l.baseIndex+1 &&
				l.entries[hintIndex-1-l.baseIndex].Term == hintTerm {
				hintIndex--
			}
			return conflictTerm, hintIndex, hintTerm
		}
		return conflictOK, 0, 0
	}
}

// firstConflict 返回新条目区间中第一个与本地"同索引不同任期"的位置；
// 没有冲突返回 0。调用方用它判断追加是否会回退掉已提交日志。
func (l *PersistentLog) firstConflict(prevLogIndex uint64, entries []Entry) uint64 {
	for i, e := range entries {
		idx := prevLogIndex + 1 + uint64(i)
		t, ok := l.termAt(idx)
		if !ok {
			return 0 // 本地没有该索引，往后都只能是追加
		}
		if t != e.Term {
			return idx
		}
	}
	return 0
}

// truncateFrom 删除 index 及其之后的所有真实条目。调用方必须先确认
// index > commitIndex，不能截断已提交内容。
func (l *PersistentLog) truncateFrom(index uint64) {
	if index <= l.baseIndex {
		return
	}
	l.entries = l.entries[:index-l.baseIndex]
}

// storeEntries 把领导者追加的条目写入 prevLogIndex 之后。
// 前置条件：checkPrev 已通过；firstConflict（若需要）已由调用方拦截。
// 满足"同索引同任期即完全相同"的 Raft 不变量，因此相同任期的重叠部分
// 直接跳过，只有遇到任期分叉才截断后重写。
func (l *PersistentLog) storeEntries(prevLogIndex uint64, entries []Entry) {
	pos := prevLogIndex + 1
	for _, e := range entries {
		if pos <= l.lastIndex() {
			t, _ := l.termAt(pos)
			if t == e.Term {
				pos++
				continue // 同索引同任期：条目必然相同，幂等跳过
			}
			l.truncateFrom(pos)
		}
		e.Index = pos
		l.entries = append(l.entries, e)
		pos++
	}
}

// appendRaw 在末尾追加已带好 Term/Index 的条目（快照恢复 / 测试构造用）。
func (l *PersistentLog) appendRaw(entries []Entry) {
	l.entries = append(l.entries, entries...)
}

// installSnapshot 压缩掉 through 及之前的日志，重置哨兵。
// 只有快照任期/索引比当前基点更新时才生效。
func (l *PersistentLog) installSnapshot(through, term uint64) {
	if through <= l.baseIndex {
		return
	}
	kept := []Entry{{Index: through, Term: term}}
	if through < l.lastIndex() {
		kept = append(kept, l.entries[through-l.baseIndex+1:]...)
	}
	l.baseIndex, l.baseTerm, l.entries = through, term, kept
}

// clone 深拷贝日志（节点以"保留持久状态"重启时使用）。
func (l *PersistentLog) clone() *PersistentLog {
	cp := &PersistentLog{
		baseIndex: l.baseIndex,
		baseTerm:  l.baseTerm,
		entries:   make([]Entry, len(l.entries)),
	}
	copy(cp.entries, l.entries)
	return cp
}
