package consensus

// LogStore 是单个节点的内存日志（节点重启时按 reset 决定是否保留）。
// 索引从 1 开始，切片下标 = index-1。
type LogStore struct {
	entries []Entry
}

func newLogStore() *LogStore {
	return &LogStore{}
}

// LastIndex 返回最后一条日志的索引，空日志为 0。
func (l *LogStore) LastIndex() uint64 {
	return uint64(len(l.entries))
}

// LastTerm 返回最后一条日志的任期，空日志为 0。
func (l *LogStore) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt 返回指定索引处日志的任期；索引不存在返回 false。
// index==0 视为日志起点，任期恒为 0。
func (l *LogStore) TermAt(index uint64) (uint64, bool) {
	if index == 0 {
		return 0, true
	}
	if index > l.LastIndex() {
		return 0, false
	}
	return l.entries[index-1].Term, true
}

// EntryAt 返回指定索引的日志拷贝。
func (l *LogStore) EntryAt(index uint64) (Entry, bool) {
	if index == 0 || index > l.LastIndex() {
		return Entry{}, false
	}
	return l.entries[index-1], true
}

// Get 返回索引区间 [lo, hi]（含两端）的日志拷贝，lo 必须 <= hi，
// 且区间必须完全落在 [1, LastIndex] 内，否则返回 ReasonInvalidIndex。
func (l *LogStore) Get(lo, hi uint64) ([]Entry, error) {
	if lo == 0 || lo > hi || hi > l.LastIndex() {
		return nil, newReject(ReasonInvalidIndex, "bad range")
	}
	out := make([]Entry, 0, hi-lo+1)
	out = append(out, l.entries[lo-1:hi]...)
	return out, nil
}

// Contains 判断 (index, term) 是否与本地日志匹配；index==0 恒匹配。
func (l *LogStore) Contains(index, term uint64) bool {
	t, ok := l.TermAt(index)
	return ok && t == term
}

// Append 将新条目（调用方已填好 Index，且 Index 必须为 LastIndex+1）追加。
func (l *LogStore) Append(e Entry) {
	l.entries = append(l.entries, e)
}

// TruncateSuffix 删除从 index 开始（含）的所有日志（冲突裁剪）。
// index==0 会清空整个日志（用于 WipeAndRestart 之外的裁剪场景兜底）。
func (l *LogStore) TruncateSuffix(index uint64) {
	if index == 0 {
		l.entries = nil
		return
	}
	if index <= l.LastIndex() {
		l.entries = l.entries[:index-1]
	}
}

// Snapshot 返回全部日志的拷贝，用于测试观察。
func (l *LogStore) Snapshot() []Entry {
	out := make([]Entry, len(l.entries))
	copy(out, l.entries)
	return out
}
