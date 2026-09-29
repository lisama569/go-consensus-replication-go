package raft

import (
	"fmt"
	"testing"
)

// TestLogger 把组件日志转发到 testing.T 日志，测试失败时可直接看到
// 每个节点的 term / log index / commit 位置变化，便于解释协议行为。
type TestLogger struct {
	t    testing.TB
	name string
}

func NewTestLogger(t testing.TB, name string) *TestLogger {
	return &TestLogger{t: t, name: name}
}

func (l *TestLogger) Printf(format string, args ...any) {
	l.t.Helper()
	l.t.Logf("[%s] %s", l.name, fmt.Sprintf(format, args...))
}
