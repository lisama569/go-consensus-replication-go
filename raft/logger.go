package raft

// Logger 是组件使用的最小日志接口。测试中会注入一个把
// term/index/commit 一并打印出来的 logger，便于解释每次决策。
type Logger interface {
	Printf(format string, args ...any)
}

type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// testLogger 也在本包内提供，实现见 testing.go，单测通过 NewTestLogger 注入。
