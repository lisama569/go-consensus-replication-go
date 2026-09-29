package raft

import "time"

// 时间函数单独成变量，方便测试替换或注入虚拟时钟。
var (
	timeNow   = time.Now
	timeAfter = time.After
)
