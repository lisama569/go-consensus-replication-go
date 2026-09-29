package raft

func quorum(n int) int { return n/2 + 1 }

func contains(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
