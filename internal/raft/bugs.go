package raft

import (
	"fmt"
	"strings"
)

type Bug uint32

const (
	BugFigure8 Bug = 1 << iota
	BugStaleTerm
	BugPrevTerm
	BugAnyVote
	BugNoFsync
	BugFollowerRead
	BugEarlyConf
)

var bugNames = []struct {
	bug  Bug
	name string
}{
	{BugFigure8, "figure8"},
	{BugStaleTerm, "staleterm"},
	{BugPrevTerm, "prevterm"},
	{BugAnyVote, "anyvote"},
	{BugNoFsync, "nofsync"},
	{BugFollowerRead, "followerread"},
	{BugEarlyConf, "earlyconf"},
}

func (b Bug) Has(x Bug) bool {
	return b&x != 0
}

func (b Bug) String() string {
	if b == 0 {
		return "none"
	}
	var names []string
	for _, n := range bugNames {
		if b.Has(n.bug) {
			names = append(names, n.name)
		}
	}
	return strings.Join(names, ",")
}

func ParseBugs(s string) (Bug, error) {
	var b Bug
	if s == "" || s == "none" {
		return 0, nil
	}
	for name := range strings.SplitSeq(s, ",") {
		found := false
		for _, n := range bugNames {
			if n.name == strings.TrimSpace(name) {
				b |= n.bug
				found = true
			}
		}
		if !found {
			return 0, fmt.Errorf("raft: unknown bug %q", name)
		}
	}
	return b, nil
}

func AllBugs() []Bug {
	out := make([]Bug, len(bugNames))
	for i, n := range bugNames {
		out[i] = n.bug
	}
	return out
}
