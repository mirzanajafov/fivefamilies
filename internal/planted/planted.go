package planted

import (
	"fmt"
	"strings"
)

type Bug uint32

const (
	Figure8 Bug = 1 << iota
	StaleTerm
	PrevTerm
	AnyVote
	NoFsync
	FollowerRead
	EarlyConf
	LocalKeepAlive
	WatchGap
	WatchKeep
)

const Watches = WatchGap | WatchKeep

var known = []struct {
	bug  Bug
	name string
}{
	{Figure8, "figure8"},
	{StaleTerm, "staleterm"},
	{PrevTerm, "prevterm"},
	{AnyVote, "anyvote"},
	{NoFsync, "nofsync"},
	{FollowerRead, "followerread"},
	{EarlyConf, "earlyconf"},
	{LocalKeepAlive, "localkeepalive"},
	{WatchGap, "watchgap"},
	{WatchKeep, "watchkeep"},
}

func (b Bug) Has(x Bug) bool {
	return b&x != 0
}

func (b Bug) String() string {
	if b == 0 {
		return "none"
	}
	var names []string
	for _, n := range known {
		if b.Has(n.bug) {
			names = append(names, n.name)
		}
	}
	return strings.Join(names, ",")
}

func Parse(s string) (Bug, error) {
	var b Bug
	if s == "" || s == "none" {
		return 0, nil
	}
	for name := range strings.SplitSeq(s, ",") {
		found := false
		for _, n := range known {
			if n.name == strings.TrimSpace(name) {
				b |= n.bug
				found = true
			}
		}
		if !found {
			return 0, fmt.Errorf("planted: unknown bug %q", name)
		}
	}
	return b, nil
}

func All() []Bug {
	out := make([]Bug, len(known))
	for i, n := range known {
		out[i] = n.bug
	}
	return out
}
