package client

import (
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestTheRouterFollowsTheLeaderHintOrMovesToTheNextNode(t *testing.T) {
	addrs := []string{"n1:3161", "n2:3161", "n3:3161"}
	hint := func(addr string) metadata.MD { return metadata.Pairs(headerLeaderAddr, addr) }
	for _, tc := range []struct {
		name   string
		failed int
		md     metadata.MD
		want   int
	}{
		{"no hint", 0, nil, 1},
		{"no hint on the last node", 2, nil, 0},
		{"a hint", 0, hint("n3:3161"), 2},
		{"a hint naming an address the client doesn't know", 0, hint("fivefamilies-n3:3161"), 1},
		{"a hint naming the node that failed", 0, hint("n1:3161"), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter(addrs)
			for r.Target() != tc.failed {
				r.MoveOn(r.Target(), nil)
			}
			r.MoveOn(tc.failed, tc.md)
			if got := r.Target(); got != tc.want {
				t.Fatalf("target %d, want %d", got, tc.want)
			}
		})
	}
}

func TestAFailureOfANodeTheRouterAlreadyLeftDoesNotMoveIt(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161"})
	r.MoveOn(0, metadata.Pairs(headerLeaderAddr, "n3:3161"))
	r.MoveOn(0, nil)
	r.MoveOn(1, metadata.Pairs(headerLeaderAddr, "n2:3161"))
	if got := r.Target(); got != 2 {
		t.Fatalf("target %d, want 2, where the first hint pointed", got)
	}
}
