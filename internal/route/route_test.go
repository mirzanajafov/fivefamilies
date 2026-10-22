package route

import (
	"testing"

	"google.golang.org/grpc/metadata"
)

func hint(addr string) metadata.MD {
	return metadata.Pairs(LeaderHeader, addr)
}

func TestTheRouterFollowsAHintToANodeItKnows(t *testing.T) {
	for _, tc := range []struct {
		name           string
		md             metadata.MD
		want           int
		wantRedirected bool
	}{
		{"no hint", nil, 1, false},
		{"a hint", hint("n3:3161"), 2, true},
		{"a hint naming an address the router doesn't know", hint("fivefamilies-n3:3161"), 1, false},
		{"a hint naming the node that failed", hint("n1:3161"), 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161"})
			redirected := r.MoveOn(0, tc.md)
			if got := r.Target(); got != tc.want || redirected != tc.wantRedirected {
				t.Fatalf("target %d, redirected %v, want %d and %v", got, redirected, tc.want, tc.wantRedirected)
			}
		})
	}
}

func TestTheRouterReachesEveryNodeWhileAFollowerPointsAtADeadLeader(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161", "n4:3161", "n5:3161"})
	var tried []int
	for range 6 {
		n := r.Target()
		tried = append(tried, n)
		if n == 1 {
			r.MoveOn(n, hint("n1:3161"))
		} else {
			r.MoveOn(n, nil)
		}
	}
	seen := map[int]bool{}
	for _, n := range tried {
		seen[n] = true
	}
	if len(seen) != 5 {
		t.Fatalf("tried %v, want every node within six attempts", tried)
	}
}

func TestTheRouterNeverMovesBackToTheNodeThatFailed(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161"})
	r.MoveOn(0, hint("n2:3161"))
	r.MoveOn(1, nil)
	if got := r.Target(); got == 1 {
		t.Fatal("the leader the hint named failed, and the router went straight back to it")
	}
}

func TestAHintWinsOverAnEarlierRotation(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161", "n4:3161"})
	r.MoveOn(0, nil)
	if !r.MoveOn(0, hint("n4:3161")) {
		t.Fatal("a hint to another node is a redirect, whoever sent it")
	}
	if got := r.Target(); got != 3 {
		t.Fatalf("target %d, want 3: a node's word on who leads beats the guess the router made after an earlier failure", got)
	}
}

func TestALateHintFromANodeTheRouterLeftDoesNotUndoANewerOne(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161", "n4:3161"})
	r.MoveOn(0, hint("n3:3161"))
	if !r.MoveOn(1, hint("n1:3161")) {
		t.Fatal("a hint to another node is a redirect, whoever sent it")
	}
	if got := r.Target(); got != 2 {
		t.Fatalf("target %d, want 2: a late answer from node 2 naming the old leader must not undo node 1's hint", got)
	}
}

func TestOnlyAFailureOfTheTargetRotatesTheRouter(t *testing.T) {
	r := NewRouter([]string{"n1:3161", "n2:3161", "n3:3161"})
	r.MoveOn(0, nil)
	r.MoveOn(0, nil)
	if got := r.Target(); got != 1 {
		t.Fatalf("target %d, want 1: a second failure of a node the router already left must not rotate it again", got)
	}
}
