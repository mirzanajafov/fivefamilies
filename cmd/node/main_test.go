package main

import (
	"slices"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func TestANodeGetsAtLeastTwoGoProcessorsUnlessToldOtherwise(t *testing.T) {
	for _, c := range []struct{ asked, picked, want int }{{0, 1, 2}, {0, 2, 2}, {0, 8, 8}, {1, 8, 1}, {3, 1, 3}} {
		if got := goProcs(c.asked, c.picked); got != c.want {
			t.Fatalf("goProcs(%d, %d) = %d, want %d", c.asked, c.picked, got, c.want)
		}
	}
}

func TestTheHTTPAPIGoesRoundTheNodesFromItsOwn(t *testing.T) {
	nodes := map[raft.NodeID]string{1: "n1", 2: "n2", 3: "n3", 4: "n4", 5: "n5"}
	for self, want := range map[raft.NodeID][]string{
		1: {"n1", "n2", "n3", "n4", "n5"},
		3: {"n3", "n4", "n5", "n1", "n2"},
		5: {"n5", "n1", "n2", "n3", "n4"},
	} {
		if got := roundFrom(self, nodes); !slices.Equal(got, want) {
			t.Errorf("node %d tries %v, want %v", self, got, want)
		}
	}
}
