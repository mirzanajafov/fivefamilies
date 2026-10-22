package main

import (
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func TestANodeGetsAtLeastTwoGoProcessorsUnlessToldOtherwise(t *testing.T) {
	for _, c := range []struct{ asked, picked, want int }{{0, 1, 2}, {0, 2, 2}, {0, 8, 8}, {1, 8, 1}, {3, 1, 3}} {
		t.Run(fmt.Sprintf("asked %d picked %d", c.asked, c.picked), func(t *testing.T) {
			if got := goProcs(c.asked, c.picked); got != c.want {
				t.Fatalf("goProcs(%d, %d) = %d, want %d", c.asked, c.picked, got, c.want)
			}
		})
	}
}

func TestTheHTTPAPIGoesRoundTheNodesFromItsOwn(t *testing.T) {
	nodes := map[raft.NodeID]string{1: "n1", 2: "n2", 3: "n3", 4: "n4", 5: "n5"}
	for self, want := range map[raft.NodeID][]string{
		1: {"n1", "n2", "n3", "n4", "n5"},
		3: {"n3", "n4", "n5", "n1", "n2"},
		5: {"n5", "n1", "n2", "n3", "n4"},
	} {
		t.Run(fmt.Sprint("node ", self), func(t *testing.T) {
			if got := roundFrom(self, nodes); !slices.Equal(got, want) {
				t.Errorf("node %d tries %v, want %v", self, got, want)
			}
		})
	}
}

func TestFlagsBecomeSettingsOrAreRefused(t *testing.T) {
	valid := nodeFlags{id: 2, cluster: "1=n1:3161,2=n2:3161", tick: 10 * time.Millisecond, heartbeat: 100 * time.Millisecond, election: time.Second, fsync: "group"}
	s, err := valid.build(slog.Default())
	if err != nil || s.listen != ":3161" || s.cfg.ElectionTick != 100 || s.cfg.HeartbeatTick != 10 || len(s.cfg.Nodes) != 2 {
		t.Fatalf("settings %+v, %v", s, err)
	}
	for _, tc := range []struct {
		name string
		edit func(*nodeFlags)
	}{
		{"a cluster entry without an address", func(f *nodeFlags) { f.cluster = "1" }},
		{"a voter that is not a number", func(f *nodeFlags) { f.voters = "x" }},
		{"a planted bug in a production build", func(f *nodeFlags) { f.bugs = "figure8" }},
		{"no fsync in a production build", func(f *nodeFlags) { f.fsync = "off" }},
		{"a node the cluster has no address for", func(f *nodeFlags) { f.id = 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := valid
			tc.edit(&f)
			if _, err := f.build(slog.Default()); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}
