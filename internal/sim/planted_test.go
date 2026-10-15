package sim

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var canaries = []struct {
	bug   raft.Bug
	seeds []uint64
}{
	{raft.BugFigure8, []uint64{3377}},
	{raft.BugStaleTerm, []uint64{391}},
	{raft.BugPrevTerm, []uint64{10}},
	{raft.BugAnyVote, []uint64{3}},
	{raft.BugNoFsync, []uint64{1}},
}

func TestPlantedBugsAreCaught(t *testing.T) {
	for _, c := range canaries {
		t.Run(c.bug.String(), func(t *testing.T) {
			t.Parallel()
			opts := DefaultOptions()
			opts.Bugs = c.bug
			for _, seed := range c.seeds {
				r := Run(seed, opts)
				if r.OK() {
					t.Errorf("seed %d no longer catches %s; search for a new canary with cmd/sim -bug %s -first", seed, c.bug, c.bug)
					continue
				}
				if r.Category != CatSafety && r.Category != CatPanic {
					t.Errorf("seed %d caught %s through %s, expected a protocol invariant: %s", seed, c.bug, r.Category, r.Violation)
				}
			}
		})
	}
}

func TestCanarySeedsPassWithoutTheBug(t *testing.T) {
	for _, c := range canaries {
		for _, seed := range c.seeds {
			if r := Run(seed, DefaultOptions()); !r.OK() {
				t.Errorf("seed %d fails on the real code: %s", seed, r.Violation)
			}
		}
	}
}

func TestEveryBugHasACanary(t *testing.T) {
	have := map[raft.Bug]bool{}
	for _, c := range canaries {
		have[c.bug] = len(c.seeds) > 0
	}
	for _, b := range raft.AllBugs() {
		if !have[b] {
			t.Errorf("planted bug %s has no canary seed", b)
		}
	}
}
