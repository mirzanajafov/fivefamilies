package sim

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var canaries = []struct {
	bug       raft.Bug
	reads     string
	noPreVote bool
	nets      []Category
	seeds     []uint64
}{
	{raft.BugFigure8, "", false, []Category{CatSafety, CatPanic}, []uint64{853}},
	{raft.BugStaleTerm, "", false, []Category{CatSafety, CatPanic}, []uint64{667}},
	{raft.BugPrevTerm, "", false, []Category{CatSafety, CatPanic}, []uint64{19}},
	{raft.BugAnyVote, "", false, []Category{CatSafety, CatPanic}, []uint64{3}},
	{raft.BugNoFsync, "", false, []Category{CatSafety, CatPanic}, []uint64{1}},
	{raft.BugFollowerRead, "follower", false, []Category{CatLinearizability}, []uint64{1}},
	{raft.BugEarlyConf, "", true, []Category{CatSafety}, []uint64{7622}},
}

func TestPlantedBugsAreCaught(t *testing.T) {
	for _, c := range canaries {
		t.Run(c.bug.String(), func(t *testing.T) {
			t.Parallel()
			opts := DefaultOptions()
			opts.Bugs = c.bug
			opts.Reads = c.reads
			opts.PreVote = !c.noPreVote
			for _, seed := range c.seeds {
				r := Run(seed, opts)
				if r.OK() {
					t.Errorf("seed %d no longer catches %s; search for a new canary with cmd/sim -bug %s -first", seed, c.bug, c.bug)
					continue
				}
				expected := false
				for _, cat := range c.nets {
					expected = expected || r.Category == cat
				}
				if !expected {
					t.Errorf("seed %d caught %s through %s, expected %v: %s", seed, c.bug, r.Category, c.nets, r.Violation)
				}
			}
		})
	}
}

func TestCanarySeedsPassWithoutTheBug(t *testing.T) {
	for _, c := range canaries {
		opts := DefaultOptions()
		opts.Reads = c.reads
		opts.PreVote = !c.noPreVote
		for _, seed := range c.seeds {
			if r := Run(seed, opts); !r.OK() {
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

func TestATickMeasuredLeaseBreaksWithoutCatchUp(t *testing.T) {
	opts := DefaultOptions()
	opts.Reads = "lease"
	opts.NoCatchUp = true
	if r := Run(172, opts); r.OK() || r.Category != CatLinearizability {
		t.Fatalf("seed 172 without tick catch-up: %s %s", r.Category, r.Violation)
	}
	opts.NoCatchUp = false
	if r := Run(172, opts); !r.OK() {
		t.Fatalf("seed 172 with tick catch-up: %s", r.Violation)
	}
}
