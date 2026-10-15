package sim

import (
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func seedCount(t *testing.T, def int) int {
	if v := os.Getenv("FF_SIM_SEEDS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("FF_SIM_SEEDS: %v", err)
		}
		return n
	}
	if testing.Short() {
		return def / 4
	}
	return def
}

func runSeeds(from uint64, n int, opts Options) []Result {
	results := make([]Result, n)
	var wg sync.WaitGroup
	next := make(chan int)
	for range runtime.GOMAXPROCS(0) {
		wg.Go(func() {
			for i := range next {
				results[i] = Run(from+uint64(i), opts)
			}
		})
	}
	for i := range n {
		next <- i
	}
	close(next)
	wg.Wait()
	return results
}

func TestSeeds(t *testing.T) {
	n := seedCount(t, 100)
	results := runSeeds(1, n, DefaultOptions())
	var total Stats
	for _, r := range results {
		if !r.OK() {
			t.Errorf("seed %d failed at %v: %s\n  swarm: %s\n%s", r.Seed, r.At, r.Violation, r.Swarm, r.Stack)
		}
		total.Crashes += r.Stats.Crashes
		total.Pauses += r.Stats.Pauses
		total.Partitions += r.Stats.Partitions
		total.SlowDisks += r.Stats.SlowDisks
		total.Overwrites += r.Stats.Overwrites
		total.MsgsDuplicated += r.Stats.MsgsDuplicated
		total.Leaders += r.Stats.Leaders
		total.MaxTerm = max(total.MaxTerm, r.Stats.MaxTerm)
	}
	if n < 20 {
		return
	}
	reached := map[string]bool{
		"crashes":                 total.Crashes > 0,
		"pauses":                  total.Pauses > 0,
		"partitions":              total.Partitions > 0,
		"slow disks":              total.SlowDisks > 0,
		"overwritten log entries": total.Overwrites > 0,
		"duplicated messages":     total.MsgsDuplicated > 0,
		"two elections per seed":  total.Leaders >= 2*n,
		"a term of 5 or more":     total.MaxTerm >= 5,
	}
	for what, ok := range reached {
		if !ok {
			t.Errorf("%d seeds never produced %s: %+v", n, what, total)
		}
	}
}

func TestSameSeedSameRun(t *testing.T) {
	n := seedCount(t, 8)
	a := runSeeds(1000, n, DefaultOptions())
	b := runSeeds(1000, n, DefaultOptions())
	for i := range a {
		if a[i].TraceHash != b[i].TraceHash || a[i].Stats != b[i].Stats {
			t.Errorf("seed %d ran differently twice: %x %+v and %x %+v", a[i].Seed, a[i].TraceHash, a[i].Stats, b[i].TraceHash, b[i].Stats)
		}
	}
	if n > 1 && a[0].TraceHash == a[1].TraceHash {
		t.Error("two different seeds produced the same trace")
	}
}

func TestThreeNodeClusters(t *testing.T) {
	opts := DefaultOptions()
	opts.Nodes = 3
	for _, r := range runSeeds(1, seedCount(t, 40), opts) {
		if !r.OK() {
			t.Errorf("seed %d failed at %v: %s\n  swarm: %s\n%s", r.Seed, r.At, r.Violation, r.Swarm, r.Stack)
		}
	}
}

type nopEnv struct{}

func (nopEnv) Send(raft.Message) {}
func (nopEnv) Write(node.Write)  {}

func soloLeader(t *testing.T, id raft.NodeID, st raft.Restore, data ...string) *raft.Raft {
	t.Helper()
	r, err := raft.New(raft.Config{
		ID: id, Voters: []raft.NodeID{id}, ElectionTick: 10, HeartbeatTick: 1,
		Rand: rand.NewPCG(1, 2),
	}, st)
	if err != nil {
		t.Fatal(err)
	}
	settle := func() {
		for r.HasReady() {
			rd := r.Ready()
			for _, m := range rd.AfterPersist {
				r.Step(m)
			}
		}
	}
	r.Campaign()
	settle()
	for _, d := range data {
		if _, err := r.Propose([]byte(d)); err != nil {
			t.Fatal(err)
		}
		settle()
	}
	return r
}

func TestCheckerCatchesTwoLeadersInOneTerm(t *testing.T) {
	c := newChecker(2)
	c.observe(0, soloLeader(t, 1, raft.Restore{}))
	c.observe(1, soloLeader(t, 2, raft.Restore{}))
	if !c.failed() {
		t.Fatal("two leaders of term 1 went unnoticed")
	}
}

func TestCheckerCatchesDifferentCommittedEntries(t *testing.T) {
	c := newChecker(2)
	c.observe(0, soloLeader(t, 1, raft.Restore{}, "a"))
	c.leaders = map[uint64]raft.NodeID{}
	c.observe(1, soloLeader(t, 2, raft.Restore{}, "b"))
	if !c.failed() {
		t.Fatal("two different entries committed at index 2 went unnoticed")
	}
}

func TestCheckerCatchesALeaderMissingCommittedEntries(t *testing.T) {
	c := newChecker(2)
	c.observe(0, soloLeader(t, 1, raft.Restore{}, "a"))
	c.observe(1, soloLeader(t, 2, raft.Restore{HardState: raft.HardState{Term: 1}}))
	if !c.failed() {
		t.Fatal("a term 2 leader without the entries committed in term 1 went unnoticed")
	}
}

func TestCheckerAllowsALateLeaderOfAnOldTerm(t *testing.T) {
	c := newChecker(2)
	c.observe(0, soloLeader(t, 1, raft.Restore{HardState: raft.HardState{Term: 1}}, "a"))
	late, err := raft.New(raft.Config{
		ID: 2, Voters: []raft.NodeID{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1,
		Rand: rand.NewPCG(1, 2),
	}, raft.Restore{})
	if err != nil {
		t.Fatal(err)
	}
	late.Campaign()
	for _, m := range late.Ready().AfterPersist {
		late.Step(m)
	}
	late.Step(raft.Message{Type: raft.MsgVoteResp, From: 3, To: 2, Term: 1})
	if late.State() != raft.StateLeader || late.Term() != 1 || late.Committed() != 0 {
		t.Fatalf("setup: %s term %d committed %d", late.State(), late.Term(), late.Committed())
	}
	c.observe(1, late)
	if c.failed() {
		t.Fatalf("a leader of term 1 seen after term 2 committed is legal: %s", c.failure)
	}
}

func TestCheckerCatchesOutOfOrderApply(t *testing.T) {
	c := newChecker(1)
	rec := &recorder{chk: c}
	rec.Apply(raft.Entry{Term: 1, Index: 1})
	rec.Apply(raft.Entry{Term: 1, Index: 3})
	if !c.failed() {
		t.Fatal("a skipped index went unnoticed")
	}
}

func TestDiskCatchesForgottenVotesAndTerms(t *testing.T) {
	cases := []struct {
		name   string
		before raft.HardState
		after  raft.HardState
	}{
		{"term going back", raft.HardState{Term: 5}, raft.HardState{Term: 4}},
		{"second vote in a term", raft.HardState{Term: 5, Vote: 2}, raft.HardState{Term: 5, Vote: 3}},
	}
	for _, tc := range cases {
		c := newChecker(1)
		d := newMemDisk()
		d.apply(node.Write{HardState: tc.before}, c, 0)
		d.apply(node.Write{HardState: tc.after}, c, 0)
		if !c.failed() {
			t.Errorf("%s went unnoticed", tc.name)
		}
	}
}

func TestDiskCatchesOverwrittenCommittedEntries(t *testing.T) {
	c := newChecker(1)
	c.committed = []entryKey{{term: 1}, {term: 1, data: "a"}}
	c.commitTerm = []uint64{1, 1}
	d := newMemDisk()
	d.apply(node.Write{Entries: []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("a")}}}, c, 0)
	d.apply(node.Write{Entries: []raft.Entry{{Term: 2, Index: 2}}}, c, 0)
	if !c.failed() {
		t.Fatal("a committed entry overwritten on disk went unnoticed")
	}
}

var _ node.Env = nopEnv{}
