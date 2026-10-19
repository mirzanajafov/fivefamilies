package sim

import (
	"math"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
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
		total.ClientOps += r.Stats.ClientOps
		total.ClientRetries += r.Stats.ClientRetries
		total.SwapsWon += r.Stats.SwapsWon
		total.TornTails += r.Stats.TornTails
		total.Writes += r.Stats.Writes
		total.Fsyncs += r.Stats.Fsyncs
		total.Installs += r.Stats.Installs
		total.LocalReads += r.Stats.LocalReads
		total.Compactions += r.Stats.Compactions
		if r.OK() && r.Check != porcupine.Ok {
			t.Errorf("seed %d: linearizability check %s", r.Seed, r.Check)
		}
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
		"client operations":       total.ClientOps >= 100*n,
		"client retries":          total.ClientRetries > 0,
		"successful swaps":        total.SwapsWon > 0,
		"torn tails on restart":   total.TornTails > 0,
		"group commit":            total.Fsyncs < total.Writes,
		"snapshot installs":       total.Installs > 0,
		"confirmed local reads":   total.LocalReads > 0,
		"segments compacted away": total.Compactions > 0,
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
		if a[i].TraceHash != b[i].TraceHash || a[i].Stats != b[i].Stats || a[i].Check != b[i].Check {
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

func TestSeveralEventsBeforeOneReady(t *testing.T) {
	opts := DefaultOptions()
	opts.Batch = true
	results := runSeeds(1, seedCount(t, 40), opts)
	for _, r := range results {
		if !r.OK() {
			t.Errorf("seed %d failed at %v: %s\n  swarm: %s\n%s", r.Seed, r.At, r.Violation, r.Swarm, r.Stack)
		}
	}
	if plain := Run(1, DefaultOptions()); plain.TraceHash == results[0].TraceHash {
		t.Error("batching changed nothing in seed 1")
	}
}

type nopEnv struct{}

func (nopEnv) Send(raft.Message)          {}
func (nopEnv) Write(node.Write)           {}
func (nopEnv) SaveSnapshot(raft.Snapshot) {}
func (nopEnv) Compacted(raft.Snapshot)    {}

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

func TestOnlyKnownReadPathsAndChecksParse(t *testing.T) {
	for _, m := range append(ReadModes(), ReadAny) {
		if got, err := ParseReadMode(m.String()); err != nil || got != m {
			t.Errorf("read path %s parsed as %s: %v", m, got, err)
		}
	}
	for _, s := range []string{"", "leases", "Lease"} {
		if _, err := ParseReadMode(s); err == nil {
			t.Errorf("read path %q was accepted", s)
		}
	}
	for _, c := range Categories() {
		if got, err := ParseCategory(string(c)); err != nil || got != c {
			t.Errorf("check %s parsed as %s: %v", c, got, err)
		}
	}
	for _, s := range []string{"", "linearizabilty", "Safety"} {
		if _, err := ParseCategory(s); err == nil {
			t.Errorf("check %q was accepted", s)
		}
	}
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
	c.leaders = map[uint64]leadership{}
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
	rec := newRecorder(c, 0, 0, 0)
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
		d := newDisk(0, nil)
		d.applyDurable(node.Write{HardState: tc.before}, c, 0)
		d.applyDurable(node.Write{HardState: tc.after}, c, 0)
		if !c.failed() {
			t.Errorf("%s went unnoticed", tc.name)
		}
	}
}

func committedOnDisks(t *testing.T, holders, total int) (*checker, []*disk) {
	t.Helper()
	c := newChecker(total)
	c.committed = []entryKey{{term: 1}, {term: 1, data: "a"}}
	c.commitTerm = []uint64{1, 1}
	for i := range total {
		c.conf.Voters = append(c.conf.Voters, raft.NodeID(i+1))
	}
	var disks []*disk
	for i := range total {
		d := newDisk(0, nil)
		if i < holders {
			d.applyDurable(node.Write{Entries: []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("a")}}}, c, i)
		}
		disks = append(disks, d)
	}
	c.disks = disks
	return c, disks
}

func TestDiskCatchesACommittedEntryFallingBelowAQuorum(t *testing.T) {
	c, disks := committedOnDisks(t, 2, 3)
	disks[0].applyDurable(node.Write{Entries: []raft.Entry{{Term: 2, Index: 2}}}, c, 0)
	if !c.failed() {
		t.Fatal("a committed entry left on one disk of three went unnoticed")
	}
}

func TestDiskAllowsALaggingNodeToOverwriteACommittedEntry(t *testing.T) {
	c, disks := committedOnDisks(t, 3, 3)
	disks[0].applyDurable(node.Write{Entries: []raft.Entry{{Term: 2, Index: 2}}}, c, 0)
	if c.failed() {
		t.Fatalf("two of three disks still hold the entry: %s", c.failure)
	}
}

func TestCheckerCatchesACommitThatIsNotDurableOnAQuorum(t *testing.T) {
	c := newChecker(1)
	c.conf = raft.ConfState{Voters: []raft.NodeID{1}}
	c.disks = []*disk{newDisk(0, nil)}
	c.observe(0, soloLeader(t, 1, raft.Restore{}, "a"))
	if !c.failed() {
		t.Fatal("entries committed with nothing on disk went unnoticed")
	}
}

var _ node.Env = nopEnv{}

func TestHistoryMarksUnfinishedWritesIndeterminateAndDropsUnfinishedReads(t *testing.T) {
	s := &Sim{history: []opRecord{
		{client: 0, call: 1, ret: 2, done: true, in: check.Input{Kind: check.Put, Key: "k", Value: "a"}, out: check.Output{Ok: true}},
		{client: 1, call: 3, in: check.Input{Kind: check.CAS, Key: "k", Expect: "a", Value: "b"}},
		{client: 2, call: 4, in: check.Input{Kind: check.Get, Key: "k"}},
	}}
	h := s.porcupineHistory()
	if len(h) != 2 {
		t.Fatalf("%d operations, want the finished put and the unfinished swap", len(h))
	}
	if h[1].Return != math.MaxInt64 || !h[1].Output.(check.Output).Unknown {
		t.Fatalf("unfinished swap: %+v", h[1])
	}
	if s.stats.Indeterminate != 1 {
		t.Fatalf("indeterminate %d", s.stats.Indeterminate)
	}
	if v := check.Linearizable(h, 10*time.Second); !v.OK() {
		t.Fatalf("verdict %s", v.Result)
	}
}

func TestCheckerCatchesALeaderLeadingTheSameTermAfterARestart(t *testing.T) {
	c := newChecker(1)
	c.restarted(0)
	c.observe(0, soloLeader(t, 1, raft.Restore{}))
	c.restarted(0)
	c.observe(0, soloLeader(t, 1, raft.Restore{}))
	if !c.failed() {
		t.Fatal("one node leading term 1 in two incarnations went unnoticed")
	}
}

func TestRegressionSeeds(t *testing.T) {
	seeds := []uint64{35, 39, 263, 281, 742, 894, 2888, 4107, 4851, 4961}
	for _, seed := range seeds {
		if r := Run(seed, DefaultOptions()); !r.OK() {
			t.Errorf("seed %d failed at %v: %s\n  swarm: %s", r.Seed, r.At, r.Violation, r.Swarm)
		}
	}
}

func TestRecoveryMustBeTheSyncedStatePlusAPrefixOfWhatWasInFlight(t *testing.T) {
	salt := rand.New(rand.NewPCG(1, 1)).Uint64
	d := newDisk(1<<20, salt)
	c := newChecker(1)
	if _, _, err := d.open(c, 0); err != nil {
		t.Fatal(err)
	}
	synced := node.Write{Seq: 1, HardState: raft.HardState{Term: 1}, Entries: []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}, Sync: true}
	if err := d.submit(synced); err != nil {
		t.Fatal(err)
	}
	if err := d.sync(c, 0); err != nil {
		t.Fatal(err)
	}
	d.submit(node.Write{Seq: 2, HardState: raft.HardState{Term: 2}, Entries: []raft.Entry{{Term: 2, Index: 3}}, Sync: true})
	d.crash(rand.New(rand.NewPCG(5, 5)))
	if _, _, err := d.open(c, 0); err != nil || c.failed() {
		t.Fatalf("an honest crash: %v %s", err, c.failure)
	}
	names, _ := d.fs.List()
	d.fs.Truncate(names[0], 24)
	d.crash(rand.New(rand.NewPCG(6, 6)))
	if _, _, err := d.open(c, 0); err != nil {
		t.Fatal(err)
	}
	if !c.failed() {
		t.Fatal("a log that lost synced entries recovered without complaint")
	}
}
