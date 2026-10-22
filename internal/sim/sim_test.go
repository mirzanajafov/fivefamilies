package sim

import (
	"math/rand/v2"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
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

func TestAddingStatsSumsEveryCounterAndKeepsTheHighestTerm(t *testing.T) {
	stats := func(n uint64) Stats {
		var s Stats
		v := reflect.ValueOf(&s).Elem()
		for i := range v.NumField() {
			if f := v.Field(i); f.CanUint() {
				f.SetUint(n)
			} else {
				f.SetInt(int64(n))
			}
		}
		return s
	}
	sum := reflect.ValueOf(stats(1).Add(stats(2)))
	for i := range sum.NumField() {
		name, f := sum.Type().Field(i).Name, sum.Field(i)
		var got uint64
		if f.CanUint() {
			got = f.Uint()
		} else {
			got = uint64(f.Int())
		}
		want := uint64(3)
		if name == "MaxTerm" {
			want = 2
		}
		if got != want {
			t.Errorf("%s adds up to %d, want %d", name, got, want)
		}
	}
}

func TestSeeds(t *testing.T) {
	n := seedCount(t, 100)
	results := runSeeds(1, n, DefaultOptions())
	var total Stats
	for _, r := range results {
		if !r.OK() {
			t.Errorf("seed %d failed at %v: %s\n  swarm: %s\n%s", r.Seed, r.At, r.Violation, r.Swarm, r.Stack)
		}
		total = total.Add(r.Stats)
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
	modes := map[string]ReadMode{"any": ReadAny, "log": ReadLog, "index": ReadIndex, "lease": ReadLease, "follower": ReadFollower}
	for s, want := range modes {
		if got, err := ParseReadMode(s); err != nil || got != want {
			t.Errorf("read path %q parsed as %s: %v", s, got, err)
		}
	}
	for _, s := range []string{"", "leases", "Lease"} {
		if _, err := ParseReadMode(s); err == nil {
			t.Errorf("read path %q was accepted", s)
		}
	}
	checks := map[string]Category{"safety": CatSafety, "liveness": CatLiveness, "linearizability": CatLinearizability, "state": CatState,
		"client": CatClient, "panic": CatPanic, "lease": CatLease, "watch": CatWatch}
	for s, want := range checks {
		if got, err := ParseCategory(s); err != nil || got != want {
			t.Errorf("check %q parsed as %s: %v", s, got, err)
		}
	}
	if len(Categories()) != len(checks) || len(ReadModes())+1 != len(modes) {
		t.Errorf("%d checks and %d read paths, this test names %d and %d", len(Categories()), len(ReadModes())+1, len(checks), len(modes))
	}
	for _, s := range []string{"", "linearizabilty", "Safety"} {
		if _, err := ParseCategory(s); err == nil {
			t.Errorf("check %q was accepted", s)
		}
	}
}

func TestACheckOrBugThatCanNeverFireIsRefused(t *testing.T) {
	full := DefaultOptions()
	for _, b := range planted.All() {
		full.Bugs |= b
	}
	for _, c := range Categories() {
		if err := full.Validate(c); err != nil {
			t.Errorf("-catch %s with everything on and every bug planted: %v", c, err)
		}
	}
	for _, reads := range []ReadMode{ReadIndex, ReadLease, ReadFollower} {
		o := DefaultOptions()
		o.Bugs, o.Reads = planted.FollowerRead, reads
		if err := o.Validate(""); err != nil {
			t.Errorf("followerread with %s reads: %v", reads, err)
		}
	}
	for name, c := range map[string]struct {
		off   func(*Options)
		bug   planted.Bug
		catch Category
	}{
		"lease check without lease holders":       {func(o *Options) { o.Leases = false }, 0, CatLease},
		"localkeepalive without lease holders":    {func(o *Options) { o.Leases = false }, planted.LocalKeepAlive, ""},
		"watch check without watchers":            {func(o *Options) { o.Watches = false }, 0, CatWatch},
		"watchgap without watchers":               {func(o *Options) { o.Watches = false }, planted.WatchGap, ""},
		"watchkeep without watchers":              {func(o *Options) { o.Watches = false }, planted.WatchKeep, ""},
		"earlyconf without membership changes":    {func(o *Options) { o.Membership = false }, planted.EarlyConf, ""},
		"followerread with reads through the log": {func(o *Options) { o.Reads = ReadLog }, planted.FollowerRead, ""},
	} {
		t.Run(name, func(t *testing.T) {
			o := DefaultOptions()
			c.off(&o)
			o.Bugs = c.bug
			if o.Validate(c.catch) == nil {
				t.Errorf("%s was accepted", name)
			}
		})
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
		d.applyDurable(node.Write{Batch: wal.Batch{HardState: tc.before}}, c, 0)
		d.applyDurable(node.Write{Batch: wal.Batch{HardState: tc.after}}, c, 0)
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
			d.applyDurable(node.Write{Batch: wal.Batch{Entries: []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("a")}}}}, c, i)
		}
		disks = append(disks, d)
	}
	c.disks = disks
	return c, disks
}

func TestDiskCatchesACommittedEntryFallingBelowAQuorum(t *testing.T) {
	c, disks := committedOnDisks(t, 2, 3)
	disks[0].applyDurable(node.Write{Batch: wal.Batch{Entries: []raft.Entry{{Term: 2, Index: 2}}}}, c, 0)
	if !c.failed() {
		t.Fatal("a committed entry left on one disk of three went unnoticed")
	}
}

func TestDiskAllowsALaggingNodeToOverwriteACommittedEntry(t *testing.T) {
	c, disks := committedOnDisks(t, 3, 3)
	disks[0].applyDurable(node.Write{Batch: wal.Batch{Entries: []raft.Entry{{Term: 2, Index: 2}}}}, c, 0)
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

func TestTheSimulatorCountsUnfinishedWritesAsIndeterminate(t *testing.T) {
	s := &Sim{history: []check.Op{
		{Client: 0, Input: check.Input{Kind: check.Put, Key: "k", Value: "a"}, Output: check.Output{Ok: true}, Call: 1, Return: 2},
		{Client: 1, Input: check.Input{Kind: check.CAS, Key: "k", Expect: "a", Value: "b"}, Output: check.Output{Unknown: true}, Call: 3},
		{Client: 2, Input: check.Input{Kind: check.Get, Key: "k"}, Output: check.Output{Unknown: true}, Call: 4},
	}}
	if h := s.porcupineHistory(); len(h) != 2 || s.stats.Indeterminate != 1 {
		t.Fatalf("%d operations with %d indeterminate, want the put and the swap with one", len(h), s.stats.Indeterminate)
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

func TestAFailedLogWriteIsReportedEvenWithSafetyMuted(t *testing.T) {
	opts := DefaultOptions()
	opts.MuteSafety = true
	s := newSim(1, opts)
	for _, sn := range s.nodes {
		s.boot(sn)
	}
	if err := s.nodes[0].disk.store.Close(); err != nil {
		t.Fatal(err)
	}
	s.loop(opts.Chaos)
	if r := s.result(); r.Category != CatPanic || !strings.HasPrefix(r.Violation, "node 1 failed: ") {
		t.Fatalf("category %q: %s", r.Category, r.Violation)
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
	synced := node.Write{Seq: 1, Batch: wal.Batch{HardState: raft.HardState{Term: 1}, Entries: []raft.Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}}}, Sync: true}
	if err := d.submit(synced); err != nil {
		t.Fatal(err)
	}
	if err := d.sync(c, 0); err != nil {
		t.Fatal(err)
	}
	if err := d.submit(node.Write{Seq: 2, Batch: wal.Batch{HardState: raft.HardState{Term: 2}, Entries: []raft.Entry{{Term: 2, Index: 3}}}, Sync: true}); err != nil {
		t.Fatal(err)
	}
	d.crash(rand.New(rand.NewPCG(5, 5)))
	if _, _, err := d.open(c, 0); err != nil || c.failed() {
		t.Fatalf("an honest crash: %v %s", err, c.failure)
	}
	names, _ := d.fs.List()
	if err := d.fs.Truncate(names[0], 24); err != nil {
		t.Fatal(err)
	}
	d.crash(rand.New(rand.NewPCG(6, 6)))
	if _, _, err := d.open(c, 0); err != nil {
		t.Fatal(err)
	}
	if !c.failed() {
		t.Fatal("a log that lost synced entries recovered without complaint")
	}
}
