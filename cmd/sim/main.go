package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/sim"
)

type config struct {
	opts     sim.Options
	catch    sim.Category
	seed     uint64
	trace    bool
	viz      string
	from     uint64
	count    int
	parallel int
	first    bool
}

func main() {
	c := parseFlags()
	if err := c.opts.Validate(c.catch); err != nil {
		fmt.Fprintln(os.Stderr, "-catch or -bug can never fire:", err)
		os.Exit(2)
	}
	if c.seed != 0 {
		os.Exit(c.replay())
	}
	os.Exit(c.batch())
}

func parseFlags() config {
	seed := flag.Uint64("seed", 0, "run one seed and print its result")
	trace := flag.Bool("trace", false, "with -seed, print every event")
	viz := flag.String("viz", "", "with -seed, write the history's linearizability visualization to this HTML file")
	from := flag.Uint64("from", 1, "first seed of a batch")
	count := flag.Int("seeds", 1000, "number of seeds in a batch")
	parallel := flag.Int("parallel", runtime.NumCPU(), "seeds run at once")
	chaos := flag.Duration("chaos", 0, "simulated time with faults (default from the simulator)")
	heal := flag.Duration("heal", 0, "simulated time after every fault stops")
	nodes := flag.Int("nodes", 0, "cluster size")
	noPreVote := flag.Bool("no-prevote", false, "run elections without PreVote")
	noMembers := flag.Bool("no-membership", false, "never change membership or hand over leadership")
	noLeases := flag.Bool("no-leases", false, "run no lease holders")
	noWatches := flag.Bool("no-watches", false, "run no watchers")
	batch := flag.Bool("batch", false, "let a node take several events before it handles its Ready, as the server loop does")
	noCatchUp := flag.Bool("no-catchup", false, "do not replay the ticks a paused node missed, to show what a lease measured in ticks does under pauses")
	var reads sim.ReadMode
	flag.Func("reads", "force one read path for every seed (any, the default, picks one per seed): "+list(sim.ReadModes()), func(s string) (err error) {
		reads, err = sim.ParseReadMode(s)
		return err
	})
	skew := flag.Float64("clock-skew", 0, "make one node's clock run this much faster (0.5 = 50%), to break the lease assumption")
	checkTimeout := flag.Duration("check-timeout", 0, "time limit for one linearizability check")
	var bugs planted.Bug
	flag.Func("bug", "planted bugs to switch on, comma separated: "+list(planted.All()), func(s string) (err error) {
		bugs, err = planted.Parse(s)
		return err
	})
	mute := flag.Bool("mute-safety", false, "record protocol invariant violations without stopping, to see what the other checks catch")
	first := flag.Bool("first", false, "stop handing out seeds after the first failure and report the lowest failing seed")
	var catch sim.Category
	flag.Func("catch", "count a seed as failing only when this check fires (a seed another check stops early is reported apart): "+list(sim.Categories()), func(s string) (err error) {
		catch, err = sim.ParseCategory(s)
		return err
	})
	flag.Parse()

	opts := sim.DefaultOptions()
	if *chaos > 0 {
		opts.Chaos = *chaos
	}
	if *heal > 0 {
		opts.Heal = *heal
	}
	if *nodes > 0 {
		opts.Nodes = *nodes
	}
	opts.ClockSkew = *skew
	opts.Reads = reads
	opts.NoCatchUp = *noCatchUp
	opts.PreVote = !*noPreVote
	opts.Membership = !*noMembers
	opts.Leases = !*noLeases
	opts.Watches = !*noWatches
	opts.Batch = *batch
	if *checkTimeout > 0 {
		opts.CheckTimeout = *checkTimeout
	}
	opts.Bugs = bugs
	opts.MuteSafety = *mute
	return config{opts: opts, catch: catch, seed: *seed, trace: *trace, viz: *viz, from: *from, count: *count, parallel: *parallel, first: *first}
}

func (c config) replay() int {
	if c.trace {
		c.opts.Trace = os.Stdout
	}
	res := sim.Run(c.seed, c.opts)
	printResult(res, c.catch)
	if c.viz != "" {
		if err := visualize(res, c.viz); err != nil {
			fmt.Fprintln(os.Stderr, "visualization:", err)
		}
	}
	fmt.Print(res.Stack)
	if _, out := verdict(res, c.catch); out != clean {
		return 1
	}
	return 0
}

type tally struct {
	ran                  int
	failed               []sim.Result
	total                sim.Stats
	checkTotal, checkMax time.Duration
	unknown, short       []uint64
	categories, detected map[sim.Category]int
	lowest               uint64
}

func (t *tally) add(res sim.Result, catch sim.Category) outcome {
	t.ran++
	for cat := range res.Detected {
		t.detected[cat]++
	}
	t.total = t.total.Add(res.Stats)
	t.checkTotal += res.CheckTook
	t.checkMax = max(t.checkMax, res.CheckTook)
	if res.Check == porcupine.Unknown {
		t.unknown = append(t.unknown, res.Seed)
	}
	cat, out := verdict(res, catch)
	switch out {
	case caught:
		t.failed = append(t.failed, res)
		t.categories[cat]++
		if t.lowest == 0 || res.Seed < t.lowest {
			t.lowest = res.Seed
		}
	case cutShort:
		t.short = append(t.short, res.Seed)
	}
	return out
}

func (c config) batch() int {
	start := time.Now()
	var next atomic.Uint64
	next.Store(c.from)
	last := c.from + uint64(c.count)
	var mu sync.Mutex
	t := &tally{categories: map[sim.Category]int{}, detected: map[sim.Category]int{}}
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range c.parallel {
		wg.Go(func() {
			for s := next.Add(1) - 1; s < last && !stop.Load(); s = next.Add(1) - 1 {
				res := sim.Run(s, c.opts)
				mu.Lock()
				if t.add(res, c.catch) == caught {
					if c.first {
						stop.Store(true)
					} else {
						printResult(res, c.catch)
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return c.report(t, time.Since(start))
}

func (c config) report(t *tally, elapsed time.Duration) int {
	fmt.Printf("%d seeds from %d in %v (%.1f ms per seed per core), %d failed\n",
		t.ran, c.from, elapsed.Round(time.Millisecond), float64(elapsed.Milliseconds())*float64(c.parallel)/float64(max(t.ran, 1)), len(t.failed))
	fmt.Printf("totals: %+v\n", t.total)
	fmt.Printf("linearizability checks: %v in total, %v at most, %d timed out\n", t.checkTotal.Round(time.Millisecond), t.checkMax.Round(time.Millisecond), len(t.unknown))
	if len(t.unknown) > 0 {
		slices.Sort(t.unknown)
		fmt.Printf("seeds whose check timed out: %v\n", t.unknown)
	}
	if len(t.detected) > 0 {
		fmt.Printf("seeds where each check fired: %v\n", t.detected)
	}
	if len(t.short) > 0 {
		slices.Sort(t.short)
		fmt.Printf("seeds another check stopped before %s could be judged: %v\n", c.catch, t.short)
	}
	if len(t.failed) == 0 {
		if len(t.short) > 0 {
			return 1
		}
		return 0
	}
	fmt.Printf("failures by category: %v, lowest failing seed %d (the %dth of the batch)\n", t.categories, t.lowest, t.lowest-c.from+1)
	if c.first {
		for _, f := range t.failed {
			if f.Seed == t.lowest {
				printResult(f, c.catch)
			}
		}
	}
	return 1
}

type outcome int

const (
	clean outcome = iota
	caught
	cutShort
)

func verdict(r sim.Result, catch sim.Category) (sim.Category, outcome) {
	if _, fired := r.Detected[catch]; fired {
		return catch, caught
	}
	switch {
	case r.OK():
		return "", clean
	case catch == "":
		return r.Category, caught
	case r.Check == "":
		return r.Category, cutShort
	}
	return "", clean
}

func printResult(r sim.Result, catch sim.Category) {
	status := "ok"
	switch cat, out := verdict(r, catch); out {
	case caught:
		status = fmt.Sprintf("FAIL (%s)", cat)
		if cat == r.Category {
			status += fmt.Sprintf(" at %v: %s", r.At, r.Violation)
		}
	case cutShort:
		status = fmt.Sprintf("STOPPED (%s) at %v before %s could be judged: %s", cat, r.At, catch, r.Violation)
	}
	fmt.Printf("seed %d: %s\n  swarm: %s\n  stats: %+v\n  check: %s in %v\n", r.Seed, status, r.Swarm, r.Stats, r.Check, r.CheckTook.Round(time.Millisecond))
	for _, cat := range sim.Categories() {
		if msg, ok := r.Detected[cat]; ok {
			fmt.Printf("  %s: %s\n", cat, msg)
		}
	}
}

func visualize(r sim.Result, path string) error {
	if r.Check == "" {
		return errors.New("the linearizability check did not run for this seed")
	}
	return porcupine.VisualizePath(check.KVModel, r.Info, path)
}

func list[T any](values []T) string {
	names := make([]string, len(values))
	for i, v := range values {
		names[i] = fmt.Sprint(v)
	}
	return strings.Join(names, ", ")
}
