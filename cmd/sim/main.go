package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/sim"
)

func main() {
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
	flag.Func("reads", "force one read path for every seed: log, index, lease or follower", func(s string) (err error) {
		reads, err = sim.ParseReadMode(s)
		return err
	})
	skew := flag.Float64("clock-skew", 0, "make one node's clock run this much faster (0.5 = 50%), to break the lease assumption")
	checkTimeout := flag.Duration("check-timeout", 0, "time limit for one linearizability check")
	var planted raft.Bug
	flag.Func("bug", "planted bugs to switch on, comma separated: figure8, staleterm, prevterm, anyvote, nofsync, followerread, earlyconf, localkeepalive, watchgap, watchkeep", func(s string) (err error) {
		planted, err = raft.ParseBugs(s)
		return err
	})
	mute := flag.Bool("mute-safety", false, "record protocol invariant violations without stopping, to see what the other checks catch")
	first := flag.Bool("first", false, "stop handing out seeds after the first failure and report the lowest failing seed")
	var catch sim.Category
	flag.Func("catch", "count a seed as failing only when this check fires (safety, liveness, linearizability, lease, watch, state, client, panic)", func(s string) (err error) {
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
	opts.Bugs = planted
	opts.MuteSafety = *mute

	if *seed != 0 {
		if *trace {
			opts.Trace = os.Stdout
		}
		res := sim.Run(*seed, opts)
		printResult(res)
		if *viz != "" {
			if err := porcupine.VisualizePath(check.KVModel, res.Info, *viz); err != nil {
				fmt.Fprintln(os.Stderr, "visualization:", err)
				os.Exit(1)
			}
		}
		if !res.OK() {
			fmt.Print(res.Stack)
			os.Exit(1)
		}
		return
	}

	start := time.Now()
	var next atomic.Uint64
	next.Store(*from)
	last := *from + uint64(*count)
	var mu sync.Mutex
	var failed []sim.Result
	var total sim.Stats
	var checkTotal, checkMax time.Duration
	var unknown []uint64
	ran := 0
	categories := map[sim.Category]int{}
	detected := map[sim.Category]int{}
	var lowest uint64
	var stop atomic.Bool
	var wg sync.WaitGroup
	for range *parallel {
		wg.Go(func() {
			for {
				s := next.Add(1) - 1
				if s >= last || stop.Load() {
					return
				}
				res := sim.Run(s, opts)
				mu.Lock()
				ran++
				for cat := range res.Detected {
					detected[cat]++
				}
				total = addStats(total, res.Stats)
				checkTotal += res.CheckTook
				checkMax = max(checkMax, res.CheckTook)
				if res.Check == porcupine.Unknown {
					unknown = append(unknown, res.Seed)
				}
				failing := !res.OK()
				if catch != "" {
					_, failing = res.Detected[catch]
				}
				if failing {
					failed = append(failed, res)
					categories[res.Category]++
					if lowest == 0 || res.Seed < lowest {
						lowest = res.Seed
					}
					if *first {
						stop.Store(true)
					} else {
						printResult(res)
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d seeds from %d in %v (%.1f ms per seed per core), %d failed\n",
		ran, *from, elapsed.Round(time.Millisecond), float64(elapsed.Milliseconds())*float64(*parallel)/float64(max(ran, 1)), len(failed))
	fmt.Printf("totals: %+v\n", total)
	fmt.Printf("linearizability checks: %v in total, %v at most, %d timed out\n", checkTotal.Round(time.Millisecond), checkMax.Round(time.Millisecond), len(unknown))
	if len(unknown) > 0 {
		slices.Sort(unknown)
		fmt.Printf("seeds whose check timed out: %v\n", unknown)
	}
	if len(detected) > 0 {
		fmt.Printf("seeds where each check fired: %v\n", detected)
	}
	if len(failed) == 0 {
		return
	}
	fmt.Printf("failures by category: %v; lowest failing seed %d, the %dth of the batch\n", categories, lowest, lowest-*from+1)
	if *first {
		for _, f := range failed {
			if f.Seed == lowest {
				printResult(f)
			}
		}
	}
	os.Exit(1)
}

func printResult(r sim.Result) {
	status := "ok"
	if !r.OK() {
		status = fmt.Sprintf("FAIL (%s) at %v: %s", r.Category, r.At, r.Violation)
	}
	fmt.Printf("seed %d: %s\n  swarm: %s\n  stats: %+v\n  check: %s in %v\n", r.Seed, status, r.Swarm, r.Stats, r.Check, r.CheckTook.Round(time.Millisecond))
	for _, cat := range sim.Categories {
		if msg, ok := r.Detected[cat]; ok {
			fmt.Printf("  %s: %s\n", cat, msg)
		}
	}
}

func addStats(a, b sim.Stats) sim.Stats {
	a.Events += b.Events
	a.MsgsSent += b.MsgsSent
	a.MsgsDropped += b.MsgsDropped
	a.MsgsDuplicated += b.MsgsDuplicated
	a.Proposals += b.Proposals
	a.Leaders += b.Leaders
	a.MaxTerm = max(a.MaxTerm, b.MaxTerm)
	a.Committed += b.Committed
	a.Crashes += b.Crashes
	a.Pauses += b.Pauses
	a.Partitions += b.Partitions
	a.SlowDisks += b.SlowDisks
	a.Overwrites += b.Overwrites
	a.ClientOps += b.ClientOps
	a.ClientRetries += b.ClientRetries
	a.SwapsWon += b.SwapsWon
	a.Indeterminate += b.Indeterminate
	a.Buggified += b.Buggified
	a.Writes += b.Writes
	a.Fsyncs += b.Fsyncs
	a.TornTails += b.TornTails
	a.Snapshots += b.Snapshots
	a.LocalReads += b.LocalReads
	a.SnapshotsSent += b.SnapshotsSent
	a.Installs += b.Installs
	a.Compactions += b.Compactions
	a.ConfChanges += b.ConfChanges
	a.ConfCommitted += b.ConfCommitted
	a.Transfers += b.Transfers
	a.LeaseGrants += b.LeaseGrants
	a.KeepAlives += b.KeepAlives
	a.LeasesLost += b.LeasesLost
	a.LeaseExpiries += b.LeaseExpiries
	a.WatchEvents += b.WatchEvents
	a.WatchResyncs += b.WatchResyncs
	a.WatchCompacted += b.WatchCompacted
	a.WatchDrops += b.WatchDrops
	return a
}
