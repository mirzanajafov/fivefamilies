package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/sim"
)

func main() {
	seed := flag.Uint64("seed", 0, "run one seed and print its result")
	trace := flag.Bool("trace", false, "with -seed, print every event")
	from := flag.Uint64("from", 1, "first seed of a batch")
	count := flag.Int("seeds", 1000, "number of seeds in a batch")
	parallel := flag.Int("parallel", runtime.NumCPU(), "seeds run at once")
	chaos := flag.Duration("chaos", 0, "simulated time with faults (default from the simulator)")
	heal := flag.Duration("heal", 0, "simulated time after every fault stops")
	nodes := flag.Int("nodes", 0, "cluster size")
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

	if *seed != 0 {
		if *trace {
			opts.Trace = os.Stdout
		}
		res := sim.Run(*seed, opts)
		printResult(res)
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
	var wg sync.WaitGroup
	for range *parallel {
		wg.Go(func() {
			for {
				s := next.Add(1) - 1
				if s >= last {
					return
				}
				res := sim.Run(s, opts)
				mu.Lock()
				total = addStats(total, res.Stats)
				if !res.OK() {
					failed = append(failed, res)
					printResult(res)
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("%d seeds from %d in %v (%.1f ms per seed per core), %d failed\n",
		*count, *from, elapsed.Round(time.Millisecond), float64(elapsed.Milliseconds())*float64(*parallel)/float64(*count), len(failed))
	fmt.Printf("totals: %+v\n", total)
	if len(failed) > 0 {
		os.Exit(1)
	}
}

func printResult(r sim.Result) {
	status := "ok"
	if !r.OK() {
		status = "FAIL at " + r.At.String() + ": " + r.Violation
	}
	fmt.Printf("seed %d: %s\n  swarm: %s\n  stats: %+v\n", r.Seed, status, r.Swarm, r.Stats)
}

func addStats(a, b sim.Stats) sim.Stats {
	a.Events += b.Events
	a.MsgsSent += b.MsgsSent
	a.MsgsDropped += b.MsgsDropped
	a.MsgsDuplicated += b.MsgsDuplicated
	a.Proposals += b.Proposals
	a.Accepted += b.Accepted
	a.Leaders += b.Leaders
	a.MaxTerm = max(a.MaxTerm, b.MaxTerm)
	a.Committed += b.Committed
	a.Crashes += b.Crashes
	a.Pauses += b.Pauses
	a.Partitions += b.Partitions
	a.SlowDisks += b.SlowDisks
	a.Overwrites += b.Overwrites
	return a
}
