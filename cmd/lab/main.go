package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/lab"
)

func main() {
	seed := flag.Uint64("seed", uint64(time.Now().UnixNano()), "fault schedule and workload seed")
	duration := flag.Duration("duration", time.Minute, "time with faults")
	settle := flag.Duration("settle", 5*time.Second, "time after the heal before the workload stops")
	workers := flag.Int("workers", 6, "concurrent clients, each with its own session")
	keys := flag.Int("keys", 3, "keys the clients share")
	read := flag.String("read", "index", "read path: index, lease or log")
	faults := flag.String("faults", "all", "comma separated fault kinds, all, or none")
	mean := flag.Duration("fault-every", 3*time.Second, "mean gap between faults")
	bug := flag.String("bug", "", "planted bug for every node")
	election := flag.String("election", "1s", "election timeout")
	heartbeat := flag.String("heartbeat", "100ms", "heartbeat interval")
	lease := flag.String("lease", "0s", "lease length (needed for -read lease)")
	opTimeout := flag.Duration("op-timeout", 2*time.Minute, "time a client keeps retrying one operation before it counts as indeterminate")
	checkTimeout := flag.Duration("check-timeout", 5*time.Minute, "limit for the linearizability check")
	out := flag.String("out", "", "results directory (default bench/results/lab/<time>)")
	keep := flag.Bool("keep", false, "leave the cluster running afterwards")
	build := flag.Bool("build", false, "build the lab images first")
	flag.Parse()

	opts := lab.Options{
		Seed: *seed, Duration: *duration, Settle: *settle, Workers: *workers, Keys: *keys, Read: *read,
		FaultMean: *mean, Bug: *bug, Election: *election, Heartbeat: *heartbeat, Lease: *lease,
		OpTimeout: *opTimeout, CheckTimeout: *checkTimeout, Out: *out, Keep: *keep,
		Project: "fflab", File: filepath.Join("lab", "compose.yml"),
	}
	switch *faults {
	case "all":
		opts.Faults = lab.AllFaults
	case "none":
	default:
		for f := range strings.SplitSeq(*faults, ",") {
			opts.Faults = append(opts.Faults, lab.FaultKind(strings.TrimSpace(f)))
		}
	}
	if opts.Out == "" {
		opts.Out = filepath.Join("bench", "results", "lab", time.Now().UTC().Format("20060102-150405"))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *build {
		cmd := exec.CommandContext(ctx, "docker", "build", "-q", "--build-arg", "LDFLAGS=-s -w -X main.plantable=yes", "-t", "fivefamilies-lab-node", ".")
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "build:", err)
			os.Exit(1)
		}
	}
	rep, err := lab.Run(ctx, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("seed %d, %v of faults (%d), %d operations (%d writes indeterminate, %d reads failed), %d leader changes\n",
		opts.Seed, opts.Duration, len(rep.Faults), rep.Ops, rep.Unknown, rep.FailedReads, len(rep.Leaders))
	fmt.Printf("linearizability: %s in %v; states equal: %v at %d %v\n", rep.Linearizable, rep.CheckTook.Round(time.Millisecond), rep.StatesEqual, rep.StateIndex, rep.Hashes)
	for _, p := range rep.Problems {
		fmt.Println("problem:", p)
	}
	fmt.Println("results:", opts.Out)
	if !rep.OK() {
		os.Exit(1)
	}
}
