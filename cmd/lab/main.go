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

func failover(args []string) {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	elections := fs.String("election", "150ms,300ms,600ms,1s", "election timeouts to sweep; the heartbeat is a third of each")
	trials := fs.Int("trials", 10, "leader kills per election timeout")
	rate := fs.Int("rate", 200, "writes per second from the generator")
	timeout := fs.Duration("timeout", 300*time.Millisecond, "per-request timeout in the generator")
	before := fs.Duration("before", 3*time.Second, "writing before the kill")
	after := fs.Duration("after", 7*time.Second, "writing after the kill")
	out := fs.String("out", filepath.Join("bench", "results", "e1", time.Now().UTC().Format("20060102-150405")), "results directory")
	build := fs.Bool("build", false, "build the lab image first")
	cold := fs.Bool("cold", false, "keep the generator's write connections to followers cold until the leader dies")
	fs.Parse(args)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *build {
		buildImage(ctx)
	}
	for e := range strings.SplitSeq(*elections, ",") {
		el, err := time.ParseDuration(e)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		res, err := lab.Failover(ctx, lab.FailoverOptions{
			Election: el.String(), Heartbeat: (el / 3).String(), Lease: "0s", Trials: *trials, Rate: *rate,
			Timeout: *timeout, Before: *before, After: *after, Cold: *cold, Project: "fflab", File: filepath.Join("lab", "compose.yml"),
		}, *out)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var us []string
		for _, t := range res.Trials {
			s := t.Unavailable.Round(time.Millisecond).String()
			if t.Err != "" {
				s += "!"
			}
			us = append(us, s)
		}
		fmt.Printf("election %v: unavailable median %v (min %v, max %v) over %d kills: %s\n", el, res.Median.Round(time.Millisecond), res.Min.Round(time.Millisecond), res.Max.Round(time.Millisecond), len(res.Trials), strings.Join(us, " "))
	}
}

func registry(args []string) {
	fs := flag.NewFlagSet("registry", flag.ExitOnError)
	election := fs.String("election", "1s", "election timeout")
	heartbeat := fs.String("heartbeat", "100ms", "heartbeat interval")
	ttl := fs.Duration("ttl", 2*time.Second, "the instance's lease TTL")
	trials := fs.Int("trials", 10, "trials per variant")
	seed := fs.Uint64("seed", 1, "seed for the time each instance is held before the kill")
	out := fs.String("out", filepath.Join("bench", "results", "registry", time.Now().UTC().Format("20060102-150405")), "results directory")
	build := fs.Bool("build", false, "build the lab image first")
	fs.Parse(args)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *build {
		buildImage(ctx)
	}
	res, err := lab.Registry(ctx, lab.RegistryOptions{
		Election: *election, Heartbeat: *heartbeat, TTL: *ttl, Trials: *trials, Seed: *seed,
		Project: "fflab", File: filepath.Join("lab", "compose.yml"),
	}, *out)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, tr := range res.Trials {
		fmt.Printf("%-16s %s held %v gone %v %s\n", tr.Variant, tr.Instance, tr.Held.Round(time.Millisecond), tr.Gone.Round(time.Millisecond), tr.Err)
	}
	for v, s := range res.Variants {
		fmt.Printf("%s: median %v (min %v, max %v) over %d kills\n", v, s.Median.Round(time.Millisecond), s.Min.Round(time.Millisecond), s.Max.Round(time.Millisecond), s.N)
	}
}

func bench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	experiments := fs.String("experiment", "e1b,e2,e3,e4,e5", "experiments to run, comma separated")
	label := fs.String("label", time.Now().Format("20060102")+"-laptop", "results go to bench/results/<experiment>/<label>")
	nodeCPUs := fs.String("node-cpus", "4,5,6,7,8,9,10", "one cpuset per node, comma separated")
	genCPUs := fs.String("gen-cpus", "12-15", "the generator's cpuset")
	quick := fs.Bool("quick", false, "one short configuration per experiment, to check the harness")
	only := fs.String("only", "", "run only the curves with these names, separated by semicolons")
	build := fs.Bool("build", false, "build the lab image first")
	fs.Parse(args)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *build {
		buildImage(ctx)
		if strings.Contains(*experiments, "e5") {
			buildEtcd(ctx)
		}
	}
	logf := func(format string, args ...any) {
		fmt.Printf("%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
	}
	for e := range strings.SplitSeq(*experiments, ",") {
		o := lab.ExperimentOptions{Out: filepath.Join("bench", "results", e, *label), NodeCPUs: strings.Split(*nodeCPUs, ","), GenCPUs: *genCPUs, Quick: *quick, Log: logf}
		if *only != "" {
			o.Only = strings.Split(*only, ";")
		}
		var err error
		switch e {
		case "e1b":
			_, err = lab.E1b(ctx, o)
		case "e2":
			_, err = lab.E2(ctx, o)
		case "e3":
			_, err = lab.E3(ctx, o)
		case "e4":
			_, err = lab.E4(ctx, o)
		case "e5":
			_, err = lab.E5(ctx, o)
		default:
			err = fmt.Errorf("unknown experiment %q", e)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, e+":", err)
			os.Exit(1)
		}
		logf("%s done: %s", e, o.Out)
	}
}

func buildEtcd(ctx context.Context) {
	for _, args := range [][]string{
		{"build", "-q", "-f", filepath.Join("bench", "etcd", "Dockerfile"), "-t", lab.EtcdGenImage, "."},
		{"pull", "-q", lab.EtcdImage},
	} {
		cmd := exec.CommandContext(ctx, "docker", args...)
		cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "etcd images:", err)
			os.Exit(1)
		}
	}
}

func buildImage(ctx context.Context) {
	cmd := exec.CommandContext(ctx, "docker", "build", "-q", "--build-arg", "LDFLAGS=-s -w -X main.plantable=yes", "-t", "fivefamilies-lab-node", ".")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "failover" {
		failover(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "registry" {
		registry(os.Args[2:])
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "bench" {
		bench(os.Args[2:])
		return
	}
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
		buildImage(ctx)
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
