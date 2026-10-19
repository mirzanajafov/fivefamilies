package lab

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

type ExperimentOptions struct {
	Out      string
	NodeCPUs []string
	GenCPUs  string
	Quick    bool
	Only     []string
	Repeat   int
	Log      func(format string, args ...any)
}

func rounds[T any](o ExperimentOptions, specs []T, name func(T) string) iter.Seq2[int, T] {
	return func(yield func(int, T) bool) {
		var picked []T
		for _, sp := range specs {
			if o.wants(name(sp)) {
				picked = append(picked, sp)
			}
		}
		for r := range max(1, o.Repeat) {
			for i := range picked {
				if !yield(r+1, picked[(i+r)%len(picked)]) {
					return
				}
			}
		}
	}
}

func (o ExperimentOptions) wants(name string) bool {
	if len(o.Only) == 0 {
		return true
	}
	for _, w := range o.Only {
		if name == w {
			return true
		}
	}
	return false
}

func (o ExperimentOptions) sweep() SweepOptions {
	s := SweepOptions{Start: 500, Floor: 30, Factor: 2, Max: 64000, Refine: 2, Duration: 10 * time.Second, Warmup: 3 * time.Second, GenCPUs: o.GenCPUs, MaxP99: time.Second}
	if o.Quick {
		s.Start, s.Max, s.Refine, s.Duration, s.Warmup = 500, 1000, 0, 2*time.Second, time.Second
	}
	return s
}

type curveSpec struct {
	name  string
	nodes int
	node  []string
	gen   []string
}

var baseNode = []string{"-election", "1s", "-heartbeat", "100ms"}

func withBase(extra ...string) []string {
	return append(slices.Clone(baseNode), extra...)
}

func runCurves(ctx context.Context, o ExperimentOptions, file string, specs []curveSpec) ([]Curve, error) {
	if o.Quick {
		specs = specs[:1]
	}
	var curves []Curve
	for round, sp := range rounds(o, specs, func(sp curveSpec) string { return sp.name }) {
		b := &BenchCluster{Nodes: sp.nodes, Flags: sp.node, NodeCPUs: o.NodeCPUs}
		o.Log("%s, round %d: starting %d nodes", sp.name, round, sp.nodes)
		if err := b.Up(ctx); err != nil {
			b.Down()
			return curves, fmt.Errorf("%s: %w", sp.name, err)
		}
		c := b.Sweep(ctx, sp.name, o.sweep(), sp.gen)
		c.Round = round
		b.Down()
		o.Log("%s, round %d: knee %.0f/s over %d steps", sp.name, round, c.Knee, len(c.Steps))
		curves = append(curves, c)
		if err := SaveJSON(o.Out, file, curves); err != nil {
			return curves, err
		}
		if ctx.Err() != nil {
			return curves, ctx.Err()
		}
	}
	return curves, nil
}

func E2(ctx context.Context, o ExperimentOptions) ([]Curve, error) {
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	return runCurves(ctx, o, "e2.json", []curveSpec{
		{"5 nodes", 5, withBase(), writes},
		{"3 nodes", 3, withBase(), writes},
		{"7 nodes", 7, withBase(), writes},
		{"5 nodes, one entry per append", 5, withBase("-max-msg-bytes", "1"), writes},
		{"5 nodes, no pipelining", 5, withBase("-max-inflight", "1"), writes},
		{"5 nodes, one event per Ready", 5, withBase("-batch", "1"), writes},
		{"5 nodes, no cap on pending writes", 5, withBase("-max-pending", "0"), writes},
		{"5 nodes, no snapshots", 5, withBase("-snapshot-every", "1000000000"), writes},
		{"5 nodes, 2 appends in flight", 5, withBase("-max-inflight", "2"), writes},
		{"5 nodes, 4 appends in flight", 5, withBase("-max-inflight", "4"), writes},
		{"5 nodes, 16 appends in flight", 5, withBase("-max-inflight", "16"), writes},
		{"5 nodes, one Go processor", 5, withBase("-gomaxprocs", "1"), writes},
		{"5 nodes, 1 KiB values", 5, withBase(), []string{"-reads", "0", "-value-bytes", "1024"}},
	})
}

func E3(ctx context.Context, o ExperimentOptions) ([]Curve, error) {
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	return runCurves(ctx, o, "e3.json", []curveSpec{
		{"group commit", 5, withBase("-fsync", "group"), writes},
		{"one fsync per write", 5, withBase("-fsync", "ready"), writes},
		{"one fsync per entry", 5, withBase("-fsync", "entry"), writes},
		{"no fsync (unsafe)", 5, withBase("-fsync", "off"), writes},
		{"group commit, leader writes before sending", 5, withBase("-fsync", "group", "-write-before-send"), writes},
	})
}

func E4(ctx context.Context, o ExperimentOptions) ([]Curve, error) {
	lease := withBase("-lease", "800ms")
	var specs []curveSpec
	for _, share := range []string{"0.95", "0.5"} {
		for _, mode := range []string{"lease", "index", "follower", "log"} {
			specs = append(specs, curveSpec{fmt.Sprintf("%s reads, %s", mode, share), 5, lease, []string{"-reads", share, "-read-mode", mode}})
		}
		specs = append(specs, curveSpec{fmt.Sprintf("index reads, %s, one event per Ready", share), 5, append(slices.Clone(lease), "-batch", "1"), []string{"-reads", share, "-read-mode", "index"}})
		specs = append(specs, curveSpec{fmt.Sprintf("local reads (unsafe), %s", share), 5, append(slices.Clone(lease), "-bug", "followerread"), []string{"-reads", share, "-read-mode", "follower"}})
	}
	return runCurves(ctx, o, "e4.json", specs)
}

type FaultOutcome struct {
	Kind        string        `json:"kind"`
	Duration    time.Duration `json:"duration_ns"`
	Target      string        `json:"target"`
	Injections  int           `json:"injections"`
	WithElected int           `json:"followed_by_an_election"`
	Elections   uint64        `json:"elections"`
}

type E1bRow struct {
	Election    time.Duration  `json:"election_ns"`
	SyncStorage bool           `json:"sync_storage"`
	Faults      []FaultOutcome `json:"faults"`
}

func maxElections(sts []*pb.StatusResponse) uint64 {
	var m uint64
	for _, st := range sts {
		if st != nil {
			m = max(m, st.GetElections())
		}
	}
	return m
}

func agreedLeader(ctx context.Context, adm *cluster) (int, error) {
	var leader int
	err := poll(ctx, 100*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		leader = agreedOn(adm.statuses(ctx, time.Second))
		return leader > 0, nil
	})
	if err != nil {
		return 0, fmt.Errorf("the nodes did not agree on a leader: %w", err)
	}
	return leader, nil
}

func E1b(ctx context.Context, o ExperimentOptions) ([]E1bRow, error) {
	timeouts := []time.Duration{150 * time.Millisecond, 300 * time.Millisecond, 600 * time.Millisecond, time.Second}
	injections := 5
	type variant struct {
		t    time.Duration
		sync bool
	}
	var variants []variant
	for _, t := range timeouts {
		variants = append(variants, variant{t, false})
	}
	variants = append(variants, variant{300 * time.Millisecond, true}, variant{time.Second, true})
	if o.Quick {
		variants, injections = variants[:1], 1
	}
	rng := rand.New(rand.NewPCG(1, 2))
	var rows []E1bRow
	for _, v := range variants {
		flags := []string{"-election", v.t.String(), "-heartbeat", (v.t / 3).String(), "-chaos"}
		if v.sync {
			flags = append(flags, "-sync-storage")
		}
		b := &BenchCluster{Nodes: 5, Flags: flags, NodeCPUs: o.NodeCPUs}
		o.Log("e1b: election %v, sync storage %v", v.t, v.sync)
		if err := b.Up(ctx); err != nil {
			b.Down()
			return rows, err
		}
		adm, err := dialAdmin(b.addrs())
		if err != nil {
			b.Down()
			return rows, err
		}
		lctx, stopLoad := context.WithCancel(ctx)
		fctx, stopFaults := context.WithCancel(ctx)
		loadDone := make(chan error, 1)
		go func() {
			_, err := b.Generate(lctx, o.GenCPUs, "-rate", "300", "-duration", "1h", "-warmup", "0s", "-reads", "0", "-preload=false")
			loadDone <- err
			stopFaults()
		}()
		row := E1bRow{Election: v.t, SyncStorage: v.sync}
		type fault struct {
			kind, target string
			d            time.Duration
		}
		faults := []fault{
			{"stall-disk", "leader", 100 * time.Millisecond}, {"stall-disk", "leader", 300 * time.Millisecond}, {"stall-disk", "leader", time.Second},
			{"stall-disk", "follower", time.Second},
			{"pause", "leader", v.t / 2}, {"pause", "leader", v.t}, {"pause", "leader", 2 * v.t},
			{"pause", "follower", 2 * v.t},
		}
		if v.sync {
			faults = faults[:3]
		}
		if o.Quick {
			faults = faults[:1]
		}
		wait(fctx, 2*time.Second)
		for _, f := range faults {
			out := FaultOutcome{Kind: f.kind, Duration: f.d, Target: f.target}
			for range injections {
				leader, err := agreedLeader(fctx, adm)
				if err != nil {
					break
				}
				n := leader
				if f.target == "follower" {
					n = 1 + (leader+rng.IntN(4))%5
				}
				before := maxElections(adm.statuses(fctx, time.Second))
				cctx, cancel := context.WithTimeout(fctx, 5*time.Second)
				_, err = adm.admin(n).Chaos(cctx, &pb.ChaosRequest{Action: f.kind, DurationMs: uint64(f.d / time.Millisecond)})
				cancel()
				if err != nil {
					o.Log("chaos on %d: %v", n, err)
					continue
				}
				wait(fctx, f.d+4*v.t+time.Second)
				after := maxElections(adm.statuses(fctx, time.Second))
				out.Injections++
				if after > before {
					out.WithElected++
					out.Elections += after - before
				}
			}
			if fctx.Err() != nil {
				break
			}
			o.Log("  %s %s %v: %d of %d followed by an election", f.kind, f.target, f.d, out.WithElected, out.Injections)
			row.Faults = append(row.Faults, out)
		}
		generatorDied := fctx.Err() != nil
		stopLoad()
		loadErr := <-loadDone
		stopFaults()
		adm.close()
		b.Down()
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		if generatorDied {
			return rows, fmt.Errorf("the load generator stopped before the faults were over, so the row would describe an idle cluster: %w", cmp.Or(loadErr, errors.New("it exited")))
		}
		rows = append(rows, row)
		if err := SaveJSON(o.Out, "e1b.json", rows); err != nil {
			return rows, err
		}
	}
	return rows, nil
}
