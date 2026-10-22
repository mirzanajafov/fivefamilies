package bench

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/rig"
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

func rounds(repeat int, specs []curveSpec) iter.Seq2[int, curveSpec] {
	return func(yield func(int, curveSpec) bool) {
		for r := range max(1, repeat) {
			for i := range specs {
				if !yield(r+1, specs[(i+r)%len(specs)]) {
					return
				}
			}
		}
	}
}

func (o ExperimentOptions) sweep() sweepOptions {
	s := sweepOptions{Start: 500, Floor: 30, Factor: 2, Max: 64000, Refine: 2, Duration: 10 * time.Second, Warmup: 3 * time.Second, GenCPUs: o.GenCPUs, MaxP99: time.Second}
	if o.Quick {
		s.Start, s.Max, s.Refine, s.Duration, s.Warmup = 500, 1000, 0, 2*time.Second, time.Second
	}
	return s
}

func (o ExperimentOptions) tearDown(name string, down func() error) {
	if err := down(); err != nil {
		o.Log("%s: tearing down: %v", name, err)
	}
}

type curveSpec struct {
	name       string
	nodes      int
	node       []string
	gen        []string
	etcd       bool
	procs      int
	leaderOnly bool
}

type system interface {
	sweepable
	Down() error
}

func (sp curveSpec) start(ctx context.Context, o ExperimentOptions) (system, []string, error) {
	if !sp.etcd {
		b := &ffCluster{Nodes: sp.nodes, Flags: sp.node, NodeCPUs: o.NodeCPUs}
		return b, sp.gen, b.Up(ctx)
	}
	e := &etcdCluster{Nodes: sp.nodes, NodeCPUs: o.NodeCPUs, Procs: sp.procs}
	if err := e.Up(ctx); err != nil || !sp.leaderOnly {
		return e, sp.gen, err
	}
	l, err := e.Leader(ctx)
	return e, append([]string{"-only", l}, sp.gen...), err
}

var baseNode = []string{"-election", "1s", "-heartbeat", "100ms"}

func withBase(extra ...string) []string {
	return append(slices.Clone(baseNode), extra...)
}

func (o ExperimentOptions) pick(specs []curveSpec) []curveSpec {
	var picked []curveSpec
	for _, sp := range specs {
		chosen := len(o.Only) == 0 || slices.Contains(o.Only, sp.name)
		systemSeen := slices.ContainsFunc(picked, func(p curveSpec) bool { return p.etcd == sp.etcd })
		if chosen && (!o.Quick || !systemSeen) {
			picked = append(picked, sp)
		}
	}
	return picked
}

var curveExperiments = map[string]func() []curveSpec{"e2": e2Specs, "e3": e3Specs, "e4": e4Specs, "e5": e5Specs}

func CurveNames(experiment string) []string {
	specs, ok := curveExperiments[experiment]
	if !ok {
		return nil
	}
	var names []string
	for _, sp := range specs() {
		names = append(names, sp.name)
	}
	return names
}

func Curves(ctx context.Context, o ExperimentOptions, experiment string) ([]Curve, error) {
	specs, ok := curveExperiments[experiment]
	if !ok {
		return nil, fmt.Errorf("no curve experiment %q", experiment)
	}
	return runCurves(ctx, o, experiment+".json", specs())
}

func runCurves(ctx context.Context, o ExperimentOptions, file string, specs []curveSpec) ([]Curve, error) {
	var curves []Curve
	for round, sp := range rounds(o.Repeat, o.pick(specs)) {
		o.Log("%s, round %d: starting %d nodes", sp.name, round, sp.nodes)
		sys, gen, err := sp.start(ctx, o)
		if err != nil {
			o.tearDown(sp.name, sys.Down)
			return curves, fmt.Errorf("%s: %w", sp.name, err)
		}
		c, err := sweep(ctx, sys, Curve{Name: sp.name, Nodes: sp.nodes, NodeFlags: sp.node, GenFlags: gen}, o.sweep())
		c.Round = round
		cut := ctx.Err()
		o.tearDown(sp.name, sys.Down)
		if err = cmp.Or(cut, err); err != nil {
			return curves, fmt.Errorf("%s: %w", sp.name, err)
		}
		o.Log("%s, round %d: knee %.0f/s over %d steps", sp.name, round, c.Knee, len(c.Steps))
		curves = append(curves, c)
		if err := rig.SaveJSON(o.Out, file, curves); err != nil {
			return curves, err
		}
	}
	return curves, nil
}

func e2Specs() []curveSpec {
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	return []curveSpec{
		{name: "5 nodes", nodes: 5, node: withBase(), gen: writes},
		{name: "3 nodes", nodes: 3, node: withBase(), gen: writes},
		{name: "7 nodes", nodes: 7, node: withBase(), gen: writes},
		{name: "5 nodes, one entry per append", nodes: 5, node: withBase("-max-msg-bytes", "1"), gen: writes},
		{name: "5 nodes, no pipelining", nodes: 5, node: withBase("-max-inflight", "1"), gen: writes},
		{name: "5 nodes, one event per Ready", nodes: 5, node: withBase("-batch", "1"), gen: writes},
		{name: "5 nodes, no cap on pending writes", nodes: 5, node: withBase("-max-pending", "0"), gen: writes},
		{name: "5 nodes, no snapshots", nodes: 5, node: withBase("-snapshot-every", "1000000000"), gen: writes},
		{name: "5 nodes, 2 appends in flight", nodes: 5, node: withBase("-max-inflight", "2"), gen: writes},
		{name: "5 nodes, 4 appends in flight", nodes: 5, node: withBase("-max-inflight", "4"), gen: writes},
		{name: "5 nodes, 16 appends in flight", nodes: 5, node: withBase("-max-inflight", "16"), gen: writes},
		{name: "5 nodes, one Go processor", nodes: 5, node: withBase("-gomaxprocs", "1"), gen: writes},
		{name: "5 nodes, 1 KiB values", nodes: 5, node: withBase(), gen: []string{"-reads", "0", "-value-bytes", "1024"}},
	}
}

func e3Specs() []curveSpec {
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	return []curveSpec{
		{name: "group commit", nodes: 5, node: withBase("-fsync", "group"), gen: writes},
		{name: "one fsync per write", nodes: 5, node: withBase("-fsync", "ready"), gen: writes},
		{name: "one fsync per entry", nodes: 5, node: withBase("-fsync", "entry"), gen: writes},
		{name: "no fsync (unsafe)", nodes: 5, node: withBase("-fsync", "off"), gen: writes},
		{name: "group commit, leader writes before sending", nodes: 5, node: withBase("-fsync", "group", "-write-before-send"), gen: writes},
	}
}

func e4Specs() []curveSpec {
	lease := withBase("-lease", "800ms")
	var specs []curveSpec
	for _, share := range []string{"0.95", "0.5"} {
		for _, mode := range []string{"lease", "index", "follower", "log"} {
			specs = append(specs, curveSpec{name: fmt.Sprintf("%s reads, %s", mode, share), nodes: 5, node: lease, gen: []string{"-reads", share, "-read-mode", mode}})
		}
		specs = append(specs, curveSpec{name: fmt.Sprintf("index reads, %s, one event per Ready", share), nodes: 5, node: append(slices.Clone(lease), "-batch", "1"), gen: []string{"-reads", share, "-read-mode", "index"}})
		specs = append(specs, curveSpec{name: fmt.Sprintf("local reads (unsafe), %s", share), nodes: 5, node: append(slices.Clone(lease), "-bug", "followerread"), gen: []string{"-reads", share, "-read-mode", "follower"}})
	}
	return specs
}

func e5Specs() []curveSpec {
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	return []curveSpec{
		{name: "fivefamilies, 3 nodes, writes", nodes: 3, node: withBase(), gen: writes},
		{name: "etcd, 3 nodes, writes", nodes: 3, gen: writes, etcd: true},
		{name: "fivefamilies, 5 nodes, writes", nodes: 5, node: withBase(), gen: writes},
		{name: "etcd, 5 nodes, writes", nodes: 5, gen: writes, etcd: true},
		{name: "etcd, 5 nodes, writes, client on the leader only", nodes: 5, gen: writes, etcd: true, leaderOnly: true},
		{name: "etcd, 5 nodes, writes, two Go processors", nodes: 5, gen: writes, etcd: true, procs: 2},
		{name: "fivefamilies, 5 nodes, ReadIndex reads 0.95", nodes: 5, node: withBase(), gen: []string{"-reads", "0.95", "-read-mode", "index"}},
		{name: "etcd, 5 nodes, linearizable reads 0.95", nodes: 5, gen: []string{"-reads", "0.95", "-read-mode", "linearizable"}, etcd: true},
		{name: "fivefamilies, 5 nodes, local reads (unsafe) 0.95", nodes: 5, node: withBase("-bug", "followerread"), gen: []string{"-reads", "0.95", "-read-mode", "follower"}},
		{name: "etcd, 5 nodes, serializable reads 0.95", nodes: 5, gen: []string{"-reads", "0.95", "-read-mode", "serializable"}, etcd: true},
	}
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

func faultName(a pb.ChaosAction) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(a.String(), "CHAOS_ACTION_"), "_", "-"))
}

type E1bResult struct {
	Rows []E1bRow `json:"rows"`
	Err  string   `json:"error,omitempty"`
}

func E1b(ctx context.Context, o ExperimentOptions) (E1bResult, error) {
	res := E1bResult{Rows: []E1bRow{}}
	err := runE1b(ctx, o, func(row E1bRow) error {
		res.Rows = append(res.Rows, row)
		return rig.SaveJSON(o.Out, "e1b.json", res)
	})
	if err == nil {
		return res, nil
	}
	err = cmp.Or(ctx.Err(), err)
	if len(res.Rows) > 0 {
		res.Err = err.Error()
		if serr := rig.SaveJSON(o.Out, "e1b.json", res); serr != nil {
			return res, errors.Join(err, serr)
		}
	}
	return res, err
}

type e1bVariant struct {
	t    time.Duration
	sync bool
}

type e1bFault struct {
	action pb.ChaosAction
	target string
	d      time.Duration
}

func (v e1bVariant) faults() []e1bFault {
	stall, pause := pb.ChaosAction_CHAOS_ACTION_STALL_DISK, pb.ChaosAction_CHAOS_ACTION_PAUSE
	faults := []e1bFault{
		{stall, "leader", 100 * time.Millisecond}, {stall, "leader", 300 * time.Millisecond}, {stall, "leader", time.Second},
		{stall, "follower", time.Second},
		{pause, "leader", v.t / 2}, {pause, "leader", v.t}, {pause, "leader", 2 * v.t},
		{pause, "follower", 2 * v.t},
	}
	if v.sync {
		return faults[:3]
	}
	return faults
}

func runE1b(ctx context.Context, o ExperimentOptions, add func(E1bRow) error) error {
	var variants []e1bVariant
	for _, t := range []time.Duration{150 * time.Millisecond, 300 * time.Millisecond, 600 * time.Millisecond, time.Second} {
		variants = append(variants, e1bVariant{t, false})
	}
	variants = append(variants, e1bVariant{300 * time.Millisecond, true}, e1bVariant{time.Second, true})
	injections := 5
	if o.Quick {
		variants, injections = variants[:1], 1
	}
	r := &e1bRun{o: o, injections: injections, rng: rand.New(rand.NewPCG(1, 2))}
	for _, v := range variants {
		row, err := r.measure(ctx, v)
		if err != nil {
			return err
		}
		if err := add(row); err != nil {
			return err
		}
	}
	return nil
}

type e1bRun struct {
	o          ExperimentOptions
	injections int
	rng        *rand.Rand
}

func (r *e1bRun) measure(ctx context.Context, v e1bVariant) (E1bRow, error) {
	row := E1bRow{Election: v.t, SyncStorage: v.sync}
	flags := []string{"-election", v.t.String(), "-heartbeat", (v.t / 3).String(), "-chaos"}
	if v.sync {
		flags = append(flags, "-sync-storage")
	}
	b := &ffCluster{Nodes: 5, Flags: flags, NodeCPUs: r.o.NodeCPUs}
	r.o.Log("e1b: election %v, sync storage %v", v.t, v.sync)
	defer r.o.tearDown("e1b", b.Down)
	if err := b.Up(ctx); err != nil {
		return row, err
	}
	lctx, stopLoad := context.WithCancel(ctx)
	loadDone := make(chan error, 1)
	go func() {
		_, err := b.Generate(lctx, r.o.GenCPUs, "-rate", "300", "-duration", "1h", "-warmup", "0s", "-reads", "0", "-preload=false")
		loadDone <- err
		stopLoad()
	}()
	faults := v.faults()
	if r.o.Quick {
		faults = faults[:1]
	}
	rig.Wait(lctx, 2*time.Second)
	for _, f := range faults {
		out := r.inject(lctx, b.adm, v, f)
		if lctx.Err() != nil {
			break
		}
		r.o.Log("  %s %s %v: %d of %d followed by an election", out.Kind, f.target, f.d, out.WithElected, out.Injections)
		row.Faults = append(row.Faults, out)
	}
	generatorDied := lctx.Err() != nil
	stopLoad()
	loadErr := <-loadDone
	if err := ctx.Err(); err != nil {
		return row, err
	}
	if generatorDied {
		return row, fmt.Errorf("the load generator stopped before the faults were over: %w", cmp.Or(loadErr, errors.New("it exited without an error")))
	}
	return row, nil
}

func (r *e1bRun) inject(ctx context.Context, adm *rig.Admin, v e1bVariant, f e1bFault) FaultOutcome {
	out := FaultOutcome{Kind: faultName(f.action), Duration: f.d, Target: f.target}
	for range r.injections {
		leader, err := adm.Agreed(ctx)
		if err != nil {
			break
		}
		n := leader
		if f.target == "follower" {
			n = 1 + (leader+r.rng.IntN(4))%5
		}
		before := maxElections(adm.Statuses(ctx, time.Second))
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err = adm.Client(n).Chaos(cctx, &pb.ChaosRequest{Action: f.action, DurationMs: uint64(f.d / time.Millisecond)})
		cancel()
		if err != nil {
			r.o.Log("chaos on %d: %v", n, err)
			continue
		}
		rig.Wait(ctx, f.d+4*v.t+time.Second)
		after := maxElections(adm.Statuses(ctx, time.Second))
		out.Injections++
		if after > before {
			out.WithElected++
			out.Elections += after - before
		}
	}
	return out
}
