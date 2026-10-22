package lab

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

func TestFaultNamesAreTheKeysInTheCommittedResults(t *testing.T) {
	for action, want := range map[pb.ChaosAction]string{
		pb.ChaosAction_CHAOS_ACTION_STALL_DISK: "stall-disk",
		pb.ChaosAction_CHAOS_ACTION_PAUSE:      "pause",
	} {
		if got := faultName(action); got != want {
			t.Errorf("%v is recorded as %q, want %q", action, got, want)
		}
	}
}

type fakeCluster struct {
	capacity float64
	rates    []string
	quiets   int
	busy     bool
}

func (f *fakeCluster) Generate(_ context.Context, _ string, args ...string) (loadgen.Report, error) {
	rate, err := strconv.ParseFloat(args[1], 64)
	if err != nil {
		return loadgen.Report{}, err
	}
	f.rates = append(f.rates, args[1])
	s := loadgen.Summary{Valid: true, Achieved: min(rate, f.capacity)}
	s.All.P99 = 10 * time.Millisecond
	return loadgen.Report{Summary: s}, nil
}

func (f *fakeCluster) Quiet(context.Context) error {
	f.quiets++
	if f.busy {
		return errors.New("still busy")
	}
	return nil
}

func (f *fakeCluster) sweep(t *testing.T, opts SweepOptions) Curve {
	t.Helper()
	opts.Factor, opts.Max, opts.MaxP99 = 2, 64000, time.Second
	c, err := sweep(context.Background(), f, Curve{Name: "fake"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestTheSweepStopsAtTheKneeAndMeasuresBelowIt(t *testing.T) {
	f := &fakeCluster{capacity: 3000}
	c := f.sweep(t, SweepOptions{Start: 500})
	if c.Knee != 2000 {
		t.Fatalf("knee %.0f: 4000/s achieved only 3000, so the last rate that kept up was 2000", c.Knee)
	}
	if got := strings.Join(f.rates, " "); got != "500 1000 2000 4000 1000 1600" {
		t.Fatalf("rates run: %s", got)
	}
	if c.At50 == nil || c.At80 == nil || c.At80.Rate != 1600 {
		t.Fatalf("fixed-share runs: %+v %+v", c.At50, c.At80)
	}
	if f.quiets != len(f.rates) {
		t.Fatalf("%d steps but the cluster was let settle %d times", len(f.rates), f.quiets)
	}
}

func TestTheSweepNarrowsTheKneeBetweenTheLastGoodAndFirstBadRate(t *testing.T) {
	f := &fakeCluster{capacity: 3000}
	c := f.sweep(t, SweepOptions{Start: 500, Refine: 2})
	if got := strings.Join(f.rates, " "); got != "500 1000 2000 4000 3000 3500 1500 2400" {
		t.Fatalf("rates run: %s", got)
	}
	if c.Knee != 3000 {
		t.Fatalf("knee %.0f", c.Knee)
	}
}

func TestTheSweepStepsDownWhenTheFirstRateIsAlreadyTooMuch(t *testing.T) {
	f := &fakeCluster{capacity: 100}
	c := f.sweep(t, SweepOptions{Start: 500, Floor: 30, Refine: 2})
	if got := strings.Join(f.rates, " "); got != "500 250 125 62.5 94 110 55 88" {
		t.Fatalf("rates run: %s", got)
	}
	if c.Knee != 100 {
		t.Fatalf("knee %.0f", c.Knee)
	}
	f = &fakeCluster{capacity: 10}
	c = f.sweep(t, SweepOptions{Start: 500, Floor: 30, Refine: 2})
	if got := strings.Join(f.rates, " "); got != "500 250 125 62.5 31.25" || c.Knee != 0 || c.At80 != nil {
		t.Fatalf("below the floor: rates %s, knee %.0f, at 80%% %+v", got, c.Knee, c.At80)
	}
}

func TestTheSweepStopsWhenTheClusterNeverSettles(t *testing.T) {
	f := &fakeCluster{capacity: 3000, busy: true}
	c := f.sweep(t, SweepOptions{Start: 500, Floor: 30, Refine: 2})
	if len(f.rates) != 0 || len(c.Steps) != 1 || c.Steps[0].Err == "" || c.Knee != 0 || c.At80 != nil {
		t.Fatalf("rates %v, steps %+v", f.rates, c.Steps)
	}
}

func TestAnInvalidOrSlowStepCountsAsSaturated(t *testing.T) {
	ok := Step{Rate: 1000, Report: loadgen.Report{Summary: loadgen.Summary{Valid: true, Achieved: 1000, Scheduled: 10000}}}
	ok.Report.Summary.All.Error = 100
	if saturated(ok, time.Second) {
		t.Fatal("a step that kept up is saturated")
	}
	invalid := ok
	invalid.Report.Summary.Valid = false
	slow := ok
	slow.Report.Summary.All.P99 = 2 * time.Second
	failed := ok
	failed.Err = "generator crashed"
	erring := ok
	erring.Report.Summary.All.Error = 101
	for _, s := range []Step{invalid, slow, failed, erring} {
		if !saturated(s, time.Second) {
			t.Fatalf("%+v is not saturated", s)
		}
	}
}

func TestTheGeneratedComposeFilesPinEveryNode(t *testing.T) {
	b := &BenchCluster{Nodes: 3, Flags: []string{"-fsync", "entry"}, NodeCPUs: []string{"4", "5", "6"}, Image: "img"}
	y := composeFile(b.services())
	for _, want := range []string{`cpuset: "6"`, `"127.0.0.1:3153:3161"`, `"-cluster", "1=n1:3161,2=n2:3161,3=n3:3161"`, `"-fsync", "entry"`} {
		if !strings.Contains(y, want) {
			t.Fatalf("missing %s in\n%s", want, y)
		}
	}
	e := &EtcdCluster{Nodes: 3, NodeCPUs: []string{"4", "5", "6"}, Procs: 2}
	y = composeFile(e.services())
	for _, want := range []string{EtcdImage, `"--heartbeat-interval", "100"`, `"--election-timeout", "1000"`, `"e1=http://e1:2380,e2=http://e2:2380,e3=http://e3:2380"`, `cpuset: "5"`, `GOMAXPROCS=2`} {
		if !strings.Contains(y, want) {
			t.Fatalf("missing %s in\n%s", want, y)
		}
	}
}

func TestTheClusterIsQuietOnlyWhenIdleTwiceAtTheSameIndex(t *testing.T) {
	seen := []struct {
		at   uint64
		idle bool
	}{{3, false}, {5, true}, {6, true}, {6, false}, {6, true}, {6, true}}
	n := 0
	err := settle(context.Background(), func(context.Context) (uint64, bool) {
		s := seen[n]
		n++
		return s.at, s.idle
	})
	if err != nil || n != len(seen) {
		t.Fatalf("settled after %d probes: %v", n, err)
	}
}

func TestRepeatsAreInterleavedAndRotated(t *testing.T) {
	var got []string
	for r, sp := range rounds(3, []curveSpec{{name: "a"}, {name: "b"}, {name: "c"}}) {
		got = append(got, fmt.Sprint(r, sp.name))
	}
	if s := strings.Join(got, " "); s != "1a 1b 1c 2b 2c 2a 3c 3a 3b" {
		t.Fatalf("order %s", s)
	}
}

func TestAQuickRunTakesTheFirstOfTheChosenCurvesOfEachSystem(t *testing.T) {
	specs := []curveSpec{{name: "ff a"}, {name: "etcd a", etcd: true}, {name: "ff b"}, {name: "etcd b", etcd: true}}
	names := func(specs []curveSpec) string {
		var out []string
		for _, sp := range specs {
			out = append(out, sp.name)
		}
		return strings.Join(out, ", ")
	}
	for _, tc := range []struct {
		o    ExperimentOptions
		want string
	}{
		{ExperimentOptions{}, "ff a, etcd a, ff b, etcd b"},
		{ExperimentOptions{Quick: true}, "ff a, etcd a"},
		{ExperimentOptions{Quick: true, Only: []string{"ff b", "etcd b"}}, "ff b, etcd b"},
		{ExperimentOptions{Only: []string{"etcd b"}}, "etcd b"},
	} {
		if got := names(tc.o.pick(specs)); got != tc.want {
			t.Errorf("quick %v, only %v: picked %s, want %s", tc.o.Quick, tc.o.Only, got, tc.want)
		}
	}
}

func TestAReportFromAnOlderGeneratorIsRefused(t *testing.T) {
	if _, err := decodeReport(`{"summary": {"config": {"rate": 500}}, "nodes": 5}`); err != nil {
		t.Fatalf("a current report: %v", err)
	}
	_, err := decodeReport(`{"summary": {"config": {"Rate": 500, "Duration": 10000000000}}, "per_node_delta": [{"Batches": 3}], "entries_per_fsync": 2}`)
	if !errors.Is(err, errUnreadableReport) {
		t.Fatalf("a report with the old field names came back as %v, want it refused", err)
	}
}

func TestEveryCurveExperimentNamesItsCurves(t *testing.T) {
	for _, e := range []string{"e2", "e3", "e4", "e5"} {
		names := CurveNames(e)
		if len(names) == 0 || len(slices.Compact(slices.Sorted(slices.Values(names)))) != len(names) {
			t.Errorf("%s names %v, want at least one curve and no name twice", e, names)
		}
	}
	if CurveNames("e1b") != nil {
		t.Error("e1b has no curves")
	}
}
