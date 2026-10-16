package lab

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

func TestTheSweepStopsAtTheKneeAndMeasuresBelowIt(t *testing.T) {
	var rates []string
	gen := func(ctx context.Context, cpus string, args ...string) (loadgen.Report, error) {
		var rate float64
		fmt.Sscan(args[1], &rate)
		rates = append(rates, args[1])
		s := loadgen.Summary{Valid: true, Achieved: min(rate, 3000)}
		s.All.P99 = 10 * time.Millisecond
		return loadgen.Report{Summary: s}, nil
	}
	c := sweep(context.Background(), gen, Curve{Name: "fake"}, SweepOptions{Start: 500, Factor: 2, Max: 64000, MaxP99: time.Second})
	if c.Knee != 2000 {
		t.Fatalf("knee %.0f: 4000/s achieved only 3000, so the last rate that kept up was 2000", c.Knee)
	}
	if got := strings.Join(rates, " "); got != "500 1000 2000 4000 1000 1600" {
		t.Fatalf("rates run: %s", got)
	}
	if c.At50 == nil || c.At80 == nil || c.At80.Rate != 1600 {
		t.Fatalf("fixed-share runs: %+v %+v", c.At50, c.At80)
	}
}

func TestAnInvalidOrSlowStepCountsAsSaturated(t *testing.T) {
	ok := Step{Rate: 1000, Report: loadgen.Report{Summary: loadgen.Summary{Valid: true, Achieved: 1000}}}
	if saturated(ok, time.Second) {
		t.Fatal("a step that kept up is saturated")
	}
	invalid := ok
	invalid.Report.Summary.Valid = false
	slow := ok
	slow.Report.Summary.All.P99 = 2 * time.Second
	failed := ok
	failed.Err = "generator crashed"
	for _, s := range []Step{invalid, slow, failed} {
		if !saturated(s, time.Second) {
			t.Fatalf("%+v is not saturated", s)
		}
	}
}

func TestTheGeneratedComposeFilesPinEveryNode(t *testing.T) {
	b := &BenchCluster{Nodes: 3, Flags: []string{"-fsync", "entry"}, NodeCPUs: []string{"4", "5", "6"}, Image: "img"}
	y := b.compose()
	for _, want := range []string{`cpuset: "6"`, `"127.0.0.1:3153:3161"`, `"-cluster", "1=n1:3161,2=n2:3161,3=n3:3161"`, `"-fsync", "entry"`} {
		if !strings.Contains(y, want) {
			t.Fatalf("missing %s in\n%s", want, y)
		}
	}
	e := &EtcdCluster{Nodes: 3, NodeCPUs: []string{"4", "5", "6"}}
	y = e.compose()
	for _, want := range []string{EtcdImage, `"--heartbeat-interval", "100"`, `"--election-timeout", "1000"`, `"e1=http://e1:2380,e2=http://e2:2380,e3=http://e3:2380"`, `cpuset: "5"`} {
		if !strings.Contains(y, want) {
			t.Fatalf("missing %s in\n%s", want, y)
		}
	}
}
