package loadgen

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAQuantileInterpolatesBetweenTheTwoClosestSamples(t *testing.T) {
	ms := func(ns ...int) []time.Duration {
		out := make([]time.Duration, len(ns))
		for i, n := range ns {
			out[i] = time.Duration(n) * time.Millisecond
		}
		return out
	}
	hundred := make([]int, 100)
	for i := range hundred {
		hundred[i] = i + 1
	}
	for _, tc := range []struct {
		name   string
		sorted []time.Duration
		q      float64
		want   time.Duration
	}{
		{"the median of an even count is the mean of the middle two", ms(10, 20, 30, 40), 0.5, 25 * time.Millisecond},
		{"the median of an odd count is the middle one", ms(10, 20, 30), 0.5, 20 * time.Millisecond},
		{"q 0 is the smallest", ms(10, 20, 30), 0, 10 * time.Millisecond},
		{"q 1 is the largest", ms(10, 20, 30), 1, 30 * time.Millisecond},
		{"p99 of 1 to 100 ms", ms(hundred...), 0.99, 99010 * time.Microsecond},
		{"nothing", nil, 0.5, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Quantile(tc.sorted, tc.q); got != tc.want {
				t.Fatalf("%v, want %v", got, tc.want)
			}
		})
	}
}

type keyRecorder struct {
	mu      sync.Mutex
	written map[string]int
	refuse  bool
}

func (k *keyRecorder) Do(_ context.Context, op Op) error {
	if k.refuse {
		return errors.New("refused")
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if !op.Read {
		k.written[op.Key]++
	}
	return nil
}

func TestAMeasurementPreloadsEveryKeyAndCountsOnlyTheMeasuredWindow(t *testing.T) {
	k := &keyRecorder{written: map[string]int{}}
	probes := []Counters{{Fsyncs: 10, Entries: 20, Sent: 100, Elections: 1}, {Fsyncs: 30, Entries: 80, Sent: 300, Elections: 2}}
	var probed []time.Time
	probe := func(context.Context) ([]Counters, error) {
		probed = append(probed, time.Now())
		c := probes[0]
		probes = probes[1:]
		return []Counters{c}, nil
	}
	cfg := Config{Rate: 200, Duration: 100 * time.Millisecond, Warmup: 50 * time.Millisecond, ReadFraction: 1, Keys: 5, ValueBytes: 8, Preload: true}
	start := time.Now()
	r, err := Measure(context.Background(), k, cfg, probe)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if k.written[key(i)] != 1 {
			t.Fatalf("%s was written %d times before a read-only run, want once: %v", key(i), k.written[key(i)], k.written)
		}
	}
	if len(probed) != 2 || probed[0].Sub(start) < cfg.Warmup {
		t.Fatalf("counters read at %v after the start, want the first after the %v warmup", probed, cfg.Warmup)
	}
	want := Counters{Fsyncs: 20, Entries: 60, Sent: 200, Elections: 1}
	if r.Nodes != 1 || len(r.PerNode) != 1 || r.PerNode[0] != want || r.Elections != 1 || r.EntriesPerFsync != 3 {
		t.Fatalf("report %+v, want one node with %+v and 3 entries per fsync", r, want)
	}
	if ops := float64(r.Summary.All.N); ops == 0 || r.FsyncsPerOp != 20/ops || r.MsgsPerOp != 200/ops {
		t.Fatalf("%v fsyncs and %v messages per op over %v operations", r.FsyncsPerOp, r.MsgsPerOp, ops)
	}
}

func TestAPreloadThatNeverSucceedsNamesTheKey(t *testing.T) {
	cfg := Config{Rate: 200, Duration: 100 * time.Millisecond, Keys: 1, Timeout: time.Millisecond, Preload: true}
	_, err := Measure(context.Background(), &keyRecorder{refuse: true}, cfg, func(context.Context) ([]Counters, error) { return nil, nil })
	if err == nil || !strings.Contains(err.Error(), key(0)) {
		t.Fatalf("got %v, want the preload's failure for %s", err, key(0))
	}
}
