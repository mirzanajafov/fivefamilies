package loadgen

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

type fake struct {
	delay  time.Duration
	reads  atomic.Int64
	writes atomic.Int64
	fail   bool
}

func (f *fake) Do(ctx context.Context, op Op) error {
	if op.Read {
		f.reads.Add(1)
	} else {
		f.writes.Add(1)
		if len(op.Value) != 128 {
			return errors.New("wrong value size")
		}
	}
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	if f.fail {
		return errors.New("refused")
	}
	return nil
}

func TestAnOpenLoopRunKeepsItsRateAndMeasuresFromTheSchedule(t *testing.T) {
	f := &fake{delay: 5 * time.Millisecond}
	s, err := Run(context.Background(), f, Config{Rate: 1000, Duration: 500 * time.Millisecond, Warmup: 100 * time.Millisecond, ReadFraction: 0.25, ValueBytes: 128, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(s.Achieved-1000) > 100 {
		t.Fatalf("achieved %.0f/s, offered 1000/s", s.Achieved)
	}
	if s.All.P50 < 5*time.Millisecond || s.All.P50 > 15*time.Millisecond {
		t.Fatalf("p50 %v for a target that takes 5 ms", s.All.P50)
	}
	share := float64(s.Reads.N) / float64(s.All.N)
	if share < 0.18 || share > 0.32 {
		t.Fatalf("read share %.2f, asked for 0.25", share)
	}
	if s.Scheduled < 450 || s.Scheduled > 550 {
		t.Fatalf("%d requests measured in a 500 ms window at 1000/s (the warmup must not count)", s.Scheduled)
	}
}

func TestErrorsAndTimeoutsAreCountedNotTimed(t *testing.T) {
	f := &fake{delay: 50 * time.Millisecond}
	s, _ := Run(context.Background(), f, Config{Rate: 200, Duration: 200 * time.Millisecond, Timeout: 10 * time.Millisecond, ValueBytes: 128})
	if s.TimedOut == 0 || s.All.N != 0 || s.Errors["timeout"] != s.TimedOut {
		t.Fatalf("timed out %d, timed %d, errors %v", s.TimedOut, s.All.N, s.Errors)
	}
	f = &fake{fail: true}
	s, _ = Run(context.Background(), f, Config{Rate: 200, Duration: 200 * time.Millisecond, ValueBytes: 128})
	if s.Errors["refused"] == 0 || s.Achieved != 0 {
		t.Fatalf("errors %v, achieved %.0f", s.Errors, s.Achieved)
	}
}

func TestARunThatHitsTheInFlightLimitIsMarkedInvalid(t *testing.T) {
	f := &fake{delay: 100 * time.Millisecond}
	s, _ := Run(context.Background(), f, Config{Rate: 1000, Duration: 200 * time.Millisecond, MaxInflight: 10, ValueBytes: 128})
	if s.Dropped == 0 || s.Valid {
		t.Fatalf("dropped %d, valid %v", s.Dropped, s.Valid)
	}
}
