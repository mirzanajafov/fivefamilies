package lab

import (
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

func TestTheWidestGapBetweenAcknowledgedWrites(t *testing.T) {
	ms := func(n int64) int64 { return n * int64(time.Millisecond) }
	samples := []loadgen.FailoverSample{
		{Done: ms(10), Ok: true},
		{Done: ms(15), Ok: true},
		{Done: ms(400), Ok: false},
		{Done: ms(20), Ok: true},
		{Done: ms(900), Ok: true},
		{Done: ms(905), Ok: true},
		{Done: ms(2000), Ok: true},
	}
	if got := gaps(samples, ms(0), ms(800)); got != 880*time.Millisecond {
		t.Fatalf("widest gap %v, want 880ms (20 to 900; failures don't count)", got)
	}
	if got := gaps(samples, ms(0), ms(100)); got != 880*time.Millisecond {
		t.Fatalf("a gap that starts inside the window counts in full: %v", got)
	}
	if got := gaps(samples, ms(950), ms(1500)); got != 1095*time.Millisecond {
		t.Fatalf("a gap that ends after the window starts counts: %v", got)
	}
	if got := gaps(nil, 0, ms(10)); got != 0 {
		t.Fatalf("no samples: %v", got)
	}
}
