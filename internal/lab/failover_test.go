package lab

import (
	"reflect"
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

func TestATrialTimesTheElectionFromTheKilledLeaderGoingQuiet(t *testing.T) {
	ms := func(n int64) int64 { return n * int64(time.Millisecond) }
	samples := []loadgen.FailoverSample{
		{Done: ms(100), Node: 2, Ok: true},
		{Done: ms(400), Node: 2, Ok: true},
		{Done: ms(900), Node: 2, Ok: true},
		{Kind: loadgen.SampleUnreachable, Done: ms(1000), Node: 3},
		{Kind: loadgen.SampleLeader, Done: ms(1050), Leader: 4},
		{Kind: loadgen.SampleUnreachable, Done: ms(1100), Node: 2},
		{Kind: loadgen.SampleUnreachable, Done: ms(1150), Node: 2},
		{Done: ms(1200), Node: 2, Err: "unavailable"},
		{Kind: loadgen.SampleLeader, Done: ms(1400), Leader: 4},
		{Kind: loadgen.SampleLeader, Done: ms(1500), Leader: 5},
		{Done: ms(1600), Node: 4, Ok: true},
		{Done: ms(1700), Node: 4, Ok: true},
	}
	got := measureTrial(samples, 2, FailoverOptions{Before: time.Second, After: 2 * time.Second})
	want := Trial{
		Killed: 2, Requests: 6, Failed: 1, Errors: map[string]int{"unavailable": 1},
		Detected: 1100 * time.Millisecond, NewLeader: 1400 * time.Millisecond, Election: 300 * time.Millisecond,
		QuietGap: 500 * time.Millisecond, Unavailable: 700 * time.Millisecond,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
