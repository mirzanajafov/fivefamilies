package check

import (
	"testing"
	"time"
)

func TestAnUnfinishedWriteMayTakeEffectAnyTimeAfterItsCallAndAnUnfinishedReadIsDropped(t *testing.T) {
	ops := []Op{
		{Client: 0, Input: put("k", "a"), Output: ok(true), Call: 1, Return: 2},
		{Client: 1, Input: cas("k", "a", "b"), Output: unknown(), Call: 3, Return: 4},
		{Client: 2, Input: get("k"), Output: unknown(), Call: 5, Return: 6},
		{Client: 2, Input: get("k"), Output: value("b"), Call: 7, Return: 8},
	}
	hist, unfinished := History(ops)
	if len(hist) != 3 || unfinished != 1 {
		t.Fatalf("%d operations with %d unfinished writes, want the put, the swap and the last read with one", len(hist), unfinished)
	}
	if hist[1].Return != never || hist[1].Call != 3 {
		t.Fatalf("the unfinished swap runs from %d to %d, want from 3 to the end of time", hist[1].Call, hist[1].Return)
	}
	if v := Linearizable(hist, 10*time.Second); !v.OK() {
		t.Fatalf("verdict %s, want the swap placed before the read that saw it", v.Result)
	}
}
