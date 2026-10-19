package main

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/sim"
)

func TestASeedFailsWhenTheCaughtCheckFiresOrAnotherStopsTheRun(t *testing.T) {
	clean := sim.Result{}
	muted := sim.Result{Detected: map[sim.Category]string{sim.CatSafety: "muted"}}
	caught := sim.Result{Detected: map[sim.Category]string{sim.CatSafety: "muted", sim.CatLinearizability: "not linearizable"}}
	stopped := sim.Result{Category: sim.CatPanic, Violation: "boom", Detected: map[sim.Category]string{sim.CatPanic: "boom"}}
	for _, c := range []struct {
		name    string
		r       sim.Result
		catch   sim.Category
		cat     sim.Category
		failing bool
	}{
		{"a clean run", clean, "", "", false},
		{"a stopped run", stopped, "", sim.CatPanic, true},
		{"a muted violation with nothing caught", muted, sim.CatLinearizability, "", false},
		{"the caught check firing", caught, sim.CatLinearizability, sim.CatLinearizability, true},
		{"another check stopping the run first", stopped, sim.CatLinearizability, sim.CatPanic, true},
	} {
		if cat, failing := verdict(c.r, c.catch); cat != c.cat || failing != c.failing {
			t.Errorf("%s: %q %v, want %q %v", c.name, cat, failing, c.cat, c.failing)
		}
	}
}
