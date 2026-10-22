package main

import (
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/sim"
)

func TestASeedIsCaughtOnlyByItsCheckAndCutShortWhenAnotherStopsItFirst(t *testing.T) {
	passed := sim.Result{Check: porcupine.Ok}
	mutedSafety := sim.Result{Check: porcupine.Ok, Detected: map[sim.Category]string{sim.CatSafety: "two leaders"}}
	illegal := sim.Result{Category: sim.CatLinearizability, Violation: "not linearizable", Check: porcupine.Illegal,
		Detected: map[sim.Category]string{sim.CatLinearizability: "not linearizable"}}
	stateAfterAGoodCheck := sim.Result{Category: sim.CatState, Violation: "hashes differ", Check: porcupine.Ok,
		Detected: map[sim.Category]string{sim.CatState: "hashes differ"}}
	panicked := sim.Result{Category: sim.CatPanic, Violation: "boom", Detected: map[sim.Category]string{sim.CatPanic: "boom"}}
	for _, c := range []struct {
		name  string
		r     sim.Result
		catch sim.Category
		cat   sim.Category
		out   outcome
	}{
		{"a clean run", passed, "", "", clean},
		{"a panic", panicked, "", sim.CatPanic, caught},
		{"a muted safety violation, caught", mutedSafety, sim.CatSafety, sim.CatSafety, caught},
		{"a muted safety violation, not what is caught", mutedSafety, sim.CatLinearizability, "", clean},
		{"a linearizability violation, caught", illegal, sim.CatLinearizability, sim.CatLinearizability, caught},
		{"a state failure after linearizability passed", stateAfterAGoodCheck, sim.CatLinearizability, "", clean},
		{"a panic before linearizability could be checked", panicked, sim.CatLinearizability, sim.CatPanic, cutShort},
	} {
		t.Run(c.name, func(t *testing.T) {
			if cat, out := verdict(c.r, c.catch); cat != c.cat || out != c.out {
				t.Errorf("%s: %q %d, want %q %d", c.name, cat, out, c.cat, c.out)
			}
		})
	}
}
