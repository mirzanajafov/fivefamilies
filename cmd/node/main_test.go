package main

import "testing"

func TestANodeGetsAtLeastTwoGoProcessorsUnlessToldOtherwise(t *testing.T) {
	for _, c := range []struct{ asked, picked, want int }{{0, 1, 2}, {0, 2, 2}, {0, 8, 8}, {1, 8, 1}, {3, 1, 3}} {
		if got := goProcs(c.asked, c.picked); got != c.want {
			t.Fatalf("goProcs(%d, %d) = %d, want %d", c.asked, c.picked, got, c.want)
		}
	}
}
