package main

import (
	"fmt"
	"testing"
)

func TestABenchRunRefusesNamesThatMatchNothing(t *testing.T) {
	for _, tc := range []struct {
		experiments, only []string
		ok                bool
	}{
		{[]string{"e1b", "e2", "e5"}, nil, true},
		{[]string{"e2", "e4"}, []string{"5 nodes", "lease reads, 0.95"}, true},
		{[]string{"e6"}, nil, false},
		{[]string{"e2"}, []string{"5 Nodes"}, false},
		{[]string{"e2"}, []string{"lease reads, 0.95"}, false},
	} {
		t.Run(fmt.Sprint(tc.experiments, tc.only), func(t *testing.T) {
			if err := checkBench(tc.experiments, tc.only); (err == nil) != tc.ok {
				t.Errorf("accepted %v, want %v: %v", err == nil, tc.ok, err)
			}
		})
	}
}
