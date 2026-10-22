package main

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/lab"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
)

func TestALabRunRefusesABugItCanNotExercise(t *testing.T) {
	for _, tc := range []struct {
		read   client.ReadMode
		bugs   planted.Bug
		faults []lab.FaultKind
		lease  string
		ok     bool
	}{
		{client.ReadIndex, 0, lab.AllFaults, "0s", true},
		{client.ReadIndex, planted.EarlyConf, lab.AllFaults, "0s", true},
		{client.ReadIndex, planted.EarlyConf, []lab.FaultKind{lab.FaultKill, lab.FaultTransfer}, "0s", false},
		{client.ReadLog, planted.FollowerRead, lab.AllFaults, "0s", false},
		{client.ReadIndex, planted.LocalKeepAlive, lab.AllFaults, "0s", false},
		{client.ReadLease, 0, lab.AllFaults, "0s", false},
		{client.ReadLease, 0, lab.AllFaults, "800ms", true},
	} {
		if err := checkRun(tc.read, tc.bugs, tc.faults, "1s", "100ms", tc.lease); (err == nil) != tc.ok {
			t.Errorf("read %v, bugs %v, faults %v, lease %s: %v", tc.read, tc.bugs, tc.faults, tc.lease, err)
		}
	}
}

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
		if err := checkBench(tc.experiments, tc.only); (err == nil) != tc.ok {
			t.Errorf("experiments %v, only %v: %v", tc.experiments, tc.only, err)
		}
	}
}
