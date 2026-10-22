package lab

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/client"
)

func TestARunRefusesWhatItCanNotExercise(t *testing.T) {
	valid := Options{Read: client.ReadIndex, Faults: AllFaults, Election: "1s", Heartbeat: "100ms", Lease: "0s"}
	for _, tc := range []struct {
		name string
		edit func(*Options)
		ok   bool
	}{
		{"the defaults", func(*Options) {}, true},
		{"earlyconf with membership changes", func(o *Options) { o.Bug = "earlyconf" }, true},
		{"earlyconf without membership changes", func(o *Options) { o.Bug, o.Faults = "earlyconf", []FaultKind{FaultKill, FaultTransfer} }, false},
		{"followerread with every read through the log", func(o *Options) { o.Bug, o.Read = "followerread", client.ReadLog }, false},
		{"a lease bug without lease holders", func(o *Options) { o.Bug = "localkeepalive" }, false},
		{"an unknown bug", func(o *Options) { o.Bug = "nosuchbug" }, false},
		{"lease reads without a lease", func(o *Options) { o.Read = client.ReadLease }, false},
		{"lease reads with a lease", func(o *Options) { o.Read, o.Lease = client.ReadLease, "800ms" }, true},
		{"an election timeout that is not a duration", func(o *Options) { o.Election = "1" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := valid
			tc.edit(&o)
			if err := o.Validate(); (err == nil) != tc.ok {
				t.Errorf("valid %v, got %v", tc.ok, err)
			}
		})
	}
}
