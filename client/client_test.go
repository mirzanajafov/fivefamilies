package client

import "testing"

func TestOnlyKnownReadModesParse(t *testing.T) {
	for s, want := range map[string]ReadMode{"index": ReadIndex, "lease": ReadLease, "log": ReadLog} {
		if got, err := ParseReadMode(s); err != nil || got != want {
			t.Errorf("%q parsed as %d: %v", s, got, err)
		}
	}
	for _, s := range []string{"", "leases", "Index"} {
		if _, err := ParseReadMode(s); err == nil {
			t.Errorf("%q was accepted", s)
		}
	}
}
