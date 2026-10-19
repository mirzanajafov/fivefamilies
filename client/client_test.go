package client

import "testing"

func TestOnlyKnownReadModesParse(t *testing.T) {
	for s, want := range map[string]ReadMode{"index": ReadIndex, "lease": ReadLease, "log": ReadLog} {
		var got ReadMode
		if err := got.UnmarshalText([]byte(s)); err != nil || got != want {
			t.Errorf("%q parsed as %s: %v", s, got, err)
		}
		if text, _ := want.MarshalText(); string(text) != s {
			t.Errorf("%s writes itself as %q", want, text)
		}
	}
	for _, s := range []string{"", "leases", "Index"} {
		var got ReadMode
		if err := got.UnmarshalText([]byte(s)); err == nil {
			t.Errorf("%q was accepted", s)
		}
	}
}
