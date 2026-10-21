package client

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

func TestRoleNamesAreTheOnesTheDashboardShows(t *testing.T) {
	for role, want := range map[pb.Role]string{
		pb.Role_ROLE_UNSPECIFIED:   "",
		pb.Role_ROLE_FOLLOWER:      "follower",
		pb.Role_ROLE_PRE_CANDIDATE: "precandidate",
		pb.Role_ROLE_CANDIDATE:     "candidate",
		pb.Role_ROLE_LEADER:        "leader",
	} {
		if got := RoleName(role); got != want {
			t.Errorf("%v is shown as %q, want %q", role, got, want)
		}
	}
}

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

func TestALeaseWithoutAPositiveTTLIsRefusedBeforeAnythingIsSent(t *testing.T) {
	c, err := New([]string{"127.0.0.1:1"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, ttl := range []time.Duration{0, -5 * time.Second} {
		if _, err := c.Grant(context.Background(), ttl); !errors.Is(err, ErrInvalid) {
			t.Errorf("a %v lease: %v, want ErrInvalid", ttl, err)
		}
	}
}
