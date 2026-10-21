package client

import (
	"testing"

	"google.golang.org/grpc/metadata"

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

func TestAFailedLeaderGuessMovesToTheNodeAfterIt(t *testing.T) {
	c := &Client{addrs: []string{"n1", "n2", "n3"}, leader: "n1"}
	c.moveOn("n1", metadata.Pairs(headerLeaderAddr, "n2"))
	if c.leader != "n2" {
		t.Fatalf("the hint was not followed: %s", c.leader)
	}
	c.moveOn("n2", nil)
	if c.leader != "n3" {
		t.Fatalf("after the hinted leader failed the guess is %s, want n3", c.leader)
	}
	c.moveOn("n3", nil)
	if c.leader != "n1" {
		t.Fatalf("after n3 failed the guess is %s, want n1", c.leader)
	}
}
