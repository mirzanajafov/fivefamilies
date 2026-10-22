package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestACommandLineWithoutAKnownCommandAndItsArgumentsIsAUsageError(t *testing.T) {
	tgt := target{addrs: []string{"127.0.0.1:1"}}
	for _, line := range []string{"", "nothing", "status extra", "get", "put k", "put k v 1 more", "member demote 2", "lease extend 3", "lease gran 10s", "config set svc key", "config drop svc"} {
		if err := run(tgt, time.Second, strings.Fields(line)); !errors.Is(err, errUsage) {
			t.Errorf("%q: %v, want a usage error", line, err)
		}
	}
}

func TestIDsAndRevisionsArePositiveNumbers(t *testing.T) {
	tgt := target{addrs: []string{"127.0.0.1:1"}}
	for _, line := range []string{"member add 0", "transfer x", "lease revoke -1", "watch p 0", "put k v lease"} {
		if err := run(tgt, time.Second, strings.Fields(line)); !errors.Is(err, errNotPositive) {
			t.Errorf("%q: %v, want a bad number", line, err)
		}
	}
}
