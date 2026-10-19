package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
)

func waitFor[T any](t *testing.T, ch <-chan T, within time.Duration, ok func(T) bool) (T, time.Duration) {
	t.Helper()
	start := time.Now()
	timeout := time.After(within)
	for {
		select {
		case v, open := <-ch:
			if !open {
				t.Fatal("the channel closed")
			}
			if ok(v) {
				return v, time.Since(start)
			}
		case <-timeout:
			var zero T
			t.Fatalf("nothing matching within %v", within)
			return zero, 0
		}
	}
}

func names(list []client.Instance) string {
	return fmt.Sprint(list)
}

func TestTheRegistryForgetsADeadInstance(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.Register(ctx, "api", "a/b", "x", time.Second); err == nil {
		t.Fatal("an instance name with a slash was accepted")
	}
	seen, err := cl.Discover(ctx, "api")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 0 })
	one, err := cl.Register(ctx, "api", "one", "10.0.0.1:80", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	two, err := cl.Register(ctx, "api", "two", "10.0.0.2:80", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool {
		return names(l) == "[{one 10.0.0.1:80} {two 10.0.0.2:80}]"
	})
	time.Sleep(time.Second)
	one.Abandon()
	_, took := waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return names(l) == "[{two 10.0.0.2:80}]" })
	t.Logf("a dead instance was gone for discoverers %v after it stopped renewing (TTL 300ms)", took)
	if took > 600*time.Millisecond {
		t.Fatalf("took %v", took)
	}
	start := time.Now()
	if err := cl.Deregister(ctx, two); err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 0 })
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Fatalf("a deregistered instance took %v to go", took)
	}

	cfg, err := cl.WatchConfig(ctx, "api")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, cfg, 5*time.Second, func(m map[string]string) bool { return len(m) == 0 })
	if _, err := cl.SetConfig(ctx, "api", "timeout", "2s"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, cfg, 5*time.Second, func(m map[string]string) bool { return m["timeout"] == "2s" })
	if v, ok, err := cl.GetConfig(ctx, "api", "timeout"); err != nil || !ok || v != "2s" {
		t.Fatalf("get: %q %v %v", v, ok, err)
	}
}

func TestARegistrationComesBackAfterTheClusterForgetsItsSession(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxSessions = 1 })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	reg, err := cl.Register(ctx, "api", "one", "10.0.0.1:80", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Deregister(context.Background(), reg)
	other, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	seen, err := other.Discover(ctx, "api")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 1 })
	lost := reg.Lease()
	if err := other.Revoke(ctx, lost); err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 0 })
	back, _ := waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 1 })
	deadline := time.Now().Add(time.Second)
	for reg.Renewals() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if back[0].Addr != "10.0.0.1:80" || reg.Renewals() != 1 || reg.Lease() == lost {
		t.Fatalf("came back as %v after %d renewals on lease %d", back, reg.Renewals(), reg.Lease())
	}
}

func TestARegistrationOutlivesALeaderAndComesBackAfterItsLeaseIsLost(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	reg, err := cl.Register(ctx, "api", "one", "10.0.0.1:80", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Deregister(context.Background(), reg)
	seen, _ := cl.Discover(ctx, "api")
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 1 })
	c.stop(c.leader(time.Second))
	time.Sleep(2 * time.Second)
	if reg.Renewals() != 0 {
		t.Fatalf("the lease was lost %d times across one failover", reg.Renewals())
	}
	if err := cl.Revoke(ctx, reg.Lease()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 0 })
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 1 })
	deadline := time.Now().Add(time.Second)
	for reg.Renewals() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reg.Renewals() != 1 {
		t.Fatalf("renewals %d after the lease was revoked under it", reg.Renewals())
	}
}
