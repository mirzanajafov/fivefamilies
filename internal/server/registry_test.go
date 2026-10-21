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

func waitRenewals(t *testing.T, reg *client.Registration, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for reg.Renewals() != n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if reg.Renewals() != n {
		t.Fatalf("renewals %d, want %d", reg.Renewals(), n)
	}
}

func instance(name, addr string) client.Instance {
	return client.Instance{Name: name, Addr: addr}
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
	if _, err := cl.Register(ctx, "api", instance("a/b", "x"), time.Second); err == nil {
		t.Fatal("an instance name with a slash was accepted")
	}
	seen, err := cl.Discover(ctx, "api")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return len(l) == 0 })
	one, err := cl.Register(ctx, "api", instance("one", "10.0.0.1:80"), 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	two, err := cl.Register(ctx, "api", instance("two", "10.0.0.2:80"), 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool {
		return fmt.Sprint(l) == "[one=10.0.0.1:80 two=10.0.0.2:80]"
	})
	time.Sleep(time.Second)
	one.Abandon()
	_, took := waitFor(t, seen, 5*time.Second, func(l []client.Instance) bool { return fmt.Sprint(l) == "[two=10.0.0.2:80]" })
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
	meta := map[string]string{"version": "v1"}
	reg, err := cl.Register(ctx, "api", client.Instance{Name: "one", Addr: "10.0.0.1:80", Meta: meta}, 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	meta["version"] = "v2"
	defer func() {
		if err := cl.Deregister(context.Background(), reg); err != nil {
			t.Error(err)
		}
	}()
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
	waitRenewals(t, reg, 1)
	if back[0].Addr != "10.0.0.1:80" || back[0].Meta["version"] != "v1" || reg.Lease() == lost {
		t.Fatalf("came back as %v on lease %d", back, reg.Lease())
	}
}

func TestDeregisteringRemovesTheInstanceAfterTheClusterForgetsTheSession(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxSessions = 1 })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	reg, err := cl.Register(ctx, "api", instance("one", "10.0.0.1:80"), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := cl.Deregister(ctx, reg); err != nil {
		t.Fatalf("deregistering on a forgotten session: %v", err)
	}
	if kvs, err := other.Range(ctx, client.RegistryPrefix, 0); err != nil || len(kvs) != 0 {
		t.Fatalf("still registered: %v %v", kvs, err)
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
	reg, err := cl.Register(ctx, "api", instance("one", "10.0.0.1:80"), 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cl.Deregister(context.Background(), reg); err != nil {
			t.Error(err)
		}
	}()
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
	waitRenewals(t, reg, 1)
}

func TestLookupAnswersOnceItsServiceChanges(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, _, err := cl.Lookup(ctx, "a/b", 0, time.Second); err == nil {
		t.Fatal("a service name with a slash was accepted")
	}
	list, index, err := cl.Lookup(ctx, "api", 0, time.Minute)
	if err != nil || len(list) != 0 || index == 0 {
		t.Fatalf("first lookup: %v at %d, %v", list, index, err)
	}

	type answer struct {
		list  []client.Instance
		index uint64
		err   error
		at    time.Time
	}
	answers := make(chan answer, 1)
	go func() {
		l, i, err := cl.Lookup(ctx, "api", index, time.Minute)
		answers <- answer{l, i, err, time.Now()}
	}()
	if _, err := cl.RegisterOnce(ctx, "web", instance("w", "10.0.0.3:80"), time.Minute); err != nil {
		t.Fatal(err)
	}
	select {
	case a := <-answers:
		t.Fatalf("answered for a change to another service: %v", a.list)
	case <-time.After(300 * time.Millisecond):
	}
	registered := time.Now()
	one := client.Instance{Name: "one", Addr: "10.0.0.1:80", Meta: map[string]string{"version": "v2"}}
	if _, err := cl.RegisterOnce(ctx, "api", one, time.Minute); err != nil {
		t.Fatal(err)
	}
	a := <-answers
	if a.err != nil || fmt.Sprint(a.list) != "[one=10.0.0.1:80 (version=v2)]" || a.index <= index {
		t.Fatalf("after the registration: %v at %d, %v", a.list, a.index, a.err)
	}
	t.Logf("a blocked lookup answered %v after the registration", a.at.Sub(registered))

	if _, err := cl.Put(ctx, client.RegistryPrefix+"api/raw", []byte("10.0.0.9:80")); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, client.RegistryPrefix+"api/x/y", []byte(`{"addr":"10.0.0.8:80"}`)); err != nil {
		t.Fatal(err)
	}
	list, index, err = cl.Lookup(ctx, "api", a.index, time.Minute)
	if err != nil || fmt.Sprint(list) != "[one=10.0.0.1:80 (version=v2)]" || index <= a.index {
		t.Fatalf("values that aren't registrations: %v at %d, %v", list, index, err)
	}

	start := time.Now()
	same, after, err := cl.Lookup(ctx, "api", index, 300*time.Millisecond)
	if err != nil || fmt.Sprint(same) != fmt.Sprint(list) || after < index {
		t.Fatalf("with nothing changed: %v at %d, %v", same, after, err)
	}
	if waited := time.Since(start); waited < 300*time.Millisecond {
		t.Fatalf("answered after %v with nothing changed", waited)
	}
}

func TestLookupAnswersAtOnceWhenItsIndexIsCompacted(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.WatchHistory = 4 })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if _, err := cl.RegisterOnce(ctx, "api", instance("one", "10.0.0.1:80"), time.Minute); err != nil {
		t.Fatal(err)
	}
	_, index, err := cl.Lookup(ctx, "api", 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		if _, err := cl.Put(ctx, "k", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
	list, _, err := cl.Lookup(ctx, "api", index, time.Minute)
	if err != nil || fmt.Sprint(list) != "[one=10.0.0.1:80]" {
		t.Fatalf("%v, %v", list, err)
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("waited %v on an index the watch history no longer has", waited)
	}
}
