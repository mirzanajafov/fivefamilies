package server

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
)

func TestAWatchSurvivesItsNodeStopping(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.WatchHistory = 64; cfg.WatchProgress = 50 * time.Millisecond })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	wcl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer wcl.Close()
	events := wcl.Watch(ctx, "/config/", true, 0)
	first := <-events
	if len(first.Events) != 0 || first.CompactRev != 0 {
		t.Fatalf("first response %+v, want an empty batch telling where the watch starts", first)
	}
	for i := range 10 {
		if _, err := cl.Put(ctx, "/config/n", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
		if _, err := cl.Put(ctx, "/other", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	c.stop(1)
	for i := 10; i < 20; i++ {
		if _, err := cl.Put(ctx, "/config/n", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	var got []string
	last := first.Rev
	for len(got) < 20 {
		select {
		case r, ok := <-events:
			if !ok {
				t.Fatalf("the watch ended after %v", got)
			}
			for _, ev := range r.Events {
				if ev.KV.ModRev <= last {
					t.Fatalf("event at %d after %d: %v", ev.KV.ModRev, last, got)
				}
				last = ev.KV.ModRev
				got = append(got, string(ev.KV.Value))
			}
		case <-ctx.Done():
			t.Fatalf("only %v arrived", got)
		}
	}
	for i, v := range got {
		if v != fmt.Sprint(i) {
			t.Fatalf("events %v: every put once, in order, across the node stopping", got)
		}
	}
}

func TestAWatchFromAForgottenRevisionSaysSo(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.WatchHistory = 4 })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := range 20 {
		if _, err := cl.Put(ctx, "/config/n", []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	r, ok := <-cl.Watch(ctx, "/config/", true, 1)
	if !ok || r.CompactRev <= 1 {
		t.Fatalf("watch from revision 1 with room for four events: %+v %v", r, ok)
	}
}

type freezer struct {
	ln     net.Listener
	target string
	frozen atomic.Bool
}

func newFreezer(t *testing.T, target string) *freezer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &freezer{ln: ln, target: target}
	go f.serve()
	t.Cleanup(func() {
		f.frozen.Store(false)
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
	})
	return f
}

func (f *freezer) serve() {
	for {
		in, err := f.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", f.target)
		if err != nil {
			_ = in.Close()
			continue
		}
		go f.pipe(in, out)
		go f.pipe(out, in)
	}
}

func (f *freezer) pipe(dst, src net.Conn) {
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		for f.frozen.Load() {
			time.Sleep(10 * time.Millisecond)
		}
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				err = werr
			}
		}
		if err != nil {
			_ = dst.Close()
			return
		}
	}
}

func firstEvent(events <-chan client.WatchResponse, within time.Duration) (client.Event, time.Duration, bool) {
	start := time.Now()
	timeout := time.After(within)
	for {
		select {
		case r := <-events:
			if len(r.Events) > 0 {
				return r.Events[0], time.Since(start), true
			}
		case <-timeout:
			return client.Event{}, within, false
		}
	}
}

func TestAWatchMovesOnFromANodeThatStoppedAnsweringAndTheNextOneStartsElsewhere(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.leader(5 * time.Second)
	follower := leader%3 + 1
	deadline := time.Now().Add(5 * time.Second)
	for c.running[follower].srv.Status().GetLeader() != uint64(leader) {
		if time.Now().After(deadline) {
			t.Fatal("the follower never learned the leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	f := newFreezer(t, c.nodes[follower])
	addrs := []string{f.ln.Addr().String()}
	for id, addr := range c.nodes {
		if id != follower {
			addrs = append(addrs, addr)
		}
	}
	wcl, err := client.New(addrs, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer wcl.Close()
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	events := wcl.Watch(ctx, "a", false, 0)
	<-events
	f.frozen.Store(true)
	if _, err := cl.Put(ctx, "a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	ev, took, ok := firstEvent(events, 20*time.Second)
	if !ok || string(ev.KV.Value) != "1" {
		t.Fatalf("no event within %v of the put, with the watch's node frozen", took)
	}
	if took < 5*time.Second {
		t.Fatalf("the event came after %v, so the watch wasn't on the frozen node and this proves nothing", took)
	}
	t.Logf("the event came %v after the put, with the watch's node frozen", took)

	if _, err := cl.Put(ctx, "b", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, took, ok := firstEvent(wcl.Watch(ctx, "b", false, 1), 3*time.Second); !ok {
		t.Fatalf("a new watch got nothing within %v, so it started on the frozen node again", took)
	}
}
