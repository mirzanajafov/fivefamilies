package server

import (
	"context"
	"fmt"
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
		cl.Put(ctx, "/other", []byte(fmt.Sprint(i)))
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
