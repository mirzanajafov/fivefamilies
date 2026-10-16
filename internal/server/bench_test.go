package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func putMany(t *testing.T, c *testCluster, writers, each int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for w := range writers {
		cl, err := client.New(c.addrs(), client.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		wg.Go(func() {
			for i := range each {
				if _, err := cl.Put(ctx, fmt.Sprintf("k%d-%d", w, i), []byte("v")); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
}

func TestFsyncModesDoWhatTheySay(t *testing.T) {
	for _, mode := range []string{FsyncGroup, FsyncReady, FsyncEntry, FsyncOff} {
		t.Run(mode, func(t *testing.T) {
			c := newTestCluster(t, 1, func(cfg *Config) { cfg.FsyncMode = mode })
			c.leader(5 * time.Second)
			putMany(t, c, 10, 5)
			st := c.status(1)
			t.Logf("%s: %d entries in %d batches with %d fsyncs", mode, st.GetEntriesWritten(), st.GetWalBatches(), st.GetFsyncs())
			switch mode {
			case FsyncOff:
				if st.GetFsyncs() != 0 {
					t.Fatalf("%d fsyncs with fsync off", st.GetFsyncs())
				}
			case FsyncEntry:
				if st.GetFsyncs() < st.GetEntriesWritten() {
					t.Fatalf("%d fsyncs for %d entries", st.GetFsyncs(), st.GetEntriesWritten())
				}
			case FsyncReady:
				if st.GetFsyncs() > st.GetWalBatches() || st.GetFsyncs() < st.GetEntriesWritten()/2 {
					t.Fatalf("%d fsyncs, %d batches, %d entries", st.GetFsyncs(), st.GetWalBatches(), st.GetEntriesWritten())
				}
			case FsyncGroup:
				if st.GetFsyncs() == 0 || st.GetFsyncs() >= st.GetEntriesWritten() {
					t.Fatalf("%d fsyncs for %d entries from ten writers at once: nothing was grouped", st.GetFsyncs(), st.GetEntriesWritten())
				}
			}
		})
	}
}

func chaos(t *testing.T, c *testCluster, id raft.NodeID, action string, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(id).Chaos(ctx, &pb.ChaosRequest{Action: action, DurationMs: uint64(d / time.Millisecond)}); err != nil {
		t.Fatalf("%s on %d: %v", action, id, err)
	}
}

func TestADiskStallOnTheLeaderCausesAnElectionOnlyWithSynchronousStorage(t *testing.T) {
	for _, syncStorage := range []bool{false, true} {
		t.Run(fmt.Sprintf("sync=%v", syncStorage), func(t *testing.T) {
			c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos, cfg.SyncStorage = true, syncStorage })
			leader := c.leader(5 * time.Second)
			cl, err := client.New(c.addrs(), client.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			putMany(t, c, 5, 1)
			before := c.status(leader).GetElections()
			chaos(t, c, leader, "stall-disk", 800*time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			start := time.Now()
			if _, err := cl.Put(ctx, "during", []byte("x")); err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			time.Sleep(time.Second)
			elections := uint64(0)
			for id := range c.running {
				elections = max(elections, c.status(id).GetElections())
			}
			t.Logf("sync storage %v: a put during an 800 ms stall took %v, elections %d -> %d", syncStorage, took.Round(time.Millisecond), before, elections)
			if syncStorage && elections == before {
				t.Fatal("the leader's event loop waited 800 ms on the disk, four election timeouts, and nobody noticed")
			}
			if !syncStorage && elections != before {
				t.Fatal("a disk stall on the leader started an election, but heartbeats should not wait for the disk")
			}
		})
	}
}

func TestChaosIsBoundedAndOffByDefault(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "pause", DurationMs: 100}); err == nil {
		t.Fatal("chaos ran on a node started without it")
	}
	var crashed atomic.Bool
	c2 := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos, cfg.Crash = true, func() { crashed.Store(true) } })
	c2.leader(5 * time.Second)
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "pause", DurationMs: uint64(2 * time.Minute / time.Millisecond)}); err == nil {
		t.Fatal("a two-minute pause was accepted")
	}
	chaos(t, c2, 1, "crash", time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if !crashed.Load() {
		t.Fatal("crash did not call the crash hook")
	}
	old := c2.leader(time.Second)
	chaos(t, c2, old, "isolate", 2*time.Second)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for id, r := range c2.running {
			if st := r.srv.Status(); id != old && st.GetState() == "leader" {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("isolating the leader did not elect another")
}

func TestOneClientCanWriteFromManyGoroutines(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if _, err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("v")); err != nil {
				t.Errorf("put %d: %v; a session takes one write at a time, so the client must queue them", i, err)
			}
		})
	}
	wg.Wait()
}
