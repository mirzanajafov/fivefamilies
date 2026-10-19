package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

func TestTheLoopTakesEverythingQueuedIntoOneReady(t *testing.T) {
	for _, batch := range []int{0, 1} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			c := newTestCluster(t, 3, func(cfg *Config) { cfg.Batch = batch })
			l := c.leader(5 * time.Second)
			srv := c.running[l].srv
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			before := c.status(l)
			release := blockLoop(t, ctx, srv)
			var wg sync.WaitGroup
			for i := range 100 {
				wg.Go(func() {
					if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: fmt.Sprint(i), Value: []byte("v")}); err != nil {
						t.Error(err)
					}
				})
			}
			for len(srv.calls) < 100 {
				time.Sleep(time.Millisecond)
			}
			release()
			wg.Wait()
			after := c.status(l)
			batches, sent := after.GetWalBatches()-before.GetWalBatches(), after.GetMsgsSent()-before.GetMsgsSent()
			t.Logf("100 queued writes: %d log batches and %d messages on the leader", batches, sent)
			if batch == 1 && batches < 100 {
				t.Fatalf("one event per Ready wrote %d batches for 100 writes", batches)
			}
			if batch == 0 && batches > 10 {
				t.Fatalf("100 writes queued at once went out in %d log batches", batches)
			}
		})
	}
}

func blockLoop(t *testing.T, ctx context.Context, srv *Server) func() {
	t.Helper()
	blocked, release := make(chan struct{}), make(chan struct{})
	go srv.inLoop(ctx, func() { close(blocked); <-release })
	<-blocked
	return func() { close(release) }
}

func TestALeaderOverItsPendingCapRefusesAtOnce(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxPending = 5 })
	l := c.leader(5 * time.Second)
	srv := c.running[l].srv
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: "warm", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	release := blockLoop(t, ctx, srv)
	var ok, busy atomic.Int32
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			_, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: fmt.Sprint(i), Value: []byte("v")})
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, raft.ErrBusy):
				busy.Add(1)
			default:
				t.Error(err)
			}
		})
	}
	for len(srv.calls) < 20 {
		time.Sleep(time.Millisecond)
	}
	release()
	wg.Wait()
	if ok.Load() != 5 || busy.Load() != 15 {
		t.Fatalf("%d written and %d refused, want 5 and 15", ok.Load(), busy.Load())
	}
	if code := status.Code(fail(ctx, srv, raft.ErrBusy)); code != codes.ResourceExhausted {
		t.Fatalf("a refusal reaches the client as %s", code)
	}
}

func TestAFullCallQueueRefusesAtOnceOnlyWithACap(t *testing.T) {
	for _, limit := range []uint64{4096, 0} {
		t.Run(fmt.Sprintf("max-pending=%d", limit), func(t *testing.T) {
			c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxPending = limit })
			l := c.leader(5 * time.Second)
			srv := c.running[l].srv
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			release := blockLoop(t, ctx, srv)
			room := cap(srv.calls)
			var ok, busy atomic.Int32
			var wg sync.WaitGroup
			for i := range room + 50 {
				wg.Go(func() {
					_, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: fmt.Sprint(i), Value: []byte("v")})
					switch {
					case err == nil:
						ok.Add(1)
					case errors.Is(err, raft.ErrBusy):
						busy.Add(1)
					default:
						t.Error(err)
					}
				})
			}
			for len(srv.calls) < room || (limit > 0 && busy.Load() < 50) {
				time.Sleep(time.Millisecond)
			}
			release()
			wg.Wait()
			want := int32(0)
			if limit > 0 {
				want = 50
			}
			if busy.Load() != want || ok.Load() != int32(room+50)-want {
				t.Fatalf("%d written and %d refused, want %d refused", ok.Load(), busy.Load(), want)
			}
		})
	}
}

func TestARequestWhoseClientGaveUpIsNotProposed(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	l := c.leader(5 * time.Second)
	srv := c.running[l].srv
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: "warm", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	before := c.status(l).GetLastIndex()
	release := blockLoop(t, ctx, srv)
	short, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	defer stop()
	done := make(chan error, 1)
	go func() {
		_, err := srv.Submit(short, kv.Command{Op: kv.OpPut, Key: "late", Value: []byte("v")})
		done <- err
	}()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err %v", err)
	}
	release()
	if err := srv.inLoop(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	if after := c.status(l).GetLastIndex(); after != before {
		t.Fatalf("the log grew from %d to %d for a request nobody waited for", before, after)
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
	c2 := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	c2.leader(5 * time.Second)
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "pause", DurationMs: uint64(2 * time.Minute / time.Millisecond)}); err == nil {
		t.Fatal("a two-minute pause was accepted")
	}
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "crash", DurationMs: 1}); err == nil {
		t.Fatal("an unknown action was accepted")
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

func TestHealClearsAnIsolationEarly(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	old := c.leader(5 * time.Second)
	chaos(t, c, old, "isolate", time.Minute)
	if c.running[old].srv.Filter.Allow(old, old%3+1) {
		t.Fatal("isolate did not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(old).Chaos(ctx, &pb.ChaosRequest{Action: "heal"}); err != nil {
		t.Fatal(err)
	}
	if !c.running[old].srv.Filter.Allow(old, old%3+1) {
		t.Fatal("heal left the isolation in place")
	}
}

func TestAnEarlierIsolationsTimerDoesNotEndALaterOne(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	c.leader(5 * time.Second)
	chaos(t, c, 1, "isolate", 200*time.Millisecond)
	chaos(t, c, 1, "isolate", time.Minute)
	time.Sleep(400 * time.Millisecond)
	if c.running[1].srv.Filter.Allow(1, 2) {
		t.Fatal("the first isolation's timer ended the second")
	}
	chaos(t, c, 1, "heal", 0)
	chaos(t, c, 1, "isolate", 200*time.Millisecond)
	time.Sleep(400 * time.Millisecond)
	if !c.running[1].srv.Filter.Allow(1, 2) {
		t.Fatal("an isolation outlived its duration")
	}
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
