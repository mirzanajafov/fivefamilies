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
	c2 := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	c2.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "pause", DurationMs: 100}); err == nil {
		t.Fatal("chaos ran on a node started without it")
	}
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "pause", DurationMs: uint64(2 * time.Minute / time.Millisecond)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a two-minute pause got %v", err)
	}
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: "crash", DurationMs: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("an unknown action got %v", err)
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
	if c.running[old].srv.isolated.Allow(old, old%3+1) {
		t.Fatal("isolate did not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(old).Chaos(ctx, &pb.ChaosRequest{Action: "heal"}); err != nil {
		t.Fatal(err)
	}
	if !c.running[old].srv.isolated.Allow(old, old%3+1) {
		t.Fatal("heal left the isolation in place")
	}
}

func TestEachIsolationOrHealReplacesTheOneBefore(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	c.leader(5 * time.Second)
	isolated := func() bool { return !c.running[1].srv.isolated.Allow(1, 2) }
	chaos(t, c, 1, "isolate", 500*time.Millisecond)
	chaos(t, c, 1, "isolate", time.Minute)
	time.Sleep(800 * time.Millisecond)
	if !isolated() {
		t.Fatal("the first isolation ended the second")
	}
	chaos(t, c, 1, "heal", 0)
	chaos(t, c, 1, "isolate", 500*time.Millisecond)
	chaos(t, c, 1, "heal", 0)
	chaos(t, c, 1, "isolate", time.Minute)
	time.Sleep(800 * time.Millisecond)
	if !isolated() {
		t.Fatal("an isolation from before a heal ended the one after it")
	}
	chaos(t, c, 1, "heal", 0)
	chaos(t, c, 1, "isolate", 200*time.Millisecond)
	time.Sleep(400 * time.Millisecond)
	if isolated() {
		t.Fatal("an isolation outlived its duration")
	}
}

func followerTheClientDoesNotUse(leader raft.NodeID) raft.NodeID {
	if leader == 2 {
		return 3
	}
	return 2
}

func waitCaughtUp(t *testing.T, c *testCluster, leader, follower raft.NodeID, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for c.running[follower].srv.Status().GetCommit() < c.running[leader].srv.Status().GetCommit() {
		if time.Now().After(deadline) {
			t.Fatalf("node %d did not catch up within %v", follower, within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestAHealEndsAStallAndAPauseThatHaveAlreadyBegun(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	leader := c.leader(5 * time.Second)
	follower := followerTheClientDoesNotUse(leader)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	chaos(t, c, follower, "pause", time.Minute)
	chaos(t, c, leader, "stall-disk", time.Minute)
	written := make(chan error, 1)
	go func() {
		_, err := cl.Put(ctx, "k", []byte("v"))
		written <- err
	}()
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-written:
		t.Fatalf("the write finished during the stall: %v", err)
	default:
	}
	if c.running[follower].srv.paused.remaining() <= 0 {
		t.Fatal("the follower is not paused")
	}
	chaos(t, c, leader, "heal", 0)
	chaos(t, c, follower, "heal", 0)
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the write still waited for the stalled disk after the heal")
	}
	waitCaughtUp(t, c, leader, follower, 5*time.Second)
}

func TestALaterPauseReplacesAnEarlierOne(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	leader := c.leader(5 * time.Second)
	follower := followerTheClientDoesNotUse(leader)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	chaos(t, c, follower, "pause", time.Minute)
	chaos(t, c, follower, "pause", 200*time.Millisecond)
	if r := c.running[follower].srv.paused.remaining(); r <= 0 || r > 200*time.Millisecond {
		t.Fatalf("the later pause left %v of pausing", r)
	}
	if _, err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	waitCaughtUp(t, c, leader, follower, 3*time.Second)
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
