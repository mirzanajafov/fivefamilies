package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func TestTheLoopTakesEverythingQueuedIntoOneReady(t *testing.T) {
	for _, batch := range []int{0, 1} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			c := newTestCluster(t, 3, func(cfg *Config) { cfg.Batch = batch })
			l := c.leader(5 * time.Second)
			srv := c.running[l].srv
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			before := c.status(l)
			var wg sync.WaitGroup
			t.Cleanup(wg.Wait)
			release := blockLoop(t, ctx, srv)
			for i := range 100 {
				wg.Go(func() {
					if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: fmt.Sprint(i), Value: []byte("v")}); err != nil {
						t.Error(err)
					}
				})
			}
			if !eventually(10*time.Second, func() bool { return len(srv.calls) >= 100 }) {
				t.Fatalf("%d of 100 writes queued behind the blocked loop", len(srv.calls))
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

func TestALeaderOverItsPendingCapRefusesAtOnce(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxPending = 5 })
	l := c.leader(5 * time.Second)
	srv := c.running[l].srv
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: "warm", Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	t.Cleanup(wg.Wait)
	release := blockLoop(t, ctx, srv)
	var ok, busy atomic.Int32
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
	if !eventually(10*time.Second, func() bool { return len(srv.calls) >= 20 }) {
		t.Fatalf("%d of 20 writes queued behind the blocked loop", len(srv.calls))
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
			var wg sync.WaitGroup
			t.Cleanup(wg.Wait)
			release := blockLoop(t, ctx, srv)
			room := cap(srv.calls)
			var ok, busy atomic.Int32
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
			if !eventually(10*time.Second, func() bool { return len(srv.calls) >= room && (limit == 0 || busy.Load() >= 50) }) {
				t.Fatalf("%d of %d calls queued and %d refused behind the blocked loop", len(srv.calls), room, busy.Load())
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
	if err := srv.onLoop(ctx, func() {}); err != nil {
		t.Fatal(err)
	}
	if after := c.status(l).GetLastIndex(); after != before {
		t.Fatalf("the log grew from %d to %d for a request nobody waited for", before, after)
	}
}

func TestALeaseReadQueuedBehindAPauseWaitsForTheTicksThePauseMissed(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos, cfg.ElectionTick, cfg.LeaseTicks = true, 200, 180 })
	old := c.leader(5 * time.Second)
	srv := c.running[old].srv
	kvc := c.kvClient(old)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := kvc.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("before")}); err != nil {
		t.Fatal(err)
	}
	if _, err := kvc.Get(ctx, &pb.GetRequest{Key: "k", Mode: pb.ReadMode_READ_MODE_INDEX}); err != nil {
		t.Fatal(err)
	}
	chaos(t, c, old, pb.ChaosAction_CHAOS_ACTION_PAUSE, 20*time.Second)
	chaos(t, c, old, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
	next := c.leaderOtherThan(old, 10*time.Second)
	if _, err := c.kvClient(next).Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("after")}); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		resp *pb.GetResponse
		err  error
	}
	read := make(chan answer, 1)
	go func() {
		resp, err := kvc.Get(ctx, &pb.GetRequest{Key: "k", Mode: pb.ReadMode_READ_MODE_LEASE})
		read <- answer{resp, err}
	}()
	for len(srv.calls) == 0 {
		select {
		case a := <-read:
			t.Fatalf("the lease read was answered with %v before it queued behind the pause", a.err)
		case <-time.After(time.Millisecond):
		}
	}
	if srv.paused.remaining() <= 0 {
		t.Fatal("the pause ended before the lease read was queued behind it")
	}
	chaos(t, c, old, pb.ChaosAction_CHAOS_ACTION_PAUSE, time.Millisecond)
	a := <-read
	if status.Code(a.err) != codes.Unavailable {
		t.Fatalf("the paused leader answered a lease read with %q and %v after node %d had written over it, want Unavailable", a.resp.GetKv().GetValue(), a.err, next)
	}
}

func blockLoop(t *testing.T, ctx context.Context, srv *Server) func() {
	t.Helper()
	blocked, release := make(chan struct{}), make(chan struct{})
	failed := make(chan error, 1)
	go func() { failed <- srv.onLoop(ctx, func() { close(blocked); <-release }) }()
	select {
	case <-blocked:
	case err := <-failed:
		t.Fatalf("the loop never ran the blocking call: %v", err)
	}
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	return unblock
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
			chaos(t, c, leader, pb.ChaosAction_CHAOS_ACTION_STALL_DISK, 800*time.Millisecond)
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
