package server

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func TestChaosIsBoundedAndOffByDefault(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	c2 := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	c2.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_PAUSE, DurationMs: 100}); err == nil {
		t.Fatal("chaos ran on a node started without it")
	}
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_PAUSE, DurationMs: uint64(2 * time.Minute / time.Millisecond)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a two-minute pause got %v", err)
	}
	if _, err := c2.admin(1).Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction(42), DurationMs: 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("an unknown action got %v", err)
	}
	old := c2.leader(time.Second)
	chaos(t, c2, old, pb.ChaosAction_CHAOS_ACTION_ISOLATE, 2*time.Second)
	c2.leaderOtherThan(old, 3*time.Second)
}

func TestHealClearsAnIsolationEarly(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	old := c.leader(5 * time.Second)
	chaos(t, c, old, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
	if c.running[old].srv.isolated.Allow(old, old%3+1) {
		t.Fatal("isolate did not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.admin(old).Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_HEAL}); err != nil {
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
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_ISOLATE, 500*time.Millisecond)
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
	time.Sleep(800 * time.Millisecond)
	if !isolated() {
		t.Fatal("the first isolation ended the second")
	}
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_ISOLATE, 500*time.Millisecond)
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
	time.Sleep(800 * time.Millisecond)
	if !isolated() {
		t.Fatal("an isolation from before a heal ended the one after it")
	}
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
	chaos(t, c, 1, pb.ChaosAction_CHAOS_ACTION_ISOLATE, 200*time.Millisecond)
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
	caughtUp := eventually(within, func() bool {
		return c.running[follower].srv.Status().GetCommit() >= c.running[leader].srv.Status().GetCommit()
	})
	if !caughtUp {
		t.Fatalf("node %d did not catch up within %v", follower, within)
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
	chaos(t, c, follower, pb.ChaosAction_CHAOS_ACTION_PAUSE, time.Minute)
	chaos(t, c, leader, pb.ChaosAction_CHAOS_ACTION_STALL_DISK, time.Minute)
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
	chaos(t, c, leader, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
	chaos(t, c, follower, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
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
	chaos(t, c, follower, pb.ChaosAction_CHAOS_ACTION_PAUSE, time.Minute)
	chaos(t, c, follower, pb.ChaosAction_CHAOS_ACTION_PAUSE, 200*time.Millisecond)
	if r := c.running[follower].srv.paused.remaining(); r <= 0 || r > 200*time.Millisecond {
		t.Fatalf("the later pause left %v of pausing", r)
	}
	if _, err := cl.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	waitCaughtUp(t, c, leader, follower, 3*time.Second)
}
