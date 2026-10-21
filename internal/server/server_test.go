package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
	"github.com/mirzanajafov/fivefamilies/internal/storage"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type running struct {
	srv    *Server
	grpc   *grpc.Server
	cancel context.CancelFunc
	done   chan struct{}
}

type testCluster struct {
	t       *testing.T
	nodes   map[raft.NodeID]string
	dirs    map[raft.NodeID]string
	running map[raft.NodeID]*running
	tweak   func(*Config)
}

func newTestCluster(t *testing.T, n int, tweak func(*Config)) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, running: map[raft.NodeID]*running{}, tweak: tweak}
	listeners := map[raft.NodeID]net.Listener{}
	for i := 1; i <= n; i++ {
		id := raft.NodeID(i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = ln
		c.nodes[id] = ln.Addr().String()
		c.dirs[id] = filepath.Join(t.TempDir(), fmt.Sprint(id))
	}
	for id, ln := range listeners {
		c.startOn(id, ln)
	}
	t.Cleanup(func() {
		for id := range c.running {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id raft.NodeID) {
	c.t.Helper()
	var ln net.Listener
	var err error
	for range 50 {
		if ln, err = net.Listen("tcp", c.nodes[id]); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatal(err)
	}
	c.startOn(id, ln)
}

func (c *testCluster) startOn(id raft.NodeID, ln net.Listener) {
	c.t.Helper()
	cfg := Config{
		ID:            id,
		Nodes:         c.nodes,
		DataDir:       c.dirs[id],
		Tick:          5 * time.Millisecond,
		HeartbeatTick: 4,
		ElectionTick:  30,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if c.tweak != nil {
		c.tweak(&cfg)
	}
	srv, err := New(cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	g := grpc.NewServer()
	srv.Register(g)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{srv: srv, grpc: g, cancel: cancel, done: make(chan struct{})}
	go func() {
		if err := g.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			c.t.Error(err)
		}
	}()
	go func() {
		if err := srv.Run(ctx); err != nil {
			c.t.Errorf("node %d: %v", id, err)
		}
		close(r.done)
	}()
	c.running[id] = r
}

func (c *testCluster) stop(id raft.NodeID) {
	r, ok := c.running[id]
	if !ok {
		return
	}
	r.grpc.Stop()
	r.cancel()
	<-r.done
	delete(c.running, id)
}

func (c *testCluster) addrs() []string {
	var out []string
	for i := 1; i <= len(c.nodes); i++ {
		out = append(out, c.nodes[raft.NodeID(i)])
	}
	return out
}

func (c *testCluster) leader(timeout time.Duration) raft.NodeID {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader raft.NodeID
		var term uint64
		for id, r := range c.running {
			st := r.srv.Status()
			if st.GetRole() == pb.Role_ROLE_LEADER && st.GetTerm() >= term {
				leader, term = id, st.GetTerm()
			}
		}
		if leader != raft.None {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return raft.None
}

func (c *testCluster) status(id raft.NodeID) *pb.StatusResponse {
	c.t.Helper()
	conn, err := grpc.NewClient(c.nodes[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := pb.NewAdminClient(conn).Status(ctx, &pb.StatusRequest{Hash: true})
	if err != nil {
		c.t.Fatal(err)
	}
	return st
}

func TestAClusterWritesReadsAndSurvivesItsLeader(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 50 {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%d", i%10), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []client.ReadMode{client.ReadIndex, client.ReadLog} {
		kv, found, err := cl.Get(ctx, "k9", mode)
		if err != nil || !found || string(kv.Value) != "49" {
			t.Fatalf("read mode %d: %q %v %v", mode, kv.Value, found, err)
		}
	}
	swapped, cur, err := cl.CompareAndSwap(ctx, "k9", client.Expect{Value: []byte("49")}, []byte("swapped"))
	if err != nil || !swapped {
		t.Fatalf("swap: %v %+v %v", swapped, cur, err)
	}
	old := c.leader(time.Second)
	c.stop(old)
	for i := range 20 {
		if _, err := cl.Put(ctx, "after", []byte(fmt.Sprint(i))); err != nil {
			t.Fatalf("write %d after the leader stopped: %v", i, err)
		}
	}
	if c.leader(5*time.Second) == old {
		t.Fatal("the stopped node is still the leader")
	}
	c.start(old)
	if _, err := cl.Put(ctx, "last", []byte("x")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		var hashes []uint64
		var revs []uint64
		for id := range c.running {
			st := c.status(id)
			hashes = append(hashes, st.GetStateHash())
			revs = append(revs, st.GetHashIndex())
		}
		if revs[0] == revs[1] && revs[1] == revs[2] {
			if hashes[0] != hashes[1] || hashes[1] != hashes[2] {
				t.Fatalf("same revision %d, different states %v", revs[0], hashes)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not converge: revisions %v", revs)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAClientStartsANewSessionAfterTheClusterForgetsItsOldOne(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.MaxSessions = 1 })
	c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	forgotten, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer forgotten.Close()
	other, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := forgotten.Put(ctx, "k", []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Put(ctx, "k", []byte("other")); err != nil {
		t.Fatal(err)
	}
	if _, err := forgotten.Put(ctx, "k", []byte("lost")); !errors.Is(err, client.ErrSessionExpired) {
		t.Fatalf("a write on a forgotten session: %v", err)
	}
	if _, err := forgotten.Put(ctx, "k", []byte("again")); err != nil {
		t.Fatalf("the write after the session expired: %v", err)
	}
	if _, err := other.Put(ctx, "k", []byte("lost")); !errors.Is(err, client.ErrSessionExpired) {
		t.Fatalf("a write on the session the new one evicted: %v", err)
	}
	if _, err := other.Put(ctx, "k", []byte("other again")); err != nil {
		t.Fatalf("the evicted client's next write: %v", err)
	}
	if kv, _, err := forgotten.Get(ctx, "k", client.ReadIndex); err != nil || string(kv.Value) != "other again" {
		t.Fatalf("read %q %v", kv.Value, err)
	}
}

func TestNoAppliedResultTellsTheClientToRetry(t *testing.T) {
	for applied := kv.OK + 1; applied <= kv.ErrTooManyLeases; applied++ {
		switch code := status.Code(outcome(kv.Result{Code: applied})); code {
		case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted:
			t.Errorf("%s reaches the client as %s, which it retries with the same sequence number", applied, code)
		}
	}
}

func TestAWriteGivesUpWhileWaitingBehindAnotherWrite(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.leader(5 * time.Second)
	for id := range c.nodes {
		if id != leader {
			c.stop(id)
		}
	}
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	stuck, cancel := context.WithCancel(context.Background())
	waiting := make(chan struct{})
	go func() {
		defer close(waiting)
		if _, err := cl.Put(stuck, "k", []byte("waits for a quorum")); err == nil {
			t.Error("a write without a quorum was acknowledged")
		}
	}()
	defer func() {
		cancel()
		<-waiting
	}()
	for c.running[leader].srv.counts.writes.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancelShort := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelShort()
	gaveUp := make(chan error, 1)
	go func() {
		_, err := cl.Put(ctx, "k", []byte("gives up"))
		gaveUp <- err
	}()
	select {
	case err := <-gaveUp:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("the second write returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the second write waited past its deadline for the first")
	}
}

func TestANodeRemovesUnfinishedSnapshotsWhenItStarts(t *testing.T) {
	c := newTestCluster(t, 1, nil)
	c.stop(1)
	unfinished := filepath.Join(c.dirs[1], snap.Name(7, 1)+vfs.TempSuffix)
	if err := os.WriteFile(unfinished, []byte("half"), 0o600); err != nil {
		t.Fatal(err)
	}
	c.start(1)
	if _, err := os.Stat(unfinished); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the unfinished snapshot is still there: %v", err)
	}
}

func TestARestartedClusterKeepsItsData(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.SnapshotEvery = 20; cfg.Retain = 5 })
	c.leader(5 * time.Second)
	cl, _ := client.New(c.addrs(), client.Options{})
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 100 {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	for id := range c.nodes {
		c.stop(id)
	}
	for id := range c.nodes {
		c.start(id)
	}
	c.leader(5 * time.Second)
	cl2, _ := client.New(c.addrs(), client.Options{})
	defer cl2.Close()
	kvs, err := cl2.Range(ctx, "k", 0)
	if err != nil || len(kvs) != 100 {
		t.Fatalf("after a full restart: %d keys, %v", len(kvs), err)
	}
	if st := c.status(c.leader(time.Second)); st.GetSnapshotIndex() == 0 {
		t.Fatal("no snapshot was taken")
	}
}

func TestASnapshotThatCannotBeSavedEndsRunWithoutItsLogRecordAndCallsStopWaiting(t *testing.T) {
	dir := t.TempDir()
	srv, err := New(Config{ID: 1, Nodes: map[raft.NodeID]string{1: "127.0.0.1:1"}, DataDir: dir, SnapshotEvery: 4, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stopRun := context.WithCancel(context.Background())
	var runErr error
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		runErr = srv.Run(runCtx)
	}()
	t.Cleanup(func() {
		stopRun()
		<-finished
	})
	deadline := time.Now().Add(5 * time.Second)
	for srv.Status().GetRole() != pb.Role_ROLE_LEADER {
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for index := range uint64(8) {
		if err := os.MkdirAll(filepath.Join(dir, snap.Name(index+1, srv.Status().GetTerm()), "taken"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var last uint64
	for i := range 8 {
		res, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}})
		if err != nil {
			if !errors.Is(err, ErrStopped) {
				t.Fatalf("a write to a stopping node: %v", err)
			}
			break
		}
		last = res.Rev
	}
	if last < 4 {
		t.Fatalf("the last answered write is at %d, so the write whose entry reached the snapshot at 4 lost its answer", last)
	}
	select {
	case <-finished:
		if runErr == nil || !strings.Contains(runErr.Error(), "saving snapshot") {
			t.Fatalf("Run returned %v", runErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run went on after its snapshot could not be saved")
	}
	d, st, _, err := storage.Open(vfs.OS{Dir: dir}, wal.Options{Salt: randomUint64})
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if st.Snapshot != nil {
		t.Fatalf("the log names snapshot %d, which was never saved", st.Snapshot.Index)
	}
	if _, err := srv.Submit(ctx, kv.Command{Op: kv.OpPut, Key: "k"}); !errors.Is(err, ErrStopped) {
		t.Fatalf("a write to a stopped node: %v", err)
	}
	if _, _, err := srv.StateHash(ctx); !errors.Is(err, ErrStopped) {
		t.Fatalf("a hash from a stopped node: %v", err)
	}
}

func TestCallsShapedByThePreviousBuildStillWork(t *testing.T) {
	c := newTestCluster(t, 1, func(cfg *Config) { cfg.Chaos = true })
	leader := c.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(c.nodes[leader], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if st := c.status(leader); st.GetState() != "leader" || st.GetRole() != pb.Role_ROLE_LEADER {
		t.Fatalf("status says %q and %v, want both to name the leader", st.GetState(), st.GetRole())
	}
	session, err := pb.NewKVClient(conn).Register(ctx, &pb.RegisterRequest{})
	if err != nil || session.GetClient() == 0 {
		t.Fatalf("registering a session the old way: %v %v", session, err)
	}
	if _, err := pb.NewKVClient(conn).Get(ctx, &pb.GetRequest{Key: "k"}); err != nil {
		t.Fatalf("a read without a mode, which was ReadIndex: %v", err)
	}
	srv := c.running[leader].srv
	for _, tc := range []struct {
		action string
		fault  *fault
	}{
		{"isolate", &srv.isolated.fault},
		{"stall-disk", &srv.stalled},
		{"pause", &srv.paused},
	} {
		if _, err := c.admin(leader).Chaos(ctx, &pb.ChaosRequest{LegacyAction: tc.action, DurationMs: 2000}); err != nil || tc.fault.remaining() <= 0 {
			t.Fatalf("chaos %q named the old way: %v", tc.action, err)
		}
	}
	if _, err := c.admin(leader).Chaos(ctx, &pb.ChaosRequest{LegacyAction: "heal"}); err != nil {
		t.Fatalf("heal named the old way: %v", err)
	}
	if srv.isolated.remaining() > 0 || srv.stalled.remaining() > 0 || srv.paused.remaining() > 0 {
		t.Fatal("the old heal left a fault running")
	}
}

func TestEveryRaftStateHasARoleOnTheWire(t *testing.T) {
	for i := range 256 {
		state := raft.StateType(i)
		if !strings.HasPrefix(state.String(), "StateType(") && roles[state] == pb.Role_ROLE_UNSPECIFIED {
			t.Errorf("%v has no role", state)
		}
	}
}

func TestErrorsClientsActOnCarryAReasonAndKeepTheirCodeAndMessage(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	leader := c.leader(5 * time.Second)
	follower := leader%3 + 1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connTo := func(id raft.NodeID) *grpc.ClientConn {
		conn, err := grpc.NewClient(c.nodes[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	kvc := pb.NewKVClient(connTo(leader))
	session, err := kvc.OpenSession(ctx, &pb.OpenSessionRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kvc.Put(ctx, &pb.PutRequest{Client: session.GetClient(), Seq: 2, Key: "k"}); err != nil {
		t.Fatal(err)
	}
	_, notLeader := pb.NewKVClient(connTo(follower)).Put(ctx, &pb.PutRequest{Key: "k"})
	_, noLease := pb.NewLeaseClient(connTo(leader)).TimeToLive(ctx, &pb.LeaseTimeToLiveRequest{Id: 404})
	_, stale := kvc.Put(ctx, &pb.PutRequest{Client: session.GetClient(), Seq: 1, Key: "k"})
	_, expired := kvc.Put(ctx, &pb.PutRequest{Client: session.GetClient() + 1000, Seq: 1, Key: "k"})
	for _, tc := range []struct {
		err    error
		code   codes.Code
		msg    string
		reason pb.ErrorReason
	}{
		{notLeader, codes.Unavailable, "not the leader", pb.ErrorReason_ERROR_REASON_NOT_LEADER},
		{stale, codes.FailedPrecondition, "stale sequence number", pb.ErrorReason_ERROR_REASON_STALE_SEQUENCE},
		{expired, codes.FailedPrecondition, "session expired", pb.ErrorReason_ERROR_REASON_SESSION_EXPIRED},
		{noLease, codes.NotFound, "lease not found", pb.ErrorReason_ERROR_REASON_LEASE_NOT_FOUND},
		{outcome(kv.Result{Code: kv.ErrTooManyLeases}), codes.FailedPrecondition, "too many leases", pb.ErrorReason_ERROR_REASON_TOO_MANY_LEASES},
	} {
		if s := status.Convert(tc.err); s.Code() != tc.code || s.Message() != tc.msg || client.Reason(tc.err) != tc.reason {
			t.Errorf("got %v %q %v, want %v %q %v", s.Code(), s.Message(), client.Reason(tc.err), tc.code, tc.msg, tc.reason)
		}
	}
}
