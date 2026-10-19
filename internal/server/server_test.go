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
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
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
		Fatal:         func(err error) { c.t.Errorf("node %d: %v", id, err) },
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
	go g.Serve(ln)
	go func() {
		srv.Run(ctx)
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
			if st.GetState() == "leader" && st.GetTerm() >= term {
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

func TestANodeRemovesUnfinishedSnapshotsWhenItStarts(t *testing.T) {
	c := newTestCluster(t, 1, nil)
	c.stop(1)
	unfinished := filepath.Join(c.dirs[1], snap.Name(7, 1)+".tmp")
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
