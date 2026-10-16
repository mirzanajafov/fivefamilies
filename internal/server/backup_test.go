package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func streamBackup(t *testing.T, c *testCluster, id raft.NodeID) backup.File {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := c.admin(id).Backup(ctx, &pb.BackupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var f backup.File
	for first := true; ; first = false {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return f
		}
		if err != nil {
			t.Fatal(err)
		}
		if first {
			f.Rev, f.Hash = chunk.GetRev(), chunk.GetHash()
		}
		f.Data = append(f.Data, chunk.GetData()...)
	}
}

func contentOf(t *testing.T, ctx context.Context, cl *client.Client) uint64 {
	t.Helper()
	kvs, err := cl.Range(ctx, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	out := make([]kv.KeyValue, len(kvs))
	for i, k := range kvs {
		out[i] = kv.KeyValue{Key: k.Key, Value: k.Value, CreateRev: k.CreateRev, ModRev: k.ModRev, Version: k.Version, Lease: k.Lease}
	}
	return kv.ContentHash(out)
}

func TestABackupStartsANewCluster(t *testing.T) {
	a := newTestCluster(t, 3, nil)
	a.leader(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, err := client.New(a.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	for i := range 50 {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%02d", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	f := streamBackup(t, a, a.leader(time.Second))
	sum, err := backup.Check(f)
	if err != nil || sum.Keys != 50 {
		t.Fatalf("backup: %+v %v", sum, err)
	}
	path := filepath.Join(t.TempDir(), "x.backup")
	if err := backup.Save(path, f); err != nil {
		t.Fatal(err)
	}
	f, _, err = backup.Load(path)
	if err != nil {
		t.Fatal(err)
	}

	b := &testCluster{t: t, nodes: map[raft.NodeID]string{}, dirs: map[raft.NodeID]string{}, running: map[raft.NodeID]*running{}}
	lns := map[raft.NodeID]net.Listener{}
	for i := 1; i <= 3; i++ {
		id := raft.NodeID(i)
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id], b.nodes[id], b.dirs[id] = ln, ln.Addr().String(), filepath.Join(t.TempDir(), fmt.Sprint(id))
		done, err := Restore(b.dirs[id], []raft.NodeID{1, 2, 3}, f)
		if err != nil || !done {
			t.Fatalf("restore %d: %v %v", id, done, err)
		}
		if again, err := Restore(b.dirs[id], []raft.NodeID{1, 2, 3}, f); err != nil || again {
			t.Fatalf("restoring over an existing log: %v %v", again, err)
		}
	}
	for id, ln := range lns {
		b.startOn(id, ln)
	}
	t.Cleanup(func() {
		for id := range b.running {
			b.stop(id)
		}
	})
	b.leader(5 * time.Second)
	bcl, err := client.New(b.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer bcl.Close()
	if got := contentOf(t, ctx, bcl); got != sum.Content {
		t.Fatalf("the restored cluster's content is %x, the backup's %x", got, sum.Content)
	}
	if kv, found, err := bcl.Get(ctx, "k49", client.ReadIndex); err != nil || !found || string(kv.Value) != "49" {
		t.Fatalf("k49 after the restore: %q %v %v", kv.Value, found, err)
	}
	if _, err := bcl.Put(ctx, "after", []byte("x")); err != nil {
		t.Fatalf("the restored cluster takes no writes: %v", err)
	}
	if st := b.status(1); st.GetSnapshotIndex() != f.Rev || st.GetCommit() <= f.Rev {
		t.Fatalf("status after the restore: %+v", st)
	}
}
