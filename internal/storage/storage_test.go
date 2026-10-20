package storage

import (
	"errors"
	"slices"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

func options(segment int64) wal.Options {
	return wal.Options{SegmentBytes: segment, Salt: func() uint64 { return 7 }}
}

func seed(t *testing.T, fs vfs.FS, s raft.Snapshot) {
	t.Helper()
	if seeded, err := Seed(fs, s, options(0)); err != nil || !seeded {
		t.Fatalf("seeding an empty directory: %v %v", seeded, err)
	}
}

func open(t *testing.T, fs vfs.FS, segment int64) (*Disk, raft.Restore) {
	t.Helper()
	d, st, _, err := Open(fs, options(segment))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	})
	return d, st
}

func TestASeededDirectoryOpensAtItsSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	s := raft.Snapshot{Index: 42, Term: 1, Data: []byte("state"), Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}
	seed(t, fs, s)
	_, st := open(t, fs, 0)
	if st.Snapshot == nil || st.Snapshot.Index != 42 || string(st.Snapshot.Data) != "state" || !slices.Equal(st.Snapshot.Conf.Voters, s.Conf.Voters) {
		t.Fatalf("restored snapshot %+v", st.Snapshot)
	}
	if st.HardState != (raft.HardState{Term: 1, Commit: 42}) || len(st.Entries) != 0 {
		t.Fatalf("restored %+v", st)
	}
}

func TestSeedingLeavesADirectoryWithALogAlone(t *testing.T) {
	fs := vfs.NewMem()
	d, _, _, err := Open(fs, options(0))
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Write(wal.Batch{HardState: raft.HardState{Term: 3, Vote: 2}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if seeded, err := Seed(fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")}, options(0)); err != nil || seeded {
		t.Fatalf("seeded over a log: %v %v", seeded, err)
	}
	_, st := open(t, fs, 0)
	if st.Snapshot != nil || st.HardState != (raft.HardState{Term: 3, Vote: 2}) {
		t.Fatalf("the log changed: snapshot %+v, hard state %+v", st.Snapshot, st.HardState)
	}
}

func TestOpeningRemovesUnfinishedSnapshots(t *testing.T) {
	fs := vfs.NewMem()
	seed(t, fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")})
	f, err := fs.Create(snap.Name(12, 1) + vfs.TempSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	open(t, fs, 0)
	names, err := fs.List()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(names, snap.Name(12, 1)+vfs.TempSuffix) {
		t.Fatalf("the unfinished snapshot is still there: %v", names)
	}
}

func TestOpeningFailsWhenTheMarkedSnapshotIsGone(t *testing.T) {
	fs := vfs.NewMem()
	seed(t, fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")})
	if err := fs.Remove(snap.Name(9, 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Open(fs, options(0)); !errors.Is(err, snap.ErrCorrupt) {
		t.Fatalf("opened a log whose snapshot is gone: %v", err)
	}
}

func TestCompactingDropsOldSegmentsAndOlderSnapshotsButKeepsTheCurrentOne(t *testing.T) {
	fs := vfs.NewMem()
	d, _, _, err := Open(fs, options(256))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 40; i++ {
		if err := d.Write(wal.Batch{HardState: raft.HardState{Term: 1, Commit: i - 1}, Entries: []raft.Entry{{Term: 1, Index: i, Data: []byte("x")}}}); err != nil {
			t.Fatal(err)
		}
	}
	for _, s := range []raft.Snapshot{{Index: 10, Term: 1}, {Index: 30, Term: 1}} {
		if err := d.SaveSnapshot(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Write(wal.Batch{HardState: raft.HardState{Term: 1, Commit: 30}, Snapshot: &raft.Snapshot{Index: 30, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := d.Sync(); err != nil {
		t.Fatal(err)
	}
	removed, err := d.Compact(30)
	if err != nil || removed == 0 {
		t.Fatalf("removed %d segments: %v", removed, err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	names, err := fs.List()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(names, snap.Name(10, 1)) || !slices.Contains(names, snap.Name(30, 1)) {
		t.Fatalf("left %v", names)
	}
	_, st := open(t, fs, 256)
	if st.Snapshot == nil || st.Snapshot.Index != 30 || len(st.Entries) != 10 || st.Entries[0].Index != 31 {
		t.Fatalf("after compaction: snapshot %+v, %d entries", st.Snapshot, len(st.Entries))
	}
}
