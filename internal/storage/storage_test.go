package storage

import (
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

func entries(term, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Data: []byte("x")})
	}
	return out
}

func TestASeededDirectoryOpensAtItsSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	s := raft.Snapshot{Index: 42, Term: 1, Data: []byte("state"), Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}
	if err := Seed(fs, s, options(0)); err != nil {
		t.Fatal(err)
	}
	d, st, _, err := Open(fs, options(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
	}()
	if st.Snapshot == nil || st.Snapshot.Index != 42 || string(st.Snapshot.Data) != "state" || !slices.Equal(st.Snapshot.Conf.Voters, s.Conf.Voters) {
		t.Fatalf("restored snapshot %+v", st.Snapshot)
	}
	if st.HardState != (raft.HardState{Term: 1, Commit: 42}) || len(st.Entries) != 0 {
		t.Fatalf("restored %+v", st)
	}
}

func TestOpeningRemovesUnfinishedSnapshotsAndFailsWithoutTheNamedOne(t *testing.T) {
	fs := vfs.NewMem()
	if err := Seed(fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")}, options(0)); err != nil {
		t.Fatal(err)
	}
	f, err := fs.Create(snap.Name(12, 1) + vfs.TempSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("half")); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove(snap.Name(9, 1)); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := Open(fs, options(0)); err == nil {
		t.Fatal("opened a log whose snapshot is gone")
	}
	names, err := fs.List()
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(names, snap.Name(12, 1)+vfs.TempSuffix) {
		t.Fatalf("the unfinished snapshot is still there: %v", names)
	}
}

func TestCompactingDropsOldSegmentsAndOlderSnapshotsButKeepsTheCurrentOne(t *testing.T) {
	fs := vfs.NewMem()
	d, _, _, err := Open(fs, options(256))
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 40; i++ {
		if err := d.Write(wal.Batch{HardState: raft.HardState{Term: 1, Commit: i - 1}, Entries: entries(1, i, i)}); err != nil {
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
	_, st, _, err := Open(fs, options(256))
	if err != nil {
		t.Fatal(err)
	}
	if st.Snapshot == nil || st.Snapshot.Index != 30 || len(st.Entries) != 10 || st.Entries[0].Index != 31 {
		t.Fatalf("after compaction: snapshot %+v, %d entries", st.Snapshot, len(st.Entries))
	}
}
