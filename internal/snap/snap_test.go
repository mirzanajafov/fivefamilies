package snap

import (
	"errors"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

func TestSaveLoad(t *testing.T) {
	fs := vfs.NewMem()
	s := raft.Snapshot{Index: 42, Term: 3, Data: []byte("state"), Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 300}, Learners: []raft.NodeID{7}}}
	if err := Save(fs, s); err != nil {
		t.Fatal(err)
	}
	got, err := Load(fs, 42, 3)
	if err != nil || string(got.Data) != "state" || got.Index != 42 || got.Term != 3 {
		t.Fatalf("%+v %v", got, err)
	}
	if !slices.Equal(got.Conf.Voters, s.Conf.Voters) || !slices.Equal(got.Conf.Learners, s.Conf.Learners) {
		t.Fatalf("conf %+v, saved %+v", got.Conf, s.Conf)
	}
	if _, err := Load(fs, 42, 4); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("loading a snapshot that does not exist: %v", err)
	}
}

func TestEveryFlippedByteIsCaught(t *testing.T) {
	s := raft.Snapshot{Index: 7, Term: 2, Data: []byte("some state to protect"), Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}}}
	fs := vfs.NewMem()
	if err := Save(fs, s); err != nil {
		t.Fatal(err)
	}
	good, _ := fs.ReadFile(Name(7, 2))
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x40
		if err := vfs.WriteAtomic(fs, Name(7, 2), bad); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(fs, 7, 2); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flipped byte %d went unnoticed", i)
		}
	}
}

func TestACrashBeforeTheRenameLeavesNoSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	f, err := fs.Create(Name(9, 1) + vfs.TempSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("half")); err != nil {
		t.Fatal(err)
	}
	fs.Crash(rand.New(rand.NewPCG(1, 1)))
	if _, err := Load(fs, 9, 1); err == nil {
		t.Fatal("a half-written snapshot loaded")
	}
	if err := Save(fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	fs.Crash(rand.New(rand.NewPCG(2, 2)))
	if s, err := Load(fs, 9, 1); err != nil || string(s.Data) != "x" {
		t.Fatalf("a saved snapshot did not survive a crash: %v", err)
	}
}

type syncHook struct {
	vfs.FS
	onSync func()
}

func (h syncHook) Create(name string) (vfs.File, error) {
	f, err := h.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return hookedFile{File: f, onSync: h.onSync}, nil
}

type hookedFile struct {
	vfs.File
	onSync func()
}

func (f hookedFile) Sync() error {
	f.onSync()
	return f.File.Sync()
}

func writeFile(t *testing.T, fs vfs.FS, name string, data []byte) {
	t.Helper()
	f, err := fs.Create(name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRemovingOldSnapshotsSparesTheOnesSavedSinceTheCompaction(t *testing.T) {
	fs := vfs.NewMem()
	writeFile(t, fs, "0000000000000001.wal", nil)
	old := raft.Snapshot{Index: 5, Term: 1, Data: []byte("old")}
	compacted := raft.Snapshot{Index: 10, Term: 1, Data: []byte("compacted")}
	saved := raft.Snapshot{Index: 20, Term: 1, Data: []byte("saved")}
	saving := raft.Snapshot{Index: 30, Term: 2, Data: []byte("saving")}
	for _, s := range []raft.Snapshot{old, compacted, saved} {
		if err := Save(fs, s); err != nil {
			t.Fatal(err)
		}
	}
	removeMidSave := syncHook{FS: fs, onSync: func() {
		if err := RemoveBelow(fs, compacted.Index); err != nil {
			t.Fatal(err)
		}
	}}
	if err := Save(removeMidSave, saving); err != nil {
		t.Fatalf("a save with a cleanup in the middle failed: %v", err)
	}
	for _, s := range []raft.Snapshot{compacted, saved, saving} {
		if got, err := Load(fs, s.Index, s.Term); err != nil || string(got.Data) != string(s.Data) {
			t.Fatalf("snapshot %d after removing those below %d: %v", s.Index, compacted.Index, err)
		}
	}
	if _, err := Load(fs, old.Index, old.Term); err == nil {
		t.Fatalf("snapshot %d survived removing those below %d", old.Index, compacted.Index)
	}
	if err := RemoveBelow(fs, saving.Index); err != nil {
		t.Fatal(err)
	}
	if names, _ := fs.List(); !slices.Equal(names, []string{"0000000000000001.wal", Name(30, 2)}) {
		t.Fatalf("left %v", names)
	}
}

func TestCleanupLeavesFilesThatSaveNeverWrites(t *testing.T) {
	fs := vfs.NewMem()
	foreign := []string{"0000000000000001-1.snap", "000000000000000A-000000000000000a.snap", "zzzzzzzzzzzzzzzz-0000000000000001.snap", "keep-1.snap",
		"0000000000000001-1.snap.tmp", "keep-1.snap.tmp", "other.tmp"}
	for _, n := range foreign {
		writeFile(t, fs, n, nil)
	}
	if err := RemoveBelow(fs, 1<<62); err != nil {
		t.Fatal(err)
	}
	if err := RemoveUnfinished(fs); err != nil {
		t.Fatal(err)
	}
	if names, _ := fs.List(); !slices.Equal(names, slices.Sorted(slices.Values(foreign))) {
		t.Fatalf("left %v of %v", names, foreign)
	}
}

func TestRemovingUnfinishedSavesLeavesEverythingElse(t *testing.T) {
	fs := vfs.NewMem()
	if err := Save(fs, raft.Snapshot{Index: 10, Term: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	writeFile(t, fs, "0000000000000001.wal", nil)
	writeFile(t, fs, Name(20, 1)+vfs.TempSuffix, []byte("half"))
	if err := RemoveUnfinished(fs); err != nil {
		t.Fatal(err)
	}
	if names, _ := fs.List(); !slices.Equal(names, []string{Name(10, 1), "0000000000000001.wal"}) {
		t.Fatalf("left %v", names)
	}
}
