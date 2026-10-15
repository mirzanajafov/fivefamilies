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
	s := raft.Snapshot{Index: 42, Term: 3, Data: []byte("state")}
	if err := Save(fs, s); err != nil {
		t.Fatal(err)
	}
	data, err := Load(fs, 42, 3)
	if err != nil || string(data) != "state" {
		t.Fatalf("%q %v", data, err)
	}
	if _, err := Load(fs, 42, 4); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("loading a snapshot that does not exist: %v", err)
	}
}

func TestEveryFlippedByteIsCaught(t *testing.T) {
	s := raft.Snapshot{Index: 7, Term: 2, Data: []byte("some state to protect")}
	fs := vfs.NewMem()
	Save(fs, s)
	good, _ := fs.ReadFile(Name(7, 2))
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x40
		fs.Truncate(Name(7, 2), 0)
		f, _ := fs.OpenAppend(Name(7, 2))
		f.Write(bad)
		if _, err := Load(fs, 7, 2); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flipped byte %d went unnoticed", i)
		}
	}
}

func TestACrashBeforeTheRenameLeavesNoSnapshot(t *testing.T) {
	fs := vfs.NewMem()
	f, _ := fs.Create(Name(9, 1) + tmpSuffix)
	f.Write([]byte("half"))
	fs.Crash(rand.New(rand.NewPCG(1, 1)))
	if _, err := Load(fs, 9, 1); err == nil {
		t.Fatal("a half-written snapshot loaded")
	}
	if err := Save(fs, raft.Snapshot{Index: 9, Term: 1, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	fs.Crash(rand.New(rand.NewPCG(2, 2)))
	if data, err := Load(fs, 9, 1); err != nil || string(data) != "x" {
		t.Fatalf("a saved snapshot did not survive a crash: %v", err)
	}
}

func TestCleanKeepsOnlyTheNewest(t *testing.T) {
	fs := vfs.NewMem()
	for i := uint64(1); i <= 3; i++ {
		Save(fs, raft.Snapshot{Index: i * 10, Term: 1, Data: []byte{byte(i)}})
	}
	f, _ := fs.Create("stray.snap.tmp")
	f.Close()
	w, _ := fs.Create("0000000000000001.wal")
	w.Close()
	if err := Clean(fs, raft.Snapshot{Index: 30, Term: 1}); err != nil {
		t.Fatal(err)
	}
	names, _ := fs.List()
	if len(names) != 2 || !slices.Contains(names, "0000000000000001.wal") || !slices.Contains(names, Name(30, 1)) {
		t.Fatalf("left %v", names)
	}
}
