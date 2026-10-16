package snap

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
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
	if s, err := Load(fs, 9, 1); err != nil || string(s.Data) != "x" {
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

func TestAVersionOneSnapshotLoadsWithoutAConfiguration(t *testing.T) {
	fs := vfs.NewMem()
	Save(fs, raft.Snapshot{Index: 5, Term: 1, Data: []byte("old")})
	b, _ := fs.ReadFile(Name(5, 1))
	v1 := append([]byte(nil), b...)
	v1[7] = 1
	binary.LittleEndian.PutUint64(v1[24:], 3)
	binary.LittleEndian.PutUint32(v1[32:], crc32.Checksum([]byte("old"), castagnoli))
	v1 = append(v1[:headerSize], "old"...)
	fs.Truncate(Name(5, 1), 0)
	f, _ := fs.OpenAppend(Name(5, 1))
	f.Write(v1)
	s, err := Load(fs, 5, 1)
	if err != nil || string(s.Data) != "old" || len(s.Conf.Voters) != 0 {
		t.Fatalf("%+v %v", s, err)
	}
}
