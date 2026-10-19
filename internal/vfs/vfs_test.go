package vfs

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"slices"
	"testing"
)

func write(t *testing.T, fs FS, name string, data []byte, sync bool) {
	t.Helper()
	f, err := fs.OpenAppend(name)
	if err != nil {
		f, err = fs.Create(name)
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if sync {
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()
}

func TestMemCrashKeepsSyncedBytesAndAPrefixOfTheRest(t *testing.T) {
	synced := bytes.Repeat([]byte{'s'}, 1000)
	unsynced := bytes.Repeat([]byte{'u'}, 3000)
	shorter, garbled := 0, 0
	for seed := range uint64(200) {
		m := NewMem()
		write(t, m, "a", synced, true)
		m.SyncDir()
		write(t, m, "a", unsynced, false)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		got, err := m.ReadFile("a")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got[:len(synced)], synced) {
			t.Fatalf("seed %d: synced bytes changed", seed)
		}
		rest := got[len(synced):]
		if len(rest) < len(unsynced) {
			shorter++
		}
		if len(rest) != len(unsynced) && len(rest) > 0 && (len(synced)+len(rest))%m.SectorSize != 0 {
			t.Fatalf("seed %d: cut at %d, not on a sector boundary", seed, len(synced)+len(rest))
		}
		if !bytes.Equal(rest, unsynced[:len(rest)]) {
			garbled++
		}
	}
	if shorter < 100 || garbled == 0 {
		t.Fatalf("over 200 crashes: %d cut short, %d garbled", shorter, garbled)
	}
}

func TestMemFilesNeedADirectorySyncToSurviveACrash(t *testing.T) {
	m := NewMem()
	write(t, m, "kept", []byte("x"), true)
	m.SyncDir()
	write(t, m, "new", []byte("y"), true)
	m.Remove("kept")
	m.Crash(rand.New(rand.NewPCG(1, 1)))
	names, _ := m.List()
	if len(names) != 1 || names[0] != "kept" {
		t.Fatalf("after a crash without a directory sync: %v", names)
	}
	m.Rename("kept", "renamed")
	m.SyncDir()
	m.Crash(rand.New(rand.NewPCG(1, 1)))
	if names, _ := m.List(); len(names) != 1 || names[0] != "renamed" {
		t.Fatalf("after a synced rename: %v", names)
	}
}

type unsyncableDir struct {
	FS
}

func (unsyncableDir) SyncDir() error {
	return errors.New("the directory could not be synced")
}

func TestAnAtomicWriteSurvivesACrashAndReportsAFailedDirectorySync(t *testing.T) {
	m := NewMem()
	if err := WriteAtomic(m, "kept", []byte("old")); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(m, "kept", []byte("new")); err != nil {
		t.Fatal(err)
	}
	m.Crash(rand.New(rand.NewPCG(1, 1)))
	if got, err := m.ReadFile("kept"); err != nil || string(got) != "new" {
		t.Fatalf("after a crash: %q %v", got, err)
	}
	if names, _ := m.List(); !slices.Equal(names, []string{"kept"}) {
		t.Fatalf("left %v", names)
	}
	if err := WriteAtomic(unsyncableDir{m}, "lost", []byte("x")); err == nil {
		t.Fatal("a directory sync that failed was not reported")
	}
}

func TestOS(t *testing.T) {
	fs := OS{Dir: t.TempDir()}
	write(t, fs, "seg", []byte("hello "), true)
	write(t, fs, "seg", []byte("world"), true)
	if err := fs.SyncDir(); err != nil {
		t.Fatal(err)
	}
	got, err := fs.ReadFile("seg")
	if err != nil || string(got) != "hello world" {
		t.Fatalf("%q %v", got, err)
	}
	if err := fs.Truncate("seg", 5); err != nil {
		t.Fatal(err)
	}
	if err := fs.Rename("seg", "old"); err != nil {
		t.Fatal(err)
	}
	names, _ := fs.List()
	if len(names) != 1 || names[0] != "old" {
		t.Fatalf("names %v", names)
	}
	if got, _ := fs.ReadFile("old"); string(got) != "hello" {
		t.Fatalf("after truncate %q", got)
	}
	if err := fs.Remove("old"); err != nil {
		t.Fatal(err)
	}
	if err := fs.Remove("old"); err != nil {
		t.Fatalf("removing a missing file: %v", err)
	}
}
