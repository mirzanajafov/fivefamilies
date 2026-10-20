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
	must(t, f.Close())
}

func TestMemCrashKeepsSyncedBytesAndAPrefixOfTheRest(t *testing.T) {
	synced := bytes.Repeat([]byte{'s'}, 1000)
	unsynced := bytes.Repeat([]byte{'u'}, 3000)
	shorter, garbled := 0, 0
	for seed := range uint64(200) {
		m := NewMem()
		write(t, m, "a", synced, true)
		must(t, m.SyncDir())
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
	must(t, m.SyncDir())
	write(t, m, "new", []byte("y"), true)
	must(t, m.Remove("kept"))
	m.Crash(rand.New(rand.NewPCG(1, 1)))
	names, _ := m.List()
	if len(names) != 1 || names[0] != "kept" {
		t.Fatalf("after a crash without a directory sync: %v", names)
	}
	must(t, m.Rename("kept", "renamed"))
	must(t, m.SyncDir())
	m.Crash(rand.New(rand.NewPCG(1, 1)))
	if names, _ := m.List(); len(names) != 1 || names[0] != "renamed" {
		t.Fatalf("after a synced rename: %v", names)
	}
}

var errInjected = errors.New("injected failure")

type faulty struct {
	FS
	step   int
	failAt int
	cut    bool
}

func (f *faulty) next() error {
	f.step++
	if f.step == f.failAt || (f.cut && f.step > f.failAt) {
		return errInjected
	}
	return nil
}

func (f *faulty) Create(name string) (File, error) {
	if err := f.next(); err != nil {
		return nil, err
	}
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &faultyFile{File: file, fs: f}, nil
}

func (f *faulty) Remove(name string) error {
	if err := f.next(); err != nil {
		return err
	}
	return f.FS.Remove(name)
}

func (f *faulty) Rename(from, to string) error {
	if err := f.next(); err != nil {
		return err
	}
	return f.FS.Rename(from, to)
}

func (f *faulty) SyncDir() error {
	if err := f.next(); err != nil {
		return err
	}
	return f.FS.SyncDir()
}

type faultyFile struct {
	File
	fs *faulty
}

func (f *faultyFile) Write(b []byte) (int, error) {
	if err := f.fs.next(); err != nil {
		return 0, err
	}
	return f.File.Write(b)
}

func (f *faultyFile) Sync() error {
	if err := f.fs.next(); err != nil {
		return err
	}
	return f.File.Sync()
}

func TestAFailedStepOfAnAtomicWriteIsReportedAndLeavesOneWholeFile(t *testing.T) {
	for failAt := 1; ; failAt++ {
		m := NewMem()
		if err := WriteAtomic(m, "f", []byte("old contents")); err != nil {
			t.Fatal(err)
		}
		w := &faulty{FS: m, failAt: failAt}
		err := WriteAtomic(w, "f", []byte("new contents"))
		got, readErr := m.ReadFile("f")
		names, _ := m.List()
		if readErr != nil || (string(got) != "old contents" && string(got) != "new contents") || !slices.Equal(names, []string{"f"}) {
			t.Fatalf("a failure at step %d (%v) left %v with %q %v", failAt, err, names, got, readErr)
		}
		if err == nil {
			if failAt <= w.step || string(got) != "new contents" {
				t.Fatalf("a failure at step %d of %d was not reported, the file holds %q", failAt, w.step, got)
			}
			return
		}
	}
}

func TestACrashAtAnyStepOfAnAtomicWriteLeavesTheOldOrTheNewFile(t *testing.T) {
	for cutAt := 1; ; cutAt++ {
		m := NewMem()
		if err := WriteAtomic(m, "f", []byte("old contents")); err != nil {
			t.Fatal(err)
		}
		err := WriteAtomic(&faulty{FS: m, failAt: cutAt, cut: true}, "f", []byte("new contents"))
		m.Crash(rand.New(rand.NewPCG(uint64(cutAt), 1)))
		got, readErr := m.ReadFile("f")
		if readErr != nil || (string(got) != "old contents" && string(got) != "new contents") {
			t.Fatalf("a crash after a power cut at step %d (%v) left %q %v", cutAt, err, got, readErr)
		}
		if err == nil {
			if string(got) != "new contents" {
				t.Fatalf("a write that reported success left %q after a crash", got)
			}
			return
		}
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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
