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

var errPowerCut = errors.New("the power went out")

type powerCut struct {
	FS
	left int
}

func (p *powerCut) step() error {
	if p.left == 0 {
		return errPowerCut
	}
	p.left--
	return nil
}

func (p *powerCut) Create(name string) (File, error) {
	if err := p.step(); err != nil {
		return nil, err
	}
	f, err := p.FS.Create(name)
	return &cutFile{File: f, p: p}, err
}

func (p *powerCut) Remove(name string) error {
	if err := p.step(); err != nil {
		return err
	}
	return p.FS.Remove(name)
}

func (p *powerCut) Rename(from, to string) error {
	if err := p.step(); err != nil {
		return err
	}
	return p.FS.Rename(from, to)
}

func (p *powerCut) SyncDir() error {
	if err := p.step(); err != nil {
		return err
	}
	return p.FS.SyncDir()
}

type cutFile struct {
	File
	p *powerCut
}

func (f *cutFile) Write(b []byte) (int, error) {
	if err := f.p.step(); err != nil {
		return 0, err
	}
	return f.File.Write(b)
}

func (f *cutFile) Sync() error {
	if err := f.p.step(); err != nil {
		return err
	}
	return f.File.Sync()
}

func TestACrashAtAnyStepOfAnAtomicWriteLeavesTheOldOrTheNewFile(t *testing.T) {
	for steps := range 8 {
		m := NewMem()
		if err := WriteAtomic(m, "f", []byte("old contents")); err != nil {
			t.Fatal(err)
		}
		err := WriteAtomic(&powerCut{FS: m, left: steps}, "f", []byte("new contents"))
		m.Crash(rand.New(rand.NewPCG(uint64(steps), 1)))
		got, readErr := m.ReadFile("f")
		if readErr != nil || (string(got) != "old contents" && string(got) != "new contents") {
			t.Fatalf("cut after %d steps (%v): %q %v", steps, err, got, readErr)
		}
		if err == nil && string(got) != "new contents" {
			t.Fatalf("a write that reported success left %q", got)
		}
	}
}

type unsyncableDir struct {
	FS
}

func (unsyncableDir) SyncDir() error {
	return errors.New("the directory could not be synced")
}

func TestAnAtomicWriteReportsAFailedDirectorySync(t *testing.T) {
	if err := WriteAtomic(unsyncableDir{NewMem()}, "f", []byte("x")); err == nil {
		t.Fatal("a directory sync that failed was not reported")
	}
}

type unsyncableFiles struct {
	FS
}

func (u unsyncableFiles) Create(name string) (File, error) {
	f, err := u.FS.Create(name)
	return failingSync{f}, err
}

type failingSync struct {
	File
}

func (failingSync) Sync() error {
	return errors.New("the disk is full")
}

func TestAFailedAtomicWriteLeavesNoTempFile(t *testing.T) {
	m := NewMem()
	if err := WriteAtomic(unsyncableFiles{m}, "f", []byte("x")); err == nil {
		t.Fatal("a sync that failed was not reported")
	}
	if names, _ := m.List(); !slices.Equal(names, []string(nil)) {
		t.Fatalf("left %v", names)
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
