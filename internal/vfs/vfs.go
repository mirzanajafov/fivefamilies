package vfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

type File interface {
	io.Writer
	Sync() error
	Close() error
}

type FS interface {
	List() ([]string, error)
	ReadFile(name string) ([]byte, error)
	Create(name string) (File, error)
	OpenAppend(name string) (File, error)
	Truncate(name string, size int64) error
	Remove(name string) error
	Rename(from, to string) error
	SyncDir() error
}

type OS struct {
	Dir string
}

func (o OS) path(name string) string {
	return filepath.Join(o.Dir, name)
}

func (o OS) List() ([]string, error) {
	ents, err := os.ReadDir(o.Dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range ents {
		if e.Type().IsRegular() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (o OS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(o.path(name))
}

func (o OS) Create(name string) (File, error) {
	return os.OpenFile(o.path(name), os.O_CREATE|os.O_TRUNC|os.O_WRONLY|os.O_APPEND, 0o600)
}

func (o OS) OpenAppend(name string) (File, error) {
	return os.OpenFile(o.path(name), os.O_WRONLY|os.O_APPEND, 0o600)
}

func (o OS) Truncate(name string, size int64) error {
	return os.Truncate(o.path(name), size)
}

func (o OS) Remove(name string) error {
	err := os.Remove(o.path(name))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (o OS) Rename(from, to string) error {
	return os.Rename(o.path(from), o.path(to))
}

func (o OS) SyncDir() error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(o.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

const TempSuffix = ".tmp"

func WriteAtomic(fs FS, name string, data []byte) error {
	tmp := name + TempSuffix
	if err := fs.Remove(tmp); err != nil {
		return err
	}
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, name); err != nil {
		return err
	}
	return fs.SyncDir()
}
