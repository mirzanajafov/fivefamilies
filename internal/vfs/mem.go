package vfs

import (
	"fmt"
	"math/rand/v2"
	"sort"
)

type memFile struct {
	name        string
	durableName string
	data        []byte
	synced      int
}

type Mem struct {
	files      map[string]*memFile
	removed    []*memFile
	SectorSize int
	Garble     float64
	Syncs      int
	Torn       int
}

func NewMem() *Mem {
	return &Mem{files: map[string]*memFile{}, SectorSize: 512, Garble: 0.3}
}

type memHandle struct {
	m      *Mem
	f      *memFile
	closed bool
}

func (h *memHandle) Write(p []byte) (int, error) {
	if h.closed {
		return 0, fmt.Errorf("vfs: write to closed file %s", h.f.name)
	}
	h.f.data = append(h.f.data, p...)
	return len(p), nil
}

func (h *memHandle) Sync() error {
	if h.closed {
		return fmt.Errorf("vfs: sync of closed file %s", h.f.name)
	}
	h.f.synced = len(h.f.data)
	h.m.Syncs++
	return nil
}

func (h *memHandle) Close() error {
	h.closed = true
	return nil
}

func (m *Mem) names() []string {
	names := make([]string, 0, len(m.files))
	for n := range m.files {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (m *Mem) List() ([]string, error) {
	return m.names(), nil
}

func (m *Mem) ReadFile(name string) ([]byte, error) {
	f, ok := m.files[name]
	if !ok {
		return nil, fmt.Errorf("vfs: %s does not exist", name)
	}
	return append([]byte(nil), f.data...), nil
}

func (m *Mem) Create(name string) (File, error) {
	if _, ok := m.files[name]; ok {
		return nil, fmt.Errorf("vfs: %s already exists", name)
	}
	f := &memFile{name: name}
	m.files[name] = f
	return &memHandle{m: m, f: f}, nil
}

func (m *Mem) OpenAppend(name string) (File, error) {
	f, ok := m.files[name]
	if !ok {
		return nil, fmt.Errorf("vfs: %s does not exist", name)
	}
	return &memHandle{m: m, f: f}, nil
}

func (m *Mem) Truncate(name string, size int64) error {
	f, ok := m.files[name]
	if !ok {
		return fmt.Errorf("vfs: %s does not exist", name)
	}
	if size < int64(len(f.data)) {
		f.data = f.data[:size]
	}
	f.synced = min(f.synced, len(f.data))
	return nil
}

func (m *Mem) Remove(name string) error {
	f, ok := m.files[name]
	if !ok {
		return nil
	}
	delete(m.files, name)
	if f.durableName != "" {
		m.removed = append(m.removed, f)
	}
	return nil
}

func (m *Mem) Rename(from, to string) error {
	f, ok := m.files[from]
	if !ok {
		return fmt.Errorf("vfs: %s does not exist", from)
	}
	if old, ok := m.files[to]; ok && old.durableName != "" {
		m.removed = append(m.removed, old)
	}
	delete(m.files, from)
	f.name = to
	m.files[to] = f
	return nil
}

func (m *Mem) SyncDir() error {
	for _, f := range m.files {
		f.durableName = f.name
	}
	m.removed = nil
	return nil
}

func (m *Mem) Unsynced() int {
	n := 0
	for _, f := range m.files {
		n += len(f.data) - f.synced
	}
	return n
}

func (m *Mem) Crash(rng *rand.Rand) {
	var survivors []*memFile
	for _, name := range m.names() {
		if f := m.files[name]; f.durableName != "" {
			survivors = append(survivors, f)
		}
	}
	survivors = append(survivors, m.removed...)
	m.files = map[string]*memFile{}
	m.removed = nil
	for _, f := range survivors {
		m.tear(rng, f)
		f.name = f.durableName
		m.files[f.name] = f
	}
}

func (m *Mem) tear(rng *rand.Rand, f *memFile) {
	unsynced := len(f.data) - f.synced
	if unsynced <= 0 {
		return
	}
	keep := rng.IntN(unsynced + 1)
	end := f.synced + keep
	if end < len(f.data) {
		end -= end % m.SectorSize
		end = max(end, f.synced)
	}
	f.data = f.data[:end]
	if end > f.synced && rng.Float64() < m.Garble {
		first := f.synced - f.synced%m.SectorSize
		sectors := (end - first + m.SectorSize - 1) / m.SectorSize
		s := first + rng.IntN(sectors)*m.SectorSize
		for i := max(s, f.synced); i < min(s+m.SectorSize, end); i++ {
			f.data[i] = byte(rng.Uint32())
		}
	}
	if end > f.synced {
		m.Torn++
	}
	f.synced = len(f.data)
}
