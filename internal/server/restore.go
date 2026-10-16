package server

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

func Restore(dataDir string, voters []raft.NodeID, f backup.File) (bool, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return false, err
	}
	fs := vfs.OS{Dir: dataDir}
	names, err := fs.List()
	if err != nil {
		return false, err
	}
	for _, n := range names {
		if strings.HasSuffix(n, ".wal") {
			return false, nil
		}
	}
	if _, err := backup.Check(f); err != nil {
		return false, err
	}
	voters = slices.Sorted(slices.Values(voters))
	s := raft.Snapshot{Index: f.Rev, Term: 1, Data: f.Data, Conf: raft.ConfState{Voters: voters}}
	if err := snap.Save(fs, s); err != nil {
		return false, fmt.Errorf("writing the snapshot: %w", err)
	}
	l, _, _, err := wal.Open(fs, wal.Options{Salt: randomUint64})
	if err != nil {
		return false, err
	}
	defer l.Close()
	if err := l.WriteBatch(wal.Batch{HardState: raft.HardState{Term: 1, Commit: f.Rev}, Snapshot: &raft.Snapshot{Index: f.Rev, Term: 1}, Reset: true}); err != nil {
		return false, err
	}
	if err := l.Sync(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Server) Backup(stop <-chan struct{}) (backup.File, error) {
	type taken struct{ f backup.File }
	ch := make(chan taken, 1)
	select {
	case s.calls <- func() {
		ch <- taken{backup.File{Rev: s.sm.Store.Rev(), Hash: s.sm.Store.Hash(), Data: s.sm.Store.Snapshot()}}
	}:
	case <-stop:
		return backup.File{}, fmt.Errorf("server: stopping")
	}
	select {
	case t := <-ch:
		return t.f, nil
	case <-stop:
		return backup.File{}, fmt.Errorf("server: stopping")
	}
}
