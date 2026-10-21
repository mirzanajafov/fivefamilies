package server

import (
	"context"
	"os"
	"slices"

	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/storage"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

func Restore(dataDir string, voters []raft.NodeID, f backup.File) (bool, error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return false, err
	}
	if _, err := backup.Check(f); err != nil {
		return false, err
	}
	voters = slices.Sorted(slices.Values(voters))
	s := raft.Snapshot{Index: f.Rev, Term: 1, Data: f.Data, Conf: raft.ConfState{Voters: voters}}
	return storage.Seed(vfs.OS{Dir: dataDir}, s, wal.Options{Salt: randomUint64})
}

func (s *Server) Backup(ctx context.Context) (backup.File, error) {
	var f backup.File
	if err := s.onLoop(ctx, func() {
		f = backup.File{Rev: s.sm.Store.Rev(), Hash: s.sm.Store.Hash(), Data: s.sm.Store.Snapshot()}
	}); err != nil {
		return backup.File{}, err
	}
	return f, nil
}
