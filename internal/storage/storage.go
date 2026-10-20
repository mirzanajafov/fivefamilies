package storage

import (
	"errors"
	"fmt"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type Disk struct {
	fs  vfs.FS
	log *wal.Log
}

func Open(fs vfs.FS, opts wal.Options) (*Disk, raft.Restore, wal.Recovery, error) {
	if err := snap.RemoveUnfinished(fs); err != nil {
		return nil, raft.Restore{}, wal.Recovery{}, fmt.Errorf("removing unfinished snapshots: %w", err)
	}
	l, st, rec, err := wal.Open(fs, opts)
	if err != nil {
		return nil, st, rec, fmt.Errorf("opening the log: %w", err)
	}
	if st.Snapshot != nil {
		s, err := snap.Load(fs, st.Snapshot.Index, st.Snapshot.Term)
		if err != nil {
			return nil, st, rec, errors.Join(fmt.Errorf("loading snapshot %d: %w", st.Snapshot.Index, err), l.Close())
		}
		st.Snapshot = &s
	}
	return &Disk{fs: fs, log: l}, st, rec, nil
}

func Seed(fs vfs.FS, s raft.Snapshot, opts wal.Options) error {
	if err := snap.Save(fs, s); err != nil {
		return fmt.Errorf("writing the snapshot: %w", err)
	}
	l, _, _, err := wal.Open(fs, opts)
	if err != nil {
		return err
	}
	err = l.WriteBatch(wal.Batch{HardState: raft.HardState{Term: s.Term, Commit: s.Index}, Snapshot: &s, Reset: true})
	if err == nil {
		err = l.Sync()
	}
	return errors.Join(err, l.Close())
}

func (d *Disk) Write(b wal.Batch) error {
	return d.log.WriteBatch(b)
}

func (d *Disk) Sync() error {
	return d.log.Sync()
}

func (d *Disk) SaveSnapshot(s raft.Snapshot) error {
	if err := snap.Save(d.fs, s); err != nil {
		return fmt.Errorf("saving snapshot %d: %w", s.Index, err)
	}
	return nil
}

func (d *Disk) Compact(index uint64) (int, error) {
	n, err := d.log.Compact(index)
	if err != nil {
		return n, fmt.Errorf("compacting the log to %d: %w", index, err)
	}
	if err := snap.RemoveBelow(d.fs, index); err != nil {
		return n, fmt.Errorf("removing snapshots below %d: %w", index, err)
	}
	return n, nil
}

func (d *Disk) Close() error {
	return d.log.Close()
}
