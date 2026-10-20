package server

import (
	"context"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type diskJob struct {
	write   *node.Write
	compact *raft.Snapshot
}

func (s *Server) Write(w node.Write) {
	select {
	case s.jobs <- diskJob{write: &w}:
	case <-s.diskDone:
	}
}

func (s *Server) SaveSnapshot(sn raft.Snapshot) error {
	return s.disk.SaveSnapshot(sn)
}

func (s *Server) Compacted(sn raft.Snapshot) {
	select {
	case s.jobs <- diskJob{compact: &sn}:
	case <-s.diskDone:
	}
}

func (s *Server) writer(ctx context.Context) {
	defer close(s.diskDone)
	for {
		var first diskJob
		select {
		case <-ctx.Done():
			return
		case first = <-s.jobs:
		}
		jobs := s.gather(first)
		last, err := s.persist(jobs)
		if err != nil {
			s.die(err)
			return
		}
		if last > 0 {
			select {
			case s.written <- last:
			case <-ctx.Done():
				return
			}
		}
		for _, j := range jobs {
			if j.compact == nil {
				continue
			}
			if _, err := s.disk.Compact(j.compact.Index); err != nil {
				s.die(err)
				return
			}
		}
	}
}

func (s *Server) gather(first diskJob) []diskJob {
	jobs := []diskJob{first}
	if s.cfg.FsyncMode != FsyncGroup {
		return jobs
	}
	for {
		select {
		case j := <-s.jobs:
			jobs = append(jobs, j)
		default:
			return jobs
		}
	}
}

func (s *Server) persist(jobs []diskJob) (uint64, error) {
	var last uint64
	pending := false
	for _, j := range jobs {
		w := j.write
		if w == nil {
			continue
		}
		for _, p := range s.parts(w) {
			if err := s.disk.Write(p); err != nil {
				return 0, err
			}
			s.counts.batches.Add(1)
			s.counts.entries.Add(uint64(len(p.Entries)))
			pending = pending || w.Sync
			if s.cfg.FsyncMode != FsyncGroup && pending {
				if err := s.sync(); err != nil {
					return 0, err
				}
				pending = false
			}
		}
		last = w.Seq
	}
	if pending {
		if err := s.sync(); err != nil {
			return 0, err
		}
	}
	return last, nil
}

func (s *Server) parts(w *node.Write) []wal.Batch {
	if s.cfg.FsyncMode != FsyncEntry || len(w.Entries) <= 1 {
		return []wal.Batch{w.Batch}
	}
	parts := make([]wal.Batch, len(w.Entries))
	for i, e := range w.Entries {
		parts[i] = wal.Batch{Entries: []raft.Entry{e}}
	}
	parts[0].HardState, parts[0].Snapshot, parts[0].Reset = w.HardState, w.Snapshot, w.Reset
	return parts
}

func (s *Server) sync() error {
	s.stalled.wait(s.quit)
	if s.cfg.FsyncMode == FsyncOff {
		return nil
	}
	if err := s.disk.Sync(); err != nil {
		return err
	}
	s.counts.fsyncs.Add(1)
	return nil
}
