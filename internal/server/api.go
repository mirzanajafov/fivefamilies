package server

import (
	"context"
	"errors"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var errLeaseNotFound = errors.New("server: lease not found")

func (s *Server) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	return s.admit(ctx, func(done node.Done) { s.node.Submit(cmd, done) })
}

func (s *Server) Read(ctx context.Context, key string, lease bool) (kv.Result, error) {
	return s.admit(ctx, func(done node.Done) { s.node.Read(key, lease, done) })
}

func (s *Server) ChangeMembers(ctx context.Context, cc raft.ConfChange) (uint64, raft.ConfState, error) {
	if _, err := s.run(ctx, func(done node.Done) { s.node.ProposeConfChange(cc, done) }); err != nil {
		return 0, raft.ConfState{}, err
	}
	var index uint64
	var conf raft.ConfState
	if err := s.onLoop(ctx, func() { index, conf = s.raft.Applied(), s.raft.Conf() }); err != nil {
		return 0, raft.ConfState{}, err
	}
	return index, conf, nil
}

func (s *Server) TransferLeadership(ctx context.Context, to raft.NodeID) (raft.NodeID, uint64, error) {
	var leader raft.NodeID
	var term uint64
	_, err := s.run(ctx, func(done node.Done) {
		s.node.TransferLeadership(to, func(l raft.NodeID, t uint64, err error) {
			leader, term = l, t
			done(kv.Result{}, err)
		})
		s.publish()
	})
	if err != nil {
		return 0, 0, err
	}
	return leader, term, nil
}

func (s *Server) MinLeaseTTL() time.Duration {
	return time.Duration(s.cfg.ElectionTick) * s.cfg.Tick * 3 / 2
}

func (s *Server) KeepAlive(ctx context.Context, id uint64) (kv.Result, error) {
	return s.run(ctx, func(done node.Done) { s.node.KeepAlive(id, done) })
}

func (s *Server) TimeToLive(ctx context.Context, id uint64) (time.Duration, kv.LeaseInfo, error) {
	var leader, tracked, exists bool
	var hint raft.NodeID
	var remaining time.Duration
	var info kv.LeaseInfo
	if err := s.onLoop(ctx, func() {
		leader, hint = s.raft.State() == raft.StateLeader, s.raft.Leader()
		remaining, tracked = s.node.LeaseRemaining(id)
		info, exists = s.sm.Store.Lease(id)
	}); err != nil {
		return 0, kv.LeaseInfo{}, err
	}
	switch {
	case !leader:
		return 0, info, &node.NotLeaderError{Leader: hint}
	case !exists || !tracked:
		return 0, info, errLeaseNotFound
	}
	return remaining, info, nil
}
