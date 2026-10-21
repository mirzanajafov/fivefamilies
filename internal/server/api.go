package server

import (
	"context"
	"errors"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var (
	ErrTransferAborted = errors.New("server: the transfer gave up, the target did not take over within an election timeout")
	errLeaseNotFound   = errors.New("server: lease not found")
)

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

func (s *Server) TransferLeadership(ctx context.Context, to raft.NodeID) error {
	var refused error
	if err := s.onLoop(ctx, func() { refused = s.node.TransferLeadership(to); s.publish() }); err != nil {
		return err
	}
	if refused != nil {
		return refused
	}
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		st := s.Status()
		switch {
		case raft.NodeID(st.GetLeader()) == to && to != s.cfg.ID:
			return nil
		case to == s.cfg.ID && st.GetRole() == pb.Role_ROLE_LEADER:
			return nil
		case st.GetRole() == pb.Role_ROLE_LEADER && st.GetTransferee() == 0:
			return ErrTransferAborted
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopped:
			return ErrStopped
		}
	}
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
