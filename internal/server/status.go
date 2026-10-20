package server

import (
	"context"
	"sync/atomic"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var roles = map[raft.StateType]pb.Role{
	raft.StateFollower:     pb.Role_ROLE_FOLLOWER,
	raft.StatePreCandidate: pb.Role_ROLE_PRECANDIDATE,
	raft.StateCandidate:    pb.Role_ROLE_CANDIDATE,
	raft.StateLeader:       pb.Role_ROLE_LEADER,
}

type counters struct {
	fsyncs, batches, entries, sent, received, reads, writes, elections atomic.Uint64
}

func (s *Server) publish() {
	r := s.raft
	st := &pb.StatusResponse{
		Id:         uint64(s.cfg.ID),
		State:      r.State().String(),
		Role:       roles[r.State()],
		Term:       r.Term(),
		Leader:     uint64(r.Leader()),
		Commit:     r.Committed(),
		Applied:    r.Applied(),
		FirstIndex: r.FirstIndex(),
		LastIndex:  r.LastIndex(),
	}
	if sn := r.LatestSnapshot(); sn != nil {
		st.SnapshotIndex = sn.Index
	}
	conf := r.Conf()
	st.Voters, st.Learners, st.Transferee = ids64(conf.Voters), ids64(conf.Learners), uint64(r.Transferring())
	if r.Term() > s.lastTerm {
		if s.lastTerm > 0 {
			s.counts.elections.Add(r.Term() - s.lastTerm)
		}
		s.lastTerm = r.Term()
	}
	c := &s.counts
	st.Fsyncs, st.WalBatches, st.EntriesWritten = c.fsyncs.Load(), c.batches.Load(), c.entries.Load()
	st.MsgsSent, st.MsgsReceived = c.sent.Load(), c.received.Load()
	st.Reads, st.Writes, st.Elections = c.reads.Load(), c.writes.Load(), c.elections.Load()
	s.status.Store(st)
}

func ids64(ids []raft.NodeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func (s *Server) Status() *pb.StatusResponse {
	return s.status.Load()
}

func (s *Server) StateHash(ctx context.Context) (uint64, uint64, error) {
	var hash, rev uint64
	if err := s.onLoop(ctx, func() { hash, rev = s.sm.Store.Hash(), s.sm.Store.Rev() }); err != nil {
		return 0, 0, err
	}
	return hash, rev, nil
}
