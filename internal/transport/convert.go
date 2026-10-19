package transport

import (
	"fmt"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func toProto(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type:    uint32(m.Type),
		From:    uint64(m.From),
		To:      uint64(m.To),
		Term:    m.Term,
		LogTerm: m.LogTerm,
		Index:   m.Index,
		Commit:  m.Commit,
		Reject:  m.Reject,
		Hint:    m.Hint,
		Context: m.Context,
	}
	if len(m.Entries) > 0 {
		out.Entries = make([]*pb.Entry, len(m.Entries))
		for i, e := range m.Entries {
			out.Entries[i] = &pb.Entry{Term: e.Term, Index: e.Index, Type: uint32(e.Type), Data: e.Data}
		}
	}
	if m.Snapshot != nil {
		out.Snapshot = &pb.SnapshotMeta{Index: m.Snapshot.Index, Term: m.Snapshot.Term, Voters: ids64(m.Snapshot.Conf.Voters), Learners: ids64(m.Snapshot.Conf.Learners)}
	}
	return out
}

func fromProto(m *pb.RaftMessage) (raft.Message, error) {
	typ := raft.MessageType(m.GetType())
	if uint32(typ) != m.GetType() || !typ.Remote() {
		return raft.Message{}, fmt.Errorf("transport: message type %d is not one a peer sends", m.GetType())
	}
	out := raft.Message{
		Type:    typ,
		From:    raft.NodeID(m.GetFrom()),
		To:      raft.NodeID(m.GetTo()),
		Term:    m.GetTerm(),
		LogTerm: m.GetLogTerm(),
		Index:   m.GetIndex(),
		Commit:  m.GetCommit(),
		Reject:  m.GetReject(),
		Hint:    m.GetHint(),
		Context: m.GetContext(),
	}
	if n := len(m.GetEntries()); n > 0 {
		out.Entries = make([]raft.Entry, n)
		for i, e := range m.GetEntries() {
			et := raft.EntryType(e.GetType())
			if uint32(et) != e.GetType() || !et.Valid() {
				return raft.Message{}, fmt.Errorf("transport: unknown entry type %d", e.GetType())
			}
			if want := m.GetIndex() + 1 + uint64(i); e.GetIndex() != want {
				return raft.Message{}, fmt.Errorf("transport: entry %d of an append after %d has index %d", i, m.GetIndex(), e.GetIndex())
			}
			out.Entries[i] = raft.Entry{Term: e.GetTerm(), Index: e.GetIndex(), Type: et, Data: e.GetData()}
		}
	}
	if s := m.GetSnapshot(); s != nil {
		out.Snapshot = &raft.Snapshot{Index: s.GetIndex(), Term: s.GetTerm(), Conf: raft.ConfState{Voters: nodeIDs(s.GetVoters()), Learners: nodeIDs(s.GetLearners())}}
	}
	return out, nil
}

func ids64(ids []raft.NodeID) []uint64 {
	if len(ids) == 0 {
		return nil
	}
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func nodeIDs(ids []uint64) []raft.NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]raft.NodeID, len(ids))
	for i, id := range ids {
		out[i] = raft.NodeID(id)
	}
	return out
}
