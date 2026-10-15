package transport

import (
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
		out.Snapshot = &pb.SnapshotMeta{Index: m.Snapshot.Index, Term: m.Snapshot.Term}
	}
	return out
}

func fromProto(m *pb.RaftMessage) raft.Message {
	out := raft.Message{
		Type:    raft.MessageType(m.GetType()),
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
			out.Entries[i] = raft.Entry{Term: e.GetTerm(), Index: e.GetIndex(), Type: raft.EntryType(e.GetType()), Data: e.GetData()}
		}
	}
	if s := m.GetSnapshot(); s != nil {
		out.Snapshot = &raft.Snapshot{Index: s.GetIndex(), Term: s.GetTerm()}
	}
	return out
}
