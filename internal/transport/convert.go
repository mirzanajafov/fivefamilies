package transport

import (
	"fmt"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var (
	wireTypes = map[raft.MessageType]pb.MessageType{
		raft.MsgApp:           pb.MessageType_MESSAGE_TYPE_APP,
		raft.MsgAppResp:       pb.MessageType_MESSAGE_TYPE_APP_RESP,
		raft.MsgHeartbeat:     pb.MessageType_MESSAGE_TYPE_HEARTBEAT,
		raft.MsgHeartbeatResp: pb.MessageType_MESSAGE_TYPE_HEARTBEAT_RESP,
		raft.MsgPreVote:       pb.MessageType_MESSAGE_TYPE_PRE_VOTE,
		raft.MsgPreVoteResp:   pb.MessageType_MESSAGE_TYPE_PRE_VOTE_RESP,
		raft.MsgVote:          pb.MessageType_MESSAGE_TYPE_VOTE,
		raft.MsgVoteResp:      pb.MessageType_MESSAGE_TYPE_VOTE_RESP,
		raft.MsgSnap:          pb.MessageType_MESSAGE_TYPE_SNAP,
		raft.MsgReadIndex:     pb.MessageType_MESSAGE_TYPE_READ_INDEX,
		raft.MsgReadIndexResp: pb.MessageType_MESSAGE_TYPE_READ_INDEX_RESP,
		raft.MsgTimeoutNow:    pb.MessageType_MESSAGE_TYPE_TIMEOUT_NOW,
	}
	wireEntryTypes = map[raft.EntryType]pb.EntryType{
		raft.EntryNormal: pb.EntryType_ENTRY_TYPE_NORMAL,
		raft.EntryConfig: pb.EntryType_ENTRY_TYPE_CONFIG,
	}
	raftTypes      = invert(wireTypes)
	raftEntryTypes = invert(wireEntryTypes)
)

func invert[K, V comparable](m map[K]V) map[V]K {
	out := make(map[V]K, len(m))
	for k, v := range m {
		out[v] = k
	}
	return out
}

func toProto(m raft.Message) *pb.RaftMessage {
	out := &pb.RaftMessage{
		Type:    wireTypes[m.Type],
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
			out.Entries[i] = &pb.Entry{Term: e.Term, Index: e.Index, Type: wireEntryTypes[e.Type], Data: e.Data}
		}
	}
	if m.Snapshot != nil {
		out.Snapshot = &pb.SnapshotMeta{Index: m.Snapshot.Index, Term: m.Snapshot.Term, Voters: ids64(m.Snapshot.Conf.Voters), Learners: ids64(m.Snapshot.Conf.Learners)}
	}
	return out
}

func fromProto(m *pb.RaftMessage) (raft.Message, error) {
	typ, ok := raftTypes[m.GetType()]
	if !ok {
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
		prevIndex, prevTerm := m.GetIndex(), max(m.GetLogTerm(), 1)
		for i, e := range m.GetEntries() {
			et, ok := raftEntryTypes[e.GetType()]
			if !ok {
				return raft.Message{}, fmt.Errorf("transport: unknown entry type %d", e.GetType())
			}
			if e.GetIndex() != prevIndex+1 || e.GetIndex() == 0 {
				return raft.Message{}, fmt.Errorf("transport: entry %d of an append after %d has index %d", i, m.GetIndex(), e.GetIndex())
			}
			if e.GetTerm() < prevTerm || e.GetTerm() > m.GetTerm() {
				return raft.Message{}, fmt.Errorf("transport: entry %d has term %d, after term %d in an append of term %d", e.GetIndex(), e.GetTerm(), prevTerm, m.GetTerm())
			}
			prevIndex, prevTerm = e.GetIndex(), e.GetTerm()
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
