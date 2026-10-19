package transport

import (
	"reflect"
	"testing"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func TestConversionRoundTrip(t *testing.T) {
	msgs := []raft.Message{
		{Type: raft.MsgApp, From: 1, To: 2, Term: 3, LogTerm: 2, Index: 7, Commit: 6,
			Entries: []raft.Entry{{Term: 3, Index: 8, Data: []byte("a")}, {Term: 3, Index: 9, Type: raft.EntryConfig}}},
		{Type: raft.MsgAppResp, From: 2, To: 1, Term: 3, Index: 7, Reject: true, Hint: 5, LogTerm: 2},
		{Type: raft.MsgHeartbeat, From: 1, To: 3, Term: 4, Commit: 9, Context: 77},
		{Type: raft.MsgSnap, From: 1, To: 3, Term: 4, Snapshot: &raft.Snapshot{Index: 40, Term: 3}},
		{Type: raft.MsgSnap, From: 1, To: 3, Term: 4, Snapshot: &raft.Snapshot{Index: 40, Term: 3, Conf: raft.ConfState{Voters: []raft.NodeID{1, 2, 3}, Learners: []raft.NodeID{9}}}},
		{Type: raft.MsgVote, From: 2, To: 1, Term: 5, Index: 40, LogTerm: 4, Context: 1},
		{Type: raft.MsgTimeoutNow, From: 1, To: 2, Term: 4},
		{Type: raft.MsgReadIndex, From: 2, To: 1, Term: 4, Context: 12},
	}
	for _, m := range msgs {
		if got, err := fromProto(toProto(m)); err != nil || !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip changed %v into %v: %v", m, got, err)
		}
	}
}

func TestAPeerCannotSendLocalOrUnknownTypes(t *testing.T) {
	bad := map[string]*pb.RaftMessage{
		"no type":               {From: 2, To: 1, Term: 3},
		"a storage ack":         {Type: uint32(raft.MsgStorageAck), From: 1, To: 1, Index: 9},
		"an append plus 256":    {Type: uint32(raft.MsgApp) + 256, From: 2, To: 1, Term: 3},
		"a type after the last": {Type: uint32(raft.MsgTimeoutNow) + 1, From: 2, To: 1},
		"an unknown entry type": {Type: uint32(raft.MsgApp), From: 2, To: 1, Term: 3,
			Entries: []*pb.Entry{{Term: 3, Index: 1, Type: uint32(raft.EntryConfig) + 1}}},
		"an entry type plus 256": {Type: uint32(raft.MsgApp), From: 2, To: 1, Term: 3,
			Entries: []*pb.Entry{{Term: 3, Index: 1, Type: uint32(raft.EntryNormal) + 256}}},
	}
	for name, m := range bad {
		if got, err := fromProto(m); err == nil {
			t.Errorf("%s was accepted as %v", name, got)
		}
	}
}
