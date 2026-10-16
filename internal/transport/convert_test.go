package transport

import (
	"reflect"
	"testing"

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
		if got := fromProto(toProto(m)); !reflect.DeepEqual(got, m) {
			t.Fatalf("round trip changed %v into %v", m, got)
		}
	}
}
