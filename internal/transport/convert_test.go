package transport

import (
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

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

func TestEveryTypeAPeerSendsKeepsTheNumberThePreviousBuildSent(t *testing.T) {
	previous := map[raft.MessageType]pb.MessageType{
		raft.MsgApp: 1, raft.MsgAppResp: 2, raft.MsgHeartbeat: 3, raft.MsgHeartbeatResp: 4, raft.MsgPreVote: 5, raft.MsgPreVoteResp: 6,
		raft.MsgVote: 7, raft.MsgVoteResp: 8, raft.MsgSnap: 10, raft.MsgReadIndex: 11, raft.MsgReadIndexResp: 12, raft.MsgTimeoutNow: 13,
	}
	for i := range 256 {
		typ := raft.MessageType(i)
		if strings.HasPrefix(typ.String(), "MessageType(") || typ == raft.MsgStorageAck {
			if _, ok := wireTypes[typ]; ok {
				t.Errorf("%v has a wire name, but no peer sends it", typ)
			}
			continue
		}
		if want, ok := previous[typ]; !ok || wireTypes[typ] != want {
			t.Errorf("%v goes out as %v, and the previous build sent %v", typ, wireTypes[typ], want)
		}
	}
	previousEntries := map[raft.EntryType]pb.EntryType{raft.EntryNormal: 0, raft.EntryConfig: 1}
	for i := range 256 {
		typ := raft.EntryType(i)
		want, sent := previousEntries[typ]
		if wire, ok := wireEntryTypes[typ]; ok != sent || ok != typ.Valid() || wire != want {
			t.Errorf("entry type %d goes out as %v, and the previous build sent %v", i, wire, want)
		}
	}
}

func TestATypeWithNoWireNameNeverLeavesTheNode(t *testing.T) {
	for _, m := range []raft.Message{
		{Type: raft.MsgStorageAck, From: 1, To: 1},
		{Type: raft.MsgApp, From: 1, To: 2, Entries: []raft.Entry{{Term: 1, Index: 1, Type: raft.EntryType(7)}}},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%v went out", m)
				}
			}()
			toProto(m)
		}()
	}
}

func TestAnAppendFromThePreviousBuildStillDecodes(t *testing.T) {
	varint := func(b []byte, field protowire.Number, v uint64) []byte {
		return protowire.AppendVarint(protowire.AppendTag(b, field, protowire.VarintType), v)
	}
	entry := varint(varint(varint(nil, 1, 3), 2, 5), 3, 1)
	b := varint(varint(varint(varint(varint(nil, 1, 1), 2, 2), 3, 1), 4, 3), 6, 4)
	b = protowire.AppendBytes(protowire.AppendTag(b, 7, protowire.BytesType), entry)
	var m pb.RaftMessage
	if err := proto.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	want := raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 3, Index: 4, Entries: []raft.Entry{{Term: 3, Index: 5, Type: raft.EntryConfig}}}
	if got, err := fromProto(&m); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("an append written as raw numbers decoded as %v: %v", got, err)
	}
}
