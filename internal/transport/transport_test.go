package transport

import (
	"io"
	"log/slog"
	"math"
	"testing"

	"google.golang.org/grpc"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type fakeStream[T any] struct {
	grpc.ServerStream
	in []*T
}

func (s *fakeStream[T]) Recv() (*T, error) {
	if len(s.in) == 0 {
		return nil, io.EOF
	}
	next := s.in[0]
	s.in = s.in[1:]
	return next, nil
}

func (s *fakeStream[T]) SendAndClose(*pb.Ack) error {
	return nil
}

func newReceiver(t *testing.T) (*Transport, *[]raft.Message) {
	t.Helper()
	var got []raft.Message
	tr, err := New(1, map[raft.NodeID]string{1: "127.0.0.1:1", 2: "127.0.0.1:2", 3: "127.0.0.1:3"}, func(m raft.Message) { got = append(got, m) }, nil, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	return tr, &got
}

func message(typ raft.MessageType, from, to raft.NodeID) *pb.RaftMessage {
	m := raft.Message{Type: typ, From: from, To: to, Term: 3}
	if typ == raft.MsgSnap {
		m.Snapshot = &raft.Snapshot{Index: 9, Term: 3}
	}
	return toProto(m)
}

func TestAPeerStreamDropsWhatAPeerMayNotSendAndKeepsTheRest(t *testing.T) {
	tr, got := newReceiver(t)
	appendOf := func(index uint64, ents ...*pb.Entry) *pb.RaftMessage {
		m := message(raft.MsgApp, 2, 1)
		m.Index, m.Entries = index, ents
		return m
	}
	withType := func(typ uint32) *pb.RaftMessage {
		m := message(raft.MsgHeartbeat, 2, 1)
		m.Type = typ
		return m
	}
	bad := map[string]*pb.RaftMessage{
		"a snapshot":                   message(raft.MsgSnap, 2, 1),
		"the node's own vote":          message(raft.MsgVoteResp, 1, 1),
		"an unknown sender":            message(raft.MsgApp, 7, 1),
		"a message for another node":   message(raft.MsgApp, 2, 3),
		"a storage ack":                withType(uint32(raft.MsgStorageAck)),
		"no type":                      withType(0),
		"a type after the last":        withType(uint32(raft.MsgTimeoutNow) + 1),
		"an append plus 256":           withType(uint32(raft.MsgApp) + 256),
		"an unknown entry type":        appendOf(4, &pb.Entry{Term: 3, Index: 5, Type: uint32(raft.EntryConfig) + 1}),
		"an entry type plus 256":       appendOf(4, &pb.Entry{Term: 3, Index: 5, Type: uint32(raft.EntryNormal) + 256}),
		"an entry after a gap":         appendOf(4, &pb.Entry{Term: 3, Index: 5}, &pb.Entry{Term: 3, Index: 7}),
		"an entry before the append":   appendOf(4, &pb.Entry{Term: 3, Index: 2}),
		"an entry past the last index": appendOf(math.MaxUint64, &pb.Entry{Term: 3, Index: 0}),
		"an entry from a later term":   appendOf(4, &pb.Entry{Term: 4, Index: 5}),
		"terms going backwards":        appendOf(4, &pb.Entry{Term: 3, Index: 5}, &pb.Entry{Term: 2, Index: 6}),
		"an entry of term zero":        appendOf(0, &pb.Entry{Term: 0, Index: 1}),
	}
	good := appendOf(4, &pb.Entry{Term: 3, Index: 5}, &pb.Entry{Term: 3, Index: 6, Type: uint32(raft.EntryConfig)})
	for name, m := range bad {
		batch := &pb.Batch{Messages: []*pb.RaftMessage{good, m, good}}
		*got = nil
		if err := tr.Server().Send(&fakeStream[pb.Batch]{in: []*pb.Batch{batch}}); err != nil {
			t.Fatal(err)
		}
		if len(*got) != 2 || (*got)[0].Type != raft.MsgApp || (*got)[1].Type != raft.MsgApp {
			t.Errorf("a batch with %s delivered %v", name, *got)
		}
	}
}

func TestTheSnapshotStreamCarriesOnlyASnapshotFromAPeerToThisNode(t *testing.T) {
	tr, got := newReceiver(t)
	chunk := func(m *pb.RaftMessage, data string) *pb.SnapshotChunk {
		return &pb.SnapshotChunk{Message: m, Data: []byte(data)}
	}
	appendWithSnapshot := message(raft.MsgApp, 2, 1)
	appendWithSnapshot.Snapshot = &pb.SnapshotMeta{Index: 9, Term: 3}
	bad := map[string]*pb.SnapshotChunk{
		"an append":                   chunk(appendWithSnapshot, "state"),
		"a snapshot from nobody":      chunk(message(raft.MsgSnap, 7, 1), "state"),
		"a snapshot for another node": chunk(message(raft.MsgSnap, 2, 3), "state"),
		"a snapshot without data":     chunk(message(raft.MsgSnap, 2, 1), ""),
	}
	for name, c := range bad {
		if err := tr.Server().InstallSnapshot(&fakeStream[pb.SnapshotChunk]{in: []*pb.SnapshotChunk{c}}); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	good := chunk(message(raft.MsgSnap, 2, 1), "state")
	if err := tr.Server().InstallSnapshot(&fakeStream[pb.SnapshotChunk]{in: []*pb.SnapshotChunk{good}}); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 1 || string((*got)[0].Snapshot.Data) != "state" {
		t.Fatalf("delivered %v", *got)
	}
}
