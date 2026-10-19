package transport

import (
	"io"
	"log/slog"
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

func TestAPeerStreamDropsWhatAPeerMayNotSendAndKeepsTheRest(t *testing.T) {
	tr, got := newReceiver(t)
	heartbeat := toProto(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: 3})
	batch := &pb.Batch{Messages: []*pb.RaftMessage{
		heartbeat,
		toProto(raft.Message{Type: raft.MsgSnap, From: 2, To: 1, Term: 3, Snapshot: &raft.Snapshot{Index: 9, Term: 3}}),
		toProto(raft.Message{Type: raft.MsgVoteResp, From: 1, To: 1, Term: 3}),
		toProto(raft.Message{Type: raft.MsgApp, From: 7, To: 1, Term: 3}),
		toProto(raft.Message{Type: raft.MsgApp, From: 2, To: 3, Term: 3}),
		{Type: uint32(raft.MsgStorageAck), From: 2, To: 1, Index: 9},
		heartbeat,
	}}
	if err := tr.Server().Send(&fakeStream[pb.Batch]{in: []*pb.Batch{batch}}); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 2 || (*got)[0].Type != raft.MsgHeartbeat || (*got)[1].Type != raft.MsgHeartbeat {
		t.Fatalf("delivered %v", *got)
	}
}

func TestTheSnapshotStreamCarriesOnlySnapshots(t *testing.T) {
	tr, got := newReceiver(t)
	notSnap := &pb.SnapshotChunk{Message: toProto(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 3, Snapshot: &raft.Snapshot{Index: 9, Term: 3}})}
	if err := tr.Server().InstallSnapshot(&fakeStream[pb.SnapshotChunk]{in: []*pb.SnapshotChunk{notSnap}}); err == nil {
		t.Fatal("an append on the snapshot stream was accepted")
	}
	snap := &pb.SnapshotChunk{Message: toProto(raft.Message{Type: raft.MsgSnap, From: 2, To: 1, Term: 3, Snapshot: &raft.Snapshot{Index: 9, Term: 3}}), Data: []byte("state")}
	if err := tr.Server().InstallSnapshot(&fakeStream[pb.SnapshotChunk]{in: []*pb.SnapshotChunk{snap}}); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 1 || string((*got)[0].Snapshot.Data) != "state" {
		t.Fatalf("delivered %v", *got)
	}
}
