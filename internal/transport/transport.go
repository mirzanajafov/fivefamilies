package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

const (
	queueSize  = 4096
	batchMax   = 256
	chunkBytes = 1 << 20
)

type Stats struct {
	Sent      atomic.Uint64
	Dropped   atomic.Uint64
	Received  atomic.Uint64
	Snapshots atomic.Uint64
}

type Filter interface {
	Allow(from, to raft.NodeID) bool
}

type Transport struct {
	id      raft.NodeID
	peers   map[raft.NodeID]*peer
	deliver func(raft.Message)
	filter  Filter
	log     *slog.Logger
	Stats   Stats
	wg      sync.WaitGroup
}

type peer struct {
	id    raft.NodeID
	conn  *grpc.ClientConn
	queue chan raft.Message
	snaps chan raft.Message
}

func New(id raft.NodeID, addrs map[raft.NodeID]string, deliver func(raft.Message), filter Filter, log *slog.Logger) (*Transport, error) {
	t := &Transport{id: id, peers: map[raft.NodeID]*peer{}, deliver: deliver, filter: filter, log: log}
	for pid, addr := range addrs {
		if pid == id {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 2 * time.Second, Timeout: time.Second, PermitWithoutStream: true}),
		)
		if err != nil {
			return nil, err
		}
		t.peers[pid] = &peer{id: pid, conn: conn, queue: make(chan raft.Message, queueSize), snaps: make(chan raft.Message, 2)}
	}
	return t, nil
}

func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: time.Second, PermitWithoutStream: true}),
		grpc.MaxRecvMsgSize(64 << 20),
	}
}

func (t *Transport) Start(ctx context.Context) {
	for _, p := range t.peers {
		t.wg.Go(func() { t.runPeer(ctx, p) })
		t.wg.Go(func() { t.runSnapshots(ctx, p) })
	}
}

func (t *Transport) Close() {
	t.wg.Wait()
	for _, p := range t.peers {
		p.conn.Close()
	}
}

func (t *Transport) Send(m raft.Message) {
	p, ok := t.peers[m.To]
	if !ok || (t.filter != nil && !t.filter.Allow(t.id, m.To)) {
		t.Stats.Dropped.Add(1)
		return
	}
	q := p.queue
	if m.Type == raft.MsgSnap {
		q = p.snaps
	}
	select {
	case q <- m:
	default:
		t.Stats.Dropped.Add(1)
	}
}

func (t *Transport) runPeer(ctx context.Context, p *peer) {
	client := pb.NewPeerClient(p.conn)
	backoff := 50 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := client.Send(ctx)
		if err != nil {
			t.sleep(ctx, &backoff)
			continue
		}
		backoff = 50 * time.Millisecond
		if err := t.pump(ctx, p, stream); err != nil && ctx.Err() == nil {
			t.log.Debug("peer stream ended", "peer", p.id, "err", err)
			t.sleep(ctx, &backoff)
		}
		if _, err := stream.CloseAndRecv(); err != nil && ctx.Err() == nil {
			t.log.Debug("closing the peer stream", "peer", p.id, "err", err)
		}
	}
}

func (t *Transport) pump(ctx context.Context, p *peer, stream grpc.ClientStreamingClient[pb.Batch, pb.Ack]) error {
	batch := &pb.Batch{}
	for {
		var m raft.Message
		select {
		case <-ctx.Done():
			return ctx.Err()
		case m = <-p.queue:
		}
		batch.Messages = append(batch.Messages[:0], toProto(m))
		for len(batch.Messages) < batchMax {
			select {
			case m = <-p.queue:
				batch.Messages = append(batch.Messages, toProto(m))
				continue
			default:
			}
			break
		}
		if err := stream.Send(batch); err != nil {
			t.Stats.Dropped.Add(uint64(len(batch.Messages)))
			return err
		}
		t.Stats.Sent.Add(uint64(len(batch.Messages)))
	}
}

func (t *Transport) runSnapshots(ctx context.Context, p *peer) {
	client := pb.NewPeerClient(p.conn)
	for {
		var m raft.Message
		select {
		case <-ctx.Done():
			return
		case m = <-p.snaps:
		}
		if err := t.sendSnapshot(ctx, client, m); err != nil {
			t.log.Info("snapshot not sent", "peer", p.id, "index", m.Snapshot.Index, "err", err)
			continue
		}
		t.Stats.Snapshots.Add(1)
	}
}

func (t *Transport) sendSnapshot(ctx context.Context, client pb.PeerClient, m raft.Message) error {
	stream, err := client.InstallSnapshot(ctx)
	if err != nil {
		return err
	}
	data := m.Snapshot.Data
	first := true
	for first || len(data) > 0 {
		n := min(len(data), chunkBytes)
		chunk := &pb.SnapshotChunk{Data: data[:n]}
		if first {
			chunk.Message = toProto(m)
			first = false
		}
		if err := stream.Send(chunk); err != nil {
			return err
		}
		data = data[n:]
	}
	_, err = stream.CloseAndRecv()
	return err
}

func (t *Transport) sleep(ctx context.Context, backoff *time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(*backoff):
	}
	*backoff = min(2**backoff, time.Second)
}

func (t *Transport) admit(pm *pb.RaftMessage, snapshotStream bool) (raft.Message, error) {
	m, err := fromProto(pm)
	switch {
	case err != nil:
		return raft.Message{}, err
	case t.peers[m.From] == nil || m.To != t.id:
		return raft.Message{}, fmt.Errorf("transport: node %d got a message from %d to %d", t.id, m.From, m.To)
	case (m.Type == raft.MsgSnap) != snapshotStream:
		return raft.Message{}, fmt.Errorf("transport: a %s on the wrong stream", m.Type)
	}
	return m, nil
}

func (t *Transport) receive(m raft.Message) {
	if t.filter != nil && !t.filter.Allow(m.From, t.id) {
		return
	}
	t.Stats.Received.Add(1)
	t.deliver(m)
}

type server struct {
	pb.UnimplementedPeerServer
	t *Transport
}

func (t *Transport) Server() pb.PeerServer {
	return server{t: t}
}

func (s server) Send(stream grpc.ClientStreamingServer[pb.Batch, pb.Ack]) error {
	refused := 0
	var logged time.Time
	for {
		b, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&pb.Ack{})
		}
		if err != nil {
			return err
		}
		for _, pm := range b.GetMessages() {
			m, err := s.t.admit(pm, false)
			if err != nil {
				refused++
				if time.Since(logged) >= time.Minute {
					s.t.log.Warn("dropped messages a peer may not send", "from", pm.GetFrom(), "dropped", refused, "err", err)
					logged, refused = time.Now(), 0
				}
				continue
			}
			s.t.receive(m)
		}
	}
}

func (s server) InstallSnapshot(stream grpc.ClientStreamingServer[pb.SnapshotChunk, pb.Ack]) error {
	var msg *raft.Message
	var data []byte
	for {
		c, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if msg == nil {
			if c.GetMessage().GetSnapshot() == nil {
				return errors.New("transport: a snapshot stream that starts without a snapshot message")
			}
			m, err := s.t.admit(c.GetMessage(), true)
			if err != nil {
				return err
			}
			msg = &m
		}
		data = append(data, c.GetData()...)
	}
	switch {
	case msg == nil:
		return errors.New("transport: an empty snapshot stream")
	case len(data) == 0:
		return fmt.Errorf("transport: snapshot %d from node %d has no data", msg.Snapshot.Index, msg.From)
	}
	msg.Snapshot.Data = data
	s.t.receive(*msg)
	return stream.SendAndClose(&pb.Ack{})
}
