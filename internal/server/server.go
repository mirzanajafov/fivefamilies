package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/storage"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
	"github.com/mirzanajafov/fivefamilies/internal/watch"
)

type Server struct {
	cfg       Config
	disk      *storage.Disk
	raft      *raft.Raft
	node      *node.Node
	sm        *watch.Machine
	tr        *transport.Transport
	inbound   chan raft.Message
	calls     chan func()
	jobs      chan diskJob
	written   chan uint64
	stopped   chan struct{}
	quit      chan struct{}
	stop      context.CancelFunc
	failOnce  sync.Once
	failure   error
	lastTick  time.Time
	lastWatch time.Time
	status    atomic.Pointer[pb.StatusResponse]
	counts    counters
	isolated  isolation
	stalled   fault
	paused    fault
	lastTerm  uint64
	log       *slog.Logger
}

func randomUint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return binary.LittleEndian.Uint64(b[:])
}

func New(cfg Config) (*Server, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, err
	}
	s := &Server{
		cfg:     cfg,
		inbound: make(chan raft.Message, 4096),
		calls:   make(chan func(), 1024),
		jobs:    make(chan diskJob, 4096),
		written: make(chan uint64, 1024),
		stopped: make(chan struct{}),
		quit:    make(chan struct{}),
		log:     cfg.Logger.With("node", cfg.ID),
	}
	disk, st, rec, err := storage.Open(vfs.OS{Dir: cfg.DataDir}, wal.Options{SegmentBytes: cfg.SegmentBytes, Salt: randomUint64})
	if err != nil {
		return nil, err
	}
	if rec.TornBytes > 0 {
		s.log.Warn("cut a torn tail from the log", "bytes", rec.TornBytes)
	}
	s.disk = disk
	if err := s.start(st); err != nil {
		return nil, errors.Join(err, disk.Close())
	}
	return s, nil
}

func (s *Server) start(st raft.Restore) error {
	cfg := s.cfg
	s.sm = watch.NewMachine(cfg.MaxSessions, cfg.WatchHistory, cfg.Bugs)
	if st.Snapshot != nil {
		if err := s.sm.Restore(st.Snapshot.Index, st.Snapshot.Data); err != nil {
			return err
		}
	}
	r, err := raft.New(raft.Config{
		ID:            cfg.ID,
		Voters:        cfg.Voters,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		PreVote:       true,
		CheckQuorum:   true,
		LeaseTicks:    cfg.LeaseTicks,
		MaxInflight:   cfg.MaxInflight,
		MaxPending:    cfg.MaxPending,
		MaxMsgBytes:   cfg.MaxMsgBytes,
		Rand:          rngSource{},
		Bugs:          cfg.Bugs,
	}, st)
	if err != nil {
		return err
	}
	s.raft = r
	s.node = node.New(r, s, s.sm, node.Config{SnapshotEvery: cfg.SnapshotEvery, Retain: cfg.Retain, Tick: cfg.Tick, WriteBeforeSend: cfg.WriteBeforeSend})
	tr, err := transport.New(cfg.ID, cfg.Nodes, s.deliver, &s.isolated, s.log)
	if err != nil {
		return err
	}
	s.tr = tr
	s.publish()
	return nil
}

type rngSource struct{}

func (rngSource) Uint64() uint64 { return randomUint64() }

func (s *Server) Register(g *grpc.Server) {
	pb.RegisterPeerServer(g, s.tr.Server())
	pb.RegisterKVServer(g, &kvService{s: s})
	pb.RegisterAdminServer(g, &adminService{s: s})
	pb.RegisterLeaseServer(g, &leaseService{s: s})
	pb.RegisterWatchServer(g, &watchService{s: s})
}

func (s *Server) Run(ctx context.Context) error {
	ctx, s.stop = context.WithCancel(ctx)
	defer s.stop()
	context.AfterFunc(ctx, func() { close(s.quit) })
	var wg sync.WaitGroup
	s.tr.Start(ctx)
	wg.Go(func() { s.writer(ctx) })
	s.lastTick = time.Now()
	if err := s.loop(ctx); err != nil {
		s.die(err)
	}
	close(s.stopped)
	wg.Wait()
	s.tr.Close()
	return errors.Join(s.failure, s.disk.Close())
}

func (s *Server) die(err error) {
	s.failOnce.Do(func() {
		s.failure = err
		s.stop()
	})
}

func (s *Server) deliver(m raft.Message) {
	s.counts.received.Add(1)
	select {
	case s.inbound <- m:
	case <-s.stopped:
	}
}

func (s *Server) Send(m raft.Message) {
	s.counts.sent.Add(1)
	s.tr.Send(m)
}
