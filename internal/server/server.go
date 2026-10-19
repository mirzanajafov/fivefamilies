package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
	"github.com/mirzanajafov/fivefamilies/internal/watch"
)

type Config struct {
	ID            raft.NodeID
	Nodes         map[raft.NodeID]string
	Voters        []raft.NodeID
	DataDir       string
	Tick          time.Duration
	HeartbeatTick int
	ElectionTick  int
	LeaseTicks    int
	SnapshotEvery uint64
	Retain        uint64
	SegmentBytes  int64
	MaxSessions   int
	WatchHistory  int
	WatchProgress time.Duration
	Bugs          raft.Bug
	Logger        *slog.Logger
	Fatal         func(error)

	FsyncMode       string
	SyncStorage     bool
	WriteBeforeSend bool
	MaxInflight     int
	MaxMsgBytes     int
	MaxPending      uint64
	Batch           int
	Chaos           bool
	Crash           func()
}

const (
	FsyncGroup = "group"
	FsyncReady = "ready"
	FsyncEntry = "entry"
	FsyncOff   = "off"
)

func (c *Config) defaults() error {
	if c.ID == raft.None || c.Nodes[c.ID] == "" {
		return errors.New("server: the node's own ID is not in the cluster")
	}
	if len(c.Voters) == 0 {
		for id := range c.Nodes {
			c.Voters = append(c.Voters, id)
		}
	}
	slices.Sort(c.Voters)
	for _, id := range c.Voters {
		if c.Nodes[id] == "" {
			return fmt.Errorf("server: voter %d has no address", id)
		}
	}
	if c.Tick <= 0 {
		c.Tick = 10 * time.Millisecond
	}
	if c.HeartbeatTick <= 0 {
		c.HeartbeatTick = 10
	}
	if c.ElectionTick <= 0 {
		c.ElectionTick = 10 * c.HeartbeatTick
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	switch c.FsyncMode {
	case "":
		c.FsyncMode = FsyncGroup
	case FsyncGroup, FsyncReady, FsyncEntry, FsyncOff:
	default:
		return fmt.Errorf("server: unknown fsync mode %q", c.FsyncMode)
	}
	if c.Crash == nil {
		c.Crash = func() { os.Exit(137) }
	}
	if c.WatchProgress <= 0 {
		c.WatchProgress = time.Second
	}
	if c.Batch <= 0 {
		c.Batch = 256
	}
	if c.Fatal == nil {
		c.Fatal = func(err error) {
			c.Logger.Error("fatal", "err", err)
			os.Exit(70)
		}
	}
	return nil
}

type counters struct {
	fsyncs, batches, entries, sent, received, reads, writes, elections atomic.Uint64
}

type diskJob struct {
	write   *node.Write
	compact *raft.Snapshot
}

type Server struct {
	cfg       Config
	fs        vfs.OS
	wal       *wal.Log
	raft      *raft.Raft
	node      *node.Node
	sm        *watch.Machine
	tr        *transport.Transport
	Filter    *Filter
	inbound   chan raft.Message
	calls     chan func()
	jobs      chan diskJob
	written   chan uint64
	stopped   chan struct{}
	lastTick  time.Time
	lastWatch time.Time
	status    atomic.Pointer[pb.StatusResponse]
	counts    counters
	stall     atomic.Int64
	lastTerm  uint64
	log       *slog.Logger
	fatalled  atomic.Bool
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
		fs:      vfs.OS{Dir: cfg.DataDir},
		Filter:  NewFilter(),
		inbound: make(chan raft.Message, 4096),
		calls:   make(chan func(), 1024),
		jobs:    make(chan diskJob, 4096),
		written: make(chan uint64, 1024),
		stopped: make(chan struct{}),
		log:     cfg.Logger.With("node", cfg.ID),
	}
	if err := snap.RemoveUnfinished(s.fs); err != nil {
		return nil, fmt.Errorf("removing unfinished snapshots: %w", err)
	}
	l, st, rec, err := wal.Open(s.fs, wal.Options{SegmentBytes: cfg.SegmentBytes, Salt: randomUint64})
	if err != nil {
		return nil, fmt.Errorf("opening the log: %w", err)
	}
	if rec.TornBytes > 0 {
		s.log.Warn("cut a torn tail from the log", "bytes", rec.TornBytes)
	}
	s.wal = l
	s.sm = watch.NewMachine(cfg.MaxSessions, cfg.WatchHistory, cfg.Bugs)
	if st.Snapshot != nil {
		sn, err := snap.Load(s.fs, st.Snapshot.Index, st.Snapshot.Term)
		if err != nil {
			return nil, fmt.Errorf("loading snapshot %d: %w", st.Snapshot.Index, err)
		}
		st.Snapshot = &sn
		if err := s.sm.Restore(sn.Index, sn.Data); err != nil {
			return nil, err
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
		return nil, err
	}
	s.raft = r
	s.node = node.New(r, s, s.sm, node.Config{SnapshotEvery: cfg.SnapshotEvery, Retain: cfg.Retain, Tick: cfg.Tick, WriteBeforeSend: cfg.WriteBeforeSend})
	tr, err := transport.New(cfg.ID, cfg.Nodes, s.deliver, s.Filter, s.log)
	if err != nil {
		return nil, err
	}
	s.tr = tr
	s.publish()
	return s, nil
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

func (s *Server) Run(ctx context.Context) {
	var wg sync.WaitGroup
	s.tr.Start(ctx)
	wg.Go(func() { s.writer(ctx) })
	s.lastTick = time.Now()
	s.loop(ctx)
	close(s.stopped)
	wg.Wait()
	s.tr.Close()
	s.wal.Close()
}

func (s *Server) die(err error) {
	if s.fatalled.CompareAndSwap(false, true) {
		s.cfg.Fatal(err)
	}
}

func (s *Server) deliver(m raft.Message) {
	s.counts.received.Add(1)
	select {
	case s.inbound <- m:
	case <-s.stopped:
	}
}

func (s *Server) catchUp() {
	n := int(time.Since(s.lastTick) / s.cfg.Tick)
	if n <= 0 {
		return
	}
	s.lastTick = s.lastTick.Add(time.Duration(n) * s.cfg.Tick)
	for range min(n, 100000) {
		s.node.Tick()
	}
}

func (s *Server) loop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.catchUp()
			s.tendWatches()
		case m := <-s.inbound:
			s.catchUp()
			s.node.Receive(m)
		case f := <-s.calls:
			s.catchUp()
			f()
		case seq := <-s.written:
			s.catchUp()
			s.node.WriteDone(seq)
		}
		s.drain()
		s.node.Process()
		if s.cfg.SyncStorage {
			s.waitForDisk(ctx)
		}
		s.publish()
	}
}

func (s *Server) drain() {
	for range s.cfg.Batch - 1 {
		select {
		case m := <-s.inbound:
			s.node.Receive(m)
		case f := <-s.calls:
			f()
		case seq := <-s.written:
			s.node.WriteDone(seq)
		default:
			return
		}
	}
}

func (s *Server) waitForDisk(ctx context.Context) {
	for s.node.PendingWrites() > 0 {
		select {
		case seq := <-s.written:
			s.node.WriteDone(seq)
			s.node.Process()
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) isolate(d time.Duration) {
	for id := range s.cfg.Nodes {
		if id != s.cfg.ID {
			s.Filter.Block(s.cfg.ID, id)
			s.Filter.Block(id, s.cfg.ID)
		}
	}
	time.AfterFunc(d, s.Filter.Clear)
}

func (s *Server) leaderless() bool {
	return s.node.LeaderlessTicks() > uint64(2*s.cfg.ElectionTick)
}

func (s *Server) tendWatches() {
	if s.sm.Hub.Len() == 0 {
		return
	}
	if s.leaderless() {
		s.sm.Hub.EndAll()
		return
	}
	if time.Since(s.lastWatch) >= s.cfg.WatchProgress {
		s.lastWatch = time.Now()
		s.sm.Hub.Progress()
	}
}

func (s *Server) Send(m raft.Message) {
	s.counts.sent.Add(1)
	s.tr.Send(m)
}

func (s *Server) Write(w node.Write) {
	select {
	case s.jobs <- diskJob{write: &w}:
	case <-s.stopped:
	}
}

func (s *Server) SaveSnapshot(sn raft.Snapshot) {
	if err := snap.Save(s.fs, sn); err != nil {
		s.die(fmt.Errorf("saving snapshot %d: %w", sn.Index, err))
	}
}

func (s *Server) Compacted(sn raft.Snapshot) {
	select {
	case s.jobs <- diskJob{compact: &sn}:
	case <-s.stopped:
	}
}

func (s *Server) writer(ctx context.Context) {
	for {
		var first diskJob
		select {
		case <-ctx.Done():
			return
		case first = <-s.jobs:
		}
		jobs := []diskJob{first}
		if s.cfg.FsyncMode == FsyncGroup {
		drain:
			for {
				select {
				case j := <-s.jobs:
					jobs = append(jobs, j)
				default:
					break drain
				}
			}
		}
		var last uint64
		pending := false
		var compacts []*raft.Snapshot
		for _, j := range jobs {
			if j.compact != nil {
				compacts = append(compacts, j.compact)
				continue
			}
			w := j.write
			b := wal.Batch{HardState: w.HardState, Entries: w.Entries, Reset: w.Reset}
			if w.Snapshot != nil {
				b.Snapshot = &raft.Snapshot{Index: w.Snapshot.Index, Term: w.Snapshot.Term}
			}
			parts := []wal.Batch{b}
			if s.cfg.FsyncMode == FsyncEntry && len(b.Entries) > 1 {
				parts = parts[:0]
				for i, e := range b.Entries {
					p := wal.Batch{Entries: []raft.Entry{e}}
					if i == 0 {
						p.HardState, p.Snapshot, p.Reset = b.HardState, b.Snapshot, b.Reset
					}
					parts = append(parts, p)
				}
			}
			for _, p := range parts {
				if err := s.wal.WriteBatch(p); err != nil {
					s.die(fmt.Errorf("writing the log: %w", err))
					return
				}
				s.counts.batches.Add(1)
				s.counts.entries.Add(uint64(len(p.Entries)))
				pending = pending || w.Sync
				if s.cfg.FsyncMode != FsyncGroup && pending {
					if !s.sync() {
						return
					}
					pending = false
				}
			}
			last = w.Seq
		}
		if pending && !s.sync() {
			return
		}
		if last > 0 {
			select {
			case s.written <- last:
			case <-ctx.Done():
				return
			}
		}
		for _, c := range compacts {
			if _, err := s.wal.Compact(c.Index); err != nil {
				s.die(fmt.Errorf("compacting the log: %w", err))
				return
			}
			if err := snap.Clean(s.fs, *c); err != nil {
				s.die(fmt.Errorf("cleaning snapshots: %w", err))
				return
			}
		}
	}
}

func (s *Server) sync() bool {
	if d := time.Until(time.Unix(0, s.stall.Load())); d > 0 {
		time.Sleep(d)
	}
	if s.cfg.FsyncMode == FsyncOff {
		return true
	}
	if err := s.wal.Sync(); err != nil {
		s.die(fmt.Errorf("syncing the log: %w", err))
		return false
	}
	s.counts.fsyncs.Add(1)
	return true
}

func (s *Server) publish() {
	r := s.raft
	st := &pb.StatusResponse{
		Id:         uint64(s.cfg.ID),
		State:      r.State().String(),
		Term:       r.Term(),
		Leader:     uint64(r.Leader()),
		Commit:     r.Committed(),
		Applied:    r.Applied(),
		FirstIndex: r.FirstIndex(),
		LastIndex:  r.LastIndex(),
	}
	if sn := r.LatestSnapshot(); sn != nil {
		st.SnapshotIndex = sn.Index
	}
	conf := r.Conf()
	st.Voters, st.Learners, st.Transferee = ids64(conf.Voters), ids64(conf.Learners), uint64(r.Transferring())
	if r.Term() > s.lastTerm {
		if s.lastTerm > 0 {
			s.counts.elections.Add(r.Term() - s.lastTerm)
		}
		s.lastTerm = r.Term()
	}
	c := &s.counts
	st.Fsyncs, st.WalBatches, st.EntriesWritten = c.fsyncs.Load(), c.batches.Load(), c.entries.Load()
	st.MsgsSent, st.MsgsReceived = c.sent.Load(), c.received.Load()
	st.Reads, st.Writes, st.Elections = c.reads.Load(), c.writes.Load(), c.elections.Load()
	s.status.Store(st)
}

func ids64(ids []raft.NodeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func (s *Server) Status() *pb.StatusResponse {
	return s.status.Load()
}

func (s *Server) LeaderAddr() (raft.NodeID, string) {
	id := raft.NodeID(s.status.Load().GetLeader())
	return id, s.cfg.Nodes[id]
}

type result struct {
	res kv.Result
	err error
}

func (s *Server) run(ctx context.Context, f func(done node.Done)) (kv.Result, error) {
	return s.call(ctx, false, f)
}

func (s *Server) admit(ctx context.Context, f func(done node.Done)) (kv.Result, error) {
	return s.call(ctx, s.cfg.MaxPending > 0, f)
}

func (s *Server) call(ctx context.Context, refuse bool, f func(done node.Done)) (kv.Result, error) {
	ch := make(chan result, 1)
	call := func() {
		if err := ctx.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		f(func(res kv.Result, err error) { ch <- result{res, err} })
	}
	if refuse {
		select {
		case s.calls <- call:
		default:
			return kv.Result{}, raft.ErrBusy
		}
	} else {
		select {
		case s.calls <- call:
		case <-ctx.Done():
			return kv.Result{}, ctx.Err()
		}
	}
	select {
	case r := <-ch:
		return r.res, r.err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}

func (s *Server) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	return s.admit(ctx, func(done node.Done) { s.node.Submit(cmd, done) })
}

func (s *Server) Read(ctx context.Context, key string, lease bool) (kv.Result, error) {
	return s.admit(ctx, func(done node.Done) { s.node.Read(key, lease, done) })
}

func (s *Server) ChangeMembers(ctx context.Context, cc raft.ConfChange) (uint64, raft.ConfState, error) {
	if _, err := s.run(ctx, func(done node.Done) { s.node.ProposeConfChange(cc, done) }); err != nil {
		return 0, raft.ConfState{}, err
	}
	var index uint64
	var conf raft.ConfState
	err := s.inLoop(ctx, func() { index, conf = s.raft.Applied(), s.raft.Conf() })
	return index, conf, err
}

func (s *Server) TransferLeadership(ctx context.Context, to raft.NodeID) error {
	var err error
	if lerr := s.inLoop(ctx, func() { err = s.node.TransferLeadership(to); s.publish() }); lerr != nil {
		return lerr
	}
	if err != nil {
		return err
	}
	t := time.NewTicker(s.cfg.Tick)
	defer t.Stop()
	for {
		st := s.Status()
		switch {
		case raft.NodeID(st.GetLeader()) == to && to != s.cfg.ID:
			return nil
		case to == s.cfg.ID && st.GetState() == raft.StateLeader.String():
			return nil
		case st.GetState() == raft.StateLeader.String() && st.GetTransferee() == 0:
			return ErrTransferAborted
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

var ErrTransferAborted = errors.New("server: the transfer gave up, the target did not take over within an election timeout")

func (s *Server) inLoop(ctx context.Context, f func()) error {
	done := make(chan struct{})
	select {
	case s.calls <- func() { f(); close(done) }:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) MinLeaseTTL() time.Duration {
	return time.Duration(s.cfg.ElectionTick) * s.cfg.Tick * 3 / 2
}

func (s *Server) KeepAlive(ctx context.Context, id uint64) (kv.Result, error) {
	return s.run(ctx, func(done node.Done) { s.node.KeepAlive(id, done) })
}

func (s *Server) TimeToLive(ctx context.Context, id uint64) (remaining time.Duration, info kv.LeaseInfo, err error) {
	var leader, tracked, exists bool
	var hint raft.NodeID
	if lerr := s.inLoop(ctx, func() {
		leader, hint = s.raft.State() == raft.StateLeader, s.raft.Leader()
		remaining, tracked = s.node.LeaseRemaining(id)
		info, exists = s.sm.Store.Lease(id)
	}); lerr != nil {
		return 0, info, lerr
	}
	switch {
	case !leader:
		return 0, info, &node.NotLeaderError{Leader: hint}
	case !exists || !tracked:
		return 0, info, errLeaseNotFound
	}
	return remaining, info, nil
}

var errLeaseNotFound = errors.New("server: lease not found")

func (s *Server) StateHash(ctx context.Context) (uint64, uint64, error) {
	type hashed struct{ hash, rev uint64 }
	ch := make(chan hashed, 1)
	select {
	case s.calls <- func() { ch <- hashed{s.sm.Store.Hash(), s.sm.Store.Rev()} }:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	select {
	case h := <-ch:
		return h.hash, h.rev, nil
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}
