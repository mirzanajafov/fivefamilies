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
	Bugs          raft.Bug
	Logger        *slog.Logger
	Fatal         func(error)
}

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
	if c.Fatal == nil {
		c.Fatal = func(err error) {
			c.Logger.Error("fatal", "err", err)
			os.Exit(70)
		}
	}
	return nil
}

type diskJob struct {
	write   *node.Write
	compact *raft.Snapshot
}

type Server struct {
	cfg      Config
	fs       vfs.OS
	wal      *wal.Log
	raft     *raft.Raft
	node     *node.Node
	sm       *machine
	tr       *transport.Transport
	Filter   *Filter
	inbound  chan raft.Message
	calls    chan func()
	jobs     chan diskJob
	written  chan uint64
	stopped  chan struct{}
	lastTick time.Time
	status   atomic.Pointer[pb.StatusResponse]
	log      *slog.Logger
	fatalled atomic.Bool
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
	l, st, rec, err := wal.Open(s.fs, wal.Options{SegmentBytes: cfg.SegmentBytes, Salt: randomUint64})
	if err != nil {
		return nil, fmt.Errorf("opening the log: %w", err)
	}
	if rec.TornBytes > 0 {
		s.log.Warn("cut a torn tail from the log", "bytes", rec.TornBytes)
	}
	s.wal = l
	s.sm = &machine{store: kv.NewStore(cfg.MaxSessions), maxSessions: cfg.MaxSessions}
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
		Rand:          rngSource{},
		Bugs:          cfg.Bugs,
	}, st)
	if err != nil {
		return nil, err
	}
	s.raft = r
	s.node = node.New(r, s, s.sm, node.Config{SnapshotEvery: cfg.SnapshotEvery, Retain: cfg.Retain, Tick: cfg.Tick})
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
		s.node.Process()
		s.publish()
	}
}

func (s *Server) Send(m raft.Message) {
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
	drain:
		for {
			select {
			case j := <-s.jobs:
				jobs = append(jobs, j)
			default:
				break drain
			}
		}
		var last uint64
		sync := false
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
			if err := s.wal.WriteBatch(b); err != nil {
				s.die(fmt.Errorf("writing the log: %w", err))
				return
			}
			last = w.Seq
			sync = sync || w.Sync
		}
		if sync {
			if err := s.wal.Sync(); err != nil {
				s.die(fmt.Errorf("syncing the log: %w", err))
				return
			}
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
	ch := make(chan result, 1)
	call := func() {
		f(func(res kv.Result, err error) { ch <- result{res, err} })
	}
	select {
	case s.calls <- call:
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
	select {
	case r := <-ch:
		return r.res, r.err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}

func (s *Server) Submit(ctx context.Context, cmd kv.Command) (kv.Result, error) {
	return s.run(ctx, func(done node.Done) { s.node.Submit(cmd, done) })
}

func (s *Server) Read(ctx context.Context, key string, lease bool) (kv.Result, error) {
	return s.run(ctx, func(done node.Done) { s.node.Read(key, lease, done) })
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
		info, exists = s.sm.store.Lease(id)
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
	case s.calls <- func() { ch <- hashed{s.sm.store.Hash(), s.sm.store.Rev()} }:
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

type machine struct {
	store       *kv.Store
	maxSessions int
}

func (m *machine) Apply(e raft.Entry) kv.Result {
	return m.store.Apply(e)
}

func (m *machine) Get(key string) kv.Result {
	kvp, found := m.store.Get(key)
	return kv.Result{Rev: m.store.Rev(), Found: found, KV: kvp, Ok: true}
}

func (m *machine) Snapshot() []byte {
	return m.store.Snapshot()
}

func (m *machine) Lease(id uint64) (kv.LeaseInfo, bool) {
	return m.store.Lease(id)
}

func (m *machine) Leases() []kv.LeaseInfo {
	return m.store.Leases()
}

func (m *machine) Restore(index uint64, data []byte) error {
	st, err := kv.Restore(data, m.maxSessions)
	if err != nil {
		return err
	}
	if st.Rev() != index {
		return fmt.Errorf("snapshot %d holds a store at revision %d", index, st.Rev())
	}
	m.store = st
	return nil
}
