package node

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type Write struct {
	wal.Batch
	Seq  uint64
	Sync bool
}

type Env interface {
	Send(m raft.Message)
	Write(w Write)
	SaveSnapshot(s raft.Snapshot) error
	Compacted(s raft.Snapshot)
}

type StateMachine interface {
	Apply(e raft.Entry) kv.Result
	Get(key string) kv.Result
	Snapshot() []byte
	Restore(index uint64, data []byte) error
	Lease(id uint64) (kv.LeaseInfo, bool)
	Leases() []kv.LeaseInfo
}

type Config struct {
	SnapshotEvery   uint64
	Retain          uint64
	Tick            time.Duration
	WriteBeforeSend bool
	Bugs            planted.Bug
}

var (
	ErrLost            = errors.New("node: the entry was replaced before it committed")
	ErrNotReady        = errors.New("node: the read could not be confirmed, try again")
	ErrTransferAborted = errors.New("node: the transfer gave up")
)

const (
	readExpiry = 64
	maxCatchUp = 100_000
)

type NotLeaderError struct {
	Leader raft.NodeID
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("node: not the leader (leader hint %d)", e.Leader)
}

type Done func(kv.Result, error)

type Transferred func(leader raft.NodeID, term uint64, err error)

type transferWaiter struct {
	to   raft.NodeID
	term uint64
	done Transferred
}

type pendingWrite struct {
	seq     uint64
	after   []raft.Message
	compact *raft.Snapshot
}

type waiter struct {
	term uint64
	done Done
}

type readWaiter struct {
	serve func() (kv.Result, error)
	done  Done
	term  uint64
	tick  uint64
}

type confirmedRead struct {
	index uint64
	serve func() (kv.Result, error)
	done  Done
}

type leaseClock struct {
	ttl      uint64
	deadline uint64
	expiring bool
}

type Node struct {
	r         *raft.Raft
	env       Env
	sm        StateMachine
	cfg       Config
	seq       uint64
	pending   []pendingWrite
	waiters   map[uint64]waiter
	lastSnap  uint64
	taking    bool
	ticks     uint64
	carry     time.Duration
	nextCtx   uint64
	reads     map[uint64]readWaiter
	ready     []confirmedRead
	transfers []transferWaiter

	leases     map[uint64]*leaseClock
	leaseTerm  uint64
	nextExpiry uint64
	leaderless uint64
}

func New(r *raft.Raft, env Env, sm StateMachine, cfg Config) *Node {
	if cfg.Tick <= 0 {
		cfg.Tick = 10 * time.Millisecond
	}
	n := &Node{r: r, env: env, sm: sm, cfg: cfg, waiters: map[uint64]waiter{}, reads: map[uint64]readWaiter{}}
	if s := r.LatestSnapshot(); s != nil {
		n.lastSnap = s.Index
	}
	return n
}

func (n *Node) Raft() *raft.Raft {
	return n.r
}

func (n *Node) Advance(elapsed time.Duration) {
	n.carry += elapsed
	due := int64(n.carry / n.cfg.Tick)
	n.carry %= n.cfg.Tick
	for range min(due, maxCatchUp) {
		n.tick()
	}
}

func (n *Node) tick() {
	n.ticks++
	n.r.Tick()
	if n.r.LeaderFresh() {
		n.leaderless = 0
	} else {
		n.leaderless++
	}
	if n.ticks%16 == 0 {
		n.expireReads(func(w readWaiter) bool { return w.tick+readExpiry < n.ticks })
	}
	n.syncLessor()
	n.expireLeases()
}

func (n *Node) LeaderlessTicks() uint64 {
	return n.leaderless
}

func (n *Node) Read(key string, lease bool, done Done) {
	n.readThen(lease, done, func() (kv.Result, error) { return n.sm.Get(key), nil })
}

func (n *Node) readThen(lease bool, done Done, serve func() (kv.Result, error)) {
	n.nextCtx++
	n.reads[n.nextCtx] = readWaiter{serve: serve, done: done, term: n.r.Term(), tick: n.ticks}
	n.r.ReadIndex(n.nextCtx, lease)
}

func (n *Node) ttlTicks(ms uint64) uint64 {
	return uint64((time.Duration(ms)*time.Millisecond+n.cfg.Tick-1)/n.cfg.Tick) + 1
}

func (n *Node) KeepAlive(id uint64, done Done) {
	if n.r.State() != raft.StateLeader {
		done(kv.Result{}, &NotLeaderError{Leader: n.r.Leader()})
		return
	}
	term := n.r.Term()
	renew := func() (kv.Result, error) {
		if n.r.State() != raft.StateLeader || n.r.Term() != term || n.leaseTerm != term {
			return kv.Result{}, &NotLeaderError{Leader: n.r.Leader()}
		}
		info, ok := n.sm.Lease(id)
		lc := n.leases[id]
		if !ok || lc == nil || lc.expiring {
			return kv.Result{Code: kv.ErrLeaseNotFound, Rev: n.r.Applied()}, nil
		}
		lc.deadline = max(lc.deadline, n.ticks+lc.ttl)
		return kv.Result{Ok: true, Rev: n.r.Applied(), Lease: id, TTL: info.TTL}, nil
	}
	if n.cfg.Bugs.Has(planted.LocalKeepAlive) {
		n.syncLessor()
		done(renew())
		return
	}
	n.readThen(false, done, renew)
}

func (n *Node) LeaseRemaining(id uint64) (time.Duration, bool) {
	lc := n.leases[id]
	if lc == nil || n.leaseTerm != n.r.Term() {
		return 0, false
	}
	if lc.deadline <= n.ticks {
		return 0, true
	}
	return time.Duration(lc.deadline-n.ticks) * n.cfg.Tick, true
}

func (n *Node) syncLessor() {
	if n.r.State() != raft.StateLeader {
		n.leases, n.leaseTerm = nil, 0
		return
	}
	if n.leaseTerm == n.r.Term() {
		return
	}
	n.leaseTerm = n.r.Term()
	n.leases = map[uint64]*leaseClock{}
	n.nextExpiry = 0
	for _, l := range n.sm.Leases() {
		n.trackLease(l.ID, l.TTL)
	}
}

func (n *Node) trackLease(id, ttlMs uint64) {
	ttl := n.ttlTicks(ttlMs)
	lc := &leaseClock{ttl: ttl, deadline: n.ticks + ttl}
	n.leases[id] = lc
	if n.nextExpiry == 0 || lc.deadline < n.nextExpiry {
		n.nextExpiry = lc.deadline
	}
}

func (n *Node) leaseApplied(id uint64) {
	if n.leaseTerm == 0 || n.leaseTerm != n.r.Term() {
		return
	}
	info, exists := n.sm.Lease(id)
	_, tracked := n.leases[id]
	switch {
	case exists && !tracked:
		n.trackLease(id, info.TTL)
	case !exists && tracked:
		delete(n.leases, id)
	}
}

func (n *Node) expireLeases() {
	if n.leaseTerm == 0 || n.leaseTerm != n.r.Term() || n.nextExpiry == 0 || n.ticks < n.nextExpiry {
		return
	}
	var due []uint64
	n.nextExpiry = 0
	for id, lc := range n.leases {
		switch {
		case lc.expiring:
		case lc.deadline <= n.ticks:
			due = append(due, id)
		case n.nextExpiry == 0 || lc.deadline < n.nextExpiry:
			n.nextExpiry = lc.deadline
		}
	}
	slices.Sort(due)
	for _, id := range due {
		if _, err := n.r.Propose(kv.Encode(kv.Command{Op: kv.OpLeaseExpire, Lease: id})); err != nil {
			n.nextExpiry = n.ticks + 1
			continue
		}
		n.leases[id].expiring = true
	}
}

func (n *Node) expireReads(expired func(readWaiter) bool) {
	var ctxs []uint64
	for ctx, w := range n.reads {
		if expired(w) {
			ctxs = append(ctxs, ctx)
		}
	}
	slices.Sort(ctxs)
	for _, ctx := range ctxs {
		w := n.reads[ctx]
		delete(n.reads, ctx)
		w.done(kv.Result{}, ErrNotReady)
	}
}

func (n *Node) serveReads() {
	applied := n.r.Applied()
	kept := n.ready[:0]
	for _, q := range n.ready {
		if q.index <= applied {
			q.done(q.serve())
		} else {
			kept = append(kept, q)
		}
	}
	n.ready = kept
}

func (n *Node) Receive(m raft.Message) {
	n.r.Step(m)
}

func (n *Node) Propose(data []byte) (uint64, error) {
	return n.r.Propose(data)
}

func (n *Node) Submit(cmd kv.Command, done Done) {
	if n.r.State() != raft.StateLeader {
		if done != nil {
			done(kv.Result{}, &NotLeaderError{Leader: n.r.Leader()})
		}
		return
	}
	idx, err := n.r.Propose(kv.Encode(cmd))
	if err != nil {
		if done != nil {
			done(kv.Result{}, err)
		}
		return
	}
	if done != nil {
		n.waiters[idx] = waiter{term: n.r.Term(), done: done}
	}
}

func (n *Node) ProposeConfChange(cc raft.ConfChange, done Done) {
	if n.r.State() != raft.StateLeader {
		done(kv.Result{}, &NotLeaderError{Leader: n.r.Leader()})
		return
	}
	idx, err := n.r.ProposeConfChange(cc)
	if err != nil {
		done(kv.Result{}, err)
		return
	}
	n.waiters[idx] = waiter{term: n.r.Term(), done: done}
}

func (n *Node) TransferLeadership(to raft.NodeID, done Transferred) {
	if n.r.State() != raft.StateLeader {
		done(n.r.Leader(), n.r.Term(), &NotLeaderError{Leader: n.r.Leader()})
		return
	}
	if err := n.r.TransferLeadership(to); err != nil || to == n.r.ID() {
		done(n.r.Leader(), n.r.Term(), err)
		return
	}
	n.transfers = append(n.transfers, transferWaiter{to: to, term: n.r.Term(), done: done})
}

func (n *Node) settleTransfers() {
	if len(n.transfers) == 0 {
		return
	}
	term, lead := n.r.Term(), n.r.Leader()
	pending := n.transfers
	n.transfers = nil
	leading := n.r.State() == raft.StateLeader
	for _, w := range pending {
		moved := term > w.term && lead != raft.None
		gaveUp := term == w.term && leading && n.r.Transferring() != w.to
		switch {
		case moved && !leading:
			w.done(lead, term, nil)
		case moved, gaveUp:
			w.done(lead, term, fmt.Errorf("%w: node %d did not take over", ErrTransferAborted, w.to))
		default:
			n.transfers = append(n.transfers, w)
		}
	}
}

func (n *Node) Waiting() int {
	return len(n.waiters)
}

func (n *Node) WriteDone(seq uint64) error {
	for len(n.pending) > 0 && n.pending[0].seq <= seq {
		pw := n.pending[0]
		n.pending = n.pending[1:]
		for _, m := range pw.after {
			if m.To == n.r.ID() {
				n.r.Step(m)
			} else {
				n.env.Send(m)
			}
		}
		if pw.compact != nil {
			n.taking = false
			if err := n.r.Compact(*pw.compact, n.cfg.Retain); err != nil {
				return fmt.Errorf("node: compacting to %d: %w", pw.compact.Index, err)
			}
			n.env.Compacted(*pw.compact)
		}
	}
	return nil
}

func (n *Node) PendingWrites() int {
	return len(n.pending)
}

func (n *Node) write(w Write, after []raft.Message, compact *raft.Snapshot) {
	n.seq++
	w.Seq = n.seq
	n.pending = append(n.pending, pendingWrite{seq: n.seq, after: after, compact: compact})
	n.env.Write(w)
}

func (n *Node) Process() error {
	for n.r.HasReady() {
		rd := n.r.Ready()
		if s := rd.Snapshot; s != nil {
			if err := n.env.SaveSnapshot(*s); err != nil {
				return err
			}
			if err := n.sm.Restore(s.Index, s.Data); err != nil {
				return fmt.Errorf("node: restoring snapshot %d: %w", s.Index, err)
			}
			n.lastSnap = s.Index
			n.loseWaitersUpTo(s.Index)
		}
		now, after := rd.Messages, rd.AfterPersist
		if n.cfg.WriteBeforeSend && len(rd.Entries) > 0 && n.r.State() == raft.StateLeader {
			now, after = nil, nil
			for _, m := range rd.Messages {
				if m.Type == raft.MsgApp {
					after = append(after, m)
				} else {
					now = append(now, m)
				}
			}
			after = append(after, rd.AfterPersist...)
		}
		if rd.NeedsWrite() || len(after) > 0 {
			n.write(Write{Batch: wal.Batch{HardState: rd.HardState, Snapshot: rd.Snapshot, Reset: rd.Snapshot != nil, Entries: rd.Entries}, Sync: rd.MustSync}, after, rd.Snapshot)
		}
		for _, m := range now {
			n.env.Send(m)
		}
		for _, rs := range rd.ReadStates {
			w, ok := n.reads[rs.Ctx]
			if !ok {
				continue
			}
			delete(n.reads, rs.Ctx)
			if rs.Ok {
				n.ready = append(n.ready, confirmedRead{index: rs.Index, serve: w.serve, done: w.done})
			} else {
				w.done(kv.Result{}, ErrNotReady)
			}
		}
		n.syncLessor()
		for _, e := range rd.Committed {
			res := n.sm.Apply(e)
			if res.Lease != 0 {
				n.leaseApplied(res.Lease)
			}
			w, ok := n.waiters[e.Index]
			if !ok {
				continue
			}
			delete(n.waiters, e.Index)
			if e.Term == w.term {
				w.done(res, nil)
			} else {
				w.done(kv.Result{}, ErrLost)
			}
		}
		n.serveReads()
		if err := n.maybeSnapshot(); err != nil {
			return err
		}
	}
	if term := n.r.Term(); len(n.reads) > 0 {
		n.expireReads(func(w readWaiter) bool { return w.term != term })
	}
	n.settleTransfers()
	return nil
}

func (n *Node) loseWaitersUpTo(index uint64) {
	var lost []uint64
	for idx := range n.waiters {
		if idx <= index {
			lost = append(lost, idx)
		}
	}
	slices.Sort(lost)
	for _, idx := range lost {
		w := n.waiters[idx]
		delete(n.waiters, idx)
		w.done(kv.Result{}, ErrLost)
	}
}

func (n *Node) maybeSnapshot() error {
	applied := n.r.Applied()
	if n.cfg.SnapshotEvery == 0 || n.taking || applied < n.lastSnap+n.cfg.SnapshotEvery {
		return nil
	}
	term, ok := n.r.LogTerm(applied)
	if !ok {
		return nil
	}
	s := raft.Snapshot{Index: applied, Term: term, Data: n.sm.Snapshot(), Conf: n.r.ConfAt(applied)}
	if err := n.env.SaveSnapshot(s); err != nil {
		return err
	}
	n.lastSnap = applied
	n.taking = true
	n.write(Write{Batch: wal.Batch{HardState: n.r.HardState(), Snapshot: &s}, Sync: true}, nil, &s)
	return nil
}
