package node

import (
	"errors"
	"fmt"
	"slices"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Write struct {
	Seq       uint64
	HardState raft.HardState
	Snapshot  *raft.Snapshot
	Reset     bool
	Entries   []raft.Entry
	Sync      bool
}

func (w Write) Empty() bool {
	return w.HardState.IsEmpty() && w.Snapshot == nil && len(w.Entries) == 0
}

type Env interface {
	Send(m raft.Message)
	Write(w Write)
	SaveSnapshot(s raft.Snapshot)
	Compacted(s raft.Snapshot)
}

type StateMachine interface {
	Apply(e raft.Entry) kv.Result
	Get(key string) kv.Result
	Snapshot() []byte
	Restore(index uint64, data []byte) error
}

type Config struct {
	SnapshotEvery uint64
	Retain        uint64
}

var (
	ErrLost     = errors.New("node: the entry was replaced before it committed")
	ErrNotReady = errors.New("node: the read could not be confirmed, try again")
)

const readExpiry = 64

type NotLeaderError struct {
	Leader raft.NodeID
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("node: not the leader (leader hint %d)", e.Leader)
}

type Done func(kv.Result, error)

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
	key  string
	done Done
	term uint64
	tick uint64
}

type confirmedRead struct {
	index uint64
	key   string
	done  Done
}

type Node struct {
	r        *raft.Raft
	env      Env
	sm       StateMachine
	cfg      Config
	seq      uint64
	pending  []pendingWrite
	waiters  map[uint64]waiter
	lastSnap uint64
	taking   bool
	ticks    uint64
	nextCtx  uint64
	reads    map[uint64]readWaiter
	ready    []confirmedRead
}

func New(r *raft.Raft, env Env, sm StateMachine, cfg Config) *Node {
	n := &Node{r: r, env: env, sm: sm, cfg: cfg, waiters: map[uint64]waiter{}, reads: map[uint64]readWaiter{}}
	if s := r.LatestSnapshot(); s != nil {
		n.lastSnap = s.Index
	}
	return n
}

func (n *Node) Raft() *raft.Raft {
	return n.r
}

func (n *Node) Tick() {
	n.ticks++
	n.r.Tick()
	if n.ticks%16 == 0 {
		n.expireReads(func(w readWaiter) bool { return w.tick+readExpiry < n.ticks })
	}
}

func (n *Node) Read(key string, lease bool, done Done) {
	n.nextCtx++
	n.reads[n.nextCtx] = readWaiter{key: key, done: done, term: n.r.Term(), tick: n.ticks}
	n.r.ReadIndex(n.nextCtx, lease)
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
			q.done(n.sm.Get(q.key), nil)
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

func (n *Node) TransferLeadership(to raft.NodeID) error {
	if n.r.State() != raft.StateLeader {
		return &NotLeaderError{Leader: n.r.Leader()}
	}
	return n.r.TransferLeadership(to)
}

func (n *Node) Waiting() int {
	return len(n.waiters)
}

func (n *Node) WriteDone(seq uint64) {
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
				panic(fmt.Sprintf("node: compacting to %d: %v", pw.compact.Index, err))
			}
			n.env.Compacted(*n.r.LatestSnapshot())
		}
	}
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

func (n *Node) Process() {
	for n.r.HasReady() {
		rd := n.r.Ready()
		if s := rd.Snapshot; s != nil {
			n.env.SaveSnapshot(*s)
			if err := n.sm.Restore(s.Index, s.Data); err != nil {
				panic(fmt.Sprintf("node: restoring snapshot %d: %v", s.Index, err))
			}
			n.lastSnap = s.Index
			n.loseWaitersUpTo(s.Index)
		}
		if rd.NeedsWrite() || len(rd.AfterPersist) > 0 {
			n.write(Write{HardState: rd.HardState, Snapshot: rd.Snapshot, Reset: rd.Snapshot != nil, Entries: rd.Entries, Sync: rd.MustSync}, rd.AfterPersist, rd.Snapshot)
		}
		for _, m := range rd.Messages {
			n.env.Send(m)
		}
		for _, rs := range rd.ReadStates {
			w, ok := n.reads[rs.Ctx]
			if !ok {
				continue
			}
			delete(n.reads, rs.Ctx)
			if rs.Ok {
				n.ready = append(n.ready, confirmedRead{index: rs.Index, key: w.key, done: w.done})
			} else {
				w.done(kv.Result{}, ErrNotReady)
			}
		}
		for _, e := range rd.Committed {
			res := n.sm.Apply(e)
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
		n.maybeSnapshot()
	}
	if term := n.r.Term(); len(n.reads) > 0 {
		n.expireReads(func(w readWaiter) bool { return w.term != term })
	}
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

func (n *Node) maybeSnapshot() {
	applied := n.r.Applied()
	if n.cfg.SnapshotEvery == 0 || n.taking || applied < n.lastSnap+n.cfg.SnapshotEvery {
		return
	}
	term, ok := n.r.LogTerm(applied)
	if !ok {
		return
	}
	s := raft.Snapshot{Index: applied, Term: term, Data: n.sm.Snapshot(), Conf: n.r.ConfAt(applied)}
	n.env.SaveSnapshot(s)
	n.lastSnap = applied
	n.taking = true
	n.write(Write{HardState: n.r.HardState(), Snapshot: &s, Sync: true}, nil, &s)
}
