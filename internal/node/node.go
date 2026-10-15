package node

import (
	"errors"
	"fmt"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Write struct {
	Seq       uint64
	HardState raft.HardState
	Entries   []raft.Entry
	Sync      bool
}

func (w Write) Empty() bool {
	return w.HardState.IsEmpty() && len(w.Entries) == 0
}

type Env interface {
	Send(m raft.Message)
	Write(w Write)
}

type StateMachine interface {
	Apply(e raft.Entry) kv.Result
}

var ErrLost = errors.New("node: the entry was replaced before it committed")

type NotLeaderError struct {
	Leader raft.NodeID
}

func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("node: not the leader (leader hint %d)", e.Leader)
}

type Done func(kv.Result, error)

type pendingWrite struct {
	seq   uint64
	after []raft.Message
}

type waiter struct {
	term uint64
	done Done
}

type Node struct {
	r       *raft.Raft
	env     Env
	sm      StateMachine
	seq     uint64
	pending []pendingWrite
	waiters map[uint64]waiter
}

func New(r *raft.Raft, env Env, sm StateMachine) *Node {
	return &Node{r: r, env: env, sm: sm, waiters: map[uint64]waiter{}}
}

func (n *Node) Raft() *raft.Raft {
	return n.r
}

func (n *Node) Tick() {
	n.r.Tick()
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
	}
}

func (n *Node) PendingWrites() int {
	return len(n.pending)
}

func (n *Node) Process() {
	for n.r.HasReady() {
		rd := n.r.Ready()
		if rd.NeedsWrite() || len(rd.AfterPersist) > 0 {
			n.seq++
			n.pending = append(n.pending, pendingWrite{seq: n.seq, after: rd.AfterPersist})
			n.env.Write(Write{Seq: n.seq, HardState: rd.HardState, Entries: rd.Entries, Sync: rd.MustSync})
		}
		for _, m := range rd.Messages {
			n.env.Send(m)
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
	}
}
