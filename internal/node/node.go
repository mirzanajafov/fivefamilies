package node

import "github.com/mirzanajafov/fivefamilies/internal/raft"

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

type Applier interface {
	Apply(e raft.Entry)
}

type pendingWrite struct {
	seq   uint64
	after []raft.Message
}

type Node struct {
	r       *raft.Raft
	env     Env
	app     Applier
	seq     uint64
	pending []pendingWrite
}

func New(r *raft.Raft, env Env, app Applier) *Node {
	return &Node{r: r, env: env, app: app}
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
			n.app.Apply(e)
		}
	}
}
