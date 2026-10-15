package node

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type recEnv struct {
	sent   []raft.Message
	writes []Write
}

func (e *recEnv) Send(m raft.Message) { e.sent = append(e.sent, m) }
func (e *recEnv) Write(w Write)       { e.writes = append(e.writes, w) }

type recApp struct{ applied []raft.Entry }

func (a *recApp) Apply(e raft.Entry) kv.Result {
	a.applied = append(a.applied, e)
	return kv.Result{Rev: e.Index, Ok: true}
}

func newTestNode(t *testing.T, id raft.NodeID, voters []raft.NodeID) (*Node, *recEnv, *recApp) {
	t.Helper()
	r, err := raft.New(raft.Config{ID: id, Voters: voters, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.NewPCG(1, uint64(id))}, raft.Restore{})
	if err != nil {
		t.Fatal(err)
	}
	env, app := &recEnv{}, &recApp{}
	return New(r, env, app), env, app
}

func TestAcksWaitForTheirWrite(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: []raft.Entry{{Term: 1, Index: 1}}})
	n.Process()
	if len(env.sent) != 0 {
		t.Fatalf("sent %v before the write finished", env.sent)
	}
	if len(env.writes) != 1 || len(env.writes[0].Entries) != 1 || !env.writes[0].Sync {
		t.Fatalf("writes %+v", env.writes)
	}
	n.WriteDone(env.writes[0].Seq)
	n.Process()
	if len(env.sent) != 1 || env.sent[0].Type != raft.MsgAppResp || env.sent[0].Index != 1 {
		t.Fatalf("after the write: %v", env.sent)
	}
}

func TestAnAckForEntriesAlreadyHandedWaitsBehindTheirWrite(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	app := raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: []raft.Entry{{Term: 1, Index: 1}}}
	n.Receive(app)
	n.Process()
	n.Receive(app)
	n.Process()
	if len(env.writes) != 2 || !env.writes[1].Empty() {
		t.Fatalf("writes %+v, want the entries and then an empty barrier", env.writes)
	}
	n.WriteDone(env.writes[1].Seq - 1)
	n.Process()
	if len(env.sent) != 1 {
		t.Fatalf("sent %d acks after the first write, want 1", len(env.sent))
	}
	n.WriteDone(env.writes[1].Seq)
	n.Process()
	if len(env.sent) != 2 {
		t.Fatalf("sent %d acks after the barrier, want 2", len(env.sent))
	}
}

func TestLeaderAppliesOnlyAfterItsOwnWrite(t *testing.T) {
	n, env, app := newTestNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	n.Process()
	for len(env.writes) > 0 {
		w := env.writes[0]
		env.writes = env.writes[1:]
		n.WriteDone(w.Seq)
		n.Process()
	}
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("state %s", n.Raft().State())
	}
	if _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	n.Process()
	before := len(app.applied)
	w := env.writes[len(env.writes)-1]
	if len(w.Entries) != 1 || len(app.applied) != before {
		t.Fatalf("applied before the write: %v", app.applied)
	}
	n.WriteDone(w.Seq)
	n.Process()
	if len(app.applied) != before+1 || string(app.applied[before].Data) != "x" {
		t.Fatalf("applied %v", app.applied)
	}
	last := env.writes[len(env.writes)-1]
	if n.PendingWrites() != 1 || last.Sync || len(last.Entries) != 0 || last.HardState.Commit != app.applied[before].Index {
		t.Fatalf("%d writes pending, last %+v; want only the commit index, written without a sync", n.PendingWrites(), last)
	}
}

func (n *Node) settle(env *recEnv) {
	n.Process()
	for len(env.writes) > 0 {
		w := env.writes[0]
		env.writes = env.writes[1:]
		n.WriteDone(w.Seq)
		n.Process()
	}
}

func TestSubmitOnAFollowerReturnsTheLeaderHint(t *testing.T) {
	n, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 3, To: 2, Term: 4})
	var got error
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, func(_ kv.Result, err error) { got = err })
	var nl *NotLeaderError
	if !errors.As(got, &nl) || nl.Leader != 3 {
		t.Fatalf("err %v, want not leader with hint 3", got)
	}
}

func TestSubmitAnswersWhenTheEntryApplies(t *testing.T) {
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	n.settle(env)
	var res kv.Result
	calls := 0
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte("v")}, func(r kv.Result, err error) {
		if err != nil {
			t.Fatal(err)
		}
		res, calls = r, calls+1
	})
	if calls != 0 || n.Waiting() != 1 {
		t.Fatal("answered before the entry committed")
	}
	n.settle(env)
	if calls != 1 || res.Rev != 2 || n.Waiting() != 0 {
		t.Fatalf("calls %d result %+v waiting %d", calls, res, n.Waiting())
	}
}

func TestSubmitReportsAnEntryReplacedByAnotherLeader(t *testing.T) {
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1, 2, 3})
	n.Raft().Campaign()
	n.settle(env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	n.settle(env)
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("setup: %s", n.Raft().State())
	}
	var got error
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, func(_ kv.Result, err error) { got = err })
	n.settle(env)
	n.Receive(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2, Commit: 2,
		Entries: []raft.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2}}})
	n.settle(env)
	if !errors.Is(got, ErrLost) {
		t.Fatalf("err %v, want ErrLost", got)
	}
}
