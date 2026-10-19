package node

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type recEnv struct {
	sent      []raft.Message
	writes    []Write
	saved     []raft.Snapshot
	compacted []raft.Snapshot
}

func (e *recEnv) Send(m raft.Message)          { e.sent = append(e.sent, m) }
func (e *recEnv) Write(w Write)                { e.writes = append(e.writes, w) }
func (e *recEnv) SaveSnapshot(s raft.Snapshot) { e.saved = append(e.saved, s) }
func (e *recEnv) Compacted(s raft.Snapshot)    { e.compacted = append(e.compacted, s) }

type recApp struct {
	applied  []raft.Entry
	restored []byte
	at       uint64
}

func (a *recApp) Apply(e raft.Entry) kv.Result {
	a.applied = append(a.applied, e)
	a.at = e.Index
	return kv.Result{Rev: e.Index, Ok: true}
}

func (a *recApp) Get(key string) kv.Result {
	return kv.Result{Rev: a.at, Ok: true}
}

func (a *recApp) Snapshot() []byte {
	return []byte(fmt.Sprintf("at %d", a.at))
}

func (a *recApp) Lease(uint64) (kv.LeaseInfo, bool) {
	return kv.LeaseInfo{}, false
}

func (a *recApp) Leases() []kv.LeaseInfo {
	return nil
}

func (a *recApp) Restore(index uint64, data []byte) error {
	a.restored, a.at = data, index
	return nil
}

func newTestNode(t *testing.T, id raft.NodeID, voters []raft.NodeID) (*Node, *recEnv, *recApp) {
	return newTestNodeConfig(t, id, voters, Config{})
}

func newTestNodeConfig(t *testing.T, id raft.NodeID, voters []raft.NodeID, cfg Config) (*Node, *recEnv, *recApp) {
	t.Helper()
	r, err := raft.New(raft.Config{ID: id, Voters: voters, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.NewPCG(1, uint64(id))}, raft.Restore{})
	if err != nil {
		t.Fatal(err)
	}
	env, app := &recEnv{}, &recApp{}
	return New(r, env, app, cfg), env, app
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

func TestWritingBeforeSendingHoldsBackOnlyTheAppends(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 1, []raft.NodeID{1, 2, 3}, Config{WriteBeforeSend: true})
	n.Raft().Campaign()
	n.settle(env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	n.settle(env)
	for _, from := range []raft.NodeID{2, 3} {
		n.Receive(raft.Message{Type: raft.MsgAppResp, From: from, To: 1, Term: 1, Index: 1})
	}
	n.settle(env)
	env.sent = nil
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, nil)
	n.Tick()
	n.Process()
	kinds := func() (apps, beats int) {
		for _, m := range env.sent {
			switch m.Type {
			case raft.MsgApp:
				apps++
			case raft.MsgHeartbeat:
				beats++
			}
		}
		return apps, beats
	}
	if apps, beats := kinds(); apps != 0 || beats != 2 {
		t.Fatalf("before the leader's write: %d appends and %d heartbeats sent, want 0 and 2", apps, beats)
	}
	n.WriteDone(env.writes[len(env.writes)-1].Seq)
	n.Process()
	if apps, _ := kinds(); apps != 2 {
		t.Fatalf("after the leader's write: %d appends sent, want 2", apps)
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

func TestSnapshotsAreTakenAndTheLogCompactedOnlyAfterTheMarkerIsDurable(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 1, []raft.NodeID{1}, Config{SnapshotEvery: 5, Retain: 2})
	n.Raft().Campaign()
	n.settle(env)
	for i := range 4 {
		n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}}, nil)
		n.settle(env)
	}
	if len(env.saved) != 1 || env.saved[0].Index != 5 || string(env.saved[0].Data) != "at 5" {
		t.Fatalf("saved %+v", env.saved)
	}
	if len(env.compacted) != 1 || n.Raft().FirstIndex() != 4 {
		t.Fatalf("compacted %+v, first index %d", env.compacted, n.Raft().FirstIndex())
	}
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, nil)
	n.Process()
	for i := range 5 {
		n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}}, nil)
		n.Process()
		for len(env.writes) > 0 && len(env.writes[len(env.writes)-1].Entries) > 0 {
			w := env.writes[0]
			env.writes = env.writes[1:]
			n.WriteDone(w.Seq)
			n.Process()
		}
	}
	var marker *Write
	for i := range env.writes {
		if env.writes[i].Snapshot != nil {
			marker = &env.writes[i]
		}
	}
	if marker == nil {
		t.Fatal("no marker write for the second snapshot")
	}
	if marker.Reset || marker.HardState.IsEmpty() || !marker.Sync {
		t.Fatalf("marker %+v", *marker)
	}
	if len(env.compacted) != 1 {
		t.Fatal("compacted before the marker write was durable")
	}
	n.settle(env)
	if len(env.compacted) != 2 {
		t.Fatalf("compacted %d times after the marker was durable", len(env.compacted))
	}
}

func TestCompactionReportsTheDurableSnapshotNotOneStillBeingInstalled(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 2, []raft.NodeID{1, 2, 3}, Config{SnapshotEvery: 5})
	var ents []raft.Entry
	for i := uint64(1); i <= 5; i++ {
		ents = append(ents, raft.Entry{Index: i, Term: 1})
	}
	n.Receive(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: ents, Commit: 5})
	n.Process()
	for len(env.writes) > 0 && env.writes[0].Snapshot == nil {
		w := env.writes[0]
		env.writes = env.writes[1:]
		n.WriteDone(w.Seq)
		n.Process()
	}
	if len(env.writes) == 0 {
		t.Fatal("the follower took no snapshot")
	}
	marker := env.writes[0]
	env.writes = env.writes[1:]
	n.Receive(raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &raft.Snapshot{Index: 30, Term: 2, Data: []byte("state")}})
	n.WriteDone(marker.Seq)
	if len(env.compacted) != 1 || env.compacted[0].Index != marker.Snapshot.Index {
		t.Fatalf("compacted %+v after the marker for %d was durable", env.compacted, marker.Snapshot.Index)
	}
	n.settle(env)
	if last := env.compacted[len(env.compacted)-1]; last.Index != 30 {
		t.Fatalf("compacted to %d after the install was durable", last.Index)
	}
}

func TestAnInstalledSnapshotRestoresTheStateMachine(t *testing.T) {
	n, env, app := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	s := &raft.Snapshot{Index: 30, Term: 2, Data: []byte("state")}
	n.Receive(raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: s})
	n.Process()
	if string(app.restored) != "state" || app.at != 30 {
		t.Fatalf("restored %q at %d", app.restored, app.at)
	}
	if len(env.saved) != 1 || len(env.writes) != 1 || !env.writes[0].Reset || env.writes[0].Snapshot.Index != 30 {
		t.Fatalf("saved %d, writes %+v", len(env.saved), env.writes)
	}
	if len(env.sent) != 0 {
		t.Fatal("acknowledged the snapshot before the write")
	}
	n.settle(env)
	if len(env.sent) != 1 || env.sent[0].Index != 30 {
		t.Fatalf("sent %v", env.sent)
	}
}

func TestAReadSeesEverythingCommittedBeforeIt(t *testing.T) {
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	n.settle(env)
	var revs []uint64
	read := func() {
		n.Read("k", false, func(r kv.Result, err error) {
			if err != nil {
				t.Fatal(err)
			}
			revs = append(revs, r.Rev)
		})
	}
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, nil)
	read()
	n.Process()
	n.settle(env)
	read()
	n.settle(env)
	if len(revs) != 2 || revs[0] != 1 || revs[1] != 2 {
		t.Fatalf("revisions seen %v: a read concurrent with the put may miss it, a read after it may not", revs)
	}
}

func TestAReadThatCannotBeConfirmedFails(t *testing.T) {
	n, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	var gotErr error
	n.Read("k", false, func(_ kv.Result, err error) { gotErr = err })
	n.Process()
	if !errors.Is(gotErr, ErrNotReady) {
		t.Fatalf("a read on a node with no leader: %v", gotErr)
	}
}

func TestAForwardedReadExpires(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
	n.Process()
	var gotErr error
	n.Read("k", false, func(_ kv.Result, err error) { gotErr = err })
	n.Process()
	if gotErr != nil || len(env.sent) == 0 {
		t.Fatalf("the read should wait for the leader: %v", gotErr)
	}
	for range readExpiry + 32 {
		n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
		n.Tick()
		n.Process()
	}
	if !errors.Is(gotErr, ErrNotReady) {
		t.Fatalf("a read the leader never answered: %v", gotErr)
	}
}
