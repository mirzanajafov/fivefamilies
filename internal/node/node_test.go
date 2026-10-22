package node

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type recEnv struct {
	sent      []raft.Message
	writes    []Write
	saved     []raft.Snapshot
	compacted []raft.Snapshot
	saveErr   error
}

func (e *recEnv) Send(m raft.Message) { e.sent = append(e.sent, m) }
func (e *recEnv) Write(w Write)       { e.writes = append(e.writes, w) }
func (e *recEnv) SaveSnapshot(s raft.Snapshot) error {
	if e.saveErr != nil {
		return e.saveErr
	}
	e.saved = append(e.saved, s)
	return nil
}
func (e *recEnv) Compacted(s raft.Snapshot) { e.compacted = append(e.compacted, s) }

type recApp struct {
	applied    []raft.Entry
	restored   []byte
	at         uint64
	restoreErr error
}

func (a *recApp) Apply(e raft.Entry) kv.Result {
	a.applied = append(a.applied, e)
	a.at = e.Index
	return kv.Result{Rev: e.Index, Ok: true}
}

func (a *recApp) Get(string) kv.Result {
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
	if a.restoreErr != nil {
		return a.restoreErr
	}
	a.restored, a.at = data, index
	return nil
}

func newTestNode(t *testing.T, id raft.NodeID, voters []raft.NodeID) (*Node, *recEnv, *recApp) {
	return newTestNodeConfig(t, id, voters, Config{})
}

func testRaftConfig(id raft.NodeID, voters []raft.NodeID) raft.Config {
	return raft.Config{ID: id, Voters: voters, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.NewPCG(1, uint64(id))}
}

func newTestNodeConfig(t *testing.T, id raft.NodeID, voters []raft.NodeID, cfg Config) (*Node, *recEnv, *recApp) {
	t.Helper()
	return newTestNodeRaft(t, testRaftConfig(id, voters), cfg)
}

func newTestNodeRaft(t *testing.T, rcfg raft.Config, cfg Config) (*Node, *recEnv, *recApp) {
	t.Helper()
	r, err := raft.New(rcfg, raft.Restore{})
	if err != nil {
		t.Fatal(err)
	}
	env, app := &recEnv{}, &recApp{}
	return New(r, env, app, cfg), env, app
}

func TestAcksWaitForTheirWrite(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: []raft.Entry{{Term: 1, Index: 1}}})
	process(t, n)
	if len(env.sent) != 0 {
		t.Fatalf("sent %v before the write finished", env.sent)
	}
	if len(env.writes) != 1 || len(env.writes[0].Entries) != 1 || !env.writes[0].Sync {
		t.Fatalf("writes %+v", env.writes)
	}
	writeDone(t, n, env.writes[0].Seq)
	process(t, n)
	if len(env.sent) != 1 || env.sent[0].Type != raft.MsgAppResp || env.sent[0].Index != 1 {
		t.Fatalf("after the write: %v", env.sent)
	}
}

func TestAnAckForEntriesAlreadyHandedWaitsBehindTheirWrite(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	app := raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: []raft.Entry{{Term: 1, Index: 1}}}
	n.Receive(app)
	process(t, n)
	n.Receive(app)
	process(t, n)
	if len(env.writes) != 2 || !env.writes[1].Empty() {
		t.Fatalf("writes %+v, want the entries and then an empty barrier", env.writes)
	}
	writeDone(t, n, env.writes[1].Seq-1)
	process(t, n)
	if len(env.sent) != 1 {
		t.Fatalf("sent %d acks after the first write, want 1", len(env.sent))
	}
	writeDone(t, n, env.writes[1].Seq)
	process(t, n)
	if len(env.sent) != 2 {
		t.Fatalf("sent %d acks after the barrier, want 2", len(env.sent))
	}
}

func TestLeaderAppliesOnlyAfterItsOwnWrite(t *testing.T) {
	n, env, app := newTestNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	process(t, n)
	for len(env.writes) > 0 {
		w := env.writes[0]
		env.writes = env.writes[1:]
		writeDone(t, n, w.Seq)
		process(t, n)
	}
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("state %s", n.Raft().State())
	}
	if _, err := n.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	process(t, n)
	before := len(app.applied)
	w := env.writes[len(env.writes)-1]
	if len(w.Entries) != 1 || len(app.applied) != before {
		t.Fatalf("applied before the write: %v", app.applied)
	}
	writeDone(t, n, w.Seq)
	process(t, n)
	if len(app.applied) != before+1 || string(app.applied[before].Data) != "x" {
		t.Fatalf("applied %v", app.applied)
	}
	last := env.writes[len(env.writes)-1]
	if n.PendingWrites() != 1 || last.Sync || len(last.Entries) != 0 || last.HardState.Commit != app.applied[before].Index {
		t.Fatalf("%d writes pending, last %+v; want only the commit index, written without a sync", n.PendingWrites(), last)
	}
}

func process(t *testing.T, n *Node) {
	t.Helper()
	if err := n.Process(); err != nil {
		t.Fatal(err)
	}
}

func writeDone(t *testing.T, n *Node, seq uint64) {
	t.Helper()
	if err := n.WriteDone(seq); err != nil {
		t.Fatal(err)
	}
}

func settle(t *testing.T, n *Node, env *recEnv) {
	t.Helper()
	process(t, n)
	for len(env.writes) > 0 {
		w := env.writes[0]
		env.writes = env.writes[1:]
		writeDone(t, n, w.Seq)
		process(t, n)
	}
}

func TestAdvanceCarriesWhatIsLeftOfATickIntoTheNextCall(t *testing.T) {
	n, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	tick := n.cfg.Tick
	n.Advance(tick * 3 / 2)
	n.Advance(tick * 2 / 5)
	if got := n.LeaderlessTicks(); got != 1 {
		t.Fatalf("%d ticks after 1.9 ticks of time", got)
	}
	n.Advance(tick / 10)
	if got := n.LeaderlessTicks(); got != 2 {
		t.Fatalf("%d ticks after 2 ticks of time", got)
	}
}

func TestAdvanceDropsTicksPastTheCatchUpCap(t *testing.T) {
	n, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	tick := n.cfg.Tick
	n.Advance((maxCatchUp+50)*tick + tick/2)
	n.Advance(tick / 2)
	if got := n.LeaderlessTicks(); got != maxCatchUp+1 {
		t.Fatalf("%d ticks, want the cap of %d and one more from the carried half tick", got, maxCatchUp)
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
	settle(t, n, env)
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
	settle(t, n, env)
	if calls != 1 || res.Rev != 2 || n.Waiting() != 0 {
		t.Fatalf("calls %d result %+v waiting %d", calls, res, n.Waiting())
	}
}

func TestWritingBeforeSendingHoldsBackOnlyTheAppends(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 1, []raft.NodeID{1, 2, 3}, Config{WriteBeforeSend: true})
	n.Raft().Campaign()
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	settle(t, n, env)
	for _, from := range []raft.NodeID{2, 3} {
		n.Receive(raft.Message{Type: raft.MsgAppResp, From: from, To: 1, Term: 1, Index: 1})
	}
	settle(t, n, env)
	env.sent = nil
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, nil)
	n.Advance(n.cfg.Tick)
	process(t, n)
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
	writeDone(t, n, env.writes[len(env.writes)-1].Seq)
	process(t, n)
	if apps, _ := kinds(); apps != 2 {
		t.Fatalf("after the leader's write: %d appends sent, want 2", apps)
	}
}

func TestSubmitReportsAnEntryReplacedByAnotherLeader(t *testing.T) {
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1, 2, 3})
	n.Raft().Campaign()
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	settle(t, n, env)
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("setup: %s", n.Raft().State())
	}
	var got error
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, func(_ kv.Result, err error) { got = err })
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2, Commit: 2,
		Entries: []raft.Entry{{Term: 2, Index: 1}, {Term: 2, Index: 2}}})
	settle(t, n, env)
	if !errors.Is(got, ErrLost) {
		t.Fatalf("err %v, want ErrLost", got)
	}
}

func TestSnapshotsAreTakenAndTheLogCompactedOnlyAfterTheMarkerIsDurable(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 1, []raft.NodeID{1}, Config{SnapshotEvery: 5, Retain: 2})
	n.Raft().Campaign()
	settle(t, n, env)
	for i := range 4 {
		n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}}, nil)
		settle(t, n, env)
	}
	if len(env.saved) != 1 || env.saved[0].Index != 5 || string(env.saved[0].Data) != "at 5" {
		t.Fatalf("saved %+v", env.saved)
	}
	if len(env.compacted) != 1 || n.Raft().FirstIndex() != 4 {
		t.Fatalf("compacted %+v, first index %d", env.compacted, n.Raft().FirstIndex())
	}
	n.Submit(kv.Command{Op: kv.OpPut, Key: "k"}, nil)
	process(t, n)
	for i := range 5 {
		n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}}, nil)
		process(t, n)
		for len(env.writes) > 0 && len(env.writes[len(env.writes)-1].Entries) > 0 {
			w := env.writes[0]
			env.writes = env.writes[1:]
			writeDone(t, n, w.Seq)
			process(t, n)
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
	settle(t, n, env)
	if len(env.compacted) != 2 {
		t.Fatalf("compacted %d times after the marker was durable", len(env.compacted))
	}
}

func TestAFailedSnapshotSaveStopsTheNodeBeforeItWritesTheMarker(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 1, []raft.NodeID{1}, Config{SnapshotEvery: 5})
	n.Raft().Campaign()
	settle(t, n, env)
	env.saveErr = errors.New("no space left on device")
	var err error
	for i := 0; i < 20 && err == nil; i++ {
		n.Submit(kv.Command{Op: kv.OpPut, Key: "k", Value: []byte{byte(i)}}, nil)
		if err = n.Process(); err == nil && len(env.writes) > 0 {
			err = n.WriteDone(env.writes[len(env.writes)-1].Seq)
		}
	}
	if !errors.Is(err, env.saveErr) {
		t.Fatalf("the node went on after its snapshot could not be saved: %v", err)
	}
	for _, w := range env.writes {
		if w.Snapshot != nil {
			t.Fatalf("queued a log record for snapshot %d, which was never saved", w.Snapshot.Index)
		}
	}
}

func TestCompactionReportsTheDurableSnapshotNotOneStillBeingInstalled(t *testing.T) {
	n, env, _ := newTestNodeConfig(t, 2, []raft.NodeID{1, 2, 3}, Config{SnapshotEvery: 5})
	var ents []raft.Entry
	for i := uint64(1); i <= 5; i++ {
		ents = append(ents, raft.Entry{Index: i, Term: 1})
	}
	n.Receive(raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 1, Entries: ents, Commit: 5})
	process(t, n)
	for len(env.writes) > 0 && env.writes[0].Snapshot == nil {
		w := env.writes[0]
		env.writes = env.writes[1:]
		writeDone(t, n, w.Seq)
		process(t, n)
	}
	if len(env.writes) == 0 {
		t.Fatal("the follower took no snapshot")
	}
	marker := env.writes[0]
	env.writes = env.writes[1:]
	n.Receive(raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &raft.Snapshot{Index: 30, Term: 2, Data: []byte("state")}})
	writeDone(t, n, marker.Seq)
	if len(env.compacted) != 1 || env.compacted[0].Index != marker.Snapshot.Index {
		t.Fatalf("compacted %+v after the marker for %d was durable", env.compacted, marker.Snapshot.Index)
	}
	settle(t, n, env)
	if last := env.compacted[len(env.compacted)-1]; last.Index != 30 {
		t.Fatalf("compacted to %d after the install was durable", last.Index)
	}
}

func TestAnInstalledSnapshotRestoresTheStateMachine(t *testing.T) {
	n, env, app := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	s := &raft.Snapshot{Index: 30, Term: 2, Data: []byte("state")}
	n.Receive(raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: s})
	process(t, n)
	if string(app.restored) != "state" || app.at != 30 {
		t.Fatalf("restored %q at %d", app.restored, app.at)
	}
	if len(env.saved) != 1 || len(env.writes) != 1 || !env.writes[0].Reset || env.writes[0].Snapshot.Index != 30 {
		t.Fatalf("saved %d, writes %+v", len(env.saved), env.writes)
	}
	if len(env.sent) != 0 {
		t.Fatal("acknowledged the snapshot before the write")
	}
	settle(t, n, env)
	if len(env.sent) != 1 || env.sent[0].Index != 30 {
		t.Fatalf("sent %v", env.sent)
	}
}

func TestASnapshotThatFailsToRestoreStopsTheNodeBeforeItWritesOrAcknowledgesTheInstall(t *testing.T) {
	n, env, app := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	app.restoreErr = errors.New("the snapshot data does not decode")
	n.Receive(raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &raft.Snapshot{Index: 30, Term: 2, Data: []byte("garbage")}})
	if err := n.Process(); !errors.Is(err, app.restoreErr) {
		t.Fatalf("the node went on after the install failed: %v", err)
	}
	if len(env.writes) != 0 || len(env.sent) != 0 {
		t.Fatalf("after a failed install: writes %+v, sent %v", env.writes, env.sent)
	}
}

func TestAReadSeesEverythingCommittedBeforeIt(t *testing.T) {
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	settle(t, n, env)
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
	process(t, n)
	settle(t, n, env)
	read()
	settle(t, n, env)
	if len(revs) != 2 || revs[0] != 1 || revs[1] != 2 {
		t.Fatalf("revisions seen %v: a read concurrent with the put may miss it, a read after it may not", revs)
	}
}

func TestAReadThatCannotBeConfirmedFails(t *testing.T) {
	n, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	var gotErr error
	n.Read("k", false, func(_ kv.Result, err error) { gotErr = err })
	process(t, n)
	if !errors.Is(gotErr, ErrNotReady) {
		t.Fatalf("a read on a node with no leader: %v", gotErr)
	}
}

func TestAForwardedReadExpires(t *testing.T) {
	n, env, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
	process(t, n)
	var gotErr error
	n.Read("k", false, func(_ kv.Result, err error) { gotErr = err })
	process(t, n)
	if gotErr != nil || len(env.sent) == 0 {
		t.Fatalf("the read should wait for the leader: %v", gotErr)
	}
	for range readExpiry + 32 {
		n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 1, To: 2, Term: 1})
		n.Advance(n.cfg.Tick)
		process(t, n)
	}
	if !errors.Is(gotErr, ErrNotReady) {
		t.Fatalf("a read the leader never answered: %v", gotErr)
	}
}

type transferOutcome struct {
	calls  int
	leader raft.NodeID
	term   uint64
	err    error
}

func (o *transferOutcome) done(leader raft.NodeID, term uint64, err error) {
	o.calls++
	o.leader, o.term, o.err = leader, term, err
}

func leaderOfThree(t *testing.T) (*Node, *recEnv) {
	t.Helper()
	n, env, _ := newTestNode(t, 1, []raft.NodeID{1, 2, 3})
	electNode1(t, n, env)
	return n, env
}

func electNode1(t *testing.T, n *Node, env *recEnv) {
	t.Helper()
	n.Raft().Campaign()
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 2, To: 1, Term: 1})
	settle(t, n, env)
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("state %s, want leader", n.Raft().State())
	}
}

func TestATransferEndsWithTheNodeThatTookOverWhoeverItIs(t *testing.T) {
	n, env := leaderOfThree(t)
	var o transferOutcome
	n.TransferLeadership(2, o.done)
	settle(t, n, env)
	if o.calls != 0 {
		t.Fatalf("the transfer ended before anyone took over: %+v", o)
	}
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 3, To: 1, Term: 3})
	settle(t, n, env)
	if o.calls != 1 || o.err != nil || o.leader != 3 || o.term != 3 {
		t.Fatalf("outcome %+v, want node 3 leading term 3", o)
	}
}

func TestATransferThatLeavesTheNodeLeadingAgainInALaterTermFails(t *testing.T) {
	n, env := leaderOfThree(t)
	var o transferOutcome
	n.TransferLeadership(2, o.done)
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVote, From: 2, To: 1, Term: 2, Index: 1, LogTerm: 1})
	settle(t, n, env)
	n.Raft().Campaign()
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 3, To: 1, Term: n.Raft().Term()})
	settle(t, n, env)
	if n.Raft().State() != raft.StateLeader || o.calls != 1 || !errors.Is(o.err, ErrTransferAborted) || o.leader != 1 {
		t.Fatalf("state %s, outcome %+v, want node 1 leading again and the transfer aborted", n.Raft().State(), o)
	}
}

func TestATransferTheCoreGivesUpOnFailsAndNamesTheTarget(t *testing.T) {
	n, env := leaderOfThree(t)
	var o transferOutcome
	n.TransferLeadership(2, o.done)
	settle(t, n, env)
	n.Advance(20 * n.cfg.Tick)
	settle(t, n, env)
	if o.calls != 1 || !errors.Is(o.err, ErrTransferAborted) || o.leader != 1 || o.term != 1 {
		t.Fatalf("outcome %+v, want node 1 still leading term 1 and the transfer aborted", o)
	}
	if want := "node 2 did not take over"; !strings.Contains(o.err.Error(), want) {
		t.Fatalf("error %q does not say %q", o.err, want)
	}
}

func TestATransferAnswersAtOnceWhenItCannotStartOrHasNothingToDo(t *testing.T) {
	n, _ := leaderOfThree(t)
	var pending, self transferOutcome
	n.TransferLeadership(2, pending.done)
	n.TransferLeadership(1, self.done)
	if self.calls != 1 || self.err != nil || self.leader != 1 {
		t.Fatalf("a transfer to the leader itself: %+v", self)
	}
	process(t, n)
	if pending.calls != 1 || !errors.Is(pending.err, ErrTransferAborted) {
		t.Fatalf("the transfer it cancelled: %+v", pending)
	}
	var refused transferOutcome
	n.TransferLeadership(9, refused.done)
	if refused.calls != 1 || !errors.Is(refused.err, raft.ErrBadConfChange) {
		t.Fatalf("a transfer to a node that isn't a voter: %+v", refused)
	}
	follower, _, _ := newTestNode(t, 2, []raft.NodeID{1, 2, 3})
	follower.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 3, To: 2, Term: 4})
	var asked transferOutcome
	follower.TransferLeadership(1, asked.done)
	var nl *NotLeaderError
	if asked.calls != 1 || !errors.As(asked.err, &nl) || nl.Leader != 3 {
		t.Fatalf("a transfer asked of a follower: %+v", asked)
	}
}

func TestATransferWaitsThroughALostQuorumForTheNextLeader(t *testing.T) {
	rcfg := testRaftConfig(1, []raft.NodeID{1, 2, 3})
	rcfg.CheckQuorum = true
	n, env, _ := newTestNodeRaft(t, rcfg, Config{})
	electNode1(t, n, env)
	var o transferOutcome
	n.TransferLeadership(2, o.done)
	n.Advance(20 * n.cfg.Tick)
	settle(t, n, env)
	if n.Raft().State() == raft.StateLeader || o.calls != 0 {
		t.Fatalf("state %s, outcome %+v, want the node stepped down and the transfer still waiting", n.Raft().State(), o)
	}
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 3, To: 1, Term: 2})
	settle(t, n, env)
	if o.calls != 1 || o.err != nil || o.leader != 3 {
		t.Fatalf("outcome %+v, want node 3 once it leads", o)
	}
}

func TestATransferThatAnotherTransferReplacedFails(t *testing.T) {
	n, env := leaderOfThree(t)
	var first, second transferOutcome
	n.TransferLeadership(2, first.done)
	n.TransferLeadership(3, second.done)
	settle(t, n, env)
	if first.calls != 1 || !errors.Is(first.err, ErrTransferAborted) || !strings.Contains(first.err.Error(), "node 2") || second.calls != 0 {
		t.Fatalf("first %+v, second %+v, want the first failed naming node 2 and the second still waiting", first, second)
	}
}
