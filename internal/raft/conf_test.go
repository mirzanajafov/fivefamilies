package raft

import (
	"errors"
	"slices"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/planted"
)

func (c *cluster) add(tweak func(*Config)) NodeID {
	c.t.Helper()
	id := NodeID(len(c.nodes) + 1)
	cfg := testConfig(id, c.cfgs[0].Voters)
	if tweak != nil {
		tweak(&cfg)
	}
	r, err := New(cfg, Restore{})
	if err != nil {
		c.t.Fatal(err)
	}
	c.cfgs = append(c.cfgs, cfg)
	c.nodes = append(c.nodes, r)
	c.stores = append(c.stores, &stableStore{})
	c.applied = append(c.applied, nil)
	c.reads = append(c.reads, nil)
	c.down = append(c.down, false)
	return id
}

func (c *cluster) change(lead NodeID, typ ConfChangeType, node NodeID) uint64 {
	c.t.Helper()
	idx, err := c.node(lead).ProposeConfChange(ConfChange{Type: typ, Node: node})
	if err != nil {
		c.t.Fatalf("%s %d: %v", typ, node, err)
	}
	c.drain()
	return idx
}

func wantConf(t *testing.T, r *Raft, voters, learners []NodeID) {
	t.Helper()
	got := r.Conf()
	if !slices.Equal(got.Voters, voters) || !slices.Equal(got.Learners, learners) {
		t.Fatalf("node %d has voters %v learners %v, want %v and %v", r.ID(), got.Voters, got.Learners, voters, learners)
	}
}

func TestConfChangeRoundTrips(t *testing.T) {
	for _, cc := range []ConfChange{{ConfAddLearner, 4}, {ConfPromote, 300}, {ConfRemove, 1 << 40}} {
		got, err := UnmarshalConfChange(cc.Marshal())
		if err != nil || got != cc {
			t.Fatalf("%+v came back as %+v, %v", cc, got, err)
		}
	}
	for _, b := range [][]byte{nil, {1}, {9, 4}, {1, 0}, {1, 0x80}, {1, 4, 4}} {
		if _, err := UnmarshalConfChange(b); err == nil {
			t.Fatalf("%v decoded without an error", b)
		}
	}
}

func TestConfStateRefusesChangesThatMakeNoSense(t *testing.T) {
	c := ConfState{Voters: []NodeID{1, 2}, Learners: []NodeID{3}}
	for _, cc := range []ConfChange{{ConfAddLearner, 2}, {ConfAddLearner, 3}, {ConfPromote, 1}, {ConfPromote, 4}, {ConfRemove, 4}} {
		if _, err := c.Apply(cc); err == nil {
			t.Fatalf("%s %d was accepted", cc.Type, cc.Node)
		}
	}
	if _, err := (ConfState{Voters: []NodeID{1}}).Apply(ConfChange{ConfRemove, 1}); err == nil {
		t.Fatal("removed the last voter")
	}
	next, err := c.Apply(ConfChange{ConfPromote, 3})
	if err != nil || !slices.Equal(next.Voters, []NodeID{1, 2, 3}) || len(next.Learners) != 0 {
		t.Fatalf("promote gave %+v, %v", next, err)
	}
	if !slices.Equal(c.Learners, []NodeID{3}) {
		t.Fatal("Apply changed the receiver")
	}
}

func TestALearnerReplicatesButNeitherVotesNorCounts(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	l := c.add(nil)
	c.change(1, ConfAddLearner, l)
	c.propose(1, "x")
	for _, id := range []NodeID{1, 2, 3, l} {
		wantConf(t, c.node(id), []NodeID{1, 2, 3}, []NodeID{4})
	}
	if got := appliedData(c.applied[l-1]); !slices.Equal(got, []string{"x"}) {
		t.Fatalf("learner applied %v", got)
	}
	c.isolate(l)
	c.tick(l, 50)
	if r := c.node(l); r.State() != StateFollower || r.Term() != c.node(1).Term() {
		t.Fatalf("an isolated learner is %s at term %d", r.State(), r.Term())
	}
	c.heal()
	c.crash(2)
	c.crash(3)
	idx := c.propose(1, "y")
	if c.node(1).Committed() >= idx {
		t.Fatal("the leader and a learner committed an entry without a quorum of voters")
	}
}

func TestPromotingALearnerChangesTheQuorum(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	l := c.add(nil)
	c.change(1, ConfAddLearner, l)
	c.change(1, ConfPromote, l)
	wantConf(t, c.node(l), []NodeID{1, 2, 3, 4}, nil)
	c.crash(3)
	if idx := c.propose(1, "x"); c.node(1).Committed() < idx {
		t.Fatal("three of four voters did not commit")
	}
	c.crash(2)
	if idx := c.propose(1, "y"); c.node(1).Committed() >= idx {
		t.Fatal("two of four voters committed")
	}
}

func TestPromotionWaitsForTheLearnerToCatchUp(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.MaxLearnerLag = 2 })
	c.elect(1)
	l := c.add(func(cfg *Config) { cfg.MaxLearnerLag = 2 })
	c.change(1, ConfAddLearner, l)
	c.crash(l)
	for range 5 {
		c.propose(1, "x")
	}
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfPromote, l}); !errors.Is(err, ErrLearnerBehind) {
		t.Fatalf("promoting a learner five entries behind: %v", err)
	}
	c.restart(l)
	c.tickAll(3)
	c.change(1, ConfPromote, l)
}

func TestOnlyOneChangeIsInFlight(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.add(nil)
	c.add(nil)
	c.isolate(1)
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfAddLearner, 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfAddLearner, 5}); !errors.Is(err, ErrConfPending) {
		t.Fatalf("second change while the first is uncommitted: %v", err)
	}
	c.heal()
	c.drain()
	c.tickAll(1)
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfAddLearner, 5}); err != nil {
		t.Fatalf("second change after the first committed: %v", err)
	}
}

func TestANewLeaderCommitsInItsOwnTermBeforeChanging(t *testing.T) {
	for _, buggy := range []bool{false, true} {
		r := newNode(t, 1, []NodeID{1, 2, 3}, Restore{}, func(cfg *Config) {
			cfg.PreVote = false
			if buggy {
				cfg.Bugs = planted.EarlyConf
			}
		})
		r.Campaign()
		for _, m := range r.Ready().AfterPersist {
			r.Step(m)
		}
		r.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1})
		_, err := r.ProposeConfChange(ConfChange{ConfAddLearner, 4})
		if buggy != (err == nil) {
			t.Fatalf("planted %v: change before the first commit gave %v", buggy, err)
		}
		if !buggy && !errors.Is(err, ErrConfNotReady) {
			t.Fatalf("error %v", err)
		}
	}
}

func TestRemovingAVoterCanCommitWhatWasStuck(t *testing.T) {
	c := newCluster(t, 4, nil)
	c.elect(1)
	c.crash(3)
	c.crash(4)
	idx := c.propose(1, "x")
	if c.node(1).Committed() >= idx {
		t.Fatal("two of four committed")
	}
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfRemove, 4}); !errors.Is(err, ErrConfPending) && err != nil {
		t.Fatal(err)
	}
	c.drain()
	wantConf(t, c.node(1), []NodeID{1, 2, 3}, nil)
	if c.node(1).Committed() < idx+1 {
		t.Fatalf("commit %d, but two of three voters hold the entry and the removal", c.node(1).Committed())
	}
}

func TestTheLeaderCannotRemoveItself(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfRemove, 1}); err == nil {
		t.Fatal("the leader removed itself")
	}
}

func TestARemovedNodeCannotDisruptTheCluster(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.PreVote, cfg.CheckQuorum = false, false })
	c.elect(1)
	c.crash(3)
	c.change(1, ConfRemove, 3)
	c.tickAll(2)
	term := c.node(1).Term()
	c.restart(3)
	wantConf(t, c.node(3), []NodeID{1, 2, 3}, nil)
	for range 100 {
		c.tick(3, 1)
		c.tick(1, 1)
		c.tick(2, 1)
	}
	if c.node(1).State() != StateLeader || c.node(1).Term() != term {
		t.Fatalf("node 1 is %s at term %d, was leader at %d, but a removed node's votes must be ignored", c.node(1).State(), c.node(1).Term(), term)
	}
	if c.node(3).Term() <= term {
		t.Fatal("the removed node never campaigned, so the test proves nothing")
	}
}

func TestAnOverwrittenChangeRollsBack(t *testing.T) {
	c := newCluster(t, 5, nil)
	c.elect(1)
	c.add(nil)
	for _, id := range []NodeID{3, 4, 5, 6} {
		c.cut[[2]NodeID{1, id}] = true
	}
	if _, err := c.node(1).ProposeConfChange(ConfChange{ConfAddLearner, 6}); err != nil {
		t.Fatal(err)
	}
	c.drain()
	wantConf(t, c.node(2), []NodeID{1, 2, 3, 4, 5}, []NodeID{6})
	c.heal()
	c.isolate(1)
	c.isolate(2)
	c.tick(4, 20)
	c.tick(5, 20)
	c.tick(3, 30)
	if c.node(3).State() != StateLeader {
		t.Fatalf("node 3 is %s", c.node(3).State())
	}
	c.heal()
	c.isolate(1)
	c.propose(3, "y")
	c.tick(3, 2)
	wantConf(t, c.node(2), []NodeID{1, 2, 3, 4, 5}, nil)
}

func TestTheConfigurationSurvivesRestartAndCompaction(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	l := c.add(nil)
	c.change(1, ConfAddLearner, l)
	c.propose(1, "x")
	c.restart(2)
	wantConf(t, c.node(2), []NodeID{1, 2, 3}, []NodeID{4})
	c.drain()
	r := c.node(1)
	snap := Snapshot{Index: r.Applied(), Term: r.Term()}
	if err := r.Compact(snap, 0); err != nil {
		t.Fatal(err)
	}
	s := r.LatestSnapshot()
	if !slices.Equal(s.Conf.Voters, []NodeID{1, 2, 3}) || !slices.Equal(s.Conf.Learners, []NodeID{4}) {
		t.Fatalf("snapshot conf %+v", s.Conf)
	}
	c.stores[0].compact(*s)
	c.restart(1)
	wantConf(t, c.node(1), []NodeID{1, 2, 3}, []NodeID{4})
}

func TestASnapshotCarriesTheConfigurationToANewNode(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	l := c.add(nil)
	c.change(1, ConfAddLearner, l)
	c.change(1, ConfPromote, l)
	c.propose(1, "x")
	r := c.node(1)
	snap := Snapshot{Index: r.Applied(), Term: r.Term()}
	if err := r.Compact(snap, 0); err != nil {
		t.Fatal(err)
	}
	n := c.add(nil)
	c.change(1, ConfAddLearner, n)
	c.tickAll(3)
	if c.node(n).FirstIndex() <= snap.Index {
		t.Fatal("the new node did not install the snapshot")
	}
	wantConf(t, c.node(n), []NodeID{1, 2, 3, 4}, []NodeID{5})
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.propose(1, "x")
	if err := c.node(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	if _, err := c.node(1).Propose([]byte("y")); !errors.Is(err, ErrTransferring) {
		t.Fatalf("proposal during a transfer: %v", err)
	}
	c.drain()
	if c.node(3).State() != StateLeader {
		t.Fatalf("node 3 is %s right after the transfer, without any ticks", c.node(3).State())
	}
	if ls := c.leaders(); len(ls) != 1 {
		t.Fatalf("leaders %v", ls)
	}
}

func TestATransferWaitsForTheTargetToCatchUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.crash(3)
	for range 5 {
		c.propose(1, "x")
	}
	c.restart(3)
	if err := c.node(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	c.drain()
	c.tickAll(2)
	if c.node(3).State() != StateLeader {
		t.Fatalf("node 3 is %s", c.node(3).State())
	}
	if got := appliedData(c.applied[2]); len(got) != 5 {
		t.Fatalf("node 3 applied %v", got)
	}
}

func TestATransferToADeadNodeGivesUp(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.crash(3)
	if err := c.node(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	c.tickAll(c.cfgs[0].ElectionTick)
	if c.node(1).State() != StateLeader || c.node(1).Transferring() != None {
		t.Fatalf("node 1 is %s, transferring to %d", c.node(1).State(), c.node(1).Transferring())
	}
	c.propose(1, "x")
}

func TestATransferToTheLeaderItselfCancelsTheOneInFlight(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.crash(3)
	if err := c.node(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	if err := c.node(1).TransferLeadership(1); err != nil || c.node(1).Transferring() != None {
		t.Fatalf("transferring to %d after a transfer to itself: %v", c.node(1).Transferring(), err)
	}
	c.propose(1, "x")
}

func TestNoLeaseReadsAfterATransferAttempt(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LeaseTicks = 6 })
	c.pastBootGuard()
	c.elect(1)
	c.tickAll(2)
	c.crash(3)
	if err := c.node(1).TransferLeadership(3); err != nil {
		t.Fatal(err)
	}
	c.tickAll(c.cfgs[0].ElectionTick)
	r := c.node(1)
	r.ReadIndex(1, true)
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatal("a lease read was served in a term where a transfer was attempted, and a late TimeoutNow could still elect another leader")
	}
}

func TestARoundSentBeforeAChangeDoesNotRenewTheLease(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LeaseTicks = 6 })
	c.pastBootGuard()
	c.elect(1)
	c.tickAll(2)
	c.add(nil)
	r := c.node(1)
	r.Tick()
	held := r.Ready().Messages
	if count(held, MsgHeartbeat) != 2 {
		t.Fatalf("%d heartbeats held back", count(held, MsgHeartbeat))
	}
	if _, err := r.ProposeConfChange(ConfChange{ConfAddLearner, 4}); err != nil {
		t.Fatal(err)
	}
	c.drain()
	c.queue = append(c.queue, held...)
	c.drain()
	r.ReadIndex(1, true)
	if rd := r.Ready(); len(rd.ReadStates) != 0 {
		t.Fatal("acks for a round sent under the old configuration renewed the lease")
	}
}

func TestTheLeaderRefusesAReadForwardedByANonMember(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.change(1, ConfRemove, 3)
	r := c.node(1)
	r.Step(Message{Type: MsgReadIndex, From: 3, To: 1, Term: r.Term(), Context: 5})
	for _, m := range r.Ready().Messages {
		if m.Type == MsgReadIndexResp && m.To == 3 && m.Reject && m.Context == 5 {
			return
		}
	}
	t.Fatal("the read was dropped, so a removed node that does not know it would wait for it until it expires")
}

func TestAReadForwardedToAnOldLeaderIsRefusedWithTheNewTerm(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.isolate(3)
	if err := c.node(1).TransferLeadership(2); err != nil {
		t.Fatal(err)
	}
	c.drain()
	if c.node(2).State() != StateLeader {
		t.Fatalf("node 2 is %s", c.node(2).State())
	}
	c.heal()
	c.cut[[2]NodeID{2, 3}] = true
	r3 := c.node(3)
	r3.ReadIndex(9, false)
	c.drain()
	rs := lastRead(t, c, 3)
	if rs.Ok || rs.Ctx != 9 {
		t.Fatalf("read state %+v", rs)
	}
	if r3.Term() != c.node(2).Term() {
		t.Fatalf("node 3 is still at term %d, the cluster is at %d", r3.Term(), c.node(2).Term())
	}
}

func TestAnEarlyChangeLetsTwoMajoritiesCommitDifferentEntries(t *testing.T) {
	for _, buggy := range []bool{false, true} {
		tweak := func(cfg *Config) {
			cfg.PreVote, cfg.CheckQuorum = false, false
			if buggy {
				cfg.Bugs = planted.EarlyConf
			}
		}
		c := newCluster(t, 4, tweak)
		c.add(tweak)
		c.elect(1)
		c.change(1, ConfAddLearner, 5)
		c.propose(1, "x")
		c.tickAll(2)
		for _, id := range []NodeID{2, 3, 4} {
			for _, x := range []NodeID{1, 5} {
				c.cut[[2]NodeID{id, x}], c.cut[[2]NodeID{x, id}] = true, true
			}
		}
		k := c.node(1).LastIndex()
		c.change(1, ConfPromote, 5)
		wantConf(t, c.node(5), []NodeID{1, 2, 3, 4, 5}, nil)
		c.drop = func(m Message) bool { return m.From == 2 && (m.Type == MsgApp || m.Type == MsgHeartbeat) }
		c.elect(2)
		_, err := c.node(2).ProposeConfChange(ConfChange{ConfRemove, 1})
		if !buggy {
			if !errors.Is(err, ErrConfNotReady) {
				t.Fatalf("a leader whose empty entry has not committed proposed a change: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		c.drop = func(m Message) bool { return m.From == 2 && m.To != 3 && (m.Type == MsgApp || m.Type == MsgHeartbeat) }
		c.tick(2, 2)
		if c.node(2).Committed() < k+2 {
			t.Fatalf("nodes 2 and 3 did not commit the removal under the new configuration: commit %d", c.node(2).Committed())
		}
		delete(c.cut, [2]NodeID{1, 4})
		delete(c.cut, [2]NodeID{4, 1})
		c.crash(1)
		c.restart(1)
		for range 2 {
			c.node(1).Campaign()
			c.drain()
		}
		if c.node(1).State() != StateLeader {
			t.Fatalf("node 1 is %s, but with nodes 4 and 5 it holds a majority of the configuration in its own log", c.node(1).State())
		}
		mine, _ := c.node(1).EntryAt(k + 1)
		theirs, _ := c.node(2).EntryAt(k + 1)
		if c.node(1).Committed() < k+1 || mine.Term == theirs.Term {
			t.Fatalf("expected two committed entries at %d: node 1 has t%d (commit %d), node 2 has t%d (commit %d)", k+1, mine.Term, c.node(1).Committed(), theirs.Term, c.node(2).Committed())
		}
	}
}

func TestSelfRemovalIsRefusedEvenBeforeTheFirstCommit(t *testing.T) {
	r := newNode(t, 1, []NodeID{1, 2, 3}, Restore{}, func(cfg *Config) { cfg.PreVote = false })
	r.Campaign()
	for _, m := range r.Ready().AfterPersist {
		r.Step(m)
	}
	r.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1})
	if _, err := r.ProposeConfChange(ConfChange{ConfRemove, 1}); !errors.Is(err, ErrBadConfChange) {
		t.Fatalf("a request that can never succeed should say so, not ask to try again: %v", err)
	}
}
