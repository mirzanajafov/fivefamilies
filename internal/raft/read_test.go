package raft

import (
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/planted"
)

func (c *cluster) pastBootGuard() {
	for _, r := range c.nodes {
		r.ticks = max(r.ticks, uint64(r.cfg.ElectionTick))
	}
}

func lastRead(t *testing.T, c *cluster, id NodeID) ReadState {
	t.Helper()
	rs := c.reads[id-1]
	if len(rs) == 0 {
		t.Fatalf("node %d has no read state", id)
	}
	return rs[len(rs)-1]
}

func TestReadIndexWaitsForAQuorumToConfirmLeadership(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.propose(1, "x")
	r := c.node(1)
	r.ReadIndex(7, false)
	rd := r.Ready()
	if len(rd.ReadStates) != 0 {
		t.Fatal("answered a read before confirming leadership")
	}
	if n := count(rd.Messages, MsgHeartbeat); n != 2 {
		t.Fatalf("%d heartbeats, want one round to both followers", n)
	}
	c.queue = append(c.queue, rd.Messages...)
	c.drain()
	rs := lastRead(t, c, 1)
	if !rs.Ok || rs.Ctx != 7 || rs.Index != r.Committed() {
		t.Fatalf("read state %+v, committed %d", rs, r.Committed())
	}
}

func TestOneHeartbeatRoundConfirmsEveryWaitingRead(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	r := c.node(1)
	for ctx := range uint64(5) {
		r.ReadIndex(ctx+1, false)
	}
	rd := r.Ready()
	if n := count(rd.Messages, MsgHeartbeat); n != 2 {
		t.Fatalf("%d heartbeats for five reads, want one round", n)
	}
	c.queue = append(c.queue, rd.Messages...)
	c.drain()
	if len(c.reads[0]) != 5 {
		t.Fatalf("%d reads answered", len(c.reads[0]))
	}
	for i, rs := range c.reads[0] {
		if !rs.Ok || rs.Ctx != uint64(i+1) {
			t.Fatalf("read %d: %+v", i, rs)
		}
	}
}

func TestAReadBeforeTheLeadersFirstCommitIsRefused(t *testing.T) {
	r := newNode(t, 1, []NodeID{1, 2, 3}, Restore{}, func(c *Config) { c.PreVote = false })
	r.Campaign()
	for _, m := range r.Ready().AfterPersist {
		r.Step(m)
	}
	r.Step(Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1})
	if r.State() != StateLeader {
		t.Fatalf("state %s", r.State())
	}
	r.ReadIndex(1, false)
	rd := r.Ready()
	if len(rd.ReadStates) != 1 || rd.ReadStates[0].Ok {
		t.Fatalf("read states %+v; the empty entry of the term is not committed yet", rd.ReadStates)
	}
}

func TestAFollowerReadGoesThroughTheLeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.propose(1, "x")
	c.node(2).ReadIndex(9, false)
	c.drain()
	rs := lastRead(t, c, 2)
	if !rs.Ok || rs.Ctx != 9 || rs.Index != c.node(1).Committed() {
		t.Fatalf("follower read state %+v", rs)
	}
	if len(c.reads[0]) != 0 {
		t.Fatal("the leader answered its own read state for a follower's request")
	}
}

func TestAMinorityLeaderCannotConfirmARead(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.propose(1, "x")
	c.isolate(1)
	c.node(1).ReadIndex(3, false)
	c.drain()
	c.tick(1, 5)
	if len(c.reads[0]) != 0 {
		t.Fatalf("an isolated leader answered a read: %+v", c.reads[0])
	}
	c.tick(1, 20)
	if rs := lastRead(t, c, 1); rs.Ok {
		t.Fatal("a pending read was answered after stepping down")
	}
}

func TestLeaseReadsSkipTheRoundTripOnlyInsideTheLease(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.LeaseTicks = 6 })
	c.pastBootGuard()
	c.elect(1)
	c.propose(1, "x")
	c.tickAll(2)
	r := c.node(1)
	r.ReadIndex(1, true)
	rd := r.Ready()
	if len(rd.ReadStates) != 1 || !rd.ReadStates[0].Ok || count(rd.Messages, MsgHeartbeat) != 0 {
		t.Fatalf("inside the lease: %d read states, %d heartbeats", len(rd.ReadStates), count(rd.Messages, MsgHeartbeat))
	}
	c.isolate(1)
	c.tick(1, 8)
	r.ReadIndex(2, true)
	rd = r.Ready()
	if len(rd.ReadStates) != 0 {
		t.Fatalf("answered a lease read %d ticks after the last confirmed round", 8)
	}
}

func TestLeaseNeedsCheckQuorum(t *testing.T) {
	cfg := testConfig(1, five())
	cfg.CheckQuorum = false
	cfg.LeaseTicks = 5
	if _, err := New(cfg, Restore{}); err == nil {
		t.Fatal("lease reads without CheckQuorum were accepted")
	}
	cfg.CheckQuorum = true
	cfg.LeaseTicks = cfg.ElectionTick
	if _, err := New(cfg, Restore{}); err == nil {
		t.Fatal("a lease as long as the election timeout was accepted")
	}
}

func TestPlantedFollowerReadAnswersLocally(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.Bugs = planted.FollowerRead })
	c.elect(1)
	c.node(2).ReadIndex(4, false)
	rd := c.node(2).Ready()
	if len(rd.ReadStates) != 1 || !rd.ReadStates[0].Ok || count(rd.Messages, MsgReadIndex) != 0 {
		t.Fatalf("with the bug the follower should answer itself: %+v", rd)
	}
}

func TestARestartedNodeDoesNotHelpElectALeaderInsideAnotherLease(t *testing.T) {
	c := newCluster(t, 5, func(cfg *Config) { cfg.LeaseTicks = 6 })
	c.pastBootGuard()
	c.elect(1)
	c.propose(1, "x")
	for _, other := range []NodeID{4, 5} {
		c.cut[[2]NodeID{1, other}] = true
		c.cut[[2]NodeID{other, 1}] = true
	}
	c.tickAll(10)
	if c.node(1).State() != StateLeader {
		t.Fatal("setup: node 1 lost leadership with 2 and 3 still acknowledging it")
	}
	c.cut[[2]NodeID{1, 2}] = true
	c.cut[[2]NodeID{2, 1}] = true
	c.crash(2)
	c.restart(2)
	c.node(4).Campaign()
	c.drain()
	if c.node(4).State() == StateLeader {
		c.propose(4, "y")
		r1 := c.node(1)
		r1.ReadIndex(99, true)
		rd := r1.Ready()
		if len(rd.ReadStates) == 1 && rd.ReadStates[0].Ok && rd.ReadStates[0].Index < c.node(4).Committed() {
			t.Fatalf("node 4 leads term %d and committed %d while node 1 still answers lease reads at %d",
				c.node(4).Term(), c.node(4).Committed(), rd.ReadStates[0].Index)
		}
	}
}

func TestAFreshNodeWithLeasesWaitsBeforeVoting(t *testing.T) {
	r := newNode(t, 1, five(), Restore{HardState: HardState{Term: 1}}, func(c *Config) { c.LeaseTicks = 6 })
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 2})
	if r.Term() != 1 {
		t.Fatal("voted right after starting, inside the lease a leader may still hold")
	}
	for range 10 {
		r.Tick()
	}
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 2})
	if r.Term() != 2 || r.HardState().Vote != 2 {
		t.Fatalf("still refusing an election timeout after starting: term %d vote %d", r.Term(), r.HardState().Vote)
	}
}
