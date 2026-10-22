package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

func newNode(t *testing.T, id NodeID, voters []NodeID, st Restore, tweak func(*Config)) *Raft {
	t.Helper()
	cfg := testConfig(id, voters)
	if tweak != nil {
		tweak(&cfg)
	}
	r, err := New(cfg, st)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func ents(terms ...uint64) []Entry {
	out := make([]Entry, len(terms))
	for i, t := range terms {
		out[i] = Entry{Term: t, Index: uint64(i + 1)}
	}
	return out
}

func five() []NodeID { return []NodeID{1, 2, 3, 4, 5} }

func find(ms []Message, typ MessageType, to NodeID) (Message, bool) {
	for _, m := range ms {
		if m.Type == typ && m.To == to {
			return m, true
		}
	}
	return Message{}, false
}

func count(ms []Message, typ MessageType) int {
	n := 0
	for _, m := range ms {
		if m.Type == typ {
			n++
		}
	}
	return n
}

func TestSingleNodeElectsItselfAndCommits(t *testing.T) {
	c := newCluster(t, 1, nil)
	c.elect(1)
	if got := c.node(1).Term(); got != 1 {
		t.Fatalf("term %d, want 1", got)
	}
	c.propose(1, "a")
	c.propose(1, "b")
	if got := c.node(1).Committed(); got != 3 {
		t.Fatalf("committed %d, want 3", got)
	}
	if got := appliedData(c.applied[0]); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("applied %v", got)
	}
}

func TestElectionAndReplication(t *testing.T) {
	c := newCluster(t, 5, nil)
	c.elect(1)
	for i := NodeID(2); i <= 5; i++ {
		r := c.node(i)
		if r.State() != StateFollower || r.Leader() != 1 || r.Term() != 1 {
			t.Fatalf("node %d: %s, leader %d, term %d", i, r.State(), r.Leader(), r.Term())
		}
	}
	for _, d := range []string{"x", "y", "z"} {
		c.propose(1, d)
	}
	for i, r := range c.nodes {
		if r.Committed() != 4 {
			t.Fatalf("node %d committed %d, want 4", i+1, r.Committed())
		}
		if got := appliedData(c.applied[i]); !slices.Equal(got, []string{"x", "y", "z"}) {
			t.Fatalf("node %d applied %v", i+1, got)
		}
	}
}

func TestElectionsHappenOnTheirOwn(t *testing.T) {
	c := newCluster(t, 5, nil)
	c.tickAll(40)
	if l := c.leaders(); len(l) != 1 {
		t.Fatalf("leaders %v, want exactly one", l)
	}
}

func TestVoteRequiresUpToDateLog(t *testing.T) {
	cases := []struct {
		lastIndex, lastTerm uint64
		grant               bool
	}{
		{5, 1, false},
		{1, 2, false},
		{2, 2, true},
		{9, 2, true},
		{1, 3, true},
	}
	for _, tc := range cases {
		r := newNode(t, 1, five(), Restore{HardState: HardState{Term: 2}, Entries: ents(1, 2)}, nil)
		r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, Index: tc.lastIndex, LogTerm: tc.lastTerm})
		rd := r.Ready()
		_, granted := find(rd.AfterPersist, MsgVoteResp, 2)
		rej, rejected := find(rd.Messages, MsgVoteResp, 2)
		if granted != tc.grant || rejected == tc.grant {
			t.Fatalf("candidate log (%d, t%d): granted %v, want %v", tc.lastIndex, tc.lastTerm, granted, tc.grant)
		}
		if rejected && !rej.Reject {
			t.Fatalf("rejection without the reject flag: %v", rej)
		}
		if r.Term() != 3 {
			t.Fatalf("term %d after a vote request for term 3", r.Term())
		}
	}
}

func TestOneVotePerTerm(t *testing.T) {
	r := newNode(t, 1, five(), Restore{}, nil)
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3})
	r.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 3})
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3})
	rd := r.Ready()
	if n := count(rd.AfterPersist, MsgVoteResp); n != 2 {
		t.Fatalf("%d grants, want 2 (both to node 2)", n)
	}
	if _, ok := find(rd.AfterPersist, MsgVoteResp, 3); ok {
		t.Fatal("voted for two candidates in one term")
	}
	if m, ok := find(rd.Messages, MsgVoteResp, 3); !ok || !m.Reject {
		t.Fatal("node 3 was not refused")
	}
	if rd.HardState.Vote != 2 || !rd.MustSync {
		t.Fatalf("hard state %+v, must sync %v", rd.HardState, rd.MustSync)
	}
}

func TestVoteIsSentOnlyAfterItIsDurable(t *testing.T) {
	r := newNode(t, 1, five(), Restore{}, nil)
	r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	rd := r.Ready()
	if _, ok := find(rd.Messages, MsgVoteResp, 2); ok {
		t.Fatal("vote sent before the vote was persisted")
	}
	if _, ok := find(rd.AfterPersist, MsgVoteResp, 2); !ok {
		t.Fatal("no vote after persistence")
	}
	if !rd.NeedsWrite() || !rd.MustSync || rd.HardState.Vote != 2 {
		t.Fatalf("vote not handed to storage: %+v", rd)
	}
}

func TestCandidateLeadsOnlyAfterItsOwnVoteIsDurable(t *testing.T) {
	r := newNode(t, 1, five(), Restore{}, func(c *Config) { c.PreVote = false })
	r.Campaign()
	rd := r.Ready()
	if r.State() != StateCandidate {
		t.Fatalf("state %s", r.State())
	}
	for _, id := range []NodeID{2, 3, 4} {
		r.Step(Message{Type: MsgVoteResp, From: id, To: 1, Term: 1})
	}
	if r.State() != StateCandidate {
		t.Fatal("became leader on three other votes before its own term and vote were durable")
	}
	self, ok := find(rd.AfterPersist, MsgVoteResp, 1)
	if !ok {
		t.Fatal("no self vote after persistence")
	}
	r.Step(self)
	if r.State() != StateLeader {
		t.Fatalf("state %s after its own vote became durable", r.State())
	}
}

func TestAppendAckIsSentOnlyAfterItIsDurable(t *testing.T) {
	r := newNode(t, 2, five(), Restore{}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1, Index: 1, Data: []byte("a")}}})
	rd := r.Ready()
	if _, ok := find(rd.Messages, MsgAppResp, 1); ok {
		t.Fatal("append acknowledged before it was persisted")
	}
	ack, ok := find(rd.AfterPersist, MsgAppResp, 1)
	if !ok || ack.Index != 1 {
		t.Fatalf("ack after persistence: %v %v", ack, ok)
	}
	if len(rd.Entries) != 1 || !rd.MustSync {
		t.Fatalf("entries %v must sync %v", rd.Entries, rd.MustSync)
	}
}

func TestLeaderCommitWaitsForItsOwnWrite(t *testing.T) {
	r := newNode(t, 1, []NodeID{1}, Restore{}, nil)
	r.Campaign()
	for r.HasReady() {
		rd := r.Ready()
		for _, m := range rd.AfterPersist {
			r.Step(m)
		}
	}
	if r.State() != StateLeader || r.Committed() != 1 {
		t.Fatalf("state %s committed %d", r.State(), r.Committed())
	}
	idx, err := r.Propose([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	rd := r.Ready()
	if r.Committed() >= idx {
		t.Fatal("committed before its own write was durable")
	}
	for _, m := range rd.AfterPersist {
		r.Step(m)
	}
	if r.Committed() != idx {
		t.Fatalf("committed %d after its write, want %d", r.Committed(), idx)
	}
	rd = r.Ready()
	if len(rd.Committed) != 1 || string(rd.Committed[0].Data) != "a" {
		t.Fatalf("to apply: %v", rd.Committed)
	}
}

func TestLeaderNeverCommitsAnEarlierTermByCounting(t *testing.T) {
	r := newNode(t, 1, five(), Restore{HardState: HardState{Term: 2}, Entries: ents(1, 2)}, func(c *Config) { c.PreVote = false })
	r.Campaign()
	r.Ready()
	for _, id := range []NodeID{1, 2, 3} {
		r.Step(Message{Type: MsgVoteResp, From: id, To: 1, Term: 3})
	}
	if r.State() != StateLeader || r.LastIndex() != 3 {
		t.Fatalf("state %s last index %d", r.State(), r.LastIndex())
	}
	r.Ready()
	for _, id := range []NodeID{2, 3} {
		r.Step(Message{Type: MsgAppResp, From: id, To: 1, Term: 3, Index: 2})
	}
	if r.Committed() != 0 {
		t.Fatalf("committed %d: an entry of term 2 committed by counting replicas in term 3", r.Committed())
	}
	r.Step(Message{Type: MsgStorageAck, From: 1, To: 1, Index: 3, LogTerm: 3})
	r.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 3, Index: 3})
	if r.Committed() != 0 {
		t.Fatalf("committed %d with the new entry on two of five", r.Committed())
	}
	r.Step(Message{Type: MsgAppResp, From: 3, To: 1, Term: 3, Index: 3})
	if r.Committed() != 3 {
		t.Fatalf("committed %d, want 3 once the term 3 entry is on a quorum", r.Committed())
	}
}

func TestFollowerReplacesConflictingSuffix(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 2, Commit: 2}, Entries: ents(1, 1, 2)}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 3, Index: 2, LogTerm: 1, Commit: 4,
		Entries: []Entry{{Term: 3, Index: 3}, {Term: 3, Index: 4}}})
	if r.LastIndex() != 4 || r.Committed() != 4 {
		t.Fatalf("last %d committed %d", r.LastIndex(), r.Committed())
	}
	if term, _ := r.LogTerm(3); term != 3 {
		t.Fatalf("entry 3 has term %d, want 3", term)
	}
	if r.log.stabled != 2 {
		t.Fatalf("stabled %d after truncation, want 2", r.log.stabled)
	}
	rd := r.Ready()
	if len(rd.Entries) != 2 || rd.Entries[0].Index != 3 {
		t.Fatalf("entries to persist %v, want 3..4", rd.Entries)
	}
	if len(rd.Committed) != 2 || rd.Committed[1].Index != 2 {
		t.Fatalf("to apply %v, but only the restored entries 1..2 are durable", rd.Committed)
	}
}

func TestRejectionHintSkipsAWholeTerm(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 2}, Entries: ents(1, 1, 2, 2, 2)}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 4, Index: 7, LogTerm: 3})
	rd := r.Ready()
	m, ok := find(rd.Messages, MsgAppResp, 1)
	if !ok || !m.Reject || m.Hint != 5 || m.LogTerm != 2 {
		t.Fatalf("rejection %v", m)
	}
	r = newNode(t, 2, five(), Restore{HardState: HardState{Term: 3}, Entries: ents(1, 1, 3, 3, 3)}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 4, Index: 5, LogTerm: 2})
	rd = r.Ready()
	m, _ = find(rd.Messages, MsgAppResp, 1)
	if !m.Reject || m.Hint != 2 || m.LogTerm != 1 {
		t.Fatalf("rejection %v, want hint 2 (the last entry with a term of at most 2)", m)
	}
}

func TestLeaderCatchesUpADivergedFollower(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.isolate(3)
	for i := range 20 {
		c.propose(1, string(rune('a'+i)))
	}
	c.crash(1)
	c.crash(2)
	c.heal()
	c.restart(1)
	c.restart(2)
	c.elect(2)
	c.propose(2, "after")
	for i, r := range c.nodes {
		if r.Committed() != c.node(2).LastIndex() {
			t.Fatalf("node %d committed %d, leader last %d", i+1, r.Committed(), c.node(2).LastIndex())
		}
	}
	want := appliedData(c.applied[1])
	if got := appliedData(c.applied[2]); !slices.Equal(got, want) {
		t.Fatalf("node 3 applied %v, want %v", got, want)
	}
}

func TestPreVoteKeepsARejoiningNodeFromDisruptingTheLeader(t *testing.T) {
	for _, preVote := range []bool{true, false} {
		t.Run(fmt.Sprintf("prevote=%v", preVote), func(t *testing.T) {
			c := newCluster(t, 3, func(cfg *Config) { cfg.PreVote = preVote })
			c.elect(1)
			c.isolate(3)
			for range 100 {
				c.tick(3, 1)
				c.tick(1, 1)
				c.tick(2, 1)
			}
			c.heal()
			c.tickAll(30)
			disrupted := c.node(1).State() != StateLeader || c.node(1).Term() != 1
			if preVote && disrupted {
				t.Fatalf("with PreVote: node 1 is %s in term %d", c.node(1).State(), c.node(1).Term())
			}
			if !preVote && !disrupted {
				t.Fatal("without PreVote the rejoining node should have forced a new term")
			}
		})
	}
}

func TestCheckQuorumDeposesAnIsolatedLeader(t *testing.T) {
	for _, checkQuorum := range []bool{true, false} {
		t.Run(fmt.Sprintf("checkquorum=%v", checkQuorum), func(t *testing.T) {
			c := newCluster(t, 3, func(cfg *Config) { cfg.CheckQuorum = checkQuorum })
			c.elect(1)
			c.isolate(1)
			c.tick(1, 25)
			stillLeader := c.node(1).State() == StateLeader
			if checkQuorum == stillLeader {
				t.Fatalf("isolated leader still leading: %v", stillLeader)
			}
		})
	}
}

func TestFollowerIgnoresVotesWhileItHearsFromALeader(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.node(2).Step(Message{Type: MsgVote, From: 3, To: 2, Term: 5, Index: 100, LogTerm: 100})
	if c.node(2).Term() != 1 || c.node(2).HardState().Vote == 3 {
		t.Fatalf("follower moved to term %d and voted %d within the lease", c.node(2).Term(), c.node(2).HardState().Vote)
	}
	c.tick(2, 10)
	c.node(2).Step(Message{Type: MsgVote, From: 3, To: 2, Term: 5, Index: 100, LogTerm: 100})
	if c.node(2).Term() != 5 {
		t.Fatalf("term %d once the lease ran out, want 5", c.node(2).Term())
	}
}

func TestStaleLeaderStepsDownOnAHigherTerm(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.isolate(1)
	c.tick(2, 30)
	c.drain()
	if c.node(2).State() != StateLeader && c.node(3).State() != StateLeader {
		c.tick(3, 30)
	}
	if l := c.leaders(); len(l) != 2 {
		t.Fatalf("leaders %v, want the stale one and a new one", l)
	}
	c.heal()
	c.tickAll(5)
	if l := c.leaders(); len(l) != 1 || l[0] == 1 {
		t.Fatalf("leaders %v after healing", l)
	}
}

func TestRestartKeepsTermAndVote(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.CheckQuorum = false })
	c.node(1).Step(Message{Type: MsgVote, From: 2, To: 1, Term: 5})
	c.drain()
	c.crash(1)
	c.restart(1)
	r := c.node(1)
	if r.Term() != 5 || r.HardState().Vote != 2 {
		t.Fatalf("restarted with term %d vote %d", r.Term(), r.HardState().Vote)
	}
	r.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 5})
	if r.HardState().Vote != 2 {
		t.Fatal("voted twice in one term across a restart")
	}
}

func TestProposalsInOneReadyShareOneMessage(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.propose(1, "warm")
	r := c.node(1)
	for i := range 10 {
		if _, err := r.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	rd := r.Ready()
	for _, to := range []NodeID{2, 3} {
		var apps []Message
		for _, m := range rd.Messages {
			if m.Type == MsgApp && m.To == to {
				apps = append(apps, m)
			}
		}
		if len(apps) != 1 || len(apps[0].Entries) != 10 {
			t.Fatalf("to %d: %d messages", to, len(apps))
		}
	}
	if len(rd.Entries) != 10 {
		t.Fatalf("%d entries to persist, want 10", len(rd.Entries))
	}
}

func TestInflightWindowPausesReplication(t *testing.T) {
	c := newCluster(t, 2, func(cfg *Config) { cfg.MaxInflight = 2 })
	c.elect(1)
	c.propose(1, "warm")
	c.cut[[2]NodeID{2, 1}] = true
	r := c.node(1)
	sent := 0
	for i := range 5 {
		if _, err := r.Propose([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		rd := r.Ready()
		sent += count(rd.Messages, MsgApp)
		for _, m := range rd.AfterPersist {
			r.Step(m)
		}
	}
	if sent != 2 {
		t.Fatalf("%d appends in flight, want the window of 2", sent)
	}
	delete(c.cut, [2]NodeID{2, 1})
	r.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Index: r.LastIndex() - 3})
	rd := r.Ready()
	m, ok := find(rd.Messages, MsgApp, 2)
	if !ok || len(m.Entries) != 3 {
		t.Fatalf("after the ack: %v", m)
	}
}

func TestALeaderRefusesProposalsPastItsPendingCap(t *testing.T) {
	c := newCluster(t, 3, func(cfg *Config) { cfg.MaxPending = 5 })
	c.elect(1)
	c.isolate(1)
	r := c.node(1)
	for i := range 5 {
		if _, err := r.Propose([]byte{byte(i)}); err != nil {
			t.Fatalf("proposal %d under the cap: %v", i, err)
		}
	}
	if _, err := r.Propose([]byte("over")); !errors.Is(err, ErrBusy) {
		t.Fatalf("the sixth uncommitted proposal: %v, want ErrBusy", err)
	}
	c.drain()
	c.heal()
	c.tickAll(5)
	if r.Committed() != r.LastIndex() {
		t.Fatalf("committed %d of %d after healing", r.Committed(), r.LastIndex())
	}
	if _, err := r.Propose([]byte("after")); err != nil {
		t.Fatalf("after the backlog committed: %v", err)
	}
}

func TestProposeOnFollowerFails(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	if _, err := c.node(2).Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("err %v", err)
	}
}

func TestElectionTimeoutStaysInRange(t *testing.T) {
	r := newNode(t, 1, five(), Restore{}, nil)
	seen := map[int]bool{}
	for range 2000 {
		r.reset(r.term)
		to := r.randomizedElectionTimeout
		if to < 10 || to >= 20 {
			t.Fatalf("timeout %d outside [10, 20)", to)
		}
		seen[to] = true
	}
	if len(seen) != 10 {
		t.Fatalf("saw %d distinct timeouts, want all 10", len(seen))
	}
}

func TestSameSeedSameRun(t *testing.T) {
	run := func() []uint64 {
		c := newCluster(t, 5, func(cfg *Config) { cfg.Rand = rand.NewPCG(99, uint64(cfg.ID)) })
		var terms []uint64
		for round := range 6 {
			c.tickAll(30)
			if l := c.leaders(); len(l) == 1 {
				terms = append(terms, uint64(l[0]), c.node(l[0]).Term())
				c.isolate(l[0])
				c.tickAll(30)
				c.heal()
			}
			terms = append(terms, uint64(round))
		}
		return terms
	}
	a, b := run(), run()
	if !slices.Equal(a, b) {
		t.Fatalf("two runs with the same seed differ:\n%v\n%v", a, b)
	}
}

func TestRestoreRejectsInconsistentState(t *testing.T) {
	bad := []Restore{
		{HardState: HardState{Term: 1, Commit: 5}, Entries: ents(1, 1)},
		{HardState: HardState{Term: 1}, Entries: ents(1, 2)},
		{HardState: HardState{Term: 3}, Entries: ents(2, 1)},
		{HardState: HardState{Term: 3}, Entries: []Entry{{Term: 1, Index: 2}}},
	}
	for i, st := range bad {
		if _, err := New(testConfig(1, five()), st); err == nil {
			t.Fatalf("case %d: restored without an error", i)
		}
	}
}

func TestTruncatingCommittedEntriesPanics(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 2, Commit: 2}, Entries: ents(1, 1, 2)}, nil)
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	r.log.append(Entry{Term: 3, Index: 2})
}

func TestHandedOutEntriesSurviveTruncation(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 1}}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{{Term: 1, Index: 1, Data: []byte("a")}, {Term: 1, Index: 2, Data: []byte("b")}}})
	rd := r.Ready()
	r.Step(Message{Type: MsgApp, From: 3, To: 2, Term: 2, Index: 1, LogTerm: 1, Entries: []Entry{{Term: 2, Index: 2, Data: []byte("c")}}})
	if string(rd.Entries[1].Data) != "b" || rd.Entries[1].Term != 1 {
		t.Fatalf("an entry already handed to storage changed: %+v", rd.Entries[1])
	}
}

func (c *cluster) snapshot(id NodeID, retain uint64) Snapshot {
	c.t.Helper()
	r := c.node(id)
	idx := r.Applied()
	term, _ := r.LogTerm(idx)
	s := Snapshot{Index: idx, Term: term, Data: []byte(fmt.Sprintf("state of %d at %d", id, idx))}
	if err := r.Compact(s, retain); err != nil {
		c.t.Fatal(err)
	}
	c.stores[id-1].compact(s)
	return s
}

func TestCompactKeepsWhatIsRetained(t *testing.T) {
	c := newCluster(t, 1, nil)
	c.elect(1)
	for i := range 10 {
		c.propose(1, fmt.Sprint(i))
	}
	r := c.node(1)
	s := c.snapshot(1, 3)
	if s.Index != 11 || r.FirstIndex() != 9 || r.LastIndex() != 11 {
		t.Fatalf("snapshot at %d, log now %d..%d", s.Index, r.FirstIndex(), r.LastIndex())
	}
	if _, ok := r.EntryAt(8); ok {
		t.Fatal("entry 8 survived compaction")
	}
	if term, ok := r.LogTerm(8); !ok || term != 1 {
		t.Fatalf("term at the compaction point: %d %v", term, ok)
	}
	if err := r.Compact(Snapshot{Index: 99, Term: 1}, 0); err == nil {
		t.Fatal("compacted past the applied index")
	}
	if err := r.Compact(Snapshot{Index: 11, Term: 7}, 0); err != nil {
		t.Fatalf("an older or equal snapshot should be ignored: %v", err)
	}
}

func TestALaggingFollowerGetsASnapshot(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.isolate(3)
	for i := range 20 {
		c.propose(1, fmt.Sprint(i))
	}
	s := c.snapshot(1, 0)
	c.heal()
	c.tickAll(3)
	r3 := c.node(3)
	if r3.LatestSnapshot() == nil || r3.LatestSnapshot().Index != s.Index || string(r3.LatestSnapshot().Data) != string(s.Data) {
		t.Fatalf("node 3 has snapshot %+v, want %d", r3.LatestSnapshot(), s.Index)
	}
	if c.stores[2].snap == nil || c.stores[2].snap.Index != s.Index {
		t.Fatal("the snapshot was not handed to storage")
	}
	c.propose(1, "after")
	for i, r := range c.nodes {
		if r.Committed() != c.node(1).LastIndex() {
			t.Fatalf("node %d committed %d, leader last %d", i+1, r.Committed(), c.node(1).LastIndex())
		}
	}
	last := c.applied[2][len(c.applied[2])-1]
	if string(last.Data) != "after" {
		t.Fatalf("node 3 last applied %q", last.Data)
	}
	c.crash(3)
	c.restart(3)
	if r := c.node(3); r.FirstIndex() != s.Index+1 || r.Committed() < s.Index {
		t.Fatalf("restarted node 3: first %d committed %d", r.FirstIndex(), r.Committed())
	}
}

func TestOnlyTheNodeItselfAcknowledgesItsWrites(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 1}}, nil)
	r.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: ents(1, 1)})
	ack, ok := find(r.Ready().AfterPersist, MsgStorageAck, 2)
	if !ok {
		t.Fatal("no storage acknowledgement after the write")
	}
	forged := ack
	forged.From = 1
	r.Step(forged)
	if r.log.stabled != 0 {
		t.Fatalf("stabled %d after another node acknowledged this node's write", r.log.stabled)
	}
	r.Step(ack)
	if r.log.stabled != 2 {
		t.Fatalf("stabled %d after the write", r.log.stabled)
	}
}

func TestSnapshotHandOffWaitsForItsWrite(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 1}}, nil)
	snap := &Snapshot{Index: 50, Term: 1, Data: []byte("s")}
	r.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 1, Snapshot: snap})
	rd := r.Ready()
	if rd.Snapshot != snap || !rd.MustSync {
		t.Fatalf("ready %+v", rd)
	}
	if _, ok := find(rd.Messages, MsgAppResp, 1); ok {
		t.Fatal("acknowledged the snapshot before it was persisted")
	}
	ack, ok := find(rd.AfterPersist, MsgAppResp, 1)
	if !ok || ack.Index != 50 {
		t.Fatalf("ack after persistence: %v", ack)
	}
	if r.Committed() != 50 || r.Applied() != 50 || r.FirstIndex() != 51 {
		t.Fatalf("committed %d applied %d first %d", r.Committed(), r.Applied(), r.FirstIndex())
	}
	for _, m := range rd.AfterPersist {
		if m.To == 2 {
			r.Step(m)
		}
	}
	if r.log.stabled != 50 {
		t.Fatalf("stabled %d after the write", r.log.stabled)
	}
}

func TestAnOldOrKnownSnapshotIsNotRestored(t *testing.T) {
	r := newNode(t, 2, five(), Restore{HardState: HardState{Term: 2, Commit: 3}, Entries: ents(1, 1, 2, 2, 2)}, nil)
	r.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &Snapshot{Index: 2, Term: 1}})
	if rd := r.Ready(); rd.Snapshot != nil || r.LastIndex() != 5 {
		t.Fatal("restored a snapshot older than the commit index")
	}
	r.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 2, Snapshot: &Snapshot{Index: 4, Term: 2}})
	rd := r.Ready()
	if rd.Snapshot != nil || r.LastIndex() != 5 || r.Committed() != 4 {
		t.Fatalf("a snapshot the log already has should only move commit: last %d committed %d", r.LastIndex(), r.Committed())
	}
}

func TestALostSnapshotIsSentAgain(t *testing.T) {
	c := newCluster(t, 3, nil)
	c.elect(1)
	c.isolate(3)
	for i := range 5 {
		c.propose(1, fmt.Sprint(i))
	}
	s := c.snapshot(1, 0)
	c.heal()
	c.cut[[2]NodeID{1, 3}] = true
	c.tickAll(2)
	delete(c.cut, [2]NodeID{1, 3})
	c.tickAll(6)
	if snap := c.node(3).LatestSnapshot(); snap == nil || snap.Index != s.Index {
		t.Fatalf("node 3 never got the snapshot again: %+v", snap)
	}
}

func TestRestoreFromASnapshot(t *testing.T) {
	st := Restore{
		HardState: HardState{Term: 3, Commit: 12},
		Snapshot:  &Snapshot{Index: 10, Term: 2, Data: []byte("x")},
		Entries:   []Entry{{Term: 3, Index: 11}, {Term: 3, Index: 12}},
	}
	r := newNode(t, 1, five(), st, nil)
	if r.FirstIndex() != 11 || r.LastIndex() != 12 || r.Applied() != 10 || r.Committed() != 12 {
		t.Fatalf("first %d last %d applied %d committed %d", r.FirstIndex(), r.LastIndex(), r.Applied(), r.Committed())
	}
	if term, _ := r.LogTerm(10); term != 2 {
		t.Fatalf("term at the snapshot %d", term)
	}
	bad := st
	bad.Entries = []Entry{{Term: 3, Index: 12}}
	if _, err := New(testConfig(1, five()), bad); err == nil {
		t.Fatal("entries that do not follow the snapshot were accepted")
	}
}
