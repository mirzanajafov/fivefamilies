package raft

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
)

var (
	ErrNotLeader     = errors.New("raft: not the leader")
	ErrTransferring  = errors.New("raft: leadership is being transferred")
	ErrConfPending   = errors.New("raft: a membership change is still in progress")
	ErrConfNotReady  = errors.New("raft: the leader has not committed an entry of its own term yet")
	ErrLearnerBehind = errors.New("raft: the learner has not caught up yet")
	ErrBadConfChange = errors.New("raft: invalid membership change")
)

const campaignTransfer = 1

type Source interface {
	Uint64() uint64
}

type Config struct {
	ID            NodeID
	Voters        []NodeID
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	MaxMsgBytes   int
	MaxInflight   int
	MaxLearnerLag uint64
	LeaseTicks    int
	Rand          Source
	Bugs          Bug
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: node ID must not be zero")
	case len(c.Voters) == 0:
		return errors.New("raft: no voters")
	case slices.Contains(c.Voters, None):
		return errors.New("raft: voter ID must not be zero")
	case c.HeartbeatTick <= 0:
		return errors.New("raft: heartbeat tick must be positive")
	case c.ElectionTick <= c.HeartbeatTick:
		return errors.New("raft: election tick must be greater than heartbeat tick")
	case c.Rand == nil:
		return errors.New("raft: no random source")
	case c.LeaseTicks > 0 && (!c.CheckQuorum || c.LeaseTicks >= c.ElectionTick):
		return errors.New("raft: lease reads need CheckQuorum and a lease shorter than the election timeout")
	}
	if c.MaxMsgBytes <= 0 {
		c.MaxMsgBytes = 1 << 20
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 256
	}
	if c.MaxLearnerLag == 0 {
		c.MaxLearnerLag = 1000
	}
	return nil
}

type Restore struct {
	HardState HardState
	Snapshot  *Snapshot
	Entries   []Entry
}

type Raft struct {
	cfg   Config
	id    NodeID
	term  uint64
	vote  NodeID
	state StateType
	lead  NodeID
	log   *raftLog

	confs []confAt
	prs   []*progress
	votes map[NodeID]bool

	transferee      NodeID
	transferElapsed int
	transferTerm    uint64
	confRound       uint64

	electionElapsed           int
	heartbeatElapsed          int
	randomizedElectionTimeout int

	snapshot        *Snapshot
	pendingSnapshot *Snapshot

	ticks            uint64
	hbRound          uint64
	roundTicks       [roundRing]uint64
	reads            []readRequest
	readStates       []ReadState
	pendingHeartbeat bool
	leaseStart       uint64
	leaseOK          bool

	pendingBcast bool
	msgs         []Message
	afterPersist []Message
	prevHS       HardState
}

func New(cfg Config, st Restore) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	l, err := newLog(st)
	if err != nil {
		return nil, err
	}
	l.skipPrevTerm = cfg.Bugs.Has(BugPrevTerm)
	if st.HardState.Term < l.lastTerm() {
		return nil, fmt.Errorf("raft: restored term %d is lower than the last entry's term %d", st.HardState.Term, l.lastTerm())
	}
	voters := slices.Clone(cfg.Voters)
	slices.Sort(voters)
	voters = slices.Compact(voters)
	base := confAt{conf: ConfState{Voters: voters}}
	if st.Snapshot != nil {
		base.index = st.Snapshot.Index
		if len(st.Snapshot.Conf.Voters) > 0 {
			base.conf = st.Snapshot.Conf.clone()
		}
	}
	r := &Raft{
		cfg:      cfg,
		id:       cfg.ID,
		term:     st.HardState.Term,
		vote:     st.HardState.Vote,
		log:      l,
		votes:    make(map[NodeID]bool, len(voters)),
		prevHS:   HardState{Term: st.HardState.Term, Vote: st.HardState.Vote, Commit: l.committed},
		snapshot: st.Snapshot,
		confs:    []confAt{base},
	}
	r.onAppend(l.firstIndex(), st.Entries)
	r.applyConf()
	l.onAppend = r.onAppend
	r.becomeFollower(r.term, None)
	return r, nil
}

func (r *Raft) ID() NodeID         { return r.id }
func (r *Raft) Term() uint64       { return r.term }
func (r *Raft) Vote() NodeID       { return r.vote }
func (r *Raft) State() StateType   { return r.state }
func (r *Raft) Leader() NodeID     { return r.lead }
func (r *Raft) Committed() uint64  { return r.log.committed }
func (r *Raft) Applied() uint64    { return r.log.applied }
func (r *Raft) Stabled() uint64    { return r.log.stabled }
func (r *Raft) FirstIndex() uint64 { return r.log.firstIndex() }
func (r *Raft) LastIndex() uint64  { return r.log.lastIndex() }

func (r *Raft) LeaderFresh() bool {
	return r.state == StateLeader || (r.lead != None && r.electionElapsed < r.cfg.ElectionTick)
}

func (r *Raft) Planted(b Bug) bool {
	return r.cfg.Bugs.Has(b)
}

func (r *Raft) HardState() HardState {
	return r.hardState()
}

func (r *Raft) LatestSnapshot() *Snapshot {
	return r.snapshot
}

func (r *Raft) Compact(s Snapshot, retain uint64) error {
	if r.snapshot != nil && s.Index <= r.snapshot.Index {
		return nil
	}
	if s.Index > r.log.applied {
		return fmt.Errorf("raft: snapshot at %d is past the applied index %d", s.Index, r.log.applied)
	}
	if t, ok := r.log.term(s.Index); !ok || t != s.Term {
		return fmt.Errorf("raft: snapshot at %d has term %d, the log has %d", s.Index, s.Term, t)
	}
	s.Conf = r.ConfAt(s.Index)
	r.snapshot = &s
	point := s.Index - min(retain, s.Index)
	r.log.compact(point)
	r.compactConfs(point)
	return nil
}

func (r *Raft) LogTerm(i uint64) (uint64, bool) {
	return r.log.term(i)
}

func (r *Raft) EntryAt(i uint64) (Entry, bool) {
	return r.log.entry(i)
}

func (r *Raft) Tick() {
	r.ticks++
	if r.state == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

func (r *Raft) Propose(data []byte) (uint64, error) {
	if r.state != StateLeader {
		return 0, ErrNotLeader
	}
	if r.transferee != None {
		return 0, ErrTransferring
	}
	e := r.appendEntry(EntryNormal, data)
	r.pendingBcast = true
	return e.Index, nil
}

func (r *Raft) Campaign() {
	r.hup()
}

func (r *Raft) ProposeConfChange(cc ConfChange) (uint64, error) {
	switch {
	case r.state != StateLeader:
		return 0, ErrNotLeader
	case cc.Type == ConfRemove && cc.Node == r.id:
		return 0, fmt.Errorf("%w: the leader cannot remove itself, transfer leadership first", ErrBadConfChange)
	case r.transferee != None:
		return 0, ErrTransferring
	case !r.committedInTerm() && !r.cfg.Bugs.Has(BugEarlyConf):
		return 0, ErrConfNotReady
	case r.pendingConf():
		return 0, ErrConfPending
	}
	if _, err := r.conf().Apply(cc); err != nil {
		return 0, fmt.Errorf("%w: %v", ErrBadConfChange, err)
	}
	if cc.Type == ConfPromote {
		if pr := r.progress(cc.Node); pr == nil || pr.match+r.cfg.MaxLearnerLag < r.log.lastIndex() {
			return 0, ErrLearnerBehind
		}
	}
	e := r.appendEntry(EntryConfig, cc.Marshal())
	r.pendingBcast = true
	return e.Index, nil
}

func (r *Raft) TransferLeadership(to NodeID) error {
	if r.state != StateLeader {
		return ErrNotLeader
	}
	if to == r.id {
		return nil
	}
	if !r.conf().IsVoter(to) {
		return fmt.Errorf("%w: node %d is not a voter", ErrBadConfChange, to)
	}
	r.transferee, r.transferElapsed, r.transferTerm = to, 0, r.term
	pr := r.progress(to)
	if pr.match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: to, Term: r.term})
	} else {
		r.maybeSendAppend(pr, true)
	}
	return nil
}

func (r *Raft) Transferring() NodeID {
	return r.transferee
}

func (r *Raft) Step(m Message) {
	if m.Type == MsgStorageAck {
		r.handleStorageAck(m)
		return
	}
	if (m.Type == MsgVote || m.Type == MsgPreVote) && !r.conf().IsMember(m.From) {
		return
	}
	switch {
	case m.Term > r.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && r.inLease() && !(m.Type == MsgVote && m.Context == campaignTransfer) {
			return
		}
		switch {
		case m.Type == MsgPreVote:
		case m.Type == MsgPreVoteResp && !m.Reject:
		case m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap:
			r.becomeFollower(m.Term, m.From)
		default:
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.term:
		if r.cfg.Bugs.Has(BugStaleTerm) && m.Type == MsgApp && r.state == StateFollower {
			r.handleAppend(m)
			return
		}
		switch {
		case (r.cfg.CheckQuorum || r.cfg.PreVote) && (m.Type == MsgApp || m.Type == MsgHeartbeat || m.Type == MsgSnap):
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term})
		case m.Type == MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
		case m.Type == MsgReadIndex:
			r.send(Message{Type: MsgReadIndexResp, To: m.From, Term: r.term, Context: m.Context, Reject: true})
		}
		return
	}

	if m.Type == MsgVote || m.Type == MsgPreVote {
		r.handleVoteRequest(m)
		return
	}

	switch r.state {
	case StateLeader:
		r.stepLeader(m)
	case StateCandidate, StatePreCandidate:
		r.stepCandidate(m)
	default:
		r.stepFollower(m)
	}
}

func (r *Raft) HasReady() bool {
	return r.pendingBcast ||
		r.pendingHeartbeat ||
		len(r.readStates) > 0 ||
		r.pendingSnapshot != nil ||
		len(r.msgs) > 0 ||
		len(r.afterPersist) > 0 ||
		r.hardState() != r.prevHS ||
		r.log.handed < r.log.lastIndex() ||
		r.log.hasToApply()
}

func (r *Raft) Ready() Ready {
	if r.cfg.Bugs.Has(BugNoFsync) && r.log.stabled < r.log.lastIndex() {
		r.log.stabled = r.log.lastIndex()
		if r.state == StateLeader && r.maybeCommit() {
			r.pendingBcast = true
		}
	}
	if r.pendingBcast {
		r.pendingBcast = false
		r.bcastAppend()
	}
	if r.pendingHeartbeat {
		r.pendingHeartbeat = false
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
	var rd Ready
	hs := r.hardState()
	if hs != r.prevHS {
		rd.HardState = hs
		rd.MustSync = hs.Term != r.prevHS.Term || hs.Vote != r.prevHS.Vote
		r.prevHS = hs
	}
	ackIndex, ackTerm := uint64(0), uint64(0)
	if r.pendingSnapshot != nil {
		rd.Snapshot = r.pendingSnapshot
		rd.MustSync = true
		ackIndex, ackTerm = r.pendingSnapshot.Index, r.pendingSnapshot.Term
		r.pendingSnapshot = nil
	}
	if ents := r.log.unhanded(); len(ents) > 0 {
		rd.Entries = ents
		rd.MustSync = true
		last := ents[len(ents)-1]
		r.log.handed = last.Index
		ackIndex, ackTerm = last.Index, last.Term
	}
	if ackIndex > 0 {
		r.afterPersist = append([]Message{{Type: MsgStorageAck, From: r.id, To: r.id, Index: ackIndex, LogTerm: ackTerm}}, r.afterPersist...)
	}
	if r.log.hasToApply() {
		hi := r.log.applyLimit()
		rd.Committed = r.log.slice(r.log.applied+1, hi+1)
		r.log.applied = hi
	}
	rd.Messages = r.msgs
	rd.AfterPersist = r.afterPersist
	rd.ReadStates = r.readStates
	r.msgs = nil
	r.afterPersist = nil
	r.readStates = nil
	return rd
}

func (r *Raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Commit: r.log.committed}
}

func (r *Raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *Raft) sendAfterPersist(m Message) {
	m.From = r.id
	if r.cfg.Bugs.Has(BugNoFsync) {
		r.msgs = append(r.msgs, m)
		return
	}
	r.afterPersist = append(r.afterPersist, m)
}

func (r *Raft) quorum() int {
	return len(r.conf().Voters)/2 + 1
}

func (r *Raft) isVoter(id NodeID) bool {
	return r.conf().IsVoter(id)
}

func (r *Raft) progress(id NodeID) *progress {
	i, ok := slices.BinarySearchFunc(r.prs, id, func(p *progress, id NodeID) int { return cmp.Compare(p.id, id) })
	if !ok {
		return nil
	}
	return r.prs[i]
}

func (r *Raft) inLease() bool {
	if !r.cfg.CheckQuorum {
		return false
	}
	if r.cfg.LeaseTicks > 0 && r.ticks < uint64(r.cfg.ElectionTick) {
		return true
	}
	return r.lead != None && r.electionElapsed < r.cfg.ElectionTick
}

func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.isVoter(r.id) && r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.hup()
	}
}

func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++
	if r.transferee != None {
		r.transferElapsed++
		if r.transferElapsed >= r.cfg.ElectionTick {
			r.transferee = None
		}
	}
	if r.electionElapsed >= r.cfg.ElectionTick {
		r.electionElapsed = 0
		if r.cfg.CheckQuorum && !r.quorumActive() {
			r.becomeFollower(r.term, None)
			return
		}
	}
	if r.heartbeatElapsed >= r.cfg.HeartbeatTick {
		r.heartbeatElapsed = 0
		r.bcastHeartbeat()
	}
}

func (r *Raft) quorumActive() bool {
	active := 0
	for _, pr := range r.prs {
		if !pr.learner && (pr.id == r.id || pr.active) {
			active++
		}
		pr.active = false
	}
	return active >= r.quorum()
}

func (r *Raft) hup() {
	if r.state == StateLeader || !r.isVoter(r.id) {
		return
	}
	r.campaign(r.cfg.PreVote, 0)
}

func (r *Raft) campaign(pre bool, ctx uint64) {
	var typ MessageType
	var term uint64
	if pre {
		r.becomePreCandidate()
		typ, term = MsgPreVote, r.term+1
	} else {
		r.becomeCandidate()
		typ, term = MsgVote, r.term
	}
	for _, id := range r.conf().Voters {
		if id == r.id {
			continue
		}
		r.send(Message{Type: typ, To: id, Term: term, Index: r.log.lastIndex(), LogTerm: r.log.lastTerm(), Context: ctx})
	}
	if pre || r.cfg.Bugs.Has(BugNoFsync) {
		r.recordVote(r.id, true)
	} else {
		r.sendAfterPersist(Message{Type: MsgVoteResp, To: r.id, Term: r.term})
	}
}

func (r *Raft) recordVote(id NodeID, granted bool) {
	if !r.isVoter(id) {
		return
	}
	if _, seen := r.votes[id]; !seen {
		r.votes[id] = granted
	}
	yes, no := 0, 0
	voters := r.conf().Voters
	for _, v := range voters {
		g, seen := r.votes[v]
		switch {
		case !seen:
		case g:
			yes++
		default:
			no++
		}
	}
	switch {
	case yes >= r.quorum():
		if r.state == StatePreCandidate {
			r.campaign(false, 0)
		} else if r.votes[r.id] {
			r.becomeLeader()
		}
	case no > len(voters)-r.quorum():
		r.becomeFollower(r.term, None)
	}
}

func (r *Raft) reset(term uint64) {
	r.dropReads()
	r.pendingHeartbeat = false
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.lead = None
	r.transferee, r.transferElapsed = None, 0
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randomizedElectionTimeout = r.cfg.ElectionTick + int(r.cfg.Rand.Uint64()%uint64(r.cfg.ElectionTick))
	clear(r.votes)
	for _, pr := range r.prs {
		pr.reset(r.log.lastIndex() + 1)
	}
	r.pendingBcast = false
}

func (r *Raft) becomeFollower(term uint64, lead NodeID) {
	r.reset(term)
	r.state = StateFollower
	r.lead = lead
}

func (r *Raft) becomePreCandidate() {
	r.state = StatePreCandidate
	r.lead = None
	clear(r.votes)
}

func (r *Raft) becomeCandidate() {
	r.reset(r.term + 1)
	r.vote = r.id
	r.state = StateCandidate
}

func (r *Raft) becomeLeader() {
	r.reset(r.term)
	r.state = StateLeader
	r.lead = r.id
	r.appendEntry(EntryNormal, nil)
	r.pendingBcast = true
}

func (r *Raft) appendEntry(typ EntryType, data []byte) Entry {
	e := Entry{Term: r.term, Index: r.log.lastIndex() + 1, Type: typ, Data: data}
	r.log.append(e)
	return e
}

func (r *Raft) handleVoteRequest(m Message) {
	resp := MsgVoteResp
	if m.Type == MsgPreVote {
		resp = MsgPreVoteResp
	}
	canVote := r.vote == m.From ||
		(r.vote == None && r.lead == None) ||
		(m.Type == MsgPreVote && m.Term > r.term)
	if !canVote || (!r.log.isUpToDate(m.Index, m.LogTerm) && !r.cfg.Bugs.Has(BugAnyVote)) {
		r.send(Message{Type: resp, To: m.From, Term: r.term, Reject: true})
		return
	}
	if m.Type == MsgPreVote {
		r.send(Message{Type: resp, To: m.From, Term: m.Term})
		return
	}
	r.electionElapsed = 0
	r.vote = m.From
	r.sendAfterPersist(Message{Type: resp, To: m.From, Term: m.Term})
}

func (r *Raft) stepFollower(m Message) {
	switch m.Type {
	case MsgApp:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppend(m)
	case MsgHeartbeat:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case MsgSnap:
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case MsgReadIndexResp:
		r.readStates = append(r.readStates, ReadState{Index: m.Index, Ctx: m.Context, Ok: !m.Reject})
	case MsgTimeoutNow:
		if r.isVoter(r.id) {
			r.campaign(false, campaignTransfer)
		}
	}
}

func (r *Raft) stepCandidate(m Message) {
	want := MsgVoteResp
	if r.state == StatePreCandidate {
		want = MsgPreVoteResp
	}
	switch m.Type {
	case MsgApp:
		r.becomeFollower(m.Term, m.From)
		r.handleAppend(m)
	case MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case MsgSnap:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case MsgReadIndexResp:
		r.readStates = append(r.readStates, ReadState{Index: m.Index, Ctx: m.Context, Ok: !m.Reject})
	case want:
		r.recordVote(m.From, !m.Reject)
	}
}

func (r *Raft) stepLeader(m Message) {
	pr := r.progress(m.From)
	if pr == nil && m.Type == MsgReadIndex {
		r.answerRead(m.From, m.Context, 0, false)
		return
	}
	if pr == nil || pr.id == r.id {
		return
	}
	switch m.Type {
	case MsgAppResp:
		pr.active = true
		r.handleAppendResp(pr, m)
	case MsgReadIndex:
		r.handleReadIndex(m.From, m.Context, false)
		return
	case MsgHeartbeatResp:
		pr.active = true
		pr.ackRound = max(pr.ackRound, m.Context)
		r.releaseReads()
		r.handleHeartbeatResp(pr)
	}
}

func (r *Raft) handleStorageAck(m Message) {
	if m.Index <= r.log.stabled || m.Index > r.log.handed || !r.log.matchTerm(m.Index, m.LogTerm) {
		return
	}
	r.log.stabled = m.Index
	if r.state == StateLeader && r.maybeCommit() {
		r.pendingBcast = true
	}
}

func (r *Raft) handleAppend(m Message) {
	if m.Index < r.log.committed {
		r.sendAfterPersist(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: r.log.committed})
		return
	}
	if last, ok := r.log.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.sendAfterPersist(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: last})
		return
	}
	hint, hintTerm := r.log.findConflictByTerm(min(m.Index, r.log.lastIndex()), m.LogTerm)
	r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: m.Index, Reject: true, Hint: hint, LogTerm: hintTerm})
}

func (r *Raft) handleSnapshot(m Message) {
	s := m.Snapshot
	if s == nil {
		return
	}
	switch {
	case s.Index <= r.log.committed:
		r.sendAfterPersist(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: r.log.committed})
	case r.log.matchTerm(s.Index, s.Term):
		r.log.commitTo(s.Index)
		r.sendAfterPersist(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: s.Index})
	default:
		r.log.restore(s)
		if len(s.Conf.Voters) > 0 {
			r.resetConfs(confAt{index: s.Index, conf: s.Conf.clone()})
		}
		r.snapshot = s
		r.pendingSnapshot = s
		r.sendAfterPersist(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: s.Index})
	}
}

func (r *Raft) handleHeartbeat(m Message) {
	r.log.commitTo(m.Commit)
	r.send(Message{Type: MsgHeartbeatResp, To: m.From, Term: r.term, Context: m.Context})
}

func (r *Raft) handleAppendResp(pr *progress, m Message) {
	if pr.state == stateSnapshot {
		if !m.Reject {
			pr.maybeUpdate(m.Index)
		}
		if m.Reject || pr.match >= pr.pendingSnapshot {
			pr.becomeProbe()
			if !m.Reject {
				pr.becomeReplicate()
			}
			r.maybeSendAppend(pr, true)
			for pr.state == stateReplicate && r.maybeSendAppend(pr, false) {
			}
		}
		return
	}
	if m.Reject {
		next := m.Hint
		if m.LogTerm > 0 {
			next, _ = r.log.findConflictByTerm(m.Hint, m.LogTerm)
		}
		if pr.maybeDecrTo(m.Index, next) {
			if pr.state == stateReplicate {
				pr.becomeProbe()
			}
			r.maybeSendAppend(pr, true)
		}
		return
	}
	if !pr.maybeUpdate(m.Index) {
		return
	}
	switch pr.state {
	case stateProbe:
		pr.becomeReplicate()
	case stateReplicate:
		pr.freeTo(m.Index)
	}
	if r.maybeCommit() {
		r.pendingBcast = true
	}
	for r.maybeSendAppend(pr, false) {
	}
	if pr.id == r.transferee && pr.match == r.log.lastIndex() {
		r.send(Message{Type: MsgTimeoutNow, To: pr.id, Term: r.term})
	}
}

func (r *Raft) handleHeartbeatResp(pr *progress) {
	if pr.state == stateSnapshot {
		pr.snapWait++
		if pr.snapWait >= 3 {
			pr.abandonSnapshot()
			r.maybeSendAppend(pr, true)
		}
		return
	}
	pr.paused = false
	if pr.state == stateReplicate && len(pr.inflight) >= r.cfg.MaxInflight {
		pr.freeFirst()
	}
	if pr.match < r.log.lastIndex() {
		r.maybeSendAppend(pr, true)
	}
}

func (r *Raft) maybeCommit() bool {
	matches := make([]uint64, 0, len(r.prs))
	for _, pr := range r.prs {
		if pr.learner {
			continue
		}
		if pr.id == r.id {
			matches = append(matches, r.log.stabled)
		} else {
			matches = append(matches, pr.match)
		}
	}
	slices.Sort(matches)
	n := matches[len(matches)-r.quorum()]
	if n <= r.log.committed {
		return false
	}
	if t, _ := r.log.term(n); t != r.term && !r.cfg.Bugs.Has(BugFigure8) {
		return false
	}
	r.log.commitTo(n)
	return true
}

func (r *Raft) bcastAppend() {
	for _, pr := range r.prs {
		if pr.id != r.id {
			r.maybeSendAppend(pr, true)
		}
	}
}

func (r *Raft) bcastHeartbeat() {
	r.hbRound++
	r.roundTicks[r.hbRound%roundRing] = r.ticks
	for _, pr := range r.prs {
		if pr.id != r.id {
			r.send(Message{Type: MsgHeartbeat, To: pr.id, Term: r.term, Commit: min(pr.match, r.log.committed), Context: r.hbRound})
		}
	}
	if len(r.reads) > 0 {
		r.releaseReads()
	}
}

func (r *Raft) maybeSendAppend(pr *progress, sendIfEmpty bool) bool {
	if pr.isPaused(r.cfg.MaxInflight) {
		return false
	}
	prevIndex := pr.next - 1
	prevTerm, ok := r.log.term(prevIndex)
	if !ok {
		if r.snapshot == nil || prevIndex > r.log.lastIndex() {
			return false
		}
		r.send(Message{Type: MsgSnap, To: pr.id, Term: r.term, Snapshot: r.snapshot})
		pr.becomeSnapshot(r.snapshot.Index)
		return true
	}
	var ents []Entry
	if pr.next <= r.log.lastIndex() {
		ents = r.log.sliceBytes(pr.next, r.log.lastIndex()+1, r.cfg.MaxMsgBytes)
	}
	if len(ents) == 0 && !sendIfEmpty {
		return false
	}
	r.send(Message{Type: MsgApp, To: pr.id, Term: r.term, Index: prevIndex, LogTerm: prevTerm, Entries: ents, Commit: r.log.committed})
	pr.sent(ents)
	return true
}
