package sim

import (
	"fmt"
	"io"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/snap"
)

type Options struct {
	Nodes         int
	Chaos         time.Duration
	Heal          time.Duration
	CheckTimeout  time.Duration
	MaxEvents     int
	Tick          time.Duration
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	Bugs          raft.Bug
	MuteSafety    bool
	LeaseTicks    int
	ClockSkew     float64
	Reads         string
	NoCatchUp     bool
	Membership    bool
	Leases        bool
	Trace         io.Writer
}

func DefaultOptions() Options {
	return Options{
		Nodes:         5,
		Chaos:         20 * time.Second,
		Heal:          10 * time.Second,
		CheckTimeout:  time.Minute,
		MaxEvents:     5_000_000,
		Tick:          10 * time.Millisecond,
		ElectionTick:  15,
		HeartbeatTick: 3,
		PreVote:       true,
		CheckQuorum:   true,
		LeaseTicks:    12,
		Membership:    true,
		Leases:        true,
	}
}

type Stats struct {
	Events         int
	MsgsSent       int
	MsgsDropped    int
	MsgsDuplicated int
	Proposals      int
	Leaders        int
	MaxTerm        uint64
	Committed      int
	Crashes        int
	Pauses         int
	Partitions     int
	SlowDisks      int
	Overwrites     int
	ClientOps      int
	ClientRetries  int
	SwapsWon       int
	Indeterminate  int
	Buggified      int
	Writes         int
	Fsyncs         int
	TornTails      int
	Snapshots      int
	LocalReads     int
	SnapshotsSent  int
	Installs       int
	Compactions    int
	ConfChanges    int
	ConfCommitted  int
	Transfers      int
	LeaseGrants    int
	KeepAlives     int
	LeasesLost     int
	LeaseExpiries  int
}

type Result struct {
	Seed      uint64
	Swarm     string
	Violation string
	Category  Category
	Muted     string
	Detected  map[Category]string
	Stack     string
	At        time.Duration
	TraceHash uint64
	Stats     Stats
	Check     porcupine.CheckResult
	CheckTook time.Duration
	Info      porcupine.LinearizationInfo
}

func (r Result) OK() bool {
	return r.Violation == ""
}

type simNode struct {
	idx         int
	id          raft.NodeID
	n           *node.Node
	incarnation uint64
	tickGen     uint64
	paused      bool
	pausedAt    time.Duration
	backlog     []event
	disk        *disk
	rate        float64
	sm          *recorder
	ledTerm     uint64
}

type nodeEnv struct {
	s  *Sim
	sn *simNode
}

func (e nodeEnv) Send(m raft.Message)          { e.s.send(e.sn, m) }
func (e nodeEnv) Write(w node.Write)           { e.s.write(e.sn, w) }
func (e nodeEnv) SaveSnapshot(s raft.Snapshot) { e.s.saveSnapshot(e.sn, s) }
func (e nodeEnv) Compacted(s raft.Snapshot)    { e.s.compacted(e.sn, s) }

type Sim struct {
	opts    Options
	seed    uint64
	rng     *rand.Rand
	mrng    *rand.Rand
	ms      memberSwarm
	lrng    *rand.Rand
	ls      leaseSwarm
	leases  map[uint64]*leaseTrack
	now     time.Duration
	seq     uint64
	queue   eventQueue
	nodes   []*simNode
	voters  []raft.NodeID
	net     network
	sw      swarm
	chk     *checker
	stats   Stats
	hash    uint64
	healing bool
	clients []*simClient
	history []opRecord
	check   check.Verdict
}

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func Run(seed uint64, opts Options) (res Result) {
	s := newSim(seed, opts)
	defer func() {
		if p := recover(); p != nil {
			res = s.result()
			res.Violation = fmt.Sprintf("panic: %v", p)
			res.Category = CatPanic
			res.Detected[CatPanic] = res.Violation
			res.Stack = string(debug.Stack())
		}
	}()
	s.start()
	s.loop(s.opts.Chaos)
	if !s.chk.failed() {
		s.heal()
		s.loop(s.opts.Chaos + s.opts.Heal)
	}
	if !s.chk.failed() {
		s.finalChecks()
	}
	return s.result()
}

func newSim(seed uint64, opts Options) *Sim {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	s := &Sim{opts: opts, seed: seed, rng: rng, hash: fnvOffset}
	s.sw = newSwarm(rng)
	for m := readLog; m <= readFollower; m++ {
		if opts.Reads == m.String() {
			s.sw.readMode = m
		}
	}
	s.net = newNetwork(opts.Nodes)
	s.net.loss, s.net.dup, s.net.base, s.net.jitter = s.sw.loss, s.sw.dup, s.sw.base, s.sw.jitter
	s.chk = newChecker(opts.Nodes)
	s.chk.mute = opts.MuteSafety
	s.mrng = rand.New(rand.NewPCG(seed^0x6d656d62657273, seed))
	s.ms = memberSwarm{voters: opts.Nodes}
	if opts.Membership {
		s.ms = newMemberSwarm(s.mrng, opts.Nodes)
	}
	for i := range s.ms.voters {
		s.voters = append(s.voters, raft.NodeID(i+1))
	}
	s.chk.conf = raft.ConfState{Voters: s.voters}
	s.lrng = rand.New(rand.NewPCG(seed^0x6c6561736573, seed))
	s.leases = map[uint64]*leaseTrack{}
	if opts.Leases {
		s.ls = newLeaseSwarm(s.lrng, time.Duration(opts.ElectionTick)*opts.Tick)
	}
	s.chk.onExpire = s.leaseExpired
	skewed := -1
	if opts.ClockSkew != 0 {
		skewed = rng.IntN(opts.Nodes)
	}
	for i := range opts.Nodes {
		rate := 1 + s.sw.clockSpread*(2*rng.Float64()-1)
		if i == skewed {
			rate += opts.ClockSkew
		}
		sn := &simNode{idx: i, id: raft.NodeID(i + 1), disk: newDisk(s.sw.segBytes, s.rng.Uint64), rate: rate}
		s.nodes = append(s.nodes, sn)
		s.chk.disks = append(s.chk.disks, sn.disk)
	}
	s.net.reorder = s.sw.reorder
	return s
}

func (s *Sim) result() Result {
	s.stats.Leaders = len(s.chk.leaders)
	s.stats.Committed = len(s.chk.committed)
	s.stats.Overwrites = s.chk.overwrites
	for _, sn := range s.nodes {
		if sn.n != nil {
			s.stats.MaxTerm = max(s.stats.MaxTerm, sn.n.Raft().Term())
		}
	}
	return Result{
		Seed:      s.seed,
		Swarm:     s.sw.String() + s.ms.String() + s.ls.String(),
		Violation: s.chk.failure,
		Category:  s.chk.category,
		Muted:     s.chk.muted,
		Detected:  s.chk.detected,
		At:        s.now,
		TraceHash: s.hash,
		Stats:     s.stats,
		Check:     s.check.Result,
		CheckTook: s.check.Took,
		Info:      s.check.Info,
	}
}

func (s *Sim) tracef(format string, args ...any) {
	if s.opts.Trace != nil {
		fmt.Fprintf(s.opts.Trace, "%12.6fs %s\n", s.now.Seconds(), fmt.Sprintf(format, args...))
	}
}

func (s *Sim) mix(v uint64) {
	s.hash ^= v
	s.hash *= fnvPrime
}

func (s *Sim) schedule(e event, d time.Duration) {
	e.at = s.now + max(d, 0)
	s.seq++
	e.seq = s.seq
	s.queue.push(e)
}

func (s *Sim) after(d time.Duration, f func()) {
	s.schedule(event{kind: evCall, call: f}, d)
}

func (s *Sim) start() {
	for _, sn := range s.nodes {
		s.boot(sn)
	}
	s.tracef("swarm: %s%s%s", s.sw, s.ms, s.ls)
	if len(s.nodes) < 3 {
		s.sw.partitionMean = 0
	}
	s.startFaults()
	s.startMembership()
	s.startLeaseHolders()
	s.startClients()
	if s.sw.proposalRate > 0 {
		s.after(exponential(s.rng, time.Duration(float64(time.Second)/s.sw.proposalRate)), s.proposeLoop)
	}
}

func (s *Sim) boot(sn *simNode) {
	cfg := raft.Config{
		ID:            sn.id,
		Voters:        s.voters,
		ElectionTick:  s.opts.ElectionTick,
		HeartbeatTick: s.opts.HeartbeatTick,
		PreVote:       s.opts.PreVote,
		CheckQuorum:   s.opts.CheckQuorum,
		MaxMsgBytes:   s.sw.maxMsgBytes,
		LeaseTicks:    s.leaseTicks(),
		Rand:          rand.NewPCG(s.rng.Uint64(), s.rng.Uint64()),
		Bugs:          s.opts.Bugs,
	}
	st, rec, err := sn.disk.open(s.chk, sn.idx)
	if err != nil {
		s.chk.safetyf("node %d could not recover its log: %v", sn.id, err)
		return
	}
	if rec.TornBytes > 0 {
		s.stats.TornTails++
	}
	r, err := raft.New(cfg, st)
	if err != nil {
		s.chk.safetyf("node %d failed to restart: %v (disk: %s)", sn.id, err, sn.disk)
		return
	}
	sn.sm = newRecorder(s.chk, sn.idx)
	if st.Snapshot != nil {
		if err := sn.sm.Restore(st.Snapshot.Index, st.Snapshot.Data); err != nil {
			s.chk.safetyf("node %d could not restore its snapshot: %v", sn.id, err)
			return
		}
	}
	sn.n = node.New(r, nodeEnv{s: s, sn: sn}, sn.sm, node.Config{SnapshotEvery: s.sw.snapEvery, Retain: s.sw.retain, Tick: s.opts.Tick})
	s.chk.restarted(sn.idx)
	sn.n.Process()
	s.observe(sn)
	s.scheduleTick(sn, time.Duration(s.rng.Int64N(int64(s.opts.Tick))))
}

func (s *Sim) leaseTicks() int {
	if s.sw.readMode != readLease {
		return 0
	}
	return s.opts.LeaseTicks
}

func (s *Sim) scheduleTick(sn *simNode, d time.Duration) {
	s.schedule(event{kind: evTick, node: sn.idx, gen: sn.tickGen}, d)
}

func (s *Sim) tickInterval(sn *simNode) time.Duration {
	return time.Duration(float64(s.opts.Tick) / sn.rate)
}

func (s *Sim) loop(until time.Duration) {
	for len(s.queue) > 0 && !s.chk.failed() {
		if s.queue[0].at > until {
			s.now = until
			return
		}
		if s.opts.MaxEvents > 0 && s.stats.Events >= s.opts.MaxEvents {
			s.chk.report(CatLiveness, "liveness: %d events in %v of simulated time, a message storm", s.stats.Events, s.now)
			return
		}
		e := s.queue.pop()
		s.now = e.at
		s.stats.Events++
		s.dispatch(e)
	}
}

func (s *Sim) dispatch(e event) {
	s.mix(uint64(e.at))
	s.mix(uint64(e.kind)<<32 | uint64(e.node))
	if e.kind == evCall {
		e.call()
		return
	}
	sn := s.nodes[e.node]
	switch e.kind {
	case evTick:
		if sn.n == nil || sn.paused || e.gen != sn.tickGen {
			return
		}
		sn.n.Tick()
		s.scheduleTick(sn, s.tickInterval(sn))
	case evDeliver:
		if sn.n == nil {
			return
		}
		if sn.paused {
			sn.backlog = append(sn.backlog, e)
			return
		}
		s.mix(uint64(e.msg.Type)<<56 | uint64(e.msg.From)<<48 | e.msg.Term<<16 | uint64(len(e.msg.Entries)))
		s.mix(e.msg.Index)
		if s.opts.Trace != nil {
			s.tracef("n%d recv %v", sn.id, e.msg)
		}
		sn.n.Receive(e.msg)
	case evWriteDone:
		if e.gen != sn.incarnation || sn.n == nil {
			return
		}
		if sn.paused {
			sn.backlog = append(sn.backlog, e)
			return
		}
		if sn.disk.covers > 0 {
			if err := sn.disk.sync(s.chk, sn.idx); err != nil {
				s.chk.safetyf("node %d could not sync its log: %v", sn.id, err)
				return
			}
		}
		sn.disk.syncing = false
		sn.n.WriteDone(e.wseq)
		s.startSync(sn)
	case evRequest:
		if sn.n == nil {
			return
		}
		if sn.paused {
			sn.backlog = append(sn.backlog, e)
			return
		}
		s.handleRequest(sn, e.req)
	case evNodeCall:
		if sn.n == nil {
			return
		}
		if sn.paused {
			sn.backlog = append(sn.backlog, e)
			return
		}
		e.on(sn)
	}
	sn.n.Process()
	s.observe(sn)
}

func (s *Sim) observe(sn *simNode) {
	if sn.n == nil {
		return
	}
	r := sn.n.Raft()
	s.chk.observe(sn.idx, r)
	if r.State() == raft.StateLeader && r.Term() != sn.ledTerm {
		sn.ledTerm = r.Term()
		s.newLeader(sn)
	}
	if s.stats.Events%512 == 0 {
		s.checkLogMatching()
	}
}

func (s *Sim) checkLogMatching() {
	for i, a := range s.nodes {
		for _, b := range s.nodes[i+1:] {
			if a.n != nil && b.n != nil {
				s.chk.logMatching(a.n.Raft(), b.n.Raft())
			}
		}
	}
}

func (s *Sim) send(from *simNode, m raft.Message) {
	s.stats.MsgsSent++
	if m.Type == raft.MsgSnap {
		s.stats.SnapshotsSent++
	}
	if s.sw.buggify > 0 && !s.healing {
		s.buggify(from, m)
	}
	to := int(m.To) - 1
	if s.net.isBlocked(from.idx, to) || (s.net.loss > 0 && s.rng.Float64() < s.net.loss) {
		s.stats.MsgsDropped++
		return
	}
	s.schedule(event{kind: evDeliver, node: to, msg: m}, s.net.arrival(s.rng, s.now, from.idx, to)-s.now)
	if s.net.dup > 0 && s.rng.Float64() < s.net.dup {
		s.stats.MsgsDuplicated++
		s.schedule(event{kind: evDeliver, node: to, msg: m}, s.net.delay(s.rng))
	}
}

func (s *Sim) write(sn *simNode, w node.Write) {
	if w.Snapshot != nil {
		if w.Reset {
			s.stats.Installs++
		} else {
			s.stats.Snapshots++
		}
	}
	if err := sn.disk.submit(w); err != nil {
		s.chk.safetyf("node %d could not write its log: %v", sn.id, err)
		return
	}
	if !sn.disk.syncing {
		s.startSync(sn)
	}
}

func (s *Sim) startSync(sn *simNode) {
	d := sn.disk
	if len(d.queue) == 0 {
		return
	}
	batch := d.queue
	d.queue = nil
	d.syncing = true
	need := false
	for _, w := range batch {
		need = need || w.Sync
	}
	var lat time.Duration
	if need {
		d.covers = len(d.written)
		s.stats.Fsyncs++
		lat = time.Duration(float64(s.sw.diskBase) * d.slow * (0.5 + s.rng.ExpFloat64()/2))
	}
	s.stats.Writes += len(batch)
	s.schedule(event{kind: evWriteDone, node: sn.idx, gen: sn.incarnation, wseq: batch[len(batch)-1].Seq}, lat)
}

func (s *Sim) buggify(from *simNode, m raft.Message) {
	switch {
	case m.Type == raft.MsgVoteResp && !m.Reject,
		m.Type == raft.MsgAppResp && !m.Reject && m.Index > 0,
		m.Type == raft.MsgApp && len(m.Entries) > 0:
		if s.rng.Float64() >= s.sw.buggify {
			return
		}
		s.stats.Buggified++
		inc := from.incarnation
		s.after(between(s.rng, 0, 200*time.Microsecond), func() {
			if from.n == nil || from.incarnation != inc || s.healing {
				return
			}
			s.tracef("buggify: crash node %d right after it sent %v", from.id, m.Type)
			s.crash(from)
			s.after(between(s.rng, 20*time.Millisecond, 500*time.Millisecond), func() {
				if from.n == nil {
					s.restart(from)
				}
			})
		})
	case m.Type == raft.MsgVote:
		if s.rng.Float64() >= s.sw.buggify {
			return
		}
		other := s.nodes[s.rng.IntN(len(s.nodes))]
		if other == from || other.n == nil || other.paused {
			return
		}
		s.stats.Buggified++
		s.after(0, func() {
			if other.n == nil || other.paused || s.healing {
				return
			}
			s.tracef("buggify: node %d campaigns alongside %d", other.id, from.id)
			other.n.Raft().Campaign()
			other.n.Process()
			s.observe(other)
		})
	}
}

func (s *Sim) saveSnapshot(sn *simNode, snapshot raft.Snapshot) {
	if err := snap.Save(sn.disk.fs, snapshot); err != nil {
		s.chk.safetyf("node %d could not save snapshot %d: %v", sn.id, snapshot.Index, err)
	}
}

func (s *Sim) compacted(sn *simNode, snapshot raft.Snapshot) {
	n, err := sn.disk.log.Compact(snapshot.Index)
	if err == nil {
		err = snap.Clean(sn.disk.fs, snapshot)
	}
	if err != nil {
		s.chk.safetyf("node %d could not compact to %d: %v", sn.id, snapshot.Index, err)
		return
	}
	s.stats.Compactions += n
}

func (s *Sim) crash(sn *simNode) {
	s.tracef("fault: crash node %d", sn.id)
	s.stats.Crashes++
	sn.n = nil
	sn.incarnation++
	sn.tickGen++
	sn.paused = false
	sn.backlog = nil
	sn.disk.crash(s.rng)
}

func (s *Sim) restart(sn *simNode) {
	s.tracef("fault: restart node %d", sn.id)
	s.boot(sn)
}

func (s *Sim) pause(sn *simNode) {
	s.tracef("fault: pause node %d", sn.id)
	s.stats.Pauses++
	sn.paused = true
	sn.pausedAt = s.now
	sn.tickGen++
}

func (s *Sim) resume(sn *simNode) {
	s.tracef("fault: resume node %d", sn.id)
	sn.paused = false
	if missed := int((s.now - sn.pausedAt) / s.tickInterval(sn)); missed > 0 && sn.n != nil && !s.opts.NoCatchUp {
		for range min(missed, 10000) {
			sn.n.Tick()
		}
		sn.n.Process()
		s.observe(sn)
	}
	backlog := sn.backlog
	sn.backlog = nil
	for _, e := range backlog {
		s.schedule(e, 0)
	}
	s.scheduleTick(sn, s.tickInterval(sn))
}

func (s *Sim) proposeLoop() {
	if s.now < s.opts.Chaos+s.opts.Heal-2*time.Second {
		s.proposeOnce()
	}
	s.after(exponential(s.rng, time.Duration(float64(time.Second)/s.sw.proposalRate)), s.proposeLoop)
}

func (s *Sim) leader() *simNode {
	var best *simNode
	for _, sn := range s.nodes {
		if sn.n == nil || sn.paused || sn.n.Raft().State() != raft.StateLeader {
			continue
		}
		if best == nil || sn.n.Raft().Term() > best.n.Raft().Term() {
			best = sn
		}
	}
	return best
}

func (s *Sim) proposeOnce() {
	sn := s.leader()
	if sn == nil {
		return
	}
	s.stats.Proposals++
	sn.n.Submit(kv.Command{Op: kv.OpPut, Key: "load", Value: []byte(fmt.Sprint(s.stats.Proposals))}, nil)
	sn.n.Process()
	s.observe(sn)
}

func (s *Sim) heal() {
	s.healing = true
	s.tracef("heal: every fault stops")
	s.net.healAll()
	s.net.loss, s.net.dup = 0, 0
	s.net.jitter = min(s.net.jitter, time.Millisecond)
	for _, sn := range s.nodes {
		sn.disk.slow = 1
		switch {
		case sn.n == nil:
			s.restart(sn)
		case sn.paused:
			s.resume(sn)
		}
	}
}

func (s *Sim) finalChecks() {
	s.checkLogMatching()
	if s.chk.failed() {
		return
	}
	s.checkLiveness()
	if !s.chk.failed() {
		s.checkLeasesExpired()
	}
	s.checkStates()
	if s.chk.failed() && !s.opts.MuteSafety {
		return
	}
	s.check = check.Linearizable(s.porcupineHistory(), s.opts.CheckTimeout)
	if s.check.Result == porcupine.Illegal {
		s.chk.report(CatLinearizability, "linearizability: the history of %d client operations is not linearizable", s.check.Ops)
	}
}

func (s *Sim) checkLiveness() {
	for _, sn := range s.nodes {
		if sn.n == nil {
			s.chk.report(CatLiveness, "liveness: node %d could not restart", sn.id)
			return
		}
	}
	l := s.leader()
	if l == nil {
		s.chk.report(CatLiveness, "liveness: no leader %v after every fault stopped", s.opts.Heal)
		return
	}
	lr := l.n.Raft()
	if t, _ := lr.LogTerm(lr.Committed()); t != lr.Term() {
		s.chk.report(CatLiveness, "liveness: leader %d of term %d has not committed an entry of its term", lr.ID(), lr.Term())
		return
	}
	conf := lr.Conf()
	for _, sn := range s.nodes {
		r := sn.n.Raft()
		if !conf.IsMember(r.ID()) {
			continue
		}
		if r.Term() != lr.Term() || r.Leader() != lr.ID() {
			s.chk.report(CatLiveness, "liveness: node %d is in term %d following %d, leader %d is in term %d", r.ID(), r.Term(), r.Leader(), lr.ID(), lr.Term())
			return
		}
		if r.Committed() != lr.LastIndex() || r.Applied() != r.Committed() {
			s.chk.report(CatLiveness, "liveness: node %d committed %d applied %d, leader's log ends at %d", r.ID(), r.Committed(), r.Applied(), lr.LastIndex())
			return
		}
	}
	for _, c := range s.clients {
		if c.busy() {
			s.chk.report(CatLiveness, "liveness: client %d is still waiting for %v (seq %d) after every fault stopped", c.idx, c.cmd.Op, c.cmd.Seq)
			return
		}
	}
}

func (s *Sim) checkStates() {
	l := s.leader()
	if l == nil {
		return
	}
	for _, sn := range s.nodes {
		if sn.n == nil || sn == l || sn.sm.store.Rev() != l.sm.store.Rev() {
			continue
		}
		if sn.sm.store.Hash() != l.sm.store.Hash() {
			s.chk.report(CatState, "state: node %d's state at revision %d differs from the leader's", sn.id, sn.sm.store.Rev())
			return
		}
	}
}
