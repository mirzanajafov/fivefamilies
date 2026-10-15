package sim

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"runtime/debug"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Options struct {
	Nodes         int
	Chaos         time.Duration
	Heal          time.Duration
	Tick          time.Duration
	ElectionTick  int
	HeartbeatTick int
	PreVote       bool
	CheckQuorum   bool
	Trace         io.Writer
}

func DefaultOptions() Options {
	return Options{
		Nodes:         5,
		Chaos:         20 * time.Second,
		Heal:          10 * time.Second,
		Tick:          10 * time.Millisecond,
		ElectionTick:  15,
		HeartbeatTick: 3,
		PreVote:       true,
		CheckQuorum:   true,
	}
}

type Stats struct {
	Events         int
	MsgsSent       int
	MsgsDropped    int
	MsgsDuplicated int
	Proposals      int
	Accepted       int
	Leaders        int
	MaxTerm        uint64
	Committed      int
	Crashes        int
	Pauses         int
	Partitions     int
	SlowDisks      int
	Overwrites     int
}

type Result struct {
	Seed      uint64
	Swarm     string
	Violation string
	Stack     string
	At        time.Duration
	TraceHash uint64
	Stats     Stats
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
	backlog     []event
	disk        *memDisk
	rate        float64
}

type nodeEnv struct {
	s  *Sim
	sn *simNode
}

func (e nodeEnv) Send(m raft.Message) { e.s.send(e.sn, m) }
func (e nodeEnv) Write(w node.Write)  { e.s.write(e.sn, w) }

type Sim struct {
	opts    Options
	seed    uint64
	rng     *rand.Rand
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
	nextID  uint64
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
	s.net = newNetwork(opts.Nodes)
	s.net.loss, s.net.dup, s.net.base, s.net.jitter = s.sw.loss, s.sw.dup, s.sw.base, s.sw.jitter
	s.chk = newChecker(opts.Nodes)
	for i := range opts.Nodes {
		s.voters = append(s.voters, raft.NodeID(i+1))
	}
	for i := range opts.Nodes {
		rate := 1 + s.sw.clockSpread*(2*rng.Float64()-1)
		s.nodes = append(s.nodes, &simNode{idx: i, id: raft.NodeID(i + 1), disk: newMemDisk(), rate: rate})
	}
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
		Swarm:     s.sw.String(),
		Violation: s.chk.failure,
		At:        s.now,
		TraceHash: s.hash,
		Stats:     s.stats,
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
	s.tracef("swarm: %s", s.sw)
	if len(s.nodes) < 3 {
		s.sw.partitionMean = 0
	}
	s.startFaults()
	s.after(exponential(s.rng, time.Duration(float64(time.Second)/s.sw.proposalRate)), s.proposeLoop)
}

func (s *Sim) boot(sn *simNode) {
	cfg := raft.Config{
		ID:            sn.id,
		Voters:        s.voters,
		ElectionTick:  s.opts.ElectionTick,
		HeartbeatTick: s.opts.HeartbeatTick,
		PreVote:       s.opts.PreVote,
		CheckQuorum:   s.opts.CheckQuorum,
		Rand:          rand.NewPCG(s.rng.Uint64(), s.rng.Uint64()),
	}
	r, err := raft.New(cfg, sn.disk.restore())
	if err != nil {
		s.chk.failf("node %d failed to restart: %v (disk: %s)", sn.id, err, sn.disk)
		return
	}
	sn.n = node.New(r, nodeEnv{s: s, sn: sn}, &recorder{chk: s.chk, idx: sn.idx})
	s.chk.restarted(sn.idx)
	sn.n.Process()
	s.observe(sn)
	s.scheduleTick(sn, time.Duration(s.rng.Int64N(int64(s.opts.Tick))))
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
		if e.gen != sn.incarnation {
			return
		}
		if !e.done {
			sn.disk.complete(e.wseq, s.chk, sn.idx)
			e.done = true
		}
		if sn.paused {
			sn.backlog = append(sn.backlog, e)
			return
		}
		sn.n.WriteDone(e.wseq)
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
	to := int(m.To) - 1
	if s.net.isBlocked(from.idx, to) || (s.net.loss > 0 && s.rng.Float64() < s.net.loss) {
		s.stats.MsgsDropped++
		return
	}
	s.schedule(event{kind: evDeliver, node: to, msg: m}, s.net.delay(s.rng))
	if s.net.dup > 0 && s.rng.Float64() < s.net.dup {
		s.stats.MsgsDuplicated++
		s.schedule(event{kind: evDeliver, node: to, msg: m}, s.net.delay(s.rng))
	}
}

func (s *Sim) write(sn *simNode, w node.Write) {
	sn.disk.submit(w)
	var lat time.Duration
	if !w.Empty() {
		lat = time.Duration(float64(s.sw.diskBase) * sn.disk.slow * (0.5 + s.rng.ExpFloat64()/2))
	}
	done := max(s.now+lat, sn.disk.lastDone)
	sn.disk.lastDone = done
	s.schedule(event{kind: evWriteDone, node: sn.idx, gen: sn.incarnation, wseq: w.Seq}, done-s.now)
}

func (s *Sim) crash(sn *simNode) {
	s.tracef("fault: crash node %d", sn.id)
	s.stats.Crashes++
	sn.n = nil
	sn.incarnation++
	sn.tickGen++
	sn.paused = false
	sn.backlog = nil
	sn.disk.crash()
	sn.disk.lastDone = s.now
}

func (s *Sim) restart(sn *simNode) {
	s.tracef("fault: restart node %d", sn.id)
	s.boot(sn)
}

func (s *Sim) pause(sn *simNode) {
	s.tracef("fault: pause node %d", sn.id)
	s.stats.Pauses++
	sn.paused = true
	sn.tickGen++
}

func (s *Sim) resume(sn *simNode) {
	s.tracef("fault: resume node %d", sn.id)
	sn.paused = false
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
	s.nextID++
	s.stats.Proposals++
	data := binary.BigEndian.AppendUint64(nil, s.nextID)
	if _, err := sn.n.Propose(data); err == nil {
		s.stats.Accepted++
	}
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
	l := s.leader()
	if l == nil {
		s.chk.failf("liveness: no leader %v after every fault stopped", s.opts.Heal)
		return
	}
	lr := l.n.Raft()
	if t, _ := lr.LogTerm(lr.Committed()); t != lr.Term() {
		s.chk.failf("liveness: leader %d of term %d has not committed an entry of its term", lr.ID(), lr.Term())
		return
	}
	for _, sn := range s.nodes {
		r := sn.n.Raft()
		if r.Term() != lr.Term() || r.Leader() != lr.ID() {
			s.chk.failf("liveness: node %d is in term %d following %d, leader %d is in term %d", r.ID(), r.Term(), r.Leader(), lr.ID(), lr.Term())
			return
		}
		if r.Committed() != lr.LastIndex() || r.Applied() != r.Committed() {
			s.chk.failf("liveness: node %d committed %d applied %d, leader's log ends at %d", r.ID(), r.Committed(), r.Applied(), lr.LastIndex())
			return
		}
	}
}
