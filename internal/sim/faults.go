package sim

import (
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"
)

type network struct {
	n       int
	blocked []bool
	last    []time.Duration
	loss    float64
	dup     float64
	reorder float64
	base    time.Duration
	jitter  time.Duration
}

func newNetwork(n int) network {
	return network{n: n, blocked: make([]bool, n*n), last: make([]time.Duration, n*n)}
}

func (nw *network) arrival(rng *rand.Rand, now time.Duration, from, to int) time.Duration {
	at := now + nw.delay(rng)
	if nw.reorder > 0 && rng.Float64() < nw.reorder {
		return at
	}
	link := from*nw.n + to
	at = max(at, nw.last[link])
	nw.last[link] = at
	return at
}

func (nw *network) isBlocked(from, to int) bool {
	return nw.blocked[from*nw.n+to]
}

func (nw *network) block(from, to int) {
	nw.blocked[from*nw.n+to] = true
}

func (nw *network) cutBoth(a, b int) {
	nw.block(a, b)
	nw.block(b, a)
}

func (nw *network) healAll() {
	clear(nw.blocked)
}

func (nw *network) delay(rng *rand.Rand) time.Duration {
	d := nw.base
	if nw.jitter > 0 {
		d += time.Duration(rng.Int64N(int64(nw.jitter)))
	}
	return d
}

type swarm struct {
	loss          float64
	dup           float64
	base          time.Duration
	jitter        time.Duration
	partitionMean time.Duration
	crashMean     time.Duration
	pauseMean     time.Duration
	slowMean      time.Duration
	diskBase      time.Duration
	proposalRate  float64
	clockSpread   float64
	clients       int
	keys          int
	think         time.Duration
	timeout       time.Duration
	buggify       float64
	maxMsgBytes   int
	reorder       float64
	segBytes      int64
	snapEvery     uint64
	retain        uint64
}

func pick[T any](rng *rand.Rand, opts ...T) T {
	return opts[rng.IntN(len(opts))]
}

func between(rng *rand.Rand, lo, hi time.Duration) time.Duration {
	if hi <= lo {
		return lo
	}
	return lo + time.Duration(rng.Int64N(int64(hi-lo)))
}

func exponential(rng *rand.Rand, mean time.Duration) time.Duration {
	return time.Duration(-float64(mean) * math.Log(1-rng.Float64()))
}

func newSwarm(rng *rand.Rand) swarm {
	sw := swarm{
		loss:         pick(rng, 0, 0, 0.01, 0.05, 0.2),
		dup:          pick(rng, 0, 0, 0.01, 0.05),
		base:         between(rng, 100*time.Microsecond, time.Millisecond),
		jitter:       pick(rng, 0, time.Millisecond, 5*time.Millisecond, 20*time.Millisecond),
		diskBase:     between(rng, 50*time.Microsecond, 2*time.Millisecond),
		proposalRate: pick(rng, 0, 20.0, 100.0, 500.0),
		clockSpread:  pick(rng, 0, 0.01, 0.05),
		clients:      1 + rng.IntN(8),
		keys:         1 + rng.IntN(5),
		think:        between(rng, time.Millisecond, 50*time.Millisecond),
		timeout:      between(rng, 200*time.Millisecond, 800*time.Millisecond),
		buggify:      pick(rng, 0, 0, 0.002, 0.01, 0.05),
		maxMsgBytes:  pick(rng, 0, 0, 1),
		reorder:      pick(rng, 0, 0, 0.001, 0.01),
		segBytes:     pick[int64](rng, 16<<10, 64<<10, 1<<20),
		snapEvery:    pick[uint64](rng, 0, 20, 200, 2000),
		retain:       pick[uint64](rng, 0, 5, 50),
	}
	if rng.Float64() < 0.6 {
		sw.partitionMean = between(rng, time.Second, 5*time.Second)
	}
	if rng.Float64() < 0.6 {
		sw.crashMean = between(rng, time.Second, 5*time.Second)
	}
	if rng.Float64() < 0.4 {
		sw.pauseMean = between(rng, time.Second, 5*time.Second)
	}
	if rng.Float64() < 0.3 {
		sw.slowMean = between(rng, 2*time.Second, 6*time.Second)
	}
	return sw
}

func (sw swarm) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "loss %.2f dup %.2f delay %v+%v disk %v load %.0f/s clock ±%.2f clients %d keys %d think %v timeout %v",
		sw.loss, sw.dup, sw.base, sw.jitter, sw.diskBase, sw.proposalRate, sw.clockSpread, sw.clients, sw.keys, sw.think, sw.timeout)
	if sw.buggify > 0 {
		fmt.Fprintf(&b, " buggify %.3f", sw.buggify)
	}
	if sw.maxMsgBytes > 0 {
		fmt.Fprintf(&b, " msg %dB", sw.maxMsgBytes)
	}
	fmt.Fprintf(&b, " segment %dKiB", sw.segBytes>>10)
	if sw.snapEvery > 0 {
		fmt.Fprintf(&b, " snapshot every %d keep %d", sw.snapEvery, sw.retain)
	}
	if sw.reorder > 0 {
		fmt.Fprintf(&b, " reorder %.3f", sw.reorder)
	}
	for _, f := range []struct {
		name string
		mean time.Duration
	}{{"partitions", sw.partitionMean}, {"crashes", sw.crashMean}, {"pauses", sw.pauseMean}, {"slow disks", sw.slowMean}} {
		if f.mean > 0 {
			fmt.Fprintf(&b, " %s/%v", f.name, f.mean.Round(time.Millisecond))
		}
	}
	return b.String()
}

func (s *Sim) startFaults() {
	if s.sw.partitionMean > 0 {
		s.after(exponential(s.rng, s.sw.partitionMean), s.partitionFault)
	}
	if s.sw.crashMean > 0 {
		s.after(exponential(s.rng, s.sw.crashMean), s.crashFault)
	}
	if s.sw.pauseMean > 0 {
		s.after(exponential(s.rng, s.sw.pauseMean), s.pauseFault)
	}
	if s.sw.slowMean > 0 {
		s.after(exponential(s.rng, s.sw.slowMean), s.slowFault)
	}
}

func (s *Sim) partitionFault() {
	if s.healing {
		return
	}
	s.net.healAll()
	order := s.rng.Perm(len(s.nodes))
	var desc string
	switch s.rng.IntN(4) {
	case 0:
		x := order[0]
		for o := range s.nodes {
			if o != x {
				s.net.cutBoth(x, o)
			}
		}
		desc = fmt.Sprintf("isolate %d", x+1)
	case 1:
		k := 1 + s.rng.IntN(max(1, (len(s.nodes)-1)/2))
		for _, a := range order[:k] {
			for _, b := range order[k:] {
				s.net.cutBoth(a, b)
			}
		}
		desc = fmt.Sprintf("split %v | %v", ids(order[:k]), ids(order[k:]))
	case 2:
		var links []string
		for range 1 + s.rng.IntN(3) {
			a, b := s.rng.IntN(len(s.nodes)), s.rng.IntN(len(s.nodes))
			if a != b {
				s.net.block(a, b)
				links = append(links, fmt.Sprintf("%d->%d", a+1, b+1))
			}
		}
		desc = "one-way " + strings.Join(links, " ")
	default:
		bridge, rest := order[0], order[1:]
		k := 1 + s.rng.IntN(max(1, len(rest)-1))
		for _, a := range rest[:k] {
			for _, b := range rest[k:] {
				s.net.cutBoth(a, b)
			}
		}
		desc = fmt.Sprintf("bridge %d between %v and %v", bridge+1, ids(rest[:k]), ids(rest[k:]))
	}
	s.stats.Partitions++
	s.tracef("fault: partition %s", desc)
	d := between(s.rng, 200*time.Millisecond, 4*time.Second)
	s.after(d, func() {
		if !s.healing {
			s.net.healAll()
			s.tracef("fault: partition healed")
		}
	})
	s.after(d+exponential(s.rng, s.sw.partitionMean), s.partitionFault)
}

func (s *Sim) crashFault() {
	if s.healing {
		return
	}
	var up []*simNode
	for _, sn := range s.nodes {
		if sn.n != nil {
			up = append(up, sn)
		}
	}
	if len(up) > 0 {
		sn := up[s.rng.IntN(len(up))]
		s.crash(sn)
		down := between(s.rng, 50*time.Millisecond, 3*time.Second)
		s.after(down, func() {
			if sn.n == nil {
				s.restart(sn)
			}
		})
	}
	s.after(exponential(s.rng, s.sw.crashMean), s.crashFault)
}

func (s *Sim) pauseFault() {
	if s.healing {
		return
	}
	var candidates []*simNode
	for _, sn := range s.nodes {
		if sn.n != nil && !sn.paused {
			candidates = append(candidates, sn)
		}
	}
	if len(candidates) > 0 {
		sn := candidates[s.rng.IntN(len(candidates))]
		inc := sn.incarnation
		s.pause(sn)
		d := pick(s.rng, between(s.rng, 10*time.Millisecond, 100*time.Millisecond), between(s.rng, 100*time.Millisecond, 2*time.Second))
		s.after(d, func() {
			if sn.paused && sn.incarnation == inc {
				s.resume(sn)
			}
		})
	}
	s.after(exponential(s.rng, s.sw.pauseMean), s.pauseFault)
}

func (s *Sim) slowFault() {
	if s.healing {
		return
	}
	sn := s.nodes[s.rng.IntN(len(s.nodes))]
	sn.disk.slow = 10 + 190*s.rng.Float64()
	s.stats.SlowDisks++
	s.tracef("fault: node %d disk slowed %.0fx", sn.idx+1, sn.disk.slow)
	d := between(s.rng, 200*time.Millisecond, 3*time.Second)
	s.after(d, func() { sn.disk.slow = 1 })
	s.after(d+exponential(s.rng, s.sw.slowMean), s.slowFault)
}

func ids(idx []int) []int {
	out := make([]int, len(idx))
	for i, x := range idx {
		out[i] = x + 1
	}
	return out
}
