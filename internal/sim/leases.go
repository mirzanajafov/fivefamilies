package sim

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
)

const CatLease Category = "lease"

type leaseSwarm struct {
	holders int
	ttlLo   time.Duration
	ttlHi   time.Duration
}

func newLeaseSwarm(rng *rand.Rand, election time.Duration) leaseSwarm {
	var ls leaseSwarm
	if rng.Float64() < 0.5 {
		ls.holders = 1 + rng.IntN(4)
		ls.ttlLo = election * 3 / 2
		ls.ttlHi = ls.ttlLo + time.Duration(rng.IntN(8))*election
	}
	return ls
}

func (ls leaseSwarm) String() string {
	if ls.holders == 0 {
		return ""
	}
	return fmt.Sprintf(" leases %d ttl %v-%v", ls.holders, ls.ttlLo, ls.ttlHi)
}

type holderStep uint8

const (
	stepGrant holderStep = iota
	stepPut
	stepKeep
	stepRevoke
	stepIdle
)

type leaseHolder struct {
	idx     int
	target  int
	attempt uint64
	step    holderStep
	busy    bool
	lease   uint64
	ttl     time.Duration
	cycleTo time.Duration
}

type leaseTrack struct {
	holder  int
	ttl     time.Duration
	anchor  time.Duration
	expired time.Duration
	gone    bool
}

func (s *Sim) startLeaseHolders() {
	for i := range s.ls.holders {
		h := &leaseHolder{idx: i, target: s.lrng.IntN(len(s.nodes))}
		s.after(between(s.lrng, 0, s.opts.Chaos/4), func() { s.holderCycle(h) })
	}
}

func (s *Sim) holdersStopAt() time.Duration {
	return s.opts.Chaos
}

func (s *Sim) holderCycle(h *leaseHolder) {
	if s.now >= s.holdersStopAt()-time.Second {
		h.step = stepIdle
		return
	}
	h.lease = 0
	h.ttl = between(s.lrng, s.ls.ttlLo, s.ls.ttlHi+time.Millisecond).Truncate(time.Millisecond)
	h.cycleTo = s.now + between(s.lrng, h.ttl, 6*h.ttl)
	h.step = stepGrant
	s.holderSend(h)
}

func (s *Sim) holderSend(h *leaseHolder) {
	h.attempt++
	h.busy = true
	a, sent := h.attempt, s.now
	reply := func(res kv.Result, err error) {
		if s.net.loss > 0 && s.lrng.Float64() < s.net.loss {
			return
		}
		s.after(s.net.delay(s.lrng), func() { s.holderResponse(h, a, sent, res, err) })
	}
	var on func(sn *simNode)
	switch h.step {
	case stepGrant:
		s.stats.LeaseGrants++
		cmd := kv.Command{Op: kv.OpLeaseGrant, TTL: uint64(h.ttl / time.Millisecond)}
		on = func(sn *simNode) { sn.n.Submit(cmd, reply) }
	case stepPut:
		cmd := kv.Command{Op: kv.OpPut, Key: fmt.Sprintf("lease/%d/%d", h.idx, h.lease), Value: []byte("held"), Lease: h.lease}
		on = func(sn *simNode) { sn.n.Submit(cmd, reply) }
	case stepKeep:
		s.stats.KeepAlives++
		id := h.lease
		on = func(sn *simNode) { sn.n.KeepAlive(id, reply) }
	case stepRevoke:
		cmd := kv.Command{Op: kv.OpLeaseRevoke, Lease: h.lease}
		on = func(sn *simNode) { sn.n.Submit(cmd, reply) }
	default:
		h.busy = false
		return
	}
	if !(s.net.loss > 0 && s.lrng.Float64() < s.net.loss) {
		s.schedule(event{kind: evNodeCall, node: h.target, on: on}, s.net.delay(s.lrng))
	}
	s.after(s.sw.timeout, func() {
		if h.attempt == a && h.busy {
			h.target = s.lrng.IntN(len(s.nodes))
			s.holderSend(h)
		}
	})
}

func (s *Sim) holderResponse(h *leaseHolder, attempt uint64, sent time.Duration, res kv.Result, err error) {
	if attempt != h.attempt || !h.busy {
		return
	}
	if err != nil {
		var nl *node.NotLeaderError
		if errors.As(err, &nl) && nl.Leader != 0 && int(nl.Leader)-1 != h.target {
			h.target = int(nl.Leader) - 1
			s.holderSend(h)
			return
		}
		h.attempt++
		a := h.attempt
		s.after(between(s.lrng, 10*time.Millisecond, 50*time.Millisecond), func() {
			if h.attempt == a && h.busy {
				h.target = s.lrng.IntN(len(s.nodes))
				s.holderSend(h)
			}
		})
		return
	}
	h.busy = false
	switch h.step {
	case stepGrant:
		if !res.Ok {
			s.after(between(s.lrng, 10*time.Millisecond, 50*time.Millisecond), func() { s.holderCycle(h) })
			return
		}
		h.lease = res.Lease
		s.leases[h.lease] = &leaseTrack{holder: h.idx, ttl: h.ttl, anchor: sent}
		s.checkLease(h.lease)
		h.step = stepPut
		s.holderSend(h)
	case stepPut, stepKeep:
		if res.Code == kv.ErrLeaseNotFound {
			s.stats.LeasesLost++
			s.tracef("lease: holder %d lost lease %d", h.idx, h.lease)
			s.holderCycle(h)
			return
		}
		if t := s.leases[h.lease]; h.step == stepKeep && t != nil {
			t.anchor = max(t.anchor, sent)
			s.checkLease(h.lease)
		}
		s.holderNext(h)
	case stepRevoke:
		if t := s.leases[h.lease]; t != nil {
			t.gone = true
		}
		s.holderCycle(h)
	}
}

func (s *Sim) holderNext(h *leaseHolder) {
	if s.now >= s.holdersStopAt() {
		h.step = stepIdle
		return
	}
	if s.now >= h.cycleTo {
		if s.lrng.IntN(2) == 0 {
			h.step = stepRevoke
			s.holderSend(h)
			return
		}
		s.tracef("lease: holder %d abandons lease %d", h.idx, h.lease)
		s.holderCycle(h)
		return
	}
	h.step = stepKeep
	s.after(h.ttl/3, func() {
		if h.step == stepKeep && !h.busy {
			s.holderSend(h)
		}
	})
}

func (s *Sim) leaseExpired(id uint64) {
	t := s.leases[id]
	if t == nil {
		t = &leaseTrack{holder: -1}
		s.leases[id] = t
	}
	if t.expired == 0 {
		t.expired = s.now
		s.stats.LeaseExpiries++
		s.checkLease(id)
	}
}

func (s *Sim) checkLease(id uint64) {
	t := s.leases[id]
	if t == nil || t.holder < 0 || t.expired == 0 {
		return
	}
	life := time.Duration(float64(t.ttl) / (1 + s.sw.clockSpread))
	if t.expired < t.anchor+life {
		s.chk.report(CatLease, "lease: lease %d of holder %d (TTL %v) expired at %v, %v after a keepalive sent at %v was acknowledged; it must live at least %v",
			id, t.holder, t.ttl, t.expired, t.expired-t.anchor, t.anchor, life)
	}
}

func (s *Sim) checkLeasesExpired() {
	l := s.leader()
	if l == nil {
		return
	}
	for _, info := range l.sm.Store.Leases() {
		s.chk.report(CatLiveness, "liveness: lease %d (TTL %vms) is still alive %v after every holder stopped", info.ID, info.TTL, s.now-s.holdersStopAt())
		return
	}
	for _, kv := range l.sm.Store.Range("lease/", 1) {
		s.chk.report(CatLiveness, "liveness: key %s outlived its lease", kv.Key)
		return
	}
}
