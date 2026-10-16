package sim

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/watch"
)

const CatWatch Category = "watch"

type watchSwarm struct {
	watchers int
	history  int
	drop     float64
}

func newWatchSwarm(rng *rand.Rand) watchSwarm {
	ws := watchSwarm{history: 10000}
	if rng.Float64() < 0.5 {
		ws.watchers = 1 + rng.IntN(3)
		ws.history = pick(rng, 16, 256, 4096)
		ws.drop = pick(rng, 0, 0, 0.001, 0.01)
	}
	return ws
}

func (ws watchSwarm) String() string {
	if ws.watchers == 0 {
		return ""
	}
	return fmt.Sprintf(" watchers %d history %d drop %.3f", ws.watchers, ws.history, ws.drop)
}

type simWatcher struct {
	idx     int
	prefix  string
	target  int
	gen     uint64
	node    *simNode
	inc     uint64
	next    uint64
	view    map[string]kv.KeyValue
	started bool
	arrival time.Duration
}

func (s *Sim) startWatchers() {
	prefixes := []string{"k", "lease/", ""}
	for i := range s.ws.watchers {
		w := &simWatcher{idx: i, prefix: prefixes[s.wrng.IntN(len(prefixes))], target: s.wrng.IntN(len(s.nodes))}
		s.watchers = append(s.watchers, w)
		s.after(between(s.wrng, 0, s.opts.Chaos/4), func() {
			w.started = true
			s.watchResync(w)
		})
	}
	if s.ws.watchers > 0 {
		s.after(100*time.Millisecond, s.watchProgress)
	}
}

func (s *Sim) leaderless(sn *simNode) bool {
	return sn.n.LeaderlessTicks() > uint64(2*s.opts.ElectionTick)
}

func (s *Sim) watchProgress() {
	for _, sn := range s.nodes {
		if sn.n == nil || sn.paused {
			continue
		}
		if s.leaderless(sn) {
			sn.sm.Hub.EndAll()
			continue
		}
		sn.sm.Hub.Progress()
	}
	s.after(100*time.Millisecond, s.watchProgress)
}

func (s *Sim) watchSend(w *simWatcher, on func(sn *simNode, g uint64)) {
	w.gen++
	g := w.gen
	s.schedule(event{kind: evNodeCall, node: w.target, on: func(sn *simNode) {
		if w.gen == g {
			on(sn, g)
		}
	}}, s.net.delay(s.wrng))
	s.after(s.sw.timeout, func() {
		if w.gen == g && w.node == nil {
			s.watchRetry(w)
		}
	})
}

func (s *Sim) watchRetry(w *simWatcher) {
	w.gen++
	w.node = nil
	w.target = s.wrng.IntN(len(s.nodes))
	if w.view == nil {
		s.watchResync(w)
		return
	}
	s.watchConnect(w)
}

func (s *Sim) watchResync(w *simWatcher) {
	w.node = nil
	s.watchSend(w, func(sn *simNode, g uint64) {
		kvs, rev := sn.sm.Store.Range(w.prefix, 0), sn.sm.Store.Rev()
		s.after(s.net.delay(s.wrng), func() {
			if w.gen != g {
				return
			}
			s.stats.WatchResyncs++
			w.view = map[string]kv.KeyValue{}
			for _, kv := range kvs {
				w.view[kv.Key] = kv
			}
			if want := s.truthView(w.prefix, rev); !maps.EqualFunc(w.view, want, sameKV) {
				s.chk.report(CatWatch, "watch: watcher %d read %d keys under %q at revision %d from node %d, the log says %d", w.idx, len(w.view), w.prefix, rev, sn.id, len(want))
				return
			}
			w.next = rev + 1
			s.watchConnect(w)
		})
	})
}

func (s *Sim) watchConnect(w *simWatcher) {
	w.node = nil
	s.watchSend(w, func(sn *simNode, g uint64) {
		if s.leaderless(sn) {
			s.after(s.net.delay(s.wrng), func() {
				if w.gen == g {
					s.watchRetry(w)
				}
			})
			return
		}
		send := func(b watch.Batch) bool {
			if s.ws.drop > 0 && s.wrng.Float64() < s.ws.drop {
				s.stats.WatchDrops++
				s.after(s.net.delay(s.wrng), func() {
					if w.gen == g {
						s.watchRetry(w)
					}
				})
				return false
			}
			w.arrival = max(w.arrival, s.now+s.net.delay(s.wrng))
			s.after(w.arrival-s.now, func() { s.watchReceive(w, g, b) })
			return true
		}
		_, err := sn.sm.Hub.Watch(w.prefix, true, w.next, send)
		if err != nil {
			s.after(s.net.delay(s.wrng), func() {
				if w.gen == g {
					s.stats.WatchCompacted++
					s.watchResync(w)
				}
			})
			return
		}
		w.node, w.inc = sn, sn.incarnation
		s.tracef("watch: watcher %d on %q follows node %d from %d", w.idx, w.prefix, sn.id, w.next)
	})
}

func (s *Sim) watchBroken(sn *simNode) {
	for _, w := range s.watchers {
		if w.node == sn && w.inc == sn.incarnation {
			g := w.gen
			s.after(s.net.delay(s.wrng), func() {
				if w.gen == g {
					s.watchRetry(w)
				}
			})
		}
	}
}

func (s *Sim) watchReceive(w *simWatcher, g uint64, b watch.Batch) {
	if w.gen != g {
		return
	}
	if b.Ended {
		s.watchRetry(w)
		return
	}
	if b.Compacted != 0 {
		s.stats.WatchCompacted++
		s.watchResync(w)
		return
	}
	if b.Rev+1 < w.next {
		s.chk.report(CatWatch, "watch: watcher %d was at %d and got a batch for %d", w.idx, w.next-1, b.Rev)
		return
	}
	want := s.truthEvents(w.prefix, w.next, b.Rev)
	if !slices.EqualFunc(b.Events, want, sameEvent) {
		s.chk.report(CatWatch, "watch: watcher %d on %q got %s for revisions %d to %d, the log has %s", w.idx, w.prefix, describe(b.Events), w.next, b.Rev, describe(want))
		return
	}
	for _, ev := range b.Events {
		s.stats.WatchEvents++
		if ev.Type == kv.EventDelete {
			delete(w.view, ev.KV.Key)
		} else {
			w.view[ev.KV.Key] = ev.KV
		}
	}
	w.next = b.Rev + 1
}

func (s *Sim) recordTruth(index uint64, evs []kv.Event) {
	if index <= uint64(len(s.truth)) {
		return
	}
	if index != uint64(len(s.truth))+1 {
		s.chk.safetyf("revision %d applied before revision %d was ever applied", index, len(s.truth)+1)
		return
	}
	s.truth = append(s.truth, slices.Clone(evs))
}

func (s *Sim) truthEvents(prefix string, from, to uint64) []kv.Event {
	var out []kv.Event
	for rev := from; rev <= to && rev <= uint64(len(s.truth)); rev++ {
		for _, ev := range s.truth[rev-1] {
			if strings.HasPrefix(ev.KV.Key, prefix) {
				out = append(out, ev)
			}
		}
	}
	return out
}

func (s *Sim) truthView(prefix string, rev uint64) map[string]kv.KeyValue {
	view := map[string]kv.KeyValue{}
	for _, ev := range s.truthEvents(prefix, 1, rev) {
		if ev.Type == kv.EventDelete {
			delete(view, ev.KV.Key)
		} else {
			view[ev.KV.Key] = ev.KV
		}
	}
	return view
}

func sameKV(a, b kv.KeyValue) bool {
	return a.Key == b.Key && string(a.Value) == string(b.Value) && a.ModRev == b.ModRev && a.CreateRev == b.CreateRev && a.Version == b.Version && a.Lease == b.Lease
}

func sameEvent(a, b kv.Event) bool {
	return a.Type == b.Type && sameKV(a.KV, b.KV)
}

func describe(evs []kv.Event) string {
	if len(evs) == 0 {
		return "nothing"
	}
	var parts []string
	for i, ev := range evs {
		if i == 4 {
			parts = append(parts, fmt.Sprintf("and %d more", len(evs)-4))
			break
		}
		parts = append(parts, fmt.Sprintf("%v %s@%d", ev.Type, ev.KV.Key, ev.KV.ModRev))
	}
	return strings.Join(parts, ", ")
}

func (s *Sim) checkWatchers() {
	l := s.leader()
	if l == nil {
		return
	}
	for _, w := range s.watchers {
		if !w.started {
			continue
		}
		if w.view == nil || w.next <= s.healRev {
			s.chk.report(CatLiveness, "liveness: watcher %d on %q is at revision %d, the cluster was at %d when the faults stopped", w.idx, w.prefix, w.next-1, s.healRev)
			return
		}
		if want := s.truthView(w.prefix, w.next-1); !maps.EqualFunc(w.view, want, sameKV) {
			s.chk.report(CatWatch, "watch: watcher %d's view of %q at revision %d has %d keys, the log says %d", w.idx, w.prefix, w.next-1, len(w.view), len(want))
			return
		}
	}
}
