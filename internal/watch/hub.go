package watch

import (
	"fmt"
	"slices"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
)

type Batch struct {
	Events    []kv.Event
	Rev       uint64
	Progress  bool
	Compacted uint64
	Ended     bool
}

type CompactedError struct {
	Start uint64
}

func (e *CompactedError) Error() string {
	return fmt.Sprintf("watch: revisions before %d are no longer kept", e.Start)
}

type entry struct {
	rev uint64
	ev  kv.Event
}

type watcher struct {
	key    string
	prefix bool
	next   uint64
	sent   uint64
	spoke  bool
	send   func(Batch) bool
}

func (w *watcher) matches(ev kv.Event) bool {
	if w.prefix {
		return strings.HasPrefix(ev.KV.Key, w.key)
	}
	return ev.KV.Key == w.key
}

type Hub struct {
	size     int
	ring     []entry
	start    uint64
	rev      uint64
	watchers map[uint64]*watcher
	nextID   uint64
	bugs     planted.Bug
}

func New(size int, rev uint64, bugs planted.Bug) *Hub {
	if size <= 0 {
		size = 10000
	}
	return &Hub{size: size, start: rev + 1, rev: rev, watchers: map[uint64]*watcher{}, bugs: bugs}
}

func (h *Hub) Rev() uint64 {
	return h.rev
}

func (h *Hub) Start() uint64 {
	return h.start
}

func (h *Hub) Len() int {
	return len(h.watchers)
}

func (h *Hub) ids() []uint64 {
	ids := make([]uint64, 0, len(h.watchers))
	for id := range h.watchers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (h *Hub) Publish(rev uint64, evs []kv.Event) {
	if rev <= h.rev {
		panic(fmt.Sprintf("watch: publishing revision %d after %d", rev, h.rev))
	}
	h.rev = rev
	for _, ev := range evs {
		h.ring = append(h.ring, entry{rev: rev, ev: ev})
	}
	if len(h.ring) > 2*h.size {
		over := len(h.ring) - h.size
		cut := h.ring[over-1].rev
		i := over
		for i < len(h.ring) && h.ring[i].rev == cut {
			i++
		}
		h.ring = slices.Clone(h.ring[i:])
		h.start = cut + 1
	}
	if len(h.watchers) == 0 {
		return
	}
	for _, id := range h.ids() {
		w := h.watchers[id]
		if w.next > rev {
			continue
		}
		var out []kv.Event
		for _, ev := range evs {
			if w.matches(ev) {
				out = append(out, ev)
			}
		}
		w.next = rev + 1
		if len(out) == 0 {
			continue
		}
		w.sent, w.spoke = rev, true
		if !w.send(Batch{Events: out, Rev: rev}) {
			delete(h.watchers, id)
		}
	}
}

func (h *Hub) Watch(key string, prefix bool, from uint64, send func(Batch) bool) (uint64, error) {
	if from == 0 {
		from = h.rev + 1
	}
	if from < h.start {
		return 0, &CompactedError{Start: h.start}
	}
	w := &watcher{key: key, prefix: prefix, next: from, send: send}
	first := Batch{Rev: max(h.rev, from-1)}
	for _, e := range h.ring {
		if e.rev >= from && w.matches(e.ev) {
			if e.rev == from && h.bugs.Has(planted.WatchGap) {
				continue
			}
			first.Events = append(first.Events, e.ev)
		}
	}
	w.next, w.sent, w.spoke = first.Rev+1, first.Rev, true
	if !send(first) {
		return 0, nil
	}
	h.nextID++
	h.watchers[h.nextID] = w
	return h.nextID, nil
}

func (h *Hub) Cancel(id uint64) {
	delete(h.watchers, id)
}

func (h *Hub) Progress() {
	for _, id := range h.ids() {
		w := h.watchers[id]
		if w.sent >= h.rev || w.next > h.rev+1 {
			continue
		}
		w.sent = h.rev
		if !w.send(Batch{Rev: h.rev, Progress: true}) {
			delete(h.watchers, id)
		}
	}
}

func (h *Hub) Beat() {
	for id, w := range h.watchers {
		if w.spoke {
			w.spoke = false
			continue
		}
		if !w.send(Batch{Rev: w.next - 1, Progress: true}) {
			delete(h.watchers, id)
		}
	}
}

func (h *Hub) EndAll() {
	for _, id := range h.ids() {
		h.watchers[id].send(Batch{Ended: true})
		delete(h.watchers, id)
	}
}

func (h *Hub) Reset(rev uint64) {
	h.ring = nil
	h.rev = rev
	h.start = rev + 1
	if h.bugs.Has(planted.WatchKeep) {
		for _, w := range h.watchers {
			w.next = max(w.next, rev+1)
		}
		return
	}
	for _, id := range h.ids() {
		h.watchers[id].send(Batch{Compacted: h.start})
		delete(h.watchers, id)
	}
}
