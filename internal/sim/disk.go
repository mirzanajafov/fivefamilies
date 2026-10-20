package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/storage"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type durableState struct {
	hs   raft.HardState
	snap *raft.Snapshot
	ents []raft.Entry
}

func (d durableState) offset() uint64 {
	if d.snap == nil {
		return 0
	}
	return d.snap.Index
}

func (d durableState) last() uint64 {
	return d.offset() + uint64(len(d.ents))
}

func (d durableState) clone() durableState {
	return durableState{hs: d.hs, snap: d.snap, ents: slices.Clone(d.ents)}
}

func (d durableState) entry(index uint64) (raft.Entry, bool) {
	if index <= d.offset() || index > d.last() {
		return raft.Entry{}, false
	}
	return d.ents[index-d.offset()-1], true
}

type disk struct {
	fs       *vfs.Mem
	store    *storage.Disk
	segBytes int64
	salt     func() uint64
	durable  durableState
	written  []node.Write
	queue    []node.Write
	inflight []node.Write
	syncing  bool
	covers   int
	slow     float64
}

func newDisk(segBytes int64, salt func() uint64) *disk {
	return &disk{fs: vfs.NewMem(), segBytes: segBytes, salt: salt, slow: 1}
}

func (d *disk) open(chk *checker, idx int) (raft.Restore, wal.Recovery, error) {
	store, st, rec, err := storage.Open(d.fs, wal.Options{SegmentBytes: d.segBytes, Salt: d.salt})
	if err != nil {
		return st, rec, err
	}
	if !recoveredPrefix(d.durable, d.inflight, st) {
		chk.safetyf("durability: node %d recovered %d entries after snapshot %v in term %d, which is not its synced state (%d entries after %d, term %d) plus a prefix of the %d writes in flight",
			idx+1, len(st.Entries), st.Snapshot != nil, st.HardState.Term, len(d.durable.ents), d.durable.offset(), d.durable.hs.Term, len(d.inflight))
	}
	d.store = store
	recovered := durableState{hs: st.HardState, ents: slices.Clone(st.Entries)}
	if st.Snapshot != nil {
		recovered.snap = &raft.Snapshot{Index: st.Snapshot.Index, Term: st.Snapshot.Term}
	}
	d.setDurable(recovered, chk, idx)
	d.inflight = nil
	return st, rec, nil
}

func (d *disk) submit(w node.Write) error {
	if err := d.store.Write(w.Batch); err != nil {
		return err
	}
	d.written = append(d.written, w)
	d.queue = append(d.queue, w)
	return nil
}

func (d *disk) sync(chk *checker, idx int) error {
	if err := d.store.Sync(); err != nil {
		return err
	}
	n := min(d.covers, len(d.written))
	if d.covers == 0 {
		n = len(d.written)
	}
	for _, w := range d.written[:n] {
		d.applyDurable(w, chk, idx)
	}
	d.written = d.written[n:]
	d.covers = 0
	return nil
}

func (d *disk) crash(rng *rand.Rand) {
	d.fs.Crash(rng)
	d.inflight = d.written
	d.written, d.queue, d.syncing, d.covers, d.store = nil, nil, false, 0, nil
}

func applyWrite(st *durableState, w node.Write, each func()) bool {
	if !w.HardState.IsEmpty() {
		st.hs = w.HardState
		each()
	}
	if w.Snapshot != nil {
		if w.Reset || w.Snapshot.Index > st.last() {
			st.ents = nil
		} else if w.Snapshot.Index > st.offset() {
			st.ents = slices.Clone(st.ents[w.Snapshot.Index-st.offset():])
		}
		st.snap = &raft.Snapshot{Index: w.Snapshot.Index, Term: w.Snapshot.Term}
		each()
	}
	for _, e := range w.Entries {
		if e.Index <= st.offset() || e.Index > st.last()+1 {
			return false
		}
		st.ents = append(st.ents[:e.Index-st.offset()-1], e)
		each()
	}
	return true
}

func (d *disk) applyDurable(w node.Write, chk *checker, idx int) {
	cur := &d.durable
	if !w.HardState.IsEmpty() {
		if w.HardState.Term < cur.hs.Term {
			chk.safetyf("node %d persisted term %d after term %d", idx+1, w.HardState.Term, cur.hs.Term)
		}
		if w.HardState.Term == cur.hs.Term && cur.hs.Vote != raft.None && w.HardState.Vote != cur.hs.Vote {
			chk.safetyf("node %d persisted a vote for %d in term %d after voting for %d", idx+1, w.HardState.Vote, w.HardState.Term, cur.hs.Vote)
		}
		cur.hs = w.HardState
	}
	if s := w.Snapshot; s != nil {
		var lost []raft.Entry
		switch {
		case w.Reset || s.Index > cur.last():
			for _, e := range cur.ents {
				if e.Index > s.Index {
					lost = append(lost, e)
				}
			}
			cur.ents = nil
		case s.Index > cur.offset():
			cur.ents = slices.Clone(cur.ents[s.Index-cur.offset():])
		}
		cur.snap = &raft.Snapshot{Index: s.Index, Term: s.Term}
		if len(lost) > 0 {
			chk.overwrote(idx, lost)
		}
	}
	if len(w.Entries) == 0 {
		return
	}
	first := w.Entries[0].Index
	if first <= cur.offset() || first > cur.last()+1 {
		chk.safetyf("node %d persisted entries %d.. that do not follow its log (after %d, up to %d)", idx+1, first, cur.offset(), cur.last())
		return
	}
	if first > cur.last() {
		cur.ents = append(cur.ents, w.Entries...)
		return
	}
	pos := first - cur.offset() - 1
	old := slices.Clone(cur.ents[pos:])
	cur.ents = append(cur.ents[:pos], w.Entries...)
	var lost []raft.Entry
	for _, e := range old {
		if now, ok := cur.entry(e.Index); !ok || keyOf(now) != keyOf(e) {
			lost = append(lost, e)
		}
	}
	if len(lost) > 0 {
		chk.overwrote(idx, lost)
	}
}

func (d *disk) setDurable(st durableState, chk *checker, idx int) {
	prev := d.durable
	d.durable = st
	d.reportLost(prev, chk, idx)
}

func (d *disk) reportLost(prev durableState, chk *checker, idx int) {
	var lost []raft.Entry
	for _, e := range prev.ents {
		if e.Index <= d.durable.offset() {
			continue
		}
		if cur, ok := d.durable.entry(e.Index); ok && keyOf(cur) == keyOf(e) {
			continue
		}
		lost = append(lost, e)
	}
	if len(lost) > 0 {
		chk.overwrote(idx, lost)
	}
}

func recoveredPrefix(base durableState, inflight []node.Write, st raft.Restore) bool {
	matches := func(c durableState) bool {
		hs := c.hs
		hs.Commit = max(min(hs.Commit, c.last()), c.offset())
		if st.HardState != hs {
			return false
		}
		if (st.Snapshot == nil) != (c.snap == nil) {
			return false
		}
		if st.Snapshot != nil && (st.Snapshot.Index != c.snap.Index || st.Snapshot.Term != c.snap.Term) {
			return false
		}
		return slices.EqualFunc(st.Entries, c.ents, func(a, b raft.Entry) bool {
			return a.Index == b.Index && keyOf(a) == keyOf(b)
		})
	}
	cur := base.clone()
	ok := matches(cur)
	for _, w := range inflight {
		if ok {
			return true
		}
		if !applyWrite(&cur, w, func() { ok = ok || matches(cur) }) {
			return ok
		}
	}
	return ok
}

func (d *disk) String() string {
	return fmt.Sprintf("durable hs %+v, %d durable entries after %d, %d writes in flight", d.durable.hs, len(d.durable.ents), d.durable.offset(), len(d.inflight))
}
