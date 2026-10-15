package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
	"github.com/mirzanajafov/fivefamilies/internal/wal"
)

type durableState struct {
	hs   raft.HardState
	ents []raft.Entry
}

type disk struct {
	fs       *vfs.Mem
	log      *wal.Log
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
	l, st, rec, err := wal.Open(d.fs, wal.Options{SegmentBytes: d.segBytes, Salt: d.salt})
	if err != nil {
		return st, rec, err
	}
	if !recoveredPrefix(d.durable, d.inflight, st) {
		chk.safetyf("durability: node %d recovered %d entries in term %d, which is not its synced state (%d entries, term %d) plus a prefix of the %d writes in flight",
			idx+1, len(st.Entries), st.HardState.Term, len(d.durable.ents), d.durable.hs.Term, len(d.inflight))
	}
	d.log = l
	d.setDurable(durableState{hs: st.HardState, ents: slices.Clone(st.Entries)}, chk, idx)
	d.inflight = nil
	return st, rec, nil
}

func (d *disk) submit(w node.Write) error {
	if err := d.log.Write(w.HardState, w.Entries); err != nil {
		return err
	}
	d.written = append(d.written, w)
	d.queue = append(d.queue, w)
	return nil
}

func (d *disk) sync(chk *checker, idx int) error {
	if err := d.log.Sync(); err != nil {
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
	d.written, d.queue, d.syncing, d.covers, d.log = nil, nil, false, 0, nil
}

func (d *disk) applyDurable(w node.Write, chk *checker, idx int) {
	if !w.HardState.IsEmpty() {
		prev, hs := d.durable.hs, w.HardState
		if hs.Term < prev.Term {
			chk.safetyf("node %d persisted term %d after term %d", idx+1, hs.Term, prev.Term)
		}
		if hs.Term == prev.Term && prev.Vote != raft.None && hs.Vote != prev.Vote {
			chk.safetyf("node %d persisted a vote for %d in term %d after voting for %d", idx+1, hs.Vote, hs.Term, prev.Vote)
		}
		d.durable.hs = hs
	}
	if len(w.Entries) == 0 {
		return
	}
	first := w.Entries[0].Index
	if first > uint64(len(d.durable.ents))+1 {
		chk.safetyf("node %d persisted entry %d with only %d entries on disk", idx+1, first, len(d.durable.ents))
		return
	}
	if first > uint64(len(d.durable.ents)) {
		d.durable.ents = append(d.durable.ents, w.Entries...)
		return
	}
	old := slices.Clone(d.durable.ents[first-1:])
	d.durable.ents = append(d.durable.ents[:first-1], w.Entries...)
	chk.overwrote(idx, old)
}

func (d *disk) setDurable(st durableState, chk *checker, idx int) {
	old := d.durable.ents
	d.durable = st
	for i := range min(len(old), len(st.ents)) {
		if keyOf(old[i]) != keyOf(st.ents[i]) {
			chk.overwrote(idx, old[i:])
			return
		}
	}
	if len(st.ents) < len(old) {
		chk.overwrote(idx, old[len(st.ents):])
	}
}

func recoveredPrefix(base durableState, inflight []node.Write, st raft.Restore) bool {
	matches := func(hs raft.HardState, ents []raft.Entry) bool {
		hs.Commit = min(hs.Commit, uint64(len(ents)))
		return st.HardState == hs && slices.EqualFunc(st.Entries, ents, func(a, b raft.Entry) bool {
			return a.Index == b.Index && keyOf(a) == keyOf(b)
		})
	}
	hs, ents := base.hs, slices.Clone(base.ents)
	if matches(hs, ents) {
		return true
	}
	for _, w := range inflight {
		if !w.HardState.IsEmpty() {
			hs = w.HardState
			if matches(hs, ents) {
				return true
			}
		}
		for _, e := range w.Entries {
			if e.Index > uint64(len(ents))+1 {
				return false
			}
			ents = append(ents[:e.Index-1], e)
			if matches(hs, ents) {
				return true
			}
		}
	}
	return false
}

func (d *disk) String() string {
	return fmt.Sprintf("durable hs %+v, %d durable entries, %d writes in flight", d.durable.hs, len(d.durable.ents), len(d.inflight))
}
