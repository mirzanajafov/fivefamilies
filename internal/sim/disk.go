package sim

import (
	"fmt"
	"slices"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type memDisk struct {
	hs       raft.HardState
	ents     []raft.Entry
	inflight []node.Write
	lastDone time.Duration
	slow     float64
}

func newMemDisk() *memDisk {
	return &memDisk{slow: 1}
}

func (d *memDisk) submit(w node.Write) {
	d.inflight = append(d.inflight, w)
}

func (d *memDisk) complete(seq uint64, chk *checker, idx int) {
	for len(d.inflight) > 0 && d.inflight[0].Seq <= seq {
		w := d.inflight[0]
		d.inflight = d.inflight[1:]
		d.apply(w, chk, idx)
	}
}

func (d *memDisk) apply(w node.Write, chk *checker, idx int) {
	if !w.HardState.IsEmpty() {
		prev, hs := d.hs, w.HardState
		if hs.Term < prev.Term {
			chk.failf("node %d persisted term %d after term %d", idx+1, hs.Term, prev.Term)
		}
		if hs.Term == prev.Term && prev.Vote != raft.None && hs.Vote != prev.Vote {
			chk.failf("node %d persisted a vote for %d in term %d after voting for %d", idx+1, hs.Vote, hs.Term, prev.Vote)
		}
		d.hs = hs
	}
	if len(w.Entries) == 0 {
		return
	}
	first := w.Entries[0].Index
	if first > uint64(len(d.ents))+1 {
		chk.failf("node %d persisted entry %d with only %d entries on disk", idx+1, first, len(d.ents))
		return
	}
	if first <= uint64(len(d.ents)) {
		chk.overwrite(idx, d.ents[first-1:], w.Entries)
		d.ents = d.ents[:first-1]
	}
	d.ents = append(d.ents, w.Entries...)
}

func (d *memDisk) crash() {
	d.inflight = nil
}

func (d *memDisk) restore() raft.Restore {
	return raft.Restore{HardState: d.hs, Entries: slices.Clone(d.ents)}
}

func (d *memDisk) String() string {
	return fmt.Sprintf("hs %+v, %d entries, %d writes in flight", d.hs, len(d.ents), len(d.inflight))
}
