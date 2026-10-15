package sim

import (
	"fmt"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type entryKey struct {
	term uint64
	data string
}

func keyOf(e raft.Entry) entryKey {
	return entryKey{term: e.Term, data: string(e.Data)}
}

type checker struct {
	leaders     map[uint64]raft.NodeID
	committed   []entryKey
	commitTerm  []uint64
	applied     []entryKey
	nodeChecked []uint64
	failure     string
	overwrites  int
}

func newChecker(nodes int) *checker {
	return &checker{
		leaders:     map[uint64]raft.NodeID{},
		nodeChecked: make([]uint64, nodes),
	}
}

func (c *checker) failf(format string, args ...any) {
	if c.failure == "" {
		c.failure = fmt.Sprintf(format, args...)
	}
}

func (c *checker) failed() bool {
	return c.failure != ""
}

func (c *checker) restarted(idx int) {
	c.nodeChecked[idx] = 0
}

func (c *checker) observe(idx int, r *raft.Raft) {
	if r.State() == raft.StateLeader {
		t := r.Term()
		switch l, ok := c.leaders[t]; {
		case !ok:
			c.leaders[t] = r.ID()
			c.leaderCompleteness(r)
		case l != r.ID():
			c.failf("election safety: nodes %d and %d both lead term %d", l, r.ID(), t)
			return
		}
	}
	for i := c.nodeChecked[idx] + 1; i <= r.Committed(); i++ {
		e, ok := r.EntryAt(i)
		if !ok {
			continue
		}
		k := keyOf(e)
		switch {
		case i <= uint64(len(c.committed)):
			if c.committed[i-1] != k {
				c.failf("state machine safety: node %d committed (t%d) at index %d, but (t%d) was committed there before", r.ID(), k.term, i, c.committed[i-1].term)
				return
			}
			c.commitTerm[i-1] = min(c.commitTerm[i-1], r.Term())
		default:
			c.committed = append(c.committed, k)
			c.commitTerm = append(c.commitTerm, r.Term())
		}
	}
	c.nodeChecked[idx] = max(c.nodeChecked[idx], r.Committed())
}

func (c *checker) leaderCompleteness(r *raft.Raft) {
	for i := range uint64(len(c.committed)) {
		idx := i + 1
		if idx < r.FirstIndex() || c.commitTerm[i] >= r.Term() {
			continue
		}
		e, ok := r.EntryAt(idx)
		if !ok {
			c.failf("leader completeness: node %d leads term %d without committed entry %d (its log ends at %d)", r.ID(), r.Term(), idx, r.LastIndex())
			return
		}
		if keyOf(e) != c.committed[i] {
			c.failf("leader completeness: node %d leads term %d with (t%d) at %d where (t%d) was committed", r.ID(), r.Term(), e.Term, idx, c.committed[i].term)
			return
		}
	}
}

func (c *checker) apply(idx int, last uint64, e raft.Entry) {
	if e.Index != last+1 {
		c.failf("node %d applied entry %d right after %d", idx+1, e.Index, last)
		return
	}
	k := keyOf(e)
	switch {
	case e.Index <= uint64(len(c.applied)):
		if c.applied[e.Index-1] != k {
			c.failf("state machine safety: node %d applied (t%d) at %d where another node applied (t%d)", idx+1, k.term, e.Index, c.applied[e.Index-1].term)
		}
	default:
		c.applied = append(c.applied, k)
	}
}

func (c *checker) overwrite(idx int, old, fresh []raft.Entry) {
	c.overwrites++
	for i := range min(len(old), len(fresh)) {
		index := fresh[i].Index
		if index > uint64(len(c.committed)) {
			return
		}
		if keyOf(fresh[i]) != c.committed[index-1] {
			c.failf("durability: node %d overwrote committed entry %d on disk", idx+1, index)
			return
		}
	}
}

func (c *checker) logMatching(a, b *raft.Raft) {
	start := max(min(a.Committed(), b.Committed())+1, max(a.FirstIndex(), b.FirstIndex()))
	hi := min(a.LastIndex(), b.LastIndex())
	match, matchTerm := uint64(0), uint64(0)
	for i := hi; i >= start && i > 0; i-- {
		ta, _ := a.LogTerm(i)
		tb, _ := b.LogTerm(i)
		if ta == tb {
			match, matchTerm = i, ta
			break
		}
	}
	for i := start; i <= match; i++ {
		ea, _ := a.EntryAt(i)
		eb, _ := b.EntryAt(i)
		if keyOf(ea) != keyOf(eb) {
			c.failf("log matching: nodes %d and %d agree on (t%d) at %d but differ at %d", a.ID(), b.ID(), matchTerm, match, i)
			return
		}
	}
}

type recorder struct {
	chk   *checker
	idx   int
	last  uint64
	store *kv.Store
}

func newRecorder(chk *checker, idx int) *recorder {
	return &recorder{chk: chk, idx: idx, store: kv.NewStore(0)}
}

func (r *recorder) Apply(e raft.Entry) kv.Result {
	r.chk.apply(r.idx, r.last, e)
	r.last = e.Index
	return r.store.Apply(e)
}
