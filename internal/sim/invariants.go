package sim

import (
	"fmt"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type entryKey struct {
	term uint64
	typ  raft.EntryType
	data string
}

func keyOf(e raft.Entry) entryKey {
	return entryKey{term: e.Term, typ: e.Type, data: string(e.Data)}
}

type Category string

const (
	CatSafety          Category = "safety"
	CatLiveness        Category = "liveness"
	CatLinearizability Category = "linearizability"
	CatState           Category = "state"
	CatClient          Category = "client"
	CatPanic           Category = "panic"
)

type leadership struct {
	id          raft.NodeID
	incarnation int
}

type checker struct {
	leaders      map[uint64]leadership
	incarnations []int
	committed    []entryKey
	commitTerm   []uint64
	applied      []entryKey
	nodeChecked  []uint64
	failure      string
	category     Category
	mute         bool
	muted        string
	detected     map[Category]string
	overwrites   int
	disks        []*disk
	conf         raft.ConfState
	onExpire     func(uint64)
}

func newChecker(nodes int) *checker {
	return &checker{
		leaders:      map[uint64]leadership{},
		detected:     map[Category]string{},
		incarnations: make([]int, nodes),
		nodeChecked:  make([]uint64, nodes),
	}
}

func (c *checker) report(cat Category, format string, args ...any) {
	if _, seen := c.detected[cat]; !seen {
		c.detected[cat] = fmt.Sprintf(format, args...)
	}
	if c.failure != "" {
		return
	}
	if cat == CatSafety && c.mute {
		if c.muted == "" {
			c.muted = fmt.Sprintf(format, args...)
		}
		return
	}
	c.failure, c.category = fmt.Sprintf(format, args...), cat
}

func (c *checker) safetyf(format string, args ...any) {
	c.report(CatSafety, format, args...)
}

func (c *checker) failed() bool {
	return c.failure != ""
}

func (c *checker) restarted(idx int) {
	c.nodeChecked[idx] = 0
	c.incarnations[idx]++
}

func (c *checker) observe(idx int, r *raft.Raft) {
	if r.State() == raft.StateLeader {
		t := r.Term()
		me := leadership{id: r.ID(), incarnation: c.incarnations[idx]}
		switch l, ok := c.leaders[t]; {
		case !ok:
			c.leaders[t] = me
			c.leaderCompleteness(r)
		case l.id != me.id:
			c.safetyf("election safety: nodes %d and %d both lead term %d", l.id, me.id, t)
			return
		case l.incarnation != me.incarnation:
			c.safetyf("election safety: node %d leads term %d a second time after a restart", me.id, t)
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
				c.safetyf("state machine safety: node %d committed (t%d) at index %d, but (t%d) was committed there before", r.ID(), k.term, i, c.committed[i-1].term)
				return
			}
			c.commitTerm[i-1] = min(c.commitTerm[i-1], r.Term())
		default:
			c.committed = append(c.committed, k)
			c.commitTerm = append(c.commitTerm, r.Term())
			if e.Type == raft.EntryConfig {
				if cc, err := raft.UnmarshalConfChange(e.Data); err == nil {
					if next, err := c.conf.Apply(cc); err == nil {
						c.conf = next
					}
				}
			}
			if n, need := c.durableOn(i); n < need {
				c.safetyf("durability: node %d sees entry %d committed while it is durable on %d of the voters %v, and every quorum of them needs to meet %d", r.ID(), i, n, c.conf.Voters, need)
				return
			}
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
			c.safetyf("leader completeness: node %d leads term %d without committed entry %d (its log ends at %d)", r.ID(), r.Term(), idx, r.LastIndex())
			return
		}
		if keyOf(e) != c.committed[i] {
			c.safetyf("leader completeness: node %d leads term %d with (t%d) at %d where (t%d) was committed", r.ID(), r.Term(), e.Term, idx, c.committed[i].term)
			return
		}
	}
}

func (c *checker) apply(idx int, last uint64, e raft.Entry) {
	if e.Index != last+1 {
		c.safetyf("node %d applied entry %d right after %d", idx+1, e.Index, last)
		return
	}
	k := keyOf(e)
	switch {
	case e.Index <= uint64(len(c.applied)):
		if c.applied[e.Index-1] != k {
			c.safetyf("state machine safety: node %d applied (t%d) at %d where another node applied (t%d)", idx+1, k.term, e.Index, c.applied[e.Index-1].term)
		}
	default:
		c.applied = append(c.applied, k)
	}
}

func (c *checker) durableOn(index uint64) (int, int) {
	need := len(c.conf.Voters) - (len(c.conf.Voters)/2 + 1) + 1
	if index == 0 || index > uint64(len(c.committed)) {
		return 0, need
	}
	want := c.committed[index-1]
	n := 0
	for _, id := range c.conf.Voters {
		d := c.disks[id-1]
		if index <= d.durable.offset() {
			n++
		} else if e, ok := d.durable.entry(index); ok && keyOf(e) == want {
			n++
		}
	}
	return n, need
}

func (c *checker) overwrote(idx int, old []raft.Entry) {
	c.overwrites++
	for _, e := range old {
		if e.Index > uint64(len(c.committed)) {
			return
		}
		if keyOf(e) != c.committed[e.Index-1] {
			continue
		}
		if n, need := c.durableOn(e.Index); n < need {
			c.safetyf("durability: node %d overwrote committed entry %d, which is now durable on %d of the voters %v, and every quorum of them needs to meet %d", idx+1, e.Index, n, c.conf.Voters, need)
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
			c.safetyf("log matching: nodes %d and %d agree on (t%d) at %d but differ at %d", a.ID(), b.ID(), matchTerm, match, i)
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
	if e.Type == raft.EntryNormal && len(e.Data) > 0 && kv.Op(e.Data[0]) == kv.OpLeaseExpire && r.chk.onExpire != nil {
		if cmd, err := kv.Decode(e.Data); err == nil {
			r.chk.onExpire(cmd.Lease)
		}
	}
	r.last = e.Index
	return r.store.Apply(e)
}

func (r *recorder) Get(key string) kv.Result {
	kvp, found := r.store.Get(key)
	return kv.Result{Rev: r.store.Rev(), Found: found, KV: kvp, Ok: true}
}

func (r *recorder) Snapshot() []byte {
	return r.store.Snapshot()
}

func (r *recorder) Lease(id uint64) (kv.LeaseInfo, bool) {
	return r.store.Lease(id)
}

func (r *recorder) Leases() []kv.LeaseInfo {
	return r.store.Leases()
}

func (r *recorder) Restore(index uint64, data []byte) error {
	s, err := kv.Restore(data, 0)
	if err != nil {
		return err
	}
	if s.Rev() != index {
		return fmt.Errorf("snapshot at %d holds a store at revision %d", index, s.Rev())
	}
	r.store, r.last = s, index
	return nil
}
