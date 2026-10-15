package raft

import (
	"math/rand/v2"
	"slices"
	"testing"
)

type stableStore struct {
	hs   HardState
	snap *Snapshot
	ents []Entry
}

func (s *stableStore) offset() uint64 {
	if s.snap == nil {
		return 0
	}
	return s.snap.Index
}

func (s *stableStore) write(rd Ready) {
	if rd.Snapshot != nil {
		s.snap = rd.Snapshot
		s.ents = nil
	}
	if !rd.HardState.IsEmpty() {
		s.hs = rd.HardState
	}
	if len(rd.Entries) > 0 {
		keep := rd.Entries[0].Index - s.offset() - 1
		s.ents = append(s.ents[:keep:keep], rd.Entries...)
	}
}

func (s *stableStore) compact(snap Snapshot) {
	drop := snap.Index - s.offset()
	s.ents = append([]Entry(nil), s.ents[drop:]...)
	s.snap = &snap
}

type cluster struct {
	t       *testing.T
	cfgs    []Config
	nodes   []*Raft
	stores  []*stableStore
	applied [][]Entry
	down    []bool
	cut     map[[2]NodeID]bool
	queue   []Message
}

func testConfig(id NodeID, voters []NodeID) Config {
	return Config{
		ID:            id,
		Voters:        voters,
		ElectionTick:  10,
		HeartbeatTick: 1,
		PreVote:       true,
		CheckQuorum:   true,
		Rand:          rand.NewPCG(7, uint64(id)),
	}
}

func newCluster(t *testing.T, n int, tweak func(*Config)) *cluster {
	t.Helper()
	ids := make([]NodeID, n)
	for i := range ids {
		ids[i] = NodeID(i + 1)
	}
	c := &cluster{t: t, cut: map[[2]NodeID]bool{}}
	for _, id := range ids {
		cfg := testConfig(id, ids)
		if tweak != nil {
			tweak(&cfg)
		}
		r, err := New(cfg, Restore{})
		if err != nil {
			t.Fatal(err)
		}
		c.cfgs = append(c.cfgs, cfg)
		c.nodes = append(c.nodes, r)
		c.stores = append(c.stores, &stableStore{})
		c.applied = append(c.applied, nil)
		c.down = append(c.down, false)
	}
	return c
}

func (c *cluster) node(id NodeID) *Raft {
	return c.nodes[id-1]
}

func (c *cluster) drain() {
	c.t.Helper()
	for steps := 0; ; steps++ {
		if steps > 100000 {
			c.t.Fatal("cluster did not settle")
		}
		for i, r := range c.nodes {
			if c.down[i] || !r.HasReady() {
				continue
			}
			rd := r.Ready()
			c.stores[i].write(rd)
			c.applied[i] = append(c.applied[i], rd.Committed...)
			c.queue = append(c.queue, rd.Messages...)
			c.queue = append(c.queue, rd.AfterPersist...)
		}
		if len(c.queue) == 0 {
			return
		}
		m := c.queue[0]
		c.queue = c.queue[1:]
		if c.down[m.To-1] || c.down[m.From-1] || c.cut[[2]NodeID{m.From, m.To}] {
			continue
		}
		c.node(m.To).Step(m)
	}
}

func (c *cluster) tickAll(n int) {
	for range n {
		for i, r := range c.nodes {
			if !c.down[i] {
				r.Tick()
			}
		}
		c.drain()
	}
}

func (c *cluster) tick(id NodeID, n int) {
	for range n {
		c.node(id).Tick()
		c.drain()
	}
}

func (c *cluster) elect(id NodeID) {
	c.t.Helper()
	c.node(id).Campaign()
	c.drain()
	if st := c.node(id).State(); st != StateLeader {
		c.t.Fatalf("node %d is %s after campaigning, want leader", id, st)
	}
}

func (c *cluster) isolate(id NodeID) {
	for i := range c.nodes {
		other := NodeID(i + 1)
		if other != id {
			c.cut[[2]NodeID{id, other}] = true
			c.cut[[2]NodeID{other, id}] = true
		}
	}
}

func (c *cluster) heal() {
	clear(c.cut)
}

func (c *cluster) crash(id NodeID) {
	c.down[id-1] = true
}

func (c *cluster) restart(id NodeID) {
	c.t.Helper()
	s := c.stores[id-1]
	r, err := New(c.cfgs[id-1], Restore{HardState: s.hs, Snapshot: s.snap, Entries: slices.Clone(s.ents)})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nodes[id-1] = r
	c.down[id-1] = false
	c.applied[id-1] = nil
}

func (c *cluster) propose(id NodeID, data string) uint64 {
	c.t.Helper()
	idx, err := c.node(id).Propose([]byte(data))
	if err != nil {
		c.t.Fatal(err)
	}
	c.drain()
	return idx
}

func (c *cluster) leaders() []NodeID {
	var out []NodeID
	for i, r := range c.nodes {
		if !c.down[i] && r.State() == StateLeader {
			out = append(out, r.ID())
		}
	}
	return out
}

func appliedData(ents []Entry) []string {
	var out []string
	for _, e := range ents {
		if len(e.Data) > 0 {
			out = append(out, string(e.Data))
		}
	}
	return out
}
