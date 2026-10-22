package controller

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
)

type NodeView struct {
	ID       int     `json:"id"`
	Up       bool    `json:"up"`
	State    string  `json:"state"`
	Term     uint64  `json:"term"`
	Leader   int     `json:"leader"`
	Commit   uint64  `json:"commit"`
	Applied  uint64  `json:"applied"`
	Last     uint64  `json:"last"`
	First    uint64  `json:"first"`
	Snapshot uint64  `json:"snapshot"`
	Voter    bool    `json:"voter"`
	Learner  bool    `json:"learner"`
	MsgsPerS float64 `json:"msgs_per_s"`
	Fault    string  `json:"fault,omitempty"`
}

type Event struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	Node int       `json:"node,omitempty"`
	Term uint64    `json:"term,omitempty"`
	Text string    `json:"text"`
}

type Window struct {
	Index  int           `json:"index"`
	From   time.Time     `json:"from"`
	To     time.Time     `json:"to"`
	Ops    int           `json:"ops"`
	Result string        `json:"result"`
	Took   time.Duration `json:"took_ns"`
	Note   string        `json:"note,omitempty"`
}

type Failover struct {
	Gap       time.Duration `json:"client_gap_ns"`
	Election  time.Duration `json:"election_ns"`
	NewLeader int           `json:"new_leader"`
}

type Fault struct {
	ID        int           `json:"id"`
	Action    string        `json:"action"`
	Node      int           `json:"node"`
	Duration  time.Duration `json:"duration_ns"`
	Started   time.Time     `json:"started"`
	Ends      time.Time     `json:"ends"`
	By        string        `json:"by"`
	OnLeader  bool          `json:"on_leader"`
	OldLeader int           `json:"old_leader"`
	OldTerm   uint64        `json:"old_term"`
	Failover  *Failover     `json:"failover,omitempty"`
}

func (f *Fault) clone() *Fault {
	c := *f
	if f.Failover != nil {
		fo := *f.Failover
		c.Failover = &fo
	}
	return &c
}

type Load struct {
	Active  bool    `json:"active"`
	OpsPerS float64 `json:"ops_per_s"`
	Viewers int     `json:"viewers"`
}

type Limits struct {
	MaxSeconds      int       `json:"max_seconds"`
	CooldownSeconds int       `json:"cooldown_seconds"`
	NextAllowed     time.Time `json:"next_allowed,omitzero"`
}

type Snapshot struct {
	At      time.Time  `json:"at"`
	Nodes   []NodeView `json:"nodes"`
	Leader  int        `json:"leader"`
	Term    uint64     `json:"term"`
	Events  []Event    `json:"events"`
	Windows []Window   `json:"windows"`
	Fault   *Fault     `json:"fault,omitempty"`
	Last    *Fault     `json:"last,omitempty"`
	Load    Load       `json:"load"`
	Limits  Limits     `json:"limits"`
}

func (c *Controller) poll(ctx context.Context) {
	for {
		c.pollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.pollEvery()):
		}
	}
}

func (c *Controller) pollEvery() time.Duration {
	active, _ := c.load.view()
	watched := c.viewers.count() > 0
	c.mu.Lock()
	defer c.mu.Unlock()
	if watched || active || c.fault != nil {
		return c.cfg.Poll
	}
	return c.cfg.IdlePoll
}

func (c *Controller) statuses(ctx context.Context) []*pb.StatusResponse {
	out := make([]*pb.StatusResponse, len(c.admins))
	var wg sync.WaitGroup
	for i, a := range c.admins {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
			defer cancel()
			if st, err := a.Status(cctx, &pb.StatusRequest{}); err == nil {
				out[i] = st
			}
		})
	}
	wg.Wait()
	return out
}

func (c *Controller) pollOnce(ctx context.Context) {
	sts := c.statuses(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.observe(sts, time.Now())
}

func (c *Controller) observe(sts []*pb.StatusResponse, now time.Time) {
	elapsed := now.Sub(c.prevAt).Seconds()
	leader, term := 0, uint64(0)
	for i, st := range sts {
		v := &c.nodes[i]
		wasUp := v.Up
		switch {
		case st != nil:
			c.silent[i], v.Up = time.Time{}, true
		case c.silent[i].IsZero():
			c.silent[i] = now
		}
		if st == nil && (c.prevAt.IsZero() || now.Sub(c.silent[i]) >= c.cfg.DownAfter) {
			v.Up = false
		}
		if v.Up != wasUp && !c.prevAt.IsZero() {
			if v.Up {
				c.event("up", i+1, 0, fmt.Sprintf("node %d answers again", i+1))
			} else {
				c.event("down", i+1, 0, fmt.Sprintf("node %d stopped answering", i+1))
			}
		}
		if st == nil {
			if !v.Up {
				v.MsgsPerS = 0
			}
			continue
		}
		v.State, v.Term, v.Leader = client.RoleName(st.GetRole()), st.GetTerm(), int(st.GetLeader())
		v.Commit, v.Applied, v.Last, v.First, v.Snapshot = st.GetCommit(), st.GetApplied(), st.GetLastIndex(), st.GetFirstIndex(), st.GetSnapshotIndex()
		v.Voter = slices.Contains(st.GetVoters(), uint64(i+1))
		v.Learner = slices.Contains(st.GetLearners(), uint64(i+1))
		if p := c.prev; p != nil && p[i] != nil && elapsed > 0 && st.GetMsgsSent() >= p[i].GetMsgsSent() {
			v.MsgsPerS = float64(st.GetMsgsSent()-p[i].GetMsgsSent()) / elapsed
		}
		if st.GetRole() == pb.Role_ROLE_LEADER && st.GetTerm() >= term {
			leader, term = i+1, st.GetTerm()
		}
		if leader == i+1 {
			if vs := fmt.Sprint(st.GetVoters(), st.GetLearners()); vs != c.voters {
				if c.voters != "" {
					c.event("membership", i+1, term, fmt.Sprintf("voters %v, learners %v", st.GetVoters(), st.GetLearners()))
				}
				c.voters = vs
			}
		}
	}
	if leader != 0 && (leader != c.leader || term != c.term) {
		c.event("succession", leader, term, fmt.Sprintf("node %d leads term %d", leader, term))
		if f := c.fault; f != nil && f.OnLeader && leader != f.OldLeader && f.Failover == nil {
			f.Failover = &Failover{Election: now.Sub(f.Started), NewLeader: leader}
		}
	}
	if leader != 0 {
		c.leader, c.term = leader, term
	}
	c.prev, c.prevAt = sts, now
	if f := c.fault; f != nil && now.After(f.Ends.Add(5*time.Second)) {
		c.closeFault(f)
	}
	for i := range c.nodes {
		c.nodes[i].Fault = ""
		if f := c.fault; f != nil && f.Node == i+1 && now.Before(f.Ends) {
			c.nodes[i].Fault = f.Action
		}
	}
}

func (c *Controller) closeFault(f *Fault) {
	if f.OnLeader {
		if f.Failover == nil {
			f.Failover = &Failover{}
		}
		f.Failover.Gap = c.load.widestGap(f.Started.Add(-2*time.Second), f.Ends.Add(5*time.Second))
	}
	c.event("healed", f.Node, 0, fmt.Sprintf("%s on node %d is over", f.Action, f.Node))
	c.last, c.fault = f, nil
}
