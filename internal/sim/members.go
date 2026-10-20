package sim

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

const minVoters = 3

type memberSwarm struct {
	voters       int
	confMean     time.Duration
	transferMean time.Duration
	eager        float64
}

func newMemberSwarm(rng *rand.Rand, nodes int) memberSwarm {
	ms := memberSwarm{voters: nodes}
	if rng.Float64() < 0.5 {
		ms.confMean = between(rng, 500*time.Millisecond, 4*time.Second)
		ms.eager = pick(rng, 0, 0.1, 0.5)
		if nodes >= minVoters+2 && rng.Float64() < 0.5 {
			ms.voters = nodes - 2
		}
	}
	if rng.Float64() < 0.4 {
		ms.transferMean = between(rng, time.Second, 5*time.Second)
	}
	return ms
}

func (ms memberSwarm) String() string {
	var b strings.Builder
	if ms.confMean > 0 {
		fmt.Fprintf(&b, " members/%v from %d voters", ms.confMean.Round(time.Millisecond), ms.voters)
		if ms.eager > 0 {
			fmt.Fprintf(&b, " eager %.1f", ms.eager)
		}
	}
	if ms.transferMean > 0 {
		fmt.Fprintf(&b, " transfers/%v", ms.transferMean.Round(time.Millisecond))
	}
	return b.String()
}

func (s *Sim) startMembership() {
	if s.ms.confMean > 0 {
		s.after(exponential(s.mrng, s.ms.confMean), s.confFault)
	}
	if s.ms.transferMean > 0 {
		s.after(exponential(s.mrng, s.ms.transferMean), s.transferFault)
	}
}

func (s *Sim) confFault() {
	if s.healing {
		return
	}
	if l := s.leader(); l != nil {
		s.changeMembers(l)
	}
	s.after(exponential(s.mrng, s.ms.confMean), s.confFault)
}

func (s *Sim) changeMembers(sn *simNode) {
	if sn.n == nil || sn.paused || s.healing {
		return
	}
	r := sn.n.Raft()
	if r.State() != raft.StateLeader {
		return
	}
	conf := r.Conf()
	var options []raft.ConfChange
	for _, x := range s.nodes {
		switch {
		case conf.IsLearner(x.id):
			options = append(options, raft.ConfChange{Type: raft.ConfPromote, Node: x.id}, raft.ConfChange{Type: raft.ConfRemove, Node: x.id})
		case conf.IsVoter(x.id):
			if len(conf.Voters) > minVoters && x.id != r.ID() {
				options = append(options, raft.ConfChange{Type: raft.ConfRemove, Node: x.id})
			}
		default:
			options = append(options, raft.ConfChange{Type: raft.ConfAddLearner, Node: x.id})
		}
	}
	if len(options) == 0 {
		return
	}
	cc := options[s.mrng.IntN(len(options))]
	s.stats.ConfChanges++
	s.tracef("members: node %d proposes %s %d", sn.id, cc.Type, cc.Node)
	sn.n.ProposeConfChange(cc, func(_ kv.Result, err error) {
		if err == nil {
			s.stats.ConfCommitted++
		}
	})
	s.process(sn)
}

func (s *Sim) transferFault() {
	if s.healing {
		return
	}
	if l := s.leader(); l != nil {
		voters := l.n.Raft().Conf().Voters
		if to := voters[s.mrng.IntN(len(voters))]; to != l.id {
			s.stats.Transfers++
			s.tracef("members: node %d hands leadership to %d", l.id, to)
			if err := l.n.TransferLeadership(to); err != nil {
				s.tracef("members: node %d refused the transfer: %v", l.id, err)
			}
			s.process(l)
		}
	}
	s.after(exponential(s.mrng, s.ms.transferMean), s.transferFault)
}

func (s *Sim) newLeader(sn *simNode) {
	if s.ms.eager > 0 && !s.healing && s.mrng.Float64() < s.ms.eager {
		s.after(0, func() { s.changeMembers(sn) })
	}
}
