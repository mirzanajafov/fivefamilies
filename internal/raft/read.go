package raft

import (
	"slices"

	"github.com/mirzanajafov/fivefamilies/internal/planted"
)

const roundRing = 64

type ReadState struct {
	Index uint64
	Ctx   uint64
	Ok    bool
}

type readRequest struct {
	ctx   uint64
	from  NodeID
	index uint64
	round uint64
}

func (r *Raft) ReadIndex(ctx uint64, lease bool) {
	r.handleReadIndex(r.id, ctx, lease)
}

func (r *Raft) handleReadIndex(from NodeID, ctx uint64, lease bool) {
	if r.state != StateLeader {
		switch {
		case from != r.id || r.lead == None:
			r.answerRead(from, ctx, 0, false)
		case r.cfg.Bugs.Has(planted.FollowerRead):
			r.answerRead(from, ctx, r.log.committed, true)
		default:
			r.send(Message{Type: MsgReadIndex, To: r.lead, Term: r.term, Context: ctx})
		}
		return
	}
	if !r.committedInTerm() {
		r.answerRead(from, ctx, 0, false)
		return
	}
	if r.quorum() == 1 && r.isVoter(r.id) {
		r.answerRead(from, ctx, r.log.committed, true)
		return
	}
	if lease && r.leaseValid() {
		r.answerRead(from, ctx, r.log.committed, true)
		return
	}
	r.reads = append(r.reads, readRequest{ctx: ctx, from: from, index: r.log.committed, round: r.hbRound + 1})
	r.pendingHeartbeat = true
}

func (r *Raft) answerRead(to NodeID, ctx, index uint64, ok bool) {
	if to == r.id {
		r.readStates = append(r.readStates, ReadState{Index: index, Ctx: ctx, Ok: ok})
		return
	}
	r.send(Message{Type: MsgReadIndexResp, To: to, Term: r.term, Context: ctx, Index: index, Reject: !ok})
}

func (r *Raft) committedInTerm() bool {
	t, _ := r.log.term(r.log.committed)
	return t == r.term
}

func (r *Raft) confirmedRound() uint64 {
	rounds := make([]uint64, 0, len(r.prs))
	for _, pr := range r.prs {
		if pr.learner {
			continue
		}
		if pr.id == r.id {
			rounds = append(rounds, r.hbRound)
		} else {
			rounds = append(rounds, pr.ackRound)
		}
	}
	slices.Sort(rounds)
	return rounds[len(rounds)-r.quorum()]
}

func (r *Raft) releaseReads() {
	c := r.confirmedRound()
	i := 0
	for i < len(r.reads) && r.reads[i].round <= c {
		q := r.reads[i]
		r.answerRead(q.from, q.ctx, q.index, true)
		i++
	}
	r.reads = r.reads[i:]
	if c > r.confRound && r.hbRound-c < roundRing && (!r.leaseOK || r.roundTicks[c%roundRing] > r.leaseStart) {
		r.leaseStart, r.leaseOK = r.roundTicks[c%roundRing], true
	}
}

func (r *Raft) leaseValid() bool {
	return r.cfg.LeaseTicks > 0 && r.leaseOK && r.transferTerm != r.term && r.ticks-r.leaseStart < uint64(r.cfg.LeaseTicks)
}

func (r *Raft) dropReads() {
	for _, q := range r.reads {
		r.answerRead(q.from, q.ctx, 0, false)
	}
	r.reads = nil
	r.leaseOK = false
}
