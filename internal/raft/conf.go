package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
)

type ConfChangeType uint8

const (
	ConfAddLearner ConfChangeType = iota + 1
	ConfPromote
	ConfRemove
)

func (t ConfChangeType) String() string {
	switch t {
	case ConfAddLearner:
		return "add-learner"
	case ConfPromote:
		return "promote"
	case ConfRemove:
		return "remove"
	}
	return fmt.Sprintf("ConfChangeType(%d)", uint8(t))
}

type ConfChange struct {
	Type ConfChangeType
	Node NodeID
}

func (c ConfChange) Marshal() []byte {
	return binary.AppendUvarint([]byte{byte(c.Type)}, uint64(c.Node))
}

func UnmarshalConfChange(b []byte) (ConfChange, error) {
	if len(b) < 2 {
		return ConfChange{}, errors.New("raft: short conf change")
	}
	node, n := binary.Uvarint(b[1:])
	if n <= 0 || n != len(b)-1 || node == 0 {
		return ConfChange{}, errors.New("raft: malformed conf change")
	}
	c := ConfChange{Type: ConfChangeType(b[0]), Node: NodeID(node)}
	if c.Type < ConfAddLearner || c.Type > ConfRemove {
		return ConfChange{}, fmt.Errorf("raft: unknown conf change type %d", b[0])
	}
	return c, nil
}

type ConfState struct {
	Voters   []NodeID
	Learners []NodeID
}

func (c ConfState) clone() ConfState {
	return ConfState{Voters: slices.Clone(c.Voters), Learners: slices.Clone(c.Learners)}
}

func (c ConfState) IsVoter(id NodeID) bool {
	_, ok := slices.BinarySearch(c.Voters, id)
	return ok
}

func (c ConfState) IsLearner(id NodeID) bool {
	_, ok := slices.BinarySearch(c.Learners, id)
	return ok
}

func (c ConfState) IsMember(id NodeID) bool {
	return c.IsVoter(id) || c.IsLearner(id)
}

func (c ConfState) Members() []NodeID {
	out := append(slices.Clone(c.Voters), c.Learners...)
	slices.Sort(out)
	return out
}

func remove(ids []NodeID, id NodeID) []NodeID {
	return slices.DeleteFunc(slices.Clone(ids), func(x NodeID) bool { return x == id })
}

func insert(ids []NodeID, id NodeID) []NodeID {
	out := append(slices.Clone(ids), id)
	slices.Sort(out)
	return out
}

func (c ConfState) Apply(cc ConfChange) (ConfState, error) {
	switch cc.Type {
	case ConfAddLearner:
		if c.IsMember(cc.Node) {
			return c, fmt.Errorf("raft: node %d is already a member", cc.Node)
		}
		return ConfState{Voters: slices.Clone(c.Voters), Learners: insert(c.Learners, cc.Node)}, nil
	case ConfPromote:
		if !c.IsLearner(cc.Node) {
			return c, fmt.Errorf("raft: node %d is not a learner", cc.Node)
		}
		return ConfState{Voters: insert(c.Voters, cc.Node), Learners: remove(c.Learners, cc.Node)}, nil
	case ConfRemove:
		if !c.IsMember(cc.Node) {
			return c, fmt.Errorf("raft: node %d is not a member", cc.Node)
		}
		if c.IsVoter(cc.Node) && len(c.Voters) == 1 {
			return c, errors.New("raft: cannot remove the last voter")
		}
		return ConfState{Voters: remove(c.Voters, cc.Node), Learners: remove(c.Learners, cc.Node)}, nil
	}
	return c, fmt.Errorf("raft: unknown conf change %v", cc.Type)
}

type confAt struct {
	index uint64
	conf  ConfState
}

func (r *Raft) conf() ConfState {
	return r.confs[len(r.confs)-1].conf
}

func (r *Raft) Conf() ConfState {
	return r.conf().clone()
}

func (r *Raft) ConfAt(index uint64) ConfState {
	c := r.confs[0].conf
	for _, ca := range r.confs {
		if ca.index <= index {
			c = ca.conf
		}
	}
	return c.clone()
}

func (r *Raft) onAppend(after uint64, ents []Entry) {
	before, depth := r.confs[len(r.confs)-1].index, len(r.confs)
	for len(r.confs) > 1 && r.confs[len(r.confs)-1].index >= after {
		r.confs = r.confs[:len(r.confs)-1]
	}
	for _, e := range ents {
		if e.Type != EntryConfig {
			continue
		}
		cc, err := UnmarshalConfChange(e.Data)
		if err != nil {
			continue
		}
		next, err := r.conf().Apply(cc)
		if err != nil {
			continue
		}
		r.confs = append(r.confs, confAt{index: e.Index, conf: next})
	}
	if r.confs[len(r.confs)-1].index != before || len(r.confs) != depth {
		r.applyConf()
	}
}

func (r *Raft) resetConfs(base confAt) {
	r.confs = []confAt{base}
	r.applyConf()
}

func (r *Raft) compactConfs(point uint64) {
	i := 0
	for i+1 < len(r.confs) && r.confs[i+1].index <= point {
		i++
	}
	r.confs = r.confs[i:]
}

func (r *Raft) applyConf() {
	c := r.conf()
	members := c.Members()
	old := map[NodeID]*progress{}
	for _, pr := range r.prs {
		old[pr.id] = pr
	}
	prs := make([]*progress, 0, len(members))
	for _, id := range members {
		pr, ok := old[id]
		if !ok {
			pr = &progress{id: id}
			pr.reset(r.log.lastIndex() + 1)
			pr.active = true
		}
		pr.learner = c.IsLearner(id)
		prs = append(prs, pr)
	}
	r.prs = prs
	r.leaseOK = false
	r.confRound = r.hbRound
	if r.transferee != None && !c.IsVoter(r.transferee) {
		r.transferee = None
	}
	if r.state == StateLeader && r.maybeCommit() {
		r.pendingBcast = true
	}
}

func (r *Raft) pendingConf() bool {
	for _, ca := range r.confs[1:] {
		if ca.index > r.log.committed {
			return true
		}
	}
	return false
}
