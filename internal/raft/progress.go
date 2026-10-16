package raft

type progressState uint8

const (
	stateProbe progressState = iota
	stateReplicate
	stateSnapshot
)

func (s progressState) String() string {
	switch s {
	case stateReplicate:
		return "replicate"
	case stateSnapshot:
		return "snapshot"
	}
	return "probe"
}

type progress struct {
	id       NodeID
	match    uint64
	next     uint64
	state    progressState
	paused   bool
	inflight []uint64
	active   bool
	learner  bool

	pendingSnapshot uint64
	snapWait        int
	ackRound        uint64
}

func (pr *progress) reset(next uint64) {
	pr.match = 0
	pr.next = next
	pr.state = stateProbe
	pr.paused = false
	pr.inflight = pr.inflight[:0]
	pr.active = false
	pr.pendingSnapshot = 0
	pr.snapWait = 0
	pr.ackRound = 0
}

func (pr *progress) becomeProbe() {
	if pr.state == stateSnapshot {
		pr.next = max(pr.match+1, pr.pendingSnapshot+1)
	} else {
		pr.next = pr.match + 1
	}
	pr.state = stateProbe
	pr.paused = false
	pr.inflight = pr.inflight[:0]
	pr.pendingSnapshot = 0
}

func (pr *progress) becomeSnapshot(index uint64) {
	pr.state = stateSnapshot
	pr.pendingSnapshot = index
	pr.snapWait = 0
	pr.paused = false
	pr.inflight = pr.inflight[:0]
}

func (pr *progress) abandonSnapshot() {
	pr.state = stateProbe
	pr.next = pr.match + 1
	pr.paused = false
	pr.pendingSnapshot = 0
	pr.snapWait = 0
}

func (pr *progress) becomeReplicate() {
	pr.state = stateReplicate
	pr.next = pr.match + 1
	pr.paused = false
	pr.inflight = pr.inflight[:0]
}

func (pr *progress) maybeUpdate(n uint64) bool {
	updated := false
	if pr.match < n {
		pr.match = n
		pr.paused = false
		updated = true
	}
	pr.next = max(pr.next, n+1)
	return updated
}

func (pr *progress) maybeDecrTo(rejected, matchHint uint64) bool {
	if pr.state == stateReplicate {
		if rejected <= pr.match {
			return false
		}
		pr.next = pr.match + 1
		return true
	}
	if pr.next-1 != rejected {
		return false
	}
	pr.next = max(min(rejected, matchHint+1), pr.match+1)
	pr.paused = false
	return true
}

func (pr *progress) isPaused(maxInflight int) bool {
	switch pr.state {
	case stateProbe:
		return pr.paused
	case stateSnapshot:
		return true
	}
	return len(pr.inflight) >= maxInflight
}

func (pr *progress) sent(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	last := ents[len(ents)-1].Index
	switch pr.state {
	case stateReplicate:
		pr.next = last + 1
		pr.inflight = append(pr.inflight, last)
	case stateProbe:
		pr.paused = true
	}
}

func (pr *progress) freeTo(index uint64) {
	i := 0
	for i < len(pr.inflight) && pr.inflight[i] <= index {
		i++
	}
	pr.inflight = append(pr.inflight[:0], pr.inflight[i:]...)
}

func (pr *progress) freeFirst() {
	if len(pr.inflight) > 0 {
		pr.inflight = append(pr.inflight[:0], pr.inflight[1:]...)
	}
}
