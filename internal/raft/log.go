package raft

import "fmt"

const entryOverhead = 24

type raftLog struct {
	snapIndex uint64
	snapTerm  uint64
	ents      []Entry
	committed uint64
	applied   uint64
	stabled   uint64
	handed    uint64
}

func newLog(st Restore) (*raftLog, error) {
	l := &raftLog{snapIndex: st.SnapIndex, snapTerm: st.SnapTerm}
	for i, e := range st.Entries {
		if e.Index != st.SnapIndex+uint64(i)+1 {
			return nil, fmt.Errorf("raft: restored entry %d has index %d, want %d", i, e.Index, st.SnapIndex+uint64(i)+1)
		}
		if e.Term < l.snapTerm || (i > 0 && e.Term < st.Entries[i-1].Term) {
			return nil, fmt.Errorf("raft: restored entry %d has term %d lower than the one before it", e.Index, e.Term)
		}
	}
	l.ents = append([]Entry(nil), st.Entries...)
	last := l.lastIndex()
	if st.HardState.Commit > last {
		return nil, fmt.Errorf("raft: restored commit %d is past the last index %d", st.HardState.Commit, last)
	}
	l.committed = max(st.HardState.Commit, st.SnapIndex)
	l.applied = st.SnapIndex
	l.stabled = last
	l.handed = last
	return l, nil
}

func (l *raftLog) firstIndex() uint64 {
	return l.snapIndex + 1
}

func (l *raftLog) lastIndex() uint64 {
	return l.snapIndex + uint64(len(l.ents))
}

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

func (l *raftLog) term(i uint64) (uint64, bool) {
	if i == l.snapIndex {
		return l.snapTerm, true
	}
	if i < l.snapIndex || i > l.lastIndex() {
		return 0, false
	}
	return l.ents[i-l.snapIndex-1].Term, true
}

func (l *raftLog) entry(i uint64) (Entry, bool) {
	if i <= l.snapIndex || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.ents[i-l.snapIndex-1], true
}

func (l *raftLog) matchTerm(i, t uint64) bool {
	lt, ok := l.term(i)
	return ok && lt == t
}

func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	ours := l.lastTerm()
	return lastTerm > ours || (lastTerm == ours && lastIndex >= l.lastIndex())
}

func (l *raftLog) slice(lo, hi uint64) []Entry {
	if lo >= hi {
		return nil
	}
	if lo <= l.snapIndex || hi > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: slice [%d, %d) out of the log's range [%d, %d]", lo, hi, l.firstIndex(), l.lastIndex()))
	}
	a, b := lo-l.snapIndex-1, hi-l.snapIndex-1
	return l.ents[a:b:b]
}

func (l *raftLog) sliceBytes(lo, hi uint64, maxBytes int) []Entry {
	ents := l.slice(lo, hi)
	size := 0
	for i, e := range ents {
		size += len(e.Data) + entryOverhead
		if i > 0 && size > maxBytes {
			return ents[:i:i]
		}
	}
	return ents
}

func (l *raftLog) append(ents ...Entry) {
	if len(ents) == 0 {
		return
	}
	after := ents[0].Index
	switch {
	case after == l.lastIndex()+1:
		l.ents = append(l.ents, ents...)
	case after <= l.committed:
		panic(fmt.Sprintf("raft: truncating at %d would remove committed entries up to %d", after, l.committed))
	case after <= l.snapIndex || after > l.lastIndex()+1:
		panic(fmt.Sprintf("raft: appending at %d outside the log's range [%d, %d]", after, l.firstIndex(), l.lastIndex()+1))
	default:
		keep := l.ents[:after-l.snapIndex-1]
		fresh := make([]Entry, len(keep), len(keep)+len(ents))
		copy(fresh, keep)
		l.ents = append(fresh, ents...)
		l.handed = min(l.handed, after-1)
		l.stabled = min(l.stabled, after-1)
	}
}

func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

func (l *raftLog) findConflictByTerm(index, term uint64) (uint64, uint64) {
	for ; index > 0; index-- {
		t, ok := l.term(index)
		if !ok {
			return index, 0
		}
		if t <= term {
			return index, t
		}
	}
	return 0, 0
}

func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	last := index + uint64(len(ents))
	if ci := l.findConflict(ents); ci != 0 {
		l.append(ents[ci-index-1:]...)
	}
	l.commitTo(min(committed, last))
	return last, true
}

func (l *raftLog) commitTo(c uint64) {
	if c <= l.committed {
		return
	}
	if c > l.lastIndex() {
		panic(fmt.Sprintf("raft: commit %d is past the last index %d; the log was lost or truncated", c, l.lastIndex()))
	}
	l.committed = c
}

func (l *raftLog) applyLimit() uint64 {
	return min(l.committed, l.stabled)
}

func (l *raftLog) hasToApply() bool {
	return l.applyLimit() > l.applied
}

func (l *raftLog) unhanded() []Entry {
	if l.handed >= l.lastIndex() {
		return nil
	}
	return l.slice(l.handed+1, l.lastIndex()+1)
}
