package sim

import (
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type eventKind uint8

const (
	evTick eventKind = iota
	evDeliver
	evWriteDone
	evRequest
	evCall
)

type event struct {
	at   time.Duration
	seq  uint64
	kind eventKind
	node int
	gen  uint64
	msg  raft.Message
	wseq uint64
	done bool
	req  *clientReq
	call func()
}

type eventQueue []event

func (q eventQueue) less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}

func (q *eventQueue) push(e event) {
	*q = append(*q, e)
	h := *q
	i := len(h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h[i], h[p] = h[p], h[i]
		i = p
	}
}

func (q *eventQueue) pop() event {
	h := *q
	top := h[0]
	last := len(h) - 1
	h[0] = h[last]
	h[last] = event{}
	h = h[:last]
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		small := i
		if l < len(h) && h.less(l, small) {
			small = l
		}
		if r < len(h) && h.less(r, small) {
			small = r
		}
		if small == i {
			break
		}
		h[i], h[small] = h[small], h[i]
		i = small
	}
	*q = h
	return top
}
