package server

import (
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type deadline struct {
	at atomic.Int64
}

func (d *deadline) set(after time.Duration) {
	d.at.Store(time.Now().Add(after).UnixNano())
}

func (d *deadline) clear() {
	d.at.Store(0)
}

func (d *deadline) remaining() time.Duration {
	return time.Until(time.Unix(0, d.at.Load()))
}

func (d *deadline) wait() {
	for r := d.remaining(); r > 0; r = d.remaining() {
		time.Sleep(min(r, 10*time.Millisecond))
	}
}

type isolation struct {
	deadline
}

func (i *isolation) Allow(_, _ raft.NodeID) bool {
	return i.remaining() <= 0
}

func (s *Server) heal() {
	s.isolated.clear()
	s.stall.clear()
	s.paused.clear()
}
