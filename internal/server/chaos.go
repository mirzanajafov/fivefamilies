package server

import (
	"sync/atomic"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var epoch = time.Now()

type fault struct {
	ends atomic.Int64
}

func (f *fault) set(d time.Duration) {
	f.ends.Store(int64(time.Since(epoch) + d))
}

func (f *fault) clear() {
	f.ends.Store(0)
}

func (f *fault) remaining() time.Duration {
	ends := f.ends.Load()
	if ends == 0 {
		return 0
	}
	return time.Duration(ends) - time.Since(epoch)
}

func (f *fault) wait() {
	for r := f.remaining(); r > 0; r = f.remaining() {
		time.Sleep(min(r, 10*time.Millisecond))
	}
}

type isolation struct {
	fault
}

func (i *isolation) Allow(_, _ raft.NodeID) bool {
	return i.remaining() <= 0
}

func (s *Server) heal() {
	s.isolated.clear()
	s.stalled.clear()
	s.paused.clear()
}
