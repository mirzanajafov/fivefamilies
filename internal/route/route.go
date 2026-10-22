package route

import (
	"slices"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc/metadata"
)

const LeaderHeader = "ff-leader-addr"

type Router struct {
	addrs  []string
	target atomic.Int64
	mu     sync.Mutex
	next   int
}

func NewRouter(addrs []string) *Router {
	return &Router{addrs: addrs}
}

func (r *Router) Target() int {
	return int(r.target.Load())
}

func (r *Router) MoveOn(failed int, md metadata.MD) (redirected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.Target() == failed
	if hint := md.Get(LeaderHeader); len(hint) > 0 {
		if i := slices.Index(r.addrs, hint[0]); i >= 0 && i != failed {
			if current {
				r.target.Store(int64(i))
			}
			return true
		}
	}
	if current {
		r.next = (r.next + 1) % len(r.addrs)
		if r.next == failed {
			r.next = (r.next + 1) % len(r.addrs)
		}
		r.target.Store(int64(r.next))
	}
	return false
}
