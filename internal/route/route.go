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

func (r *Router) MoveOn(failed int, md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hint := md.Get(LeaderHeader); len(hint) > 0 {
		if i := slices.Index(r.addrs, hint[0]); i >= 0 && hint[0] != r.addrs[failed] {
			r.target.Store(int64(i))
			return
		}
	}
	if r.Target() == failed {
		r.next = (r.next + 1) % len(r.addrs)
		if r.next == failed {
			r.next = (r.next + 1) % len(r.addrs)
		}
		r.target.Store(int64(r.next))
	}
}
