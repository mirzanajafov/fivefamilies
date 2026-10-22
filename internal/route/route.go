package route

import (
	"slices"
	"sync"

	"google.golang.org/grpc/metadata"
)

const LeaderHeader = "ff-leader-addr"

type Router struct {
	addrs  []string
	mu     sync.Mutex
	target int
	next   int
}

func NewRouter(addrs []string) *Router {
	return &Router{addrs: addrs}
}

func (r *Router) Target() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.target
}

func (r *Router) MoveOn(failed int, md metadata.MD) (redirected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hint := md.Get(LeaderHeader); len(hint) > 0 {
		if i := slices.Index(r.addrs, hint[0]); i >= 0 && i != failed {
			if r.target == failed {
				r.target = i
			}
			return true
		}
	}
	if r.target == failed {
		r.next = (r.next + 1) % len(r.addrs)
		if r.next == failed {
			r.next = (r.next + 1) % len(r.addrs)
		}
		r.target = r.next
	}
	return false
}
