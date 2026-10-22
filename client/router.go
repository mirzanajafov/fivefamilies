package client

import (
	"slices"
	"sync"

	"google.golang.org/grpc/metadata"
)

const headerLeaderAddr = "ff-leader-addr"

type Router struct {
	addrs  []string
	mu     sync.Mutex
	target int
}

func NewRouter(addrs []string) *Router {
	return &Router{addrs: addrs}
}

func (r *Router) Target() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.target
}

func (r *Router) MoveOn(failed int, md metadata.MD) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.target != failed {
		return
	}
	if hint := md.Get(headerLeaderAddr); len(hint) > 0 {
		if i := slices.Index(r.addrs, hint[0]); i >= 0 && i != failed {
			r.target = i
			return
		}
	}
	r.target = (failed + 1) % len(r.addrs)
}
