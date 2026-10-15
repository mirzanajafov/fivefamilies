package server

import (
	"sync"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Filter struct {
	mu      sync.RWMutex
	blocked map[[2]raft.NodeID]bool
}

func NewFilter() *Filter {
	return &Filter{blocked: map[[2]raft.NodeID]bool{}}
}

func (f *Filter) Allow(from, to raft.NodeID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return !f.blocked[[2]raft.NodeID{from, to}]
}

func (f *Filter) Block(from, to raft.NodeID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked[[2]raft.NodeID{from, to}] = true
}

func (f *Filter) Clear() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.blocked)
}
