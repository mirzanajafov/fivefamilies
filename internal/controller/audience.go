package controller

import (
	"sync"
	"time"
)

type audience struct {
	mu       sync.Mutex
	viewers  int
	lastSeen time.Time
}

func (a *audience) join() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.viewers++
}

func (a *audience) leave() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.viewers--
	a.lastSeen = time.Now()
}

func (a *audience) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.viewers
}

func (a *audience) seenWithin(idle time.Duration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.viewers > 0 || time.Since(a.lastSeen) < idle
}
