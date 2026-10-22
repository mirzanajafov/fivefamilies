package controller

import (
	"sync"
	"time"
)

type loadMeter struct {
	mu     sync.Mutex
	active bool
	acks   []time.Time
	rateAt time.Time
	rate   float64
}

func (m *loadMeter) setActive(on bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.active = on
	if !on {
		m.rate = 0
	}
}

func (m *loadMeter) ack(at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, at)
	if cut := len(m.acks) - 20000; cut > 0 {
		m.acks = m.acks[cut:]
	}
}

func (m *loadMeter) measured(ops int, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if since := now.Sub(m.rateAt).Seconds(); !m.rateAt.IsZero() && since > 0 {
		m.rate = float64(ops) / since
	}
	m.rateAt = now
}

func (m *loadMeter) view() (active bool, rate float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active, m.rate
}

func (m *loadMeter) widestGap(from, to time.Time) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	gap := time.Duration(0)
	prev := from
	for _, a := range m.acks {
		if a.Before(from) {
			continue
		}
		if a.After(to) {
			break
		}
		gap = max(gap, a.Sub(prev))
		prev = a
	}
	return max(gap, to.Sub(prev))
}

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

func (a *audience) leave(now time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.viewers--
	a.lastSeen = now
}

func (a *audience) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.viewers
}

func (a *audience) seenWithin(idle time.Duration, now time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.viewers > 0 || now.Sub(a.lastSeen) < idle
}
