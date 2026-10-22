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

func (m *loadMeter) ack() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.acks = append(m.acks, time.Now())
	if cut := len(m.acks) - 20000; cut > 0 {
		m.acks = m.acks[cut:]
	}
}

func (m *loadMeter) measured(ops int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
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
