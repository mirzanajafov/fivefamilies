package watch

import (
	"fmt"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Machine struct {
	Store       *kv.Store
	Hub         *Hub
	maxSessions int
	pending     []kv.Event
}

func NewMachine(maxSessions, history int, bugs planted.Bug) *Machine {
	m := &Machine{Store: kv.NewStore(maxSessions), Hub: New(history, 0, bugs), maxSessions: maxSessions}
	m.Store.Observe(m.collect)
	return m
}

func (m *Machine) collect(ev kv.Event) {
	m.pending = append(m.pending, ev)
}

func (m *Machine) Apply(e raft.Entry) kv.Result {
	m.pending = m.pending[:0]
	res := m.Store.Apply(e)
	m.Hub.Publish(e.Index, m.pending)
	return res
}

func (m *Machine) LastEvents() []kv.Event {
	return m.pending
}

func (m *Machine) Get(key string) kv.Result {
	kvp, found := m.Store.Get(key)
	return kv.Result{Rev: m.Store.Rev(), Found: found, KV: kvp, Ok: true}
}

func (m *Machine) Snapshot() []byte {
	return m.Store.Snapshot()
}

func (m *Machine) Lease(id uint64) (kv.LeaseInfo, bool) {
	return m.Store.Lease(id)
}

func (m *Machine) Leases() []kv.LeaseInfo {
	return m.Store.Leases()
}

func (m *Machine) Restore(index uint64, data []byte) error {
	st, err := kv.Restore(data, m.maxSessions)
	if err != nil {
		return err
	}
	if st.Rev() != index {
		return fmt.Errorf("snapshot at %d holds a store at revision %d", index, st.Rev())
	}
	st.Observe(m.collect)
	m.Store = st
	m.Hub.Reset(index)
	return nil
}
