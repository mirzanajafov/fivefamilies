package node

import (
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type kvApp struct {
	store *kv.Store
}

func (a *kvApp) Apply(e raft.Entry) kv.Result { return a.store.Apply(e) }
func (a *kvApp) Get(key string) kv.Result {
	v, ok := a.store.Get(key)
	return kv.Result{Rev: a.store.Rev(), Found: ok, KV: v, Ok: true}
}
func (a *kvApp) Snapshot() []byte                     { return a.store.Snapshot() }
func (a *kvApp) Lease(id uint64) (kv.LeaseInfo, bool) { return a.store.Lease(id) }
func (a *kvApp) Leases() []kv.LeaseInfo               { return a.store.Leases() }
func (a *kvApp) Restore(uint64, []byte) error         { return nil }

func newLeaseNode(t *testing.T, id raft.NodeID, voters []raft.NodeID) (*Node, *recEnv, *kvApp) {
	t.Helper()
	r, err := raft.New(raft.Config{ID: id, Voters: voters, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.NewPCG(1, uint64(id))}, raft.Restore{})
	if err != nil {
		t.Fatal(err)
	}
	env, app := &recEnv{}, &kvApp{store: kv.NewStore(0)}
	return New(r, env, app, Config{Tick: 10 * time.Millisecond}), env, app
}

func grant(t *testing.T, n *Node, env *recEnv, ttl uint64, key string) uint64 {
	t.Helper()
	var id uint64
	n.Submit(kv.Command{Op: kv.OpLeaseGrant, TTL: ttl}, func(r kv.Result, err error) {
		if err != nil || !r.Ok {
			t.Fatalf("grant: %+v %v", r, err)
		}
		id = r.Lease
	})
	settle(t, n, env)
	n.Submit(kv.Command{Op: kv.OpPut, Key: key, Value: []byte("x"), Lease: id}, nil)
	settle(t, n, env)
	return id
}

func tickAndSettle(t *testing.T, n *Node, env *recEnv, k int) {
	t.Helper()
	for range k {
		n.Advance(n.cfg.Tick)
		settle(t, n, env)
	}
}

func TestALeaseExpiresOnTheLeaderAfterItsTTL(t *testing.T) {
	n, env, app := newLeaseNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	settle(t, n, env)
	grant(t, n, env, 100, "k")
	tickAndSettle(t, n, env, 10)
	if _, ok := app.store.Get("k"); !ok {
		t.Fatal("the lease expired after its TTL in ticks; the tick it was granted in was already partly over, so that can be less than the TTL in time")
	}
	tickAndSettle(t, n, env, 1)
	if _, ok := app.store.Get("k"); ok {
		t.Fatal("the lease outlived its TTL")
	}
	if len(app.store.Leases()) != 0 {
		t.Fatal("an expired lease is still in the store")
	}
}

func TestAKeepAliveMovesTheDeadline(t *testing.T) {
	n, env, app := newLeaseNode(t, 1, []raft.NodeID{1})
	n.Raft().Campaign()
	settle(t, n, env)
	id := grant(t, n, env, 100, "k")
	tickAndSettle(t, n, env, 8)
	var res kv.Result
	n.KeepAlive(id, func(r kv.Result, err error) {
		if err != nil {
			t.Fatal(err)
		}
		res = r
	})
	settle(t, n, env)
	if !res.Ok || res.TTL != 100 {
		t.Fatalf("keepalive: %+v", res)
	}
	tickAndSettle(t, n, env, 8)
	if _, ok := app.store.Get("k"); !ok {
		t.Fatal("the lease expired from its grant, not from the keepalive")
	}
	tickAndSettle(t, n, env, 3)
	if _, ok := app.store.Get("k"); ok {
		t.Fatal("the lease outlived the keepalive's TTL")
	}
	var after kv.Result
	n.KeepAlive(id, func(r kv.Result, _ error) { after = r })
	settle(t, n, env)
	if after.Code != kv.ErrLeaseNotFound {
		t.Fatalf("keepalive on an expired lease: %+v", after)
	}
}

func TestAKeepAliveOnAFollowerPointsToTheLeader(t *testing.T) {
	n, _, _ := newLeaseNode(t, 2, []raft.NodeID{1, 2, 3})
	n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 3, To: 2, Term: 4})
	var got error
	n.KeepAlive(7, func(_ kv.Result, err error) { got = err })
	var nl *NotLeaderError
	if !errors.As(got, &nl) || nl.Leader != 3 {
		t.Fatalf("err %v", got)
	}
}

func TestANewLeaderGivesEveryLeaseAFullTTL(t *testing.T) {
	n, env, app := newLeaseNode(t, 1, []raft.NodeID{1, 2, 3})
	g := raft.Entry{Term: 1, Index: 1, Data: kv.Encode(kv.Command{Op: kv.OpLeaseGrant, TTL: 100})}
	n.Receive(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 1, Commit: 1, Entries: []raft.Entry{g}})
	settle(t, n, env)
	if len(app.store.Leases()) != 1 {
		t.Fatal("setup: the grant did not apply")
	}
	for range 30 {
		n.Receive(raft.Message{Type: raft.MsgHeartbeat, From: 2, To: 1, Term: 1, Commit: 1})
		n.Advance(n.cfg.Tick)
		settle(t, n, env)
	}
	if n.Raft().LastIndex() != 1 {
		t.Fatal("a follower proposed an expiry")
	}
	n.Raft().Campaign()
	settle(t, n, env)
	n.Receive(raft.Message{Type: raft.MsgVoteResp, From: 3, To: 1, Term: 2})
	settle(t, n, env)
	if n.Raft().State() != raft.StateLeader {
		t.Fatalf("setup: %s", n.Raft().State())
	}
	last := n.Raft().LastIndex()
	tickAndSettle(t, n, env, 9)
	if n.Raft().LastIndex() != last {
		t.Fatal("the new leader expired a lease it had held for less than its TTL")
	}
	tickAndSettle(t, n, env, 2)
	e, _ := n.Raft().EntryAt(n.Raft().LastIndex())
	if cmd, err := kv.Decode(e.Data); err != nil || cmd.Op != kv.OpLeaseExpire || cmd.Lease != 1 {
		t.Fatalf("last entry %+v: want the expiry of lease 1", e)
	}
}
