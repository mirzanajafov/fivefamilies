package kv

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type applier struct {
	s     *Store
	index uint64
}

func (a *applier) do(c Command) Result {
	a.index++
	return a.s.Apply(raft.Entry{Term: 1, Index: a.index, Data: Encode(c)})
}

func (a *applier) noop() {
	a.index++
	a.s.Apply(raft.Entry{Term: 1, Index: a.index})
}

func newApplier(maxSessions int) *applier {
	return &applier{s: NewStore(maxSessions)}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cmds := []Command{
		{Op: OpRegister},
		{Op: OpGet, Key: "a"},
		{Op: OpRange, Key: "/registry/", Limit: 10},
		{Op: OpPut, Client: 7, Seq: 3, Key: "k", Value: []byte("v")},
		{Op: OpPut, Client: 1 << 60, Seq: 1 << 40, Key: "", Value: nil},
		{Op: OpDelete, Client: 2, Seq: 9, Key: "gone"},
		{Op: OpCAS, Client: 3, Seq: 1, Key: "k", Value: []byte("new"), Expect: []byte("old")},
		{Op: OpCAS, Client: 3, Seq: 2, Key: "k", Value: []byte("first"), ExpectMissing: true},
		{Op: OpCASRev, Client: 4, Seq: 5, Key: "k", Value: []byte("x"), ExpectRev: 12345},
		{Op: OpPut, Client: 7, Seq: 4, Key: "k", Value: []byte("v"), Lease: 99},
		{Op: OpCAS, Client: 3, Seq: 3, Key: "k", Value: []byte("v"), ExpectMissing: true, Lease: 1 << 50},
		{Op: OpCASRev, Client: 4, Seq: 6, Key: "k", Value: []byte("x"), ExpectRev: 7, Lease: 3},
		{Op: OpLeaseGrant, Client: 5, Seq: 1, TTL: 10000},
		{Op: OpLeaseRevoke, Client: 5, Seq: 2, Lease: 42},
		{Op: OpLeaseExpire, Lease: 42},
	}
	for _, c := range cmds {
		got, err := Decode(Encode(c))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		if got.Op != c.Op || got.Client != c.Client || got.Seq != c.Seq || got.Key != c.Key ||
			!bytes.Equal(got.Value, c.Value) || !bytes.Equal(got.Expect, c.Expect) ||
			got.ExpectMissing != c.ExpectMissing || got.ExpectRev != c.ExpectRev || got.Limit != c.Limit ||
			got.Lease != c.Lease || got.TTL != c.TTL {
			t.Fatalf("round trip changed %+v into %+v", c, got)
		}
	}
}

func TestDecodeRejectsMalformedInput(t *testing.T) {
	good := Encode(Command{Op: OpCAS, Client: 1, Seq: 1, Key: "k", Value: []byte("v"), Expect: []byte("e")})
	for i := range len(good) {
		if _, err := Decode(good[:i]); err == nil {
			t.Fatalf("accepted a command cut at byte %d", i)
		}
	}
	bad := [][]byte{
		nil,
		{0},
		{99, 0, 0, 0},
		append(Encode(Command{Op: OpGet, Key: "k"}), 0),
		{byte(OpPut), 0, 0, 1, 'k', 200, 1},
		{byte(OpCAS), 0, 0, 0, 0, 2},
		append(Encode(Command{Op: OpPut, Key: "k", Value: []byte("v")}), 0),
		append(Encode(Command{Op: OpPut, Key: "k", Value: []byte("v"), Lease: 5}), 1),
		append(Encode(Command{Op: OpDelete, Key: "k"}), 5),
	}
	for i, b := range bad {
		if _, err := Decode(b); err == nil {
			t.Fatalf("case %d: accepted %v", i, b)
		}
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(Encode(Command{Op: OpPut, Client: 1, Seq: 2, Key: "k", Value: []byte("v")}))
	f.Add(Encode(Command{Op: OpCAS, Key: "k", ExpectMissing: true}))
	f.Add(Encode(Command{Op: OpRange, Key: "p", Limit: 3}))
	f.Fuzz(func(t *testing.T, data []byte) {
		c, err := Decode(data)
		if err != nil {
			return
		}
		again, err := Decode(Encode(c))
		if err != nil {
			t.Fatalf("re-encoding a decoded command failed: %v", err)
		}
		if !reflect.DeepEqual(normalize(c), normalize(again)) {
			t.Fatalf("decode(encode(c)) != c: %+v %+v", c, again)
		}
	})
}

func normalize(c Command) Command {
	if len(c.Value) == 0 {
		c.Value = nil
	}
	if len(c.Expect) == 0 {
		c.Expect = nil
	}
	return c
}

func TestPutGetDeleteAndRevisions(t *testing.T) {
	a := newApplier(0)
	a.do(Command{Op: OpPut, Key: "k", Value: []byte("1")})
	a.noop()
	a.do(Command{Op: OpPut, Key: "k", Value: []byte("2")})
	r := a.do(Command{Op: OpGet, Key: "k"})
	if !r.Found || string(r.KV.Value) != "2" || r.KV.CreateRev != 1 || r.KV.ModRev != 3 || r.KV.Version != 2 {
		t.Fatalf("get: %+v", r)
	}
	if r := a.do(Command{Op: OpDelete, Key: "k"}); !r.Ok {
		t.Fatalf("delete: %+v", r)
	}
	if r := a.do(Command{Op: OpDelete, Key: "k"}); r.Ok {
		t.Fatalf("second delete reported a deletion: %+v", r)
	}
	a.do(Command{Op: OpPut, Key: "k", Value: []byte("3")})
	r = a.do(Command{Op: OpGet, Key: "k"})
	if r.KV.CreateRev != 7 || r.KV.Version != 1 {
		t.Fatalf("a key created again starts over: %+v", r.KV)
	}
	if a.s.Rev() != 8 {
		t.Fatalf("rev %d", a.s.Rev())
	}
}

func TestCompareAndSwap(t *testing.T) {
	a := newApplier(0)
	if r := a.do(Command{Op: OpCAS, Key: "k", Value: []byte("a"), ExpectMissing: true}); !r.Ok {
		t.Fatalf("create on a missing key: %+v", r)
	}
	if r := a.do(Command{Op: OpCAS, Key: "k", Value: []byte("b"), ExpectMissing: true}); r.Ok || string(r.KV.Value) != "a" {
		t.Fatalf("create on an existing key: %+v", r)
	}
	if r := a.do(Command{Op: OpCAS, Key: "k", Value: []byte("b"), Expect: []byte("x")}); r.Ok {
		t.Fatalf("swap from the wrong value: %+v", r)
	}
	if r := a.do(Command{Op: OpCAS, Key: "k", Value: []byte("b"), Expect: []byte("a")}); !r.Ok {
		t.Fatalf("swap from the right value: %+v", r)
	}
	if r := a.do(Command{Op: OpCASRev, Key: "k", Value: []byte("c"), ExpectRev: 1}); r.Ok {
		t.Fatalf("swap on a stale revision: %+v", r)
	}
	if r := a.do(Command{Op: OpCASRev, Key: "k", Value: []byte("c"), ExpectRev: 4}); !r.Ok {
		t.Fatalf("swap on the current revision: %+v", r)
	}
	if kv, _ := a.s.Get("k"); string(kv.Value) != "c" || kv.Version != 3 {
		t.Fatalf("final %+v", kv)
	}
}

func TestRangeByPrefix(t *testing.T) {
	a := newApplier(0)
	for _, k := range []string{"/a/1", "/a/2", "/a/3", "/b/1", "/a"} {
		a.do(Command{Op: OpPut, Key: k, Value: []byte(k)})
	}
	r := a.do(Command{Op: OpRange, Key: "/a/"})
	var keys []string
	for _, kv := range r.Range {
		keys = append(keys, kv.Key)
	}
	if !reflect.DeepEqual(keys, []string{"/a/1", "/a/2", "/a/3"}) {
		t.Fatalf("range: %v", keys)
	}
	if r := a.do(Command{Op: OpRange, Key: "/a/", Limit: 2}); len(r.Range) != 2 {
		t.Fatalf("limit: %d", len(r.Range))
	}
}

func TestRetriedCommandAppliesOnce(t *testing.T) {
	a := newApplier(0)
	reg := a.do(Command{Op: OpRegister})
	id := reg.Client
	put := Command{Op: OpCAS, Client: id, Seq: 1, Key: "k", Value: []byte("a"), ExpectMissing: true}
	first := a.do(put)
	a.do(Command{Op: OpPut, Key: "k", Value: []byte("someone else")})
	retry := a.do(put)
	if !first.Ok || !retry.Ok || retry.Rev != first.Rev {
		t.Fatalf("first %+v retry %+v, but the retry must return the first result", first, retry)
	}
	if kv, _ := a.s.Get("k"); string(kv.Value) != "someone else" {
		t.Fatalf("the retry applied again: %q", kv.Value)
	}
	a.do(Command{Op: OpPut, Client: id, Seq: 2, Key: "k", Value: []byte("b")})
	if r := a.do(put); r.Code != ErrStaleSequence {
		t.Fatalf("an older sequence number: %+v", r)
	}
}

func TestUnknownSessionIsRefused(t *testing.T) {
	a := newApplier(0)
	if r := a.do(Command{Op: OpPut, Client: 42, Seq: 1, Key: "k", Value: []byte("v")}); r.Code != ErrSessionExpired {
		t.Fatalf("%+v", r)
	}
	if _, ok := a.s.Get("k"); ok {
		t.Fatal("a command from an unknown session was applied")
	}
}

func TestLeastRecentlyUsedSessionIsEvicted(t *testing.T) {
	a := newApplier(2)
	c1 := a.do(Command{Op: OpRegister}).Client
	c2 := a.do(Command{Op: OpRegister}).Client
	a.do(Command{Op: OpPut, Client: c1, Seq: 1, Key: "k", Value: []byte("v")})
	c3 := a.do(Command{Op: OpRegister}).Client
	if r := a.do(Command{Op: OpPut, Client: c2, Seq: 1, Key: "k", Value: []byte("v")}); r.Code != ErrSessionExpired {
		t.Fatalf("the idle session survived: %+v", r)
	}
	for _, c := range []uint64{c1, c3} {
		if r := a.do(Command{Op: OpPut, Client: c, Seq: 2, Key: "k", Value: []byte("v")}); r.Code != OK {
			t.Fatalf("session %d: %+v", c, r)
		}
	}
	if len(a.s.sessions) != 2 {
		t.Fatalf("%d sessions", len(a.s.sessions))
	}
}

func TestSameCommandsSameHash(t *testing.T) {
	build := func() *Store {
		a := newApplier(0)
		id := a.do(Command{Op: OpRegister}).Client
		a.do(Command{Op: OpPut, Client: id, Seq: 1, Key: "x", Value: []byte("1")})
		a.do(Command{Op: OpPut, Key: "y", Value: []byte("2")})
		return a.s
	}
	x, y := build(), build()
	if x.Hash() != y.Hash() {
		t.Fatal("same history, different hash")
	}
	a := newApplier(0)
	a.do(Command{Op: OpPut, Key: "y", Value: []byte("2")})
	if a.s.Hash() == build().Hash() {
		t.Fatal("different state, same hash")
	}
}

func TestBadCommandsAndEmptyEntriesOnlyAdvanceTheRevision(t *testing.T) {
	s := NewStore(0)
	if r := s.Apply(raft.Entry{Term: 1, Index: 1, Data: []byte{0xff}}); r.Code != ErrBadCommand {
		t.Fatalf("%+v", r)
	}
	s.Apply(raft.Entry{Term: 1, Index: 2})
	if kvs := s.Range("", 0); s.Rev() != 2 || len(kvs) != 0 {
		t.Fatalf("rev %d keys %v", s.Rev(), kvs)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("applying an old index did not panic")
		}
	}()
	s.Apply(raft.Entry{Term: 1, Index: 2})
}

func populated() *applier {
	a := newApplier(0)
	id := a.do(Command{Op: OpRegister}).Client
	a.do(Command{Op: OpCAS, Client: id, Seq: 1, Key: "k", Value: []byte("v1"), ExpectMissing: true})
	a.do(Command{Op: OpPut, Key: "a", Value: []byte("1")})
	a.do(Command{Op: OpPut, Key: "b", Value: nil})
	a.do(Command{Op: OpRange, Client: id, Seq: 2, Key: ""})
	a.do(Command{Op: OpDelete, Key: "a"})
	a.do(Command{Op: OpRegister})
	l := a.do(Command{Op: OpLeaseGrant, Client: id, Seq: 3, TTL: 5000}).Lease
	a.do(Command{Op: OpPut, Key: "leased", Value: []byte("x"), Lease: l})
	a.do(Command{Op: OpLeaseGrant, TTL: 700})
	return a
}

func TestSnapshotRoundTrip(t *testing.T) {
	a := populated()
	data := a.s.Snapshot()
	r, err := Restore(data, 0)
	if err != nil {
		t.Fatal(err)
	}
	if r.Hash() != a.s.Hash() || r.Rev() != a.s.Rev() {
		t.Fatalf("restored store differs: rev %d/%d", r.Rev(), a.s.Rev())
	}
	if string(r.Snapshot()) != string(data) {
		t.Fatal("snapshot of a restored store is not byte for byte the same")
	}
	retry := Command{Op: OpRange, Client: 1, Seq: 2, Key: ""}
	want := a.do(retry)
	b := &applier{s: r, index: a.index - 1}
	if got := b.do(retry); string(appendResult(nil, got)) != string(appendResult(nil, want)) {
		t.Fatalf("a retry after restore answered %+v, want %+v", got, want)
	}
}

func TestSnapshotRejectsTruncatedData(t *testing.T) {
	data := populated().s.Snapshot()
	for i := range len(data) {
		if _, err := Restore(data[:i], 0); err == nil {
			t.Fatalf("restored from the first %d of %d bytes", i, len(data))
		}
	}
	if _, err := Restore(append(data, 0), 0); err == nil {
		t.Fatal("restored with a trailing byte")
	}
}

func FuzzRestore(f *testing.F) {
	f.Add(populated().s.Snapshot())
	f.Fuzz(func(t *testing.T, data []byte) {
		s, err := Restore(data, 0)
		if err != nil {
			return
		}
		again, err := Restore(s.Snapshot(), 0)
		if err != nil || again.Hash() != s.Hash() {
			t.Fatalf("a restored store did not survive a second round trip: %v", err)
		}
	})
}

func TestALeaseTakesItsKeysWithIt(t *testing.T) {
	a := newApplier(0)
	g := a.do(Command{Op: OpLeaseGrant, TTL: 3000})
	if !g.Ok || g.Lease != a.index || g.TTL != 3000 {
		t.Fatalf("grant: %+v", g)
	}
	a.do(Command{Op: OpPut, Key: "/registry/api/1", Value: []byte("10.0.0.1"), Lease: g.Lease})
	a.do(Command{Op: OpPut, Key: "/registry/api/2", Value: []byte("10.0.0.2"), Lease: g.Lease})
	a.do(Command{Op: OpPut, Key: "/registry/api/3", Value: []byte("10.0.0.3"), Lease: g.Lease})
	a.do(Command{Op: OpPut, Key: "/registry/api/3", Value: []byte("10.0.0.3")})
	a.do(Command{Op: OpDelete, Key: "/registry/api/2"})
	if info, _ := a.s.Lease(g.Lease); info.Keys != 1 {
		t.Fatalf("lease holds %d keys, want 1 after one was overwritten without it and one deleted", info.Keys)
	}
	r := a.do(Command{Op: OpLeaseExpire, Lease: g.Lease})
	if !r.Ok || r.Rev != a.index {
		t.Fatalf("expire: %+v", r)
	}
	if _, ok := a.s.Get("/registry/api/1"); ok {
		t.Fatal("a key outlived its lease")
	}
	if kv, ok := a.s.Get("/registry/api/3"); !ok || kv.Lease != 0 {
		t.Fatalf("a key put again without the lease went with it: %+v %v", kv, ok)
	}
	if r := a.do(Command{Op: OpLeaseExpire, Lease: g.Lease}); r.Code != ErrLeaseNotFound {
		t.Fatalf("expiring twice: %+v", r)
	}
	if r := a.do(Command{Op: OpLeaseRevoke, Lease: g.Lease}); r.Code != ErrLeaseNotFound {
		t.Fatalf("revoking an expired lease: %+v", r)
	}
}

func TestAWriteOnAMissingLeaseChangesNothing(t *testing.T) {
	a, untouched := newApplier(0), newApplier(0)
	for _, x := range []*applier{a, untouched} {
		x.do(Command{Op: OpPut, Key: "k", Value: []byte("v")})
	}
	for _, c := range []Command{
		{Op: OpPut, Key: "k", Value: []byte("w"), Lease: 77},
		{Op: OpCAS, Key: "k", Value: []byte("w"), Expect: []byte("v"), Lease: 77},
		{Op: OpCASRev, Key: "k", Value: []byte("w"), ExpectRev: 1, Lease: 77},
	} {
		if r := a.do(c); r.Code != ErrLeaseNotFound {
			t.Fatalf("%v on a missing lease: %+v", c.Op, r)
		}
		untouched.noop()
	}
	if a.s.Hash() != untouched.s.Hash() {
		t.Fatal("a refused write changed more than the revision")
	}
}

func TestAKeyMovesBetweenLeases(t *testing.T) {
	a := newApplier(0)
	one := a.do(Command{Op: OpLeaseGrant, TTL: 1000}).Lease
	two := a.do(Command{Op: OpLeaseGrant, TTL: 1000}).Lease
	a.do(Command{Op: OpPut, Key: "k", Value: []byte("v"), Lease: one})
	a.do(Command{Op: OpCAS, Key: "k", Value: []byte("w"), Expect: []byte("v"), Lease: two})
	a.do(Command{Op: OpLeaseRevoke, Lease: one})
	if kv, ok := a.s.Get("k"); !ok || kv.Lease != two {
		t.Fatalf("revoking the old lease took the key: %+v %v", kv, ok)
	}
	a.do(Command{Op: OpLeaseRevoke, Lease: two})
	if _, ok := a.s.Get("k"); ok {
		t.Fatal("revoking the new lease left the key")
	}
}

func TestLeasesAreBounded(t *testing.T) {
	a := newApplier(0)
	for range MaxLeases {
		a.do(Command{Op: OpLeaseGrant, TTL: 1})
	}
	if r := a.do(Command{Op: OpLeaseGrant, TTL: 1}); r.Code != ErrTooManyLeases {
		t.Fatalf("lease %d: %+v", MaxLeases+1, r)
	}
	if r := a.do(Command{Op: OpLeaseGrant}); r.Code != ErrBadCommand {
		t.Fatalf("a lease with no TTL: %+v", r)
	}
}

func TestLeasesSurviveASnapshot(t *testing.T) {
	a := populated()
	r, err := Restore(a.s.Snapshot(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Leases(), a.s.Leases()) {
		t.Fatalf("leases %+v, want %+v", r.Leases(), a.s.Leases())
	}
	b := &applier{s: r, index: a.index}
	id := a.s.Leases()[0].ID
	b.do(Command{Op: OpLeaseExpire, Lease: id})
	if _, ok := r.Get("leased"); ok {
		t.Fatal("after a restore, the lease no longer knew its keys")
	}
}
