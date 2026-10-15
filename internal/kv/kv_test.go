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
	}
	for _, c := range cmds {
		got, err := Decode(Encode(c))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		if got.Op != c.Op || got.Client != c.Client || got.Seq != c.Seq || got.Key != c.Key ||
			!bytes.Equal(got.Value, c.Value) || !bytes.Equal(got.Expect, c.Expect) ||
			got.ExpectMissing != c.ExpectMissing || got.ExpectRev != c.ExpectRev || got.Limit != c.Limit {
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
		t.Fatalf("first %+v retry %+v; the retry must return the first result", first, retry)
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
	if a.s.Sessions() != 2 {
		t.Fatalf("%d sessions", a.s.Sessions())
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
	if s.Rev() != 2 || s.Len() != 0 {
		t.Fatalf("rev %d len %d", s.Rev(), s.Len())
	}
	defer func() {
		if recover() == nil {
			t.Fatal("applying an old index did not panic")
		}
	}()
	s.Apply(raft.Entry{Term: 1, Index: 2})
}
