package kv

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"

	"github.com/google/btree"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

type Code uint8

const (
	OK Code = iota
	ErrSessionExpired
	ErrStaleSequence
	ErrBadCommand
	ErrLeaseNotFound
	ErrTooManyLeases
)

func (c Code) String() string {
	switch c {
	case OK:
		return "ok"
	case ErrSessionExpired:
		return "session expired"
	case ErrStaleSequence:
		return "stale sequence"
	case ErrBadCommand:
		return "bad command"
	case ErrLeaseNotFound:
		return "lease not found"
	case ErrTooManyLeases:
		return "too many leases"
	}
	return fmt.Sprintf("Code(%d)", uint8(c))
}

type KeyValue struct {
	Key       string
	Value     []byte
	CreateRev uint64
	ModRev    uint64
	Version   uint64
	Lease     uint64
}

type Result struct {
	Code   Code
	Rev    uint64
	Client uint64
	Found  bool
	Ok     bool
	KV     KeyValue
	Range  []KeyValue
	Lease  uint64
	TTL    uint64
}

type lease struct {
	ttl  uint64
	keys map[string]struct{}
}

type LeaseInfo struct {
	ID   uint64
	TTL  uint64
	Keys int
}

type session struct {
	seq    uint64
	result Result
	last   uint64
}

const (
	DefaultMaxSessions = 10000
	MaxLeases          = 10000
)

type Store struct {
	tree        *btree.BTreeG[KeyValue]
	rev         uint64
	sessions    map[uint64]*session
	maxSessions int
	leases      map[uint64]*lease
}

func lessKey(a, b KeyValue) bool {
	return a.Key < b.Key
}

func NewStore(maxSessions int) *Store {
	if maxSessions <= 0 {
		maxSessions = DefaultMaxSessions
	}
	return &Store{
		tree:        btree.NewG(32, lessKey),
		sessions:    map[uint64]*session{},
		maxSessions: maxSessions,
		leases:      map[uint64]*lease{},
	}
}

func (s *Store) Lease(id uint64) (LeaseInfo, bool) {
	l, ok := s.leases[id]
	if !ok {
		return LeaseInfo{}, false
	}
	return LeaseInfo{ID: id, TTL: l.ttl, Keys: len(l.keys)}, true
}

func (s *Store) Leases() []LeaseInfo {
	out := make([]LeaseInfo, 0, len(s.leases))
	for id, l := range s.leases {
		out = append(out, LeaseInfo{ID: id, TTL: l.ttl, Keys: len(l.keys)})
	}
	slices.SortFunc(out, func(a, b LeaseInfo) int { return cmpUint(a.ID, b.ID) })
	return out
}

func cmpUint(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func (s *Store) Rev() uint64 {
	return s.rev
}

func (s *Store) Len() int {
	return s.tree.Len()
}

func (s *Store) Sessions() int {
	return len(s.sessions)
}

func (s *Store) Get(key string) (KeyValue, bool) {
	return s.tree.Get(KeyValue{Key: key})
}

func (s *Store) Apply(e raft.Entry) Result {
	if e.Index <= s.rev {
		panic(fmt.Sprintf("kv: applying entry %d after %d", e.Index, s.rev))
	}
	s.rev = e.Index
	if e.Type != raft.EntryNormal || len(e.Data) == 0 {
		return Result{Rev: e.Index}
	}
	cmd, err := Decode(e.Data)
	if err != nil {
		return Result{Code: ErrBadCommand, Rev: e.Index}
	}
	if cmd.Op == OpRegister {
		s.register(e.Index)
		return Result{Rev: e.Index, Client: e.Index}
	}
	if cmd.Op == OpLeaseExpire {
		return s.revoke(cmd.Lease, e.Index)
	}
	if cmd.Client == 0 {
		return s.exec(cmd, e.Index)
	}
	sess, ok := s.sessions[cmd.Client]
	if !ok {
		return Result{Code: ErrSessionExpired, Rev: e.Index}
	}
	sess.last = e.Index
	switch {
	case cmd.Seq < sess.seq:
		return Result{Code: ErrStaleSequence, Rev: e.Index}
	case cmd.Seq == sess.seq:
		return sess.result
	}
	res := s.exec(cmd, e.Index)
	sess.seq, sess.result = cmd.Seq, res
	return res
}

func (s *Store) register(index uint64) {
	if len(s.sessions) >= s.maxSessions {
		var oldest uint64
		oldestLast := ^uint64(0)
		for id, sess := range s.sessions {
			if sess.last < oldestLast {
				oldest, oldestLast = id, sess.last
			}
		}
		delete(s.sessions, oldest)
	}
	s.sessions[index] = &session{last: index}
}

func (s *Store) exec(cmd Command, rev uint64) Result {
	if cmd.writes() && cmd.Lease != 0 {
		if _, ok := s.leases[cmd.Lease]; !ok {
			return Result{Code: ErrLeaseNotFound, Rev: rev}
		}
	}
	cur, found := s.tree.Get(KeyValue{Key: cmd.Key})
	switch cmd.Op {
	case OpGet:
		return Result{Rev: rev, Found: found, KV: cur, Ok: true}
	case OpRange:
		return Result{Rev: rev, Range: s.rangePrefix(cmd.Key, cmd.Limit), Ok: true}
	case OpPut:
		s.put(cmd.Key, cmd.Value, cmd.Lease, rev)
		return Result{Rev: rev, Ok: true, Found: found, KV: cur}
	case OpDelete:
		if found {
			s.remove(cur)
		}
		return Result{Rev: rev, Ok: found, Found: found, KV: cur}
	case OpCAS:
		match := (cmd.ExpectMissing && !found) || (!cmd.ExpectMissing && found && bytes.Equal(cur.Value, cmd.Expect))
		if match {
			s.put(cmd.Key, cmd.Value, cmd.Lease, rev)
		}
		return Result{Rev: rev, Ok: match, Found: found, KV: cur}
	case OpCASRev:
		match := (cmd.ExpectRev == 0 && !found) || (found && cur.ModRev == cmd.ExpectRev)
		if match {
			s.put(cmd.Key, cmd.Value, cmd.Lease, rev)
		}
		return Result{Rev: rev, Ok: match, Found: found, KV: cur}
	case OpLeaseGrant:
		switch {
		case cmd.TTL == 0:
			return Result{Code: ErrBadCommand, Rev: rev}
		case len(s.leases) >= MaxLeases:
			return Result{Code: ErrTooManyLeases, Rev: rev}
		}
		s.leases[rev] = &lease{ttl: cmd.TTL, keys: map[string]struct{}{}}
		return Result{Rev: rev, Ok: true, Lease: rev, TTL: cmd.TTL}
	case OpLeaseRevoke:
		return s.revoke(cmd.Lease, rev)
	}
	return Result{Code: ErrBadCommand, Rev: rev}
}

func (s *Store) put(key string, value []byte, leaseID, rev uint64) {
	kv := KeyValue{Key: key, Value: value, CreateRev: rev, ModRev: rev, Version: 1, Lease: leaseID}
	if cur, ok := s.tree.Get(KeyValue{Key: key}); ok {
		kv.CreateRev = cur.CreateRev
		kv.Version = cur.Version + 1
		s.detach(cur)
	}
	if leaseID != 0 {
		s.leases[leaseID].keys[key] = struct{}{}
	}
	s.tree.ReplaceOrInsert(kv)
}

func (s *Store) remove(kv KeyValue) {
	s.detach(kv)
	s.tree.Delete(kv)
}

func (s *Store) detach(kv KeyValue) {
	if l, ok := s.leases[kv.Lease]; ok {
		delete(l.keys, kv.Key)
	}
}

func (s *Store) revoke(id, rev uint64) Result {
	l, ok := s.leases[id]
	if !ok {
		return Result{Code: ErrLeaseNotFound, Rev: rev}
	}
	keys := make([]string, 0, len(l.keys))
	for k := range l.keys {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		s.tree.Delete(KeyValue{Key: k})
	}
	delete(s.leases, id)
	return Result{Rev: rev, Ok: true, Lease: id, TTL: l.ttl}
}

func (s *Store) Range(prefix string, limit uint64) []KeyValue {
	return s.rangePrefix(prefix, limit)
}

func (s *Store) rangePrefix(prefix string, limit uint64) []KeyValue {
	var out []KeyValue
	s.tree.AscendGreaterOrEqual(KeyValue{Key: prefix}, func(kv KeyValue) bool {
		if !strings.HasPrefix(kv.Key, prefix) {
			return false
		}
		out = append(out, kv)
		return limit == 0 || uint64(len(out)) < limit
	})
	return out
}

func (s *Store) Hash() uint64 {
	h := fnv.New64a()
	buf := binary.AppendUvarint(nil, s.rev)
	h.Write(buf)
	s.tree.Ascend(func(kv KeyValue) bool {
		buf = binary.AppendUvarint(buf[:0], uint64(len(kv.Key)))
		buf = append(buf, kv.Key...)
		buf = binary.AppendUvarint(buf, uint64(len(kv.Value)))
		buf = append(buf, kv.Value...)
		buf = binary.AppendUvarint(buf, kv.CreateRev)
		buf = binary.AppendUvarint(buf, kv.ModRev)
		buf = binary.AppendUvarint(buf, kv.Version)
		buf = binary.AppendUvarint(buf, kv.Lease)
		h.Write(buf)
		return true
	})
	for _, l := range s.Leases() {
		buf = binary.AppendUvarint(buf[:0], l.ID)
		buf = binary.AppendUvarint(buf, l.TTL)
		buf = binary.AppendUvarint(buf, uint64(l.Keys))
		h.Write(buf)
	}
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		sess := s.sessions[id]
		buf = binary.AppendUvarint(buf[:0], id)
		buf = binary.AppendUvarint(buf, sess.seq)
		buf = binary.AppendUvarint(buf, sess.last)
		buf = binary.AppendUvarint(buf, sess.result.Rev)
		h.Write(buf)
	}
	return h.Sum64()
}
