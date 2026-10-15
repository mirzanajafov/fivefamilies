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
	}
	return fmt.Sprintf("Code(%d)", uint8(c))
}

type KeyValue struct {
	Key       string
	Value     []byte
	CreateRev uint64
	ModRev    uint64
	Version   uint64
}

type Result struct {
	Code   Code
	Rev    uint64
	Client uint64
	Found  bool
	Ok     bool
	KV     KeyValue
	Range  []KeyValue
}

type session struct {
	seq    uint64
	result Result
	last   uint64
}

const DefaultMaxSessions = 10000

type Store struct {
	tree        *btree.BTreeG[KeyValue]
	rev         uint64
	sessions    map[uint64]*session
	maxSessions int
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
	}
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
	cur, found := s.tree.Get(KeyValue{Key: cmd.Key})
	switch cmd.Op {
	case OpGet:
		return Result{Rev: rev, Found: found, KV: cur, Ok: true}
	case OpRange:
		return Result{Rev: rev, Range: s.rangePrefix(cmd.Key, cmd.Limit), Ok: true}
	case OpPut:
		s.put(cmd.Key, cmd.Value, rev)
		return Result{Rev: rev, Ok: true, Found: found, KV: cur}
	case OpDelete:
		if found {
			s.tree.Delete(cur)
		}
		return Result{Rev: rev, Ok: found, Found: found, KV: cur}
	case OpCAS:
		match := (cmd.ExpectMissing && !found) || (!cmd.ExpectMissing && found && bytes.Equal(cur.Value, cmd.Expect))
		if match {
			s.put(cmd.Key, cmd.Value, rev)
		}
		return Result{Rev: rev, Ok: match, Found: found, KV: cur}
	case OpCASRev:
		match := (cmd.ExpectRev == 0 && !found) || (found && cur.ModRev == cmd.ExpectRev)
		if match {
			s.put(cmd.Key, cmd.Value, rev)
		}
		return Result{Rev: rev, Ok: match, Found: found, KV: cur}
	}
	return Result{Code: ErrBadCommand, Rev: rev}
}

func (s *Store) put(key string, value []byte, rev uint64) {
	kv := KeyValue{Key: key, Value: value, CreateRev: rev, ModRev: rev, Version: 1}
	if cur, ok := s.tree.Get(KeyValue{Key: key}); ok {
		kv.CreateRev = cur.CreateRev
		kv.Version = cur.Version + 1
	}
	s.tree.ReplaceOrInsert(kv)
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
		h.Write(buf)
		return true
	})
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
