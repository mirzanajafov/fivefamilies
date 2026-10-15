package kv

import (
	"encoding/binary"
	"errors"
	"slices"
)

var snapshotMagic = []byte("FFKV1")

var ErrBadSnapshot = errors.New("kv: malformed snapshot")

func appendKV(b []byte, kv KeyValue) []byte {
	b = appendBytes(b, []byte(kv.Key))
	b = appendBytes(b, kv.Value)
	b = binary.AppendUvarint(b, kv.CreateRev)
	b = binary.AppendUvarint(b, kv.ModRev)
	return binary.AppendUvarint(b, kv.Version)
}

func appendResult(b []byte, r Result) []byte {
	b = append(b, byte(r.Code))
	b = binary.AppendUvarint(b, r.Rev)
	b = binary.AppendUvarint(b, r.Client)
	var flags byte
	if r.Found {
		flags |= 1
	}
	if r.Ok {
		flags |= 2
	}
	b = append(b, flags)
	b = appendKV(b, r.KV)
	b = binary.AppendUvarint(b, uint64(len(r.Range)))
	for _, kv := range r.Range {
		b = appendKV(b, kv)
	}
	return b
}

func (s *Store) Snapshot() []byte {
	b := append([]byte(nil), snapshotMagic...)
	b = binary.AppendUvarint(b, s.rev)
	ids := make([]uint64, 0, len(s.sessions))
	for id := range s.sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	b = binary.AppendUvarint(b, uint64(len(ids)))
	for _, id := range ids {
		sess := s.sessions[id]
		b = binary.AppendUvarint(b, id)
		b = binary.AppendUvarint(b, sess.seq)
		b = binary.AppendUvarint(b, sess.last)
		b = appendResult(b, sess.result)
	}
	b = binary.AppendUvarint(b, uint64(s.tree.Len()))
	s.tree.Ascend(func(kv KeyValue) bool {
		b = appendKV(b, kv)
		return true
	})
	return b
}

func (d *decoder) kv() KeyValue {
	var kv KeyValue
	kv.Key = string(d.bytes())
	if v := d.bytes(); len(v) > 0 {
		kv.Value = append([]byte(nil), v...)
	}
	kv.CreateRev = d.uvarint()
	kv.ModRev = d.uvarint()
	kv.Version = d.uvarint()
	return kv
}

func (d *decoder) count() int {
	n := d.uvarint()
	if n > uint64(len(d.b)) {
		d.err = true
		return 0
	}
	return int(n)
}

func (d *decoder) result() Result {
	var r Result
	r.Code = Code(d.byte())
	r.Rev = d.uvarint()
	r.Client = d.uvarint()
	flags := d.byte()
	r.Found, r.Ok = flags&1 != 0, flags&2 != 0
	r.KV = d.kv()
	for range d.count() {
		r.Range = append(r.Range, d.kv())
	}
	return r
}

func Restore(data []byte, maxSessions int) (*Store, error) {
	if len(data) < len(snapshotMagic) || string(data[:len(snapshotMagic)]) != string(snapshotMagic) {
		return nil, ErrBadSnapshot
	}
	s := NewStore(maxSessions)
	d := decoder{b: data[len(snapshotMagic):]}
	s.rev = d.uvarint()
	for range d.count() {
		id := d.uvarint()
		sess := &session{seq: d.uvarint(), last: d.uvarint()}
		sess.result = d.result()
		if _, dup := s.sessions[id]; dup {
			d.err = true
		}
		s.sessions[id] = sess
	}
	var prev string
	for i := range d.count() {
		kv := d.kv()
		if d.err || (i > 0 && kv.Key <= prev) {
			return nil, ErrBadSnapshot
		}
		prev = kv.Key
		s.tree.ReplaceOrInsert(kv)
	}
	if d.err || len(d.b) != 0 {
		return nil, ErrBadSnapshot
	}
	return s, nil
}

func (s *Store) Clone() *Store {
	sessions := make(map[uint64]*session, len(s.sessions))
	for id, sess := range s.sessions {
		c := *sess
		sessions[id] = &c
	}
	return &Store{tree: s.tree.Clone(), rev: s.rev, sessions: sessions, maxSessions: s.maxSessions}
}
