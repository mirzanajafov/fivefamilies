package kv

import (
	"encoding/binary"
	"errors"
	"slices"
)

var (
	snapshotMagic   = []byte("FFKV2")
	snapshotMagicV1 = []byte("FFKV1")
)

var ErrBadSnapshot = errors.New("kv: malformed snapshot")

func appendKV(b []byte, kv KeyValue) []byte {
	b = appendBytes(b, []byte(kv.Key))
	b = appendBytes(b, kv.Value)
	b = binary.AppendUvarint(b, kv.CreateRev)
	b = binary.AppendUvarint(b, kv.ModRev)
	b = binary.AppendUvarint(b, kv.Version)
	return binary.AppendUvarint(b, kv.Lease)
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
	b = binary.AppendUvarint(b, r.Lease)
	return binary.AppendUvarint(b, r.TTL)
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
	leases := s.Leases()
	b = binary.AppendUvarint(b, uint64(len(leases)))
	for _, l := range leases {
		b = binary.AppendUvarint(b, l.ID)
		b = binary.AppendUvarint(b, l.TTL)
	}
	b = binary.AppendUvarint(b, uint64(s.tree.Len()))
	s.tree.Ascend(func(kv KeyValue) bool {
		b = appendKV(b, kv)
		return true
	})
	return b
}

func (d *decoder) kv(v1 bool) KeyValue {
	var kv KeyValue
	kv.Key = string(d.bytes())
	if v := d.bytes(); len(v) > 0 {
		kv.Value = append([]byte(nil), v...)
	}
	kv.CreateRev = d.uvarint()
	kv.ModRev = d.uvarint()
	kv.Version = d.uvarint()
	if !v1 {
		kv.Lease = d.uvarint()
	}
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

func (d *decoder) result(v1 bool) Result {
	var r Result
	r.Code = Code(d.byte())
	r.Rev = d.uvarint()
	r.Client = d.uvarint()
	flags := d.byte()
	r.Found, r.Ok = flags&1 != 0, flags&2 != 0
	r.KV = d.kv(v1)
	for range d.count() {
		r.Range = append(r.Range, d.kv(v1))
	}
	if !v1 {
		r.Lease = d.uvarint()
		r.TTL = d.uvarint()
	}
	return r
}

func Restore(data []byte, maxSessions int) (*Store, error) {
	var v1 bool
	switch {
	case len(data) >= len(snapshotMagic) && string(data[:len(snapshotMagic)]) == string(snapshotMagic):
	case len(data) >= len(snapshotMagicV1) && string(data[:len(snapshotMagicV1)]) == string(snapshotMagicV1):
		v1 = true
	default:
		return nil, ErrBadSnapshot
	}
	s := NewStore(maxSessions)
	d := decoder{b: data[len(snapshotMagic):]}
	s.rev = d.uvarint()
	for range d.count() {
		id := d.uvarint()
		sess := &session{seq: d.uvarint(), last: d.uvarint()}
		sess.result = d.result(v1)
		if _, dup := s.sessions[id]; dup {
			d.err = true
		}
		s.sessions[id] = sess
	}
	if !v1 {
		var prev uint64
		for i := range d.count() {
			id, ttl := d.uvarint(), d.uvarint()
			if d.err || id == 0 || ttl == 0 || (i > 0 && id <= prev) {
				return nil, ErrBadSnapshot
			}
			prev = id
			s.leases[id] = &lease{ttl: ttl, keys: map[string]struct{}{}}
		}
	}
	var prev string
	for i := range d.count() {
		kv := d.kv(v1)
		if d.err || (i > 0 && kv.Key <= prev) {
			return nil, ErrBadSnapshot
		}
		if kv.Lease != 0 {
			l, ok := s.leases[kv.Lease]
			if !ok {
				return nil, ErrBadSnapshot
			}
			l.keys[kv.Key] = struct{}{}
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
	leases := make(map[uint64]*lease, len(s.leases))
	for id, l := range s.leases {
		keys := make(map[string]struct{}, len(l.keys))
		for k := range l.keys {
			keys[k] = struct{}{}
		}
		leases[id] = &lease{ttl: l.ttl, keys: keys}
	}
	return &Store{tree: s.tree.Clone(), rev: s.rev, sessions: sessions, maxSessions: s.maxSessions, leases: leases}
}
