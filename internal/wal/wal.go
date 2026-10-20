package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"strconv"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

const (
	headerSize    = 24
	recordHeader  = 9
	maxRecord     = 64 << 20
	segmentSuffix = ".wal"
)

const (
	recEntry    byte = 1
	recState    byte = 2
	recSnapshot byte = 3
)

var (
	magic      = [8]byte{'F', 'F', 'W', 'A', 'L', 0, 0, 1}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt = errors.New("wal: corrupt log")
)

type Options struct {
	SegmentBytes int64
	Salt         func() uint64
}

type Recovery struct {
	Segments     int
	Records      int
	TornBytes    int64
	DroppedEmpty bool
}

type Batch struct {
	HardState raft.HardState
	Snapshot  *raft.Snapshot
	Reset     bool
	Entries   []raft.Entry
}

func (b Batch) Empty() bool {
	return b.HardState.IsEmpty() && b.Snapshot == nil && len(b.Entries) == 0
}

type segment struct {
	seq      uint64
	maxIndex uint64
}

type Log struct {
	fs        vfs.FS
	opts      Options
	segs      []segment
	markerSeq uint64
	f         vfs.File
	size      int64
	salt      uint64
	buf       []byte
}

func segmentName(seq uint64) string {
	return fmt.Sprintf("%016x%s", seq, segmentSuffix)
}

func parseSegmentName(name string) (uint64, bool) {
	hex, ok := strings.CutSuffix(name, segmentSuffix)
	if !ok || len(hex) != 16 {
		return 0, false
	}
	seq, err := strconv.ParseUint(hex, 16, 64)
	return seq, err == nil && seq > 0
}

func Exists(fs vfs.FS) (bool, error) {
	names, err := fs.List()
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(names, func(name string) bool {
		_, ok := parseSegmentName(name)
		return ok
	}), nil
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

type record struct {
	seg     int
	typ     byte
	payload []byte
}

type replay struct {
	hs        raft.HardState
	snap      *raft.Snapshot
	ents      []raft.Entry
	off       uint64
	strict    bool
	markerSeq uint64
}

func (r *replay) last() uint64 {
	return r.off + uint64(len(r.ents))
}

func (r *replay) entry(e raft.Entry) error {
	last := r.last()
	if !r.strict {
		if e.Index <= r.off || e.Index > last+1 {
			r.ents, r.off = nil, e.Index-1
		} else if e.Index <= last {
			r.ents = r.ents[:e.Index-r.off-1]
		}
		r.ents = append(r.ents, e)
		return nil
	}
	switch {
	case e.Index <= r.off:
		return corrupt("entry %d at or before the snapshot at %d", e.Index, r.off)
	case e.Index > last+1:
		return corrupt("entry %d follows entry %d", e.Index, last)
	case e.Index <= last:
		r.ents = r.ents[:e.Index-r.off-1]
	}
	r.ents = append(r.ents, e)
	return nil
}

func (r *replay) snapshot(index, term uint64, reset bool) error {
	switch {
	case reset:
		r.ents = nil
	case !r.strict && (len(r.ents) == 0 || index > r.last()):
		r.ents = nil
	case !r.strict && index < r.off:
		return corrupt("the log was compacted and entries %d..%d are missing", index+1, r.off)
	case index < r.off || index > r.last():
		return corrupt("compaction to %d outside the log %d..%d", index, r.off, r.last())
	default:
		if index > r.off && r.ents[index-r.off-1].Term != term {
			return corrupt("compaction to %d in term %d, the log has term %d there", index, term, r.ents[index-r.off-1].Term)
		}
		r.ents = r.ents[index-r.off:]
	}
	r.off = index
	r.snap = &raft.Snapshot{Index: index, Term: term}
	r.strict = true
	return nil
}

func Open(fs vfs.FS, opts Options) (*Log, raft.Restore, Recovery, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if opts.Salt == nil {
		return nil, raft.Restore{}, Recovery{}, errors.New("wal: no salt source")
	}
	l := &Log{fs: fs, opts: opts}
	var rec Recovery
	names, err := fs.List()
	if err != nil {
		return nil, raft.Restore{}, rec, err
	}
	var seqs []uint64
	for _, n := range names {
		if seq, ok := parseSegmentName(n); ok {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, raft.Restore{}, rec, corrupt("segment %d follows segment %d", seqs[i], seqs[i-1])
		}
	}
	next := uint64(1)
	if len(seqs) > 0 {
		next = seqs[0]
	}
	var records []record
	for i := 0; i < len(seqs); i++ {
		seq, name, last := seqs[i], segmentName(seqs[i]), i == len(seqs)-1
		data, err := fs.ReadFile(name)
		if err != nil {
			return nil, raft.Restore{}, rec, err
		}
		salt, ok := readHeader(data, seq)
		if !ok {
			if !last {
				return nil, raft.Restore{}, rec, corrupt("segment %s has a bad header", name)
			}
			if err := fs.Remove(name); err != nil {
				return nil, raft.Restore{}, rec, err
			}
			if err := fs.SyncDir(); err != nil {
				return nil, raft.Restore{}, rec, err
			}
			rec.DroppedEmpty = true
			rec.TornBytes += int64(len(data))
			break
		}
		rec.Segments++
		off := headerSize
		for off < len(data) {
			n, typ, payload, ok := readRecord(data[off:], salt)
			if !ok {
				if !last {
					return nil, raft.Restore{}, rec, corrupt("segment %s has a bad record at offset %d", name, off)
				}
				rec.TornBytes += int64(len(data) - off)
				if err := truncateTail(fs, name, int64(off)); err != nil {
					return nil, raft.Restore{}, rec, err
				}
				data = data[:off]
				break
			}
			records = append(records, record{seg: len(l.segs), typ: typ, payload: payload})
			off += n
		}
		l.segs = append(l.segs, segment{seq: seq})
		l.size, l.salt = int64(len(data)), salt
	}
	compacted := next > 1
	lastMarker := -1
	for i, rc := range records {
		if rc.typ == recSnapshot {
			lastMarker = i
		}
	}
	if compacted && lastMarker < 0 {
		return nil, raft.Restore{}, rec, corrupt("the log starts at segment %d and has no snapshot record", next)
	}
	r := replay{strict: !compacted}
	for i, rc := range records {
		var err error
		switch rc.typ {
		case recState:
			r.hs, err = decodeState(rc.payload)
		case recSnapshot:
			index, term, reset, derr := decodeSnapshot(rc.payload)
			switch {
			case derr != nil:
				err = derr
			case compacted && i < lastMarker:
			default:
				err = r.snapshot(index, term, reset)
				r.markerSeq = l.segs[rc.seg].seq
			}
		case recEntry:
			var e raft.Entry
			if e, err = decodeEntry(rc.payload); err == nil {
				err = r.entry(e)
				l.segs[rc.seg].maxIndex = max(l.segs[rc.seg].maxIndex, e.Index)
			}
		default:
			err = corrupt("unknown record type %d", rc.typ)
		}
		if err != nil {
			return nil, raft.Restore{}, rec, fmt.Errorf("segment %s: %w", segmentName(l.segs[rc.seg].seq), err)
		}
		rec.Records++
	}
	if len(l.segs) == 0 {
		if err := l.create(next); err != nil {
			return nil, raft.Restore{}, rec, err
		}
	} else {
		f, err := fs.OpenAppend(segmentName(l.current().seq))
		if err != nil {
			return nil, raft.Restore{}, rec, err
		}
		l.f = f
	}
	l.markerSeq = r.markerSeq
	hs := r.hs
	hs.Commit = max(min(hs.Commit, r.last()), r.off)
	return l, raft.Restore{HardState: hs, Snapshot: r.snap, Entries: r.ents}, rec, nil
}

func truncateTail(fs vfs.FS, name string, size int64) error {
	if err := fs.Truncate(name, size); err != nil {
		return err
	}
	f, err := fs.OpenAppend(name)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}

func (l *Log) current() *segment {
	return &l.segs[len(l.segs)-1]
}

func (l *Log) create(seq uint64) error {
	f, err := l.fs.Create(segmentName(seq))
	if err != nil {
		return err
	}
	salt := l.opts.Salt()
	hdr := make([]byte, headerSize)
	copy(hdr, magic[:])
	binary.LittleEndian.PutUint64(hdr[8:], seq)
	binary.LittleEndian.PutUint64(hdr[16:], salt)
	if _, err := f.Write(hdr); err != nil {
		return errors.Join(err, f.Close())
	}
	if err := l.fs.SyncDir(); err != nil {
		return errors.Join(err, f.Close())
	}
	l.f, l.size, l.salt = f, headerSize, salt
	l.segs = append(l.segs, segment{seq: seq})
	return nil
}

func (l *Log) rotate() error {
	if err := l.f.Sync(); err != nil {
		return err
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	return l.create(l.current().seq + 1)
}

func (l *Log) Write(hs raft.HardState, ents []raft.Entry) error {
	return l.WriteBatch(Batch{HardState: hs, Entries: ents})
}

func (l *Log) WriteBatch(b Batch) error {
	if b.Empty() {
		return nil
	}
	if b.Snapshot != nil && b.HardState.IsEmpty() {
		return errors.New("wal: a snapshot record must carry the hard state, or compacting old segments could lose the term and vote")
	}
	if l.size >= l.opts.SegmentBytes {
		if err := l.rotate(); err != nil {
			return err
		}
	}
	buf := l.buf[:0]
	if !b.HardState.IsEmpty() {
		buf = appendRecord(buf, l.salt, recState, func(p []byte) []byte { return encodeState(p, b.HardState) })
	}
	if b.Snapshot != nil {
		buf = appendRecord(buf, l.salt, recSnapshot, func(p []byte) []byte { return encodeSnapshot(p, b.Snapshot, b.Reset) })
		l.markerSeq = l.current().seq
	}
	for _, e := range b.Entries {
		buf = appendRecord(buf, l.salt, recEntry, func(p []byte) []byte { return encodeEntry(p, e) })
	}
	if n := len(b.Entries); n > 0 {
		cur := l.current()
		cur.maxIndex = max(cur.maxIndex, b.Entries[n-1].Index)
	}
	l.buf = buf
	n, err := l.f.Write(buf)
	l.size += int64(n)
	return err
}

func (l *Log) Sync() error {
	return l.f.Sync()
}

func (l *Log) Close() error {
	return l.f.Close()
}

func (l *Log) Segments() int {
	return len(l.segs)
}

func (l *Log) Compact(index uint64) (int, error) {
	removed := 0
	for len(l.segs) > 1 && l.segs[0].seq < l.markerSeq && l.segs[0].maxIndex <= index {
		if err := l.fs.Remove(segmentName(l.segs[0].seq)); err != nil {
			return removed, err
		}
		l.segs = l.segs[1:]
		removed++
	}
	if removed > 0 {
		return removed, l.fs.SyncDir()
	}
	return 0, nil
}

func readHeader(data []byte, seq uint64) (uint64, bool) {
	if len(data) < headerSize || [8]byte(data[:8]) != magic {
		return 0, false
	}
	if binary.LittleEndian.Uint64(data[8:]) != seq {
		return 0, false
	}
	return binary.LittleEndian.Uint64(data[16:]), true
}

func checksum(salt uint64, typ byte, payload []byte) uint32 {
	var s [9]byte
	binary.LittleEndian.PutUint64(s[:], salt)
	s[8] = typ
	return crc32.Update(crc32.Checksum(s[:], castagnoli), castagnoli, payload)
}

func appendRecord(buf []byte, salt uint64, typ byte, encode func([]byte) []byte) []byte {
	start := len(buf)
	buf = append(buf, make([]byte, recordHeader)...)
	buf = encode(buf)
	payload := buf[start+recordHeader:]
	binary.LittleEndian.PutUint32(buf[start:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[start+4:], checksum(salt, typ, payload))
	buf[start+8] = typ
	return buf
}

func readRecord(b []byte, salt uint64) (int, byte, []byte, bool) {
	if len(b) < recordHeader {
		return 0, 0, nil, false
	}
	n := binary.LittleEndian.Uint32(b)
	if n > maxRecord || uint64(len(b)-recordHeader) < uint64(n) {
		return 0, 0, nil, false
	}
	typ := b[8]
	payload := b[recordHeader : recordHeader+int(n)]
	if binary.LittleEndian.Uint32(b[4:]) != checksum(salt, typ, payload) {
		return 0, 0, nil, false
	}
	return recordHeader + int(n), typ, payload, true
}

func encodeState(b []byte, hs raft.HardState) []byte {
	b = binary.AppendUvarint(b, hs.Term)
	b = binary.AppendUvarint(b, uint64(hs.Vote))
	return binary.AppendUvarint(b, hs.Commit)
}

func decodeState(p []byte) (raft.HardState, error) {
	var hs raft.HardState
	var vote uint64
	var ok bool
	if hs.Term, p, ok = uvarint(p); !ok {
		return hs, corrupt("bad hard state")
	}
	if vote, p, ok = uvarint(p); !ok {
		return hs, corrupt("bad hard state")
	}
	if hs.Commit, p, ok = uvarint(p); !ok || len(p) != 0 {
		return hs, corrupt("bad hard state")
	}
	hs.Vote = raft.NodeID(vote)
	return hs, nil
}

func encodeSnapshot(b []byte, s *raft.Snapshot, reset bool) []byte {
	b = binary.AppendUvarint(b, s.Index)
	b = binary.AppendUvarint(b, s.Term)
	if reset {
		return append(b, 1)
	}
	return append(b, 0)
}

func decodeSnapshot(p []byte) (uint64, uint64, bool, error) {
	index, p, ok := uvarint(p)
	if !ok {
		return 0, 0, false, corrupt("bad snapshot record")
	}
	term, p, ok := uvarint(p)
	if !ok || len(p) != 1 || p[0] > 1 {
		return 0, 0, false, corrupt("bad snapshot record")
	}
	return index, term, p[0] == 1, nil
}

func encodeEntry(b []byte, e raft.Entry) []byte {
	b = binary.AppendUvarint(b, e.Term)
	b = binary.AppendUvarint(b, e.Index)
	b = append(b, byte(e.Type))
	return append(b, e.Data...)
}

func decodeEntry(p []byte) (raft.Entry, error) {
	var e raft.Entry
	var ok bool
	if e.Term, p, ok = uvarint(p); !ok {
		return e, corrupt("bad entry")
	}
	if e.Index, p, ok = uvarint(p); !ok || len(p) == 0 {
		return e, corrupt("bad entry")
	}
	e.Type = raft.EntryType(p[0])
	if !e.Type.Valid() {
		return e, corrupt("bad entry type %d", p[0])
	}
	if len(p) > 1 {
		e.Data = append([]byte(nil), p[1:]...)
	}
	return e, nil
}

func uvarint(p []byte) (uint64, []byte, bool) {
	v, n := binary.Uvarint(p)
	if n <= 0 {
		return 0, p, false
	}
	return v, p[n:], true
}
