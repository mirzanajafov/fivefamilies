package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

const (
	headerSize     = 24
	recordHeader   = 9
	sectorSize     = 512
	maxRecord      = 64 << 20
	readBufferSize = 64 << 10
	segmentSuffix  = ".wal"
)

const (
	recEntry    byte = 1
	recState    byte = 2
	recSnapshot byte = 3
)

var (
	magic        = [8]byte{'F', 'F', 'W', 'A', 'L', 0, 0, 1}
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt   = errors.New("wal: corrupt log")
	errBadRecord = errors.New("wal: bad record")
)

const DefaultSegmentBytes = 8 << 20

type Options struct {
	SegmentBytes int64
	Salt         func() uint64
}

type Recovery struct {
	TornBytes int64
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
	syncedSeq uint64
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
	seqs, err := segments(fs)
	return len(seqs) > 0, err
}

func segments(fs vfs.FS) ([]uint64, error) {
	names, err := fs.List()
	if err != nil {
		return nil, err
	}
	var seqs []uint64
	for _, n := range names {
		if seq, ok := parseSegmentName(n); ok {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	return seqs, nil
}

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

type replay struct {
	hs          raft.HardState
	snap        *raft.Snapshot
	ents        []raft.Entry
	off         uint64
	last        uint64
	keepFrom    uint64
	skipMarkers int
	strict      bool
	markerSeq   uint64
}

func (r *replay) firstKept() uint64 {
	return max(r.off+1, r.keepFrom)
}

func (r *replay) record(seg *segment, typ byte, payload []byte) error {
	switch typ {
	case recState:
		var err error
		r.hs, err = decodeState(payload)
		return err
	case recSnapshot:
		index, term, reset, err := decodeSnapshot(payload)
		switch {
		case err != nil:
			return err
		case r.skipMarkers > 0:
			r.skipMarkers--
			return nil
		}
		r.markerSeq = seg.seq
		return r.snapshot(index, term, reset)
	case recEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		seg.maxIndex = max(seg.maxIndex, e.Index)
		return r.entry(e)
	}
	return corrupt("unknown record type %d", typ)
}

func (r *replay) entry(e raft.Entry) error {
	switch {
	case !r.strict && (e.Index <= r.off || e.Index > r.last+1):
		r.ents, r.off = nil, e.Index-1
	case r.strict && e.Index <= r.off:
		return corrupt("entry %d at or before the snapshot at %d", e.Index, r.off)
	case r.strict && e.Index > r.last+1:
		return corrupt("entry %d follows entry %d", e.Index, r.last)
	}
	if first := r.firstKept(); e.Index >= first {
		r.ents = append(r.ents[:e.Index-first], e)
	} else {
		r.ents = r.ents[:0]
	}
	r.last = e.Index
	return nil
}

func (r *replay) snapshot(index, term uint64, reset bool) error {
	first := r.firstKept()
	switch {
	case reset, !r.strict && (r.last == r.off || index > r.last):
		r.ents, r.last = nil, index
	case !r.strict && index < r.off:
		return corrupt("the log was compacted and entries %d..%d are missing", index+1, r.off)
	case index < r.off || index > r.last:
		return corrupt("compaction to %d outside the log %d..%d", index, r.off, r.last)
	case index > r.off && r.ents[index-first].Term != term:
		return corrupt("compaction to %d in term %d, the log has term %d there", index, term, r.ents[index-first].Term)
	default:
		r.ents = slices.Delete(r.ents, 0, int(index+1-first))
	}
	r.off = index
	r.snap = &raft.Snapshot{Index: index, Term: term}
	r.strict = true
	return nil
}

func Open(fs vfs.FS, opts Options) (*Log, raft.Restore, Recovery, error) {
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if opts.Salt == nil {
		return nil, raft.Restore{}, Recovery{}, errors.New("wal: no salt source")
	}
	l := &Log{fs: fs, opts: opts}
	var rec Recovery
	seqs, err := segments(fs)
	if err != nil {
		return nil, raft.Restore{}, rec, err
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] != seqs[i-1]+1 {
			return nil, raft.Restore{}, rec, corrupt("segment %d follows segment %d", seqs[i], seqs[i-1])
		}
	}
	next := uint64(1)
	if len(seqs) > 0 {
		next = seqs[0]
	}
	rd := reader{buf: bufio.NewReaderSize(nil, readBufferSize)}
	markers, lastMarker := 0, uint64(0)
	seqs, rec.TornBytes, err = rd.scan(fs, seqs, func(typ byte, payload []byte) error {
		if typ != recSnapshot {
			return nil
		}
		markers++
		if index, _, _, err := decodeSnapshot(payload); err == nil {
			lastMarker = index
		}
		return nil
	})
	if err != nil {
		return nil, raft.Restore{}, rec, err
	}
	compacted := next > 1
	if compacted && markers == 0 {
		return nil, raft.Restore{}, rec, corrupt("the log starts at segment %d and has no snapshot record", next)
	}
	r := replay{strict: !compacted}
	if compacted {
		r.skipMarkers, r.keepFrom = markers-1, lastMarker
	}
	for _, seq := range seqs {
		l.segs = append(l.segs, segment{seq: seq})
		seg := l.current()
		end, _, err := rd.records(fs, seq, false, func(typ byte, payload []byte) error {
			return r.record(seg, typ, payload)
		})
		if err != nil {
			return nil, raft.Restore{}, rec, err
		}
		l.size, l.salt = end, rd.salt
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
	l.markerSeq, l.syncedSeq = r.markerSeq, r.markerSeq
	hs := r.hs
	hs.Commit = max(min(hs.Commit, r.last), r.off)
	return l, raft.Restore{HardState: hs, Snapshot: r.snap, Entries: r.ents}, rec, nil
}

type reader struct {
	buf  *bufio.Reader
	rec  []byte
	salt uint64
}

func (rd *reader) scan(fs vfs.FS, seqs []uint64, each func(byte, []byte) error) ([]uint64, int64, error) {
	for i, seq := range seqs {
		end, size, err := rd.records(fs, seq, i == len(seqs)-1, each)
		if err != nil {
			return nil, 0, err
		}
		switch name := segmentName(seq); {
		case end == 0:
			if err := fs.Remove(name); err != nil {
				return nil, 0, err
			}
			return seqs[:i], size, fs.SyncDir()
		case end < size:
			return seqs, size - end, truncateTail(fs, name, end)
		}
	}
	return seqs, 0, nil
}

func (rd *reader) records(fs vfs.FS, seq uint64, mayBeTorn bool, each func(byte, []byte) error) (end, size int64, err error) {
	name := segmentName(seq)
	f, err := fs.Open(name)
	if err != nil {
		return 0, 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	size = f.Size()
	rd.buf.Reset(io.NewSectionReader(f, 0, size))
	hdr := make([]byte, min(headerSize, size))
	if _, err := io.ReadFull(rd.buf, hdr); err != nil {
		return 0, 0, err
	}
	salt, ok := readHeader(hdr, seq)
	if !ok {
		if !mayBeTorn || (len(hdr) == headerSize && !zeros(hdr)) {
			return 0, 0, corrupt("segment %s has a bad header", name)
		}
		return 0, size, nil
	}
	rd.salt = salt
	for off := int64(headerSize); off < size; {
		n, typ, payload, err := rd.next(f, off, size)
		if errors.Is(err, errBadRecord) {
			if mayBeTorn {
				if torn, err := tornAt(f, size, off); err != nil || torn {
					return off, size, err
				}
			}
			return 0, 0, corrupt("segment %s has a bad record at offset %d", name, off)
		}
		if err != nil {
			return 0, 0, err
		}
		if err := each(typ, payload); err != nil {
			return 0, 0, fmt.Errorf("segment %s: %w", name, err)
		}
		off += n
	}
	return size, size, nil
}

func (rd *reader) next(f io.ReaderAt, off, size int64) (int64, byte, []byte, error) {
	if size-off < recordHeader {
		return 0, 0, nil, errBadRecord
	}
	hdr, err := rd.buf.Peek(recordHeader)
	if err != nil {
		return 0, 0, nil, err
	}
	n := recordHeader + int64(binary.LittleEndian.Uint32(hdr))
	switch {
	case n > size-off || n > recordHeader+maxRecord:
		return 0, 0, nil, errBadRecord
	case n > readBufferSize:
		return rd.nextLarge(f, off, n)
	}
	rd.rec = slices.Grow(rd.rec[:0], int(n))[:n]
	if _, err := io.ReadFull(rd.buf, rd.rec); err != nil {
		return 0, 0, nil, err
	}
	_, typ, payload, ok := readRecord(rd.rec, rd.salt)
	if !ok {
		return 0, 0, nil, errBadRecord
	}
	return n, typ, payload, nil
}

func (rd *reader) nextLarge(f io.ReaderAt, off, n int64) (int64, byte, []byte, error) {
	hdr, err := rd.buf.Peek(recordHeader)
	if err != nil {
		return 0, 0, nil, err
	}
	want, typ := binary.LittleEndian.Uint32(hdr[4:]), hdr[8]
	sum := checksum(rd.salt, typ, nil)
	if _, err := rd.buf.Discard(recordHeader); err != nil {
		return 0, 0, nil, err
	}
	for rest := n - recordHeader; rest > 0; {
		chunk, err := rd.buf.Peek(int(min(rest, readBufferSize)))
		if err != nil {
			return 0, 0, nil, err
		}
		sum = crc32.Update(sum, castagnoli, chunk)
		if _, err := rd.buf.Discard(len(chunk)); err != nil {
			return 0, 0, nil, err
		}
		rest -= int64(len(chunk))
	}
	if sum != want {
		return 0, 0, nil, errBadRecord
	}
	rd.rec = slices.Grow(rd.rec[:0], int(n-recordHeader))[:n-recordHeader]
	if _, err := f.ReadAt(rd.rec, off+recordHeader); err != nil {
		return 0, 0, nil, err
	}
	return n, typ, rd.rec, nil
}

func tornAt(f io.ReaderAt, size, off int64) (bool, error) {
	if size-off < recordHeader {
		return true, nil
	}
	var length [4]byte
	if _, err := f.ReadAt(length[:], off); err != nil {
		return false, err
	}
	n := int64(binary.LittleEndian.Uint32(length[:]))
	if n > maxRecord {
		return false, nil
	}
	end := off + recordHeader + n
	if end > size {
		return true, nil
	}
	sector := make([]byte, sectorSize)
	for from := off; from < end; {
		to := min(from-from%sectorSize+sectorSize, size)
		if _, err := f.ReadAt(sector[:to-from], from); err != nil {
			return false, err
		}
		if zeros(sector[:to-from]) {
			return true, nil
		}
		from = to
	}
	return false, nil
}

func zeros(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
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
	if err := l.Sync(); err != nil {
		return err
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	return l.create(l.current().seq + 1)
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
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.syncedSeq = l.markerSeq
	return nil
}

func (l *Log) Close() error {
	return l.f.Close()
}

func (l *Log) Segments() int {
	return len(l.segs)
}

func (l *Log) Compact(index uint64) (int, error) {
	removed := 0
	for len(l.segs) > 1 && l.segs[0].seq < l.syncedSeq && l.segs[0].maxIndex <= index {
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
