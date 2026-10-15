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
	recEntry byte = 1
	recState byte = 2
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

type Log struct {
	fs   vfs.FS
	opts Options
	seq  uint64
	f    vfs.File
	size int64
	salt uint64
	buf  []byte
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

func corrupt(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, args...))
}

type replay struct {
	hs   raft.HardState
	ents []raft.Entry
}

func (r *replay) apply(typ byte, payload []byte) error {
	switch typ {
	case recState:
		hs, err := decodeState(payload)
		if err != nil {
			return err
		}
		r.hs = hs
	case recEntry:
		e, err := decodeEntry(payload)
		if err != nil {
			return err
		}
		last := uint64(len(r.ents))
		switch {
		case e.Index == 0 || e.Index > last+1:
			return corrupt("entry %d follows entry %d", e.Index, last)
		case e.Index <= last:
			r.ents = r.ents[:e.Index-1]
		}
		r.ents = append(r.ents, e)
	default:
		return corrupt("unknown record type %d", typ)
	}
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
	var r replay
	next := uint64(1)
	if len(seqs) > 0 {
		next = seqs[0]
	}
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
			seqs = seqs[:i]
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
				if err := l.truncateTail(name, int64(off)); err != nil {
					return nil, raft.Restore{}, rec, err
				}
				data = data[:off]
				break
			}
			if err := r.apply(typ, payload); err != nil {
				return nil, raft.Restore{}, rec, fmt.Errorf("segment %s offset %d: %w", name, off, err)
			}
			rec.Records++
			off += n
		}
		l.seq, l.size, l.salt = seq, int64(len(data)), salt
	}
	if len(seqs) == 0 {
		if err := l.create(next); err != nil {
			return nil, raft.Restore{}, rec, err
		}
	} else {
		f, err := fs.OpenAppend(segmentName(l.seq))
		if err != nil {
			return nil, raft.Restore{}, rec, err
		}
		l.f = f
	}
	hs := r.hs
	hs.Commit = min(hs.Commit, uint64(len(r.ents)))
	return l, raft.Restore{HardState: hs, Entries: r.ents}, rec, nil
}

func (l *Log) truncateTail(name string, size int64) error {
	if err := l.fs.Truncate(name, size); err != nil {
		return err
	}
	f, err := l.fs.OpenAppend(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
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
		f.Close()
		return err
	}
	if err := l.fs.SyncDir(); err != nil {
		f.Close()
		return err
	}
	l.f, l.seq, l.size, l.salt = f, seq, headerSize, salt
	return nil
}

func (l *Log) rotate() error {
	if err := l.f.Sync(); err != nil {
		return err
	}
	if err := l.f.Close(); err != nil {
		return err
	}
	return l.create(l.seq + 1)
}

func (l *Log) Write(hs raft.HardState, ents []raft.Entry) error {
	if hs.IsEmpty() && len(ents) == 0 {
		return nil
	}
	if l.size >= l.opts.SegmentBytes {
		if err := l.rotate(); err != nil {
			return err
		}
	}
	buf := l.buf[:0]
	if !hs.IsEmpty() {
		buf = appendRecord(buf, l.salt, recState, func(b []byte) []byte { return encodeState(b, hs) })
	}
	for _, e := range ents {
		buf = appendRecord(buf, l.salt, recEntry, func(b []byte) []byte { return encodeEntry(b, e) })
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

func (l *Log) Segment() uint64 {
	return l.seq
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
	if e.Type != raft.EntryNormal && e.Type != raft.EntryConfig {
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
