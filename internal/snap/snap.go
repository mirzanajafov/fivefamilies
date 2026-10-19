package snap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

const (
	suffix     = ".snap"
	tmpSuffix  = ".tmp"
	headerSize = 36
)

var (
	magic      = [8]byte{'F', 'F', 'S', 'N', 'A', 'P', 0, 2}
	magicV1    = [8]byte{'F', 'F', 'S', 'N', 'A', 'P', 0, 1}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt = errors.New("snap: corrupt snapshot")
)

func Name(index, term uint64) string {
	return fmt.Sprintf("%016x-%016x%s", term, index, suffix)
}

func Save(fs vfs.FS, s raft.Snapshot) error {
	name := Name(s.Index, s.Term)
	tmp := name + tmpSuffix
	if err := fs.Remove(tmp); err != nil {
		return err
	}
	f, err := fs.Create(tmp)
	if err != nil {
		return err
	}
	body := append(appendConf(nil, s.Conf), s.Data...)
	hdr := make([]byte, headerSize)
	copy(hdr, magic[:])
	binary.LittleEndian.PutUint64(hdr[8:], s.Index)
	binary.LittleEndian.PutUint64(hdr[16:], s.Term)
	binary.LittleEndian.PutUint64(hdr[24:], uint64(len(body)))
	binary.LittleEndian.PutUint32(hdr[32:], crc32.Checksum(body, castagnoli))
	if _, err := f.Write(append(hdr, body...)); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := fs.Rename(tmp, name); err != nil {
		return err
	}
	return fs.SyncDir()
}

func Load(fs vfs.FS, index, term uint64) (raft.Snapshot, error) {
	b, err := fs.ReadFile(Name(index, term))
	if err != nil {
		return raft.Snapshot{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if len(b) < headerSize || ([8]byte(b[:8]) != magic && [8]byte(b[:8]) != magicV1) {
		return raft.Snapshot{}, fmt.Errorf("%w: bad header", ErrCorrupt)
	}
	if binary.LittleEndian.Uint64(b[8:]) != index || binary.LittleEndian.Uint64(b[16:]) != term {
		return raft.Snapshot{}, fmt.Errorf("%w: the header names another snapshot", ErrCorrupt)
	}
	body := b[headerSize:]
	if binary.LittleEndian.Uint64(b[24:]) != uint64(len(body)) {
		return raft.Snapshot{}, fmt.Errorf("%w: %d bytes, the header says %d", ErrCorrupt, len(body), binary.LittleEndian.Uint64(b[24:]))
	}
	if binary.LittleEndian.Uint32(b[32:]) != crc32.Checksum(body, castagnoli) {
		return raft.Snapshot{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	if [8]byte(b[:8]) == magicV1 {
		return raft.Snapshot{Index: index, Term: term, Data: body}, nil
	}
	conf, data, ok := readConf(body)
	if !ok {
		return raft.Snapshot{}, fmt.Errorf("%w: bad configuration", ErrCorrupt)
	}
	return raft.Snapshot{Index: index, Term: term, Data: data, Conf: conf}, nil
}

func appendConf(b []byte, c raft.ConfState) []byte {
	for _, ids := range [][]raft.NodeID{c.Voters, c.Learners} {
		b = binary.AppendUvarint(b, uint64(len(ids)))
		for _, id := range ids {
			b = binary.AppendUvarint(b, uint64(id))
		}
	}
	return b
}

func readConf(b []byte) (raft.ConfState, []byte, bool) {
	var lists [2][]raft.NodeID
	for i := range lists {
		n, k := binary.Uvarint(b)
		if k <= 0 || n > uint64(len(b)) {
			return raft.ConfState{}, nil, false
		}
		b = b[k:]
		for range n {
			id, k := binary.Uvarint(b)
			if k <= 0 || id == 0 {
				return raft.ConfState{}, nil, false
			}
			lists[i] = append(lists[i], raft.NodeID(id))
			b = b[k:]
		}
	}
	return raft.ConfState{Voters: lists[0], Learners: lists[1]}, b, true
}

func RemoveBelow(fs vfs.FS, index uint64) error {
	return removeIf(fs, func(n string) bool {
		i, ok := parseIndex(n)
		return ok && i < index
	})
}

func RemoveUnfinished(fs vfs.FS) error {
	return removeIf(fs, func(n string) bool { return strings.HasSuffix(n, suffix+tmpSuffix) })
}

func parseIndex(name string) (uint64, bool) {
	base, ok := strings.CutSuffix(name, suffix)
	if !ok {
		return 0, false
	}
	term, index, ok := strings.Cut(base, "-")
	if !ok || len(term) != 16 || len(index) != 16 {
		return 0, false
	}
	_, termErr := strconv.ParseUint(term, 16, 64)
	i, indexErr := strconv.ParseUint(index, 16, 64)
	return i, termErr == nil && indexErr == nil
}

func removeIf(fs vfs.FS, match func(string) bool) error {
	names, err := fs.List()
	if err != nil {
		return err
	}
	removed := false
	for _, n := range names {
		if match(n) {
			if err := fs.Remove(n); err != nil {
				return err
			}
			removed = true
		}
	}
	if removed {
		return fs.SyncDir()
	}
	return nil
}
