package snap

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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
	magic      = [8]byte{'F', 'F', 'S', 'N', 'A', 'P', 0, 1}
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
	hdr := make([]byte, headerSize)
	copy(hdr, magic[:])
	binary.LittleEndian.PutUint64(hdr[8:], s.Index)
	binary.LittleEndian.PutUint64(hdr[16:], s.Term)
	binary.LittleEndian.PutUint64(hdr[24:], uint64(len(s.Data)))
	binary.LittleEndian.PutUint32(hdr[32:], crc32.Checksum(s.Data, castagnoli))
	if _, err := f.Write(append(hdr, s.Data...)); err != nil {
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

func Load(fs vfs.FS, index, term uint64) ([]byte, error) {
	b, err := fs.ReadFile(Name(index, term))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if len(b) < headerSize || [8]byte(b[:8]) != magic {
		return nil, fmt.Errorf("%w: bad header", ErrCorrupt)
	}
	if binary.LittleEndian.Uint64(b[8:]) != index || binary.LittleEndian.Uint64(b[16:]) != term {
		return nil, fmt.Errorf("%w: the header names another snapshot", ErrCorrupt)
	}
	data := b[headerSize:]
	if binary.LittleEndian.Uint64(b[24:]) != uint64(len(data)) {
		return nil, fmt.Errorf("%w: %d bytes, the header says %d", ErrCorrupt, len(data), binary.LittleEndian.Uint64(b[24:]))
	}
	if binary.LittleEndian.Uint32(b[32:]) != crc32.Checksum(data, castagnoli) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	return data, nil
}

func Clean(fs vfs.FS, keep raft.Snapshot) error {
	names, err := fs.List()
	if err != nil {
		return err
	}
	removed := false
	for _, n := range names {
		if (strings.HasSuffix(n, suffix) && n != Name(keep.Index, keep.Term)) || strings.HasSuffix(n, suffix+tmpSuffix) {
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
