package backup

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

var (
	magic      = [8]byte{'F', 'F', 'B', 'K', 'U', 'P', '0', '1'}
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	ErrCorrupt = errors.New("backup: corrupt backup")
)

const headerSize = 8 + 8 + 8 + 8 + 4

type File struct {
	Rev  uint64
	Hash uint64
	Data []byte
}

type Summary struct {
	Rev     uint64
	Hash    uint64
	Keys    int
	Content uint64
	Bytes   int
}

func Encode(w io.Writer, f File) error {
	hdr := make([]byte, headerSize)
	copy(hdr, magic[:])
	binary.LittleEndian.PutUint64(hdr[8:], f.Rev)
	binary.LittleEndian.PutUint64(hdr[16:], f.Hash)
	binary.LittleEndian.PutUint64(hdr[24:], uint64(len(f.Data)))
	binary.LittleEndian.PutUint32(hdr[32:], crc32.Checksum(f.Data, castagnoli))
	if _, err := w.Write(hdr); err != nil {
		return err
	}
	_, err := w.Write(f.Data)
	return err
}

func Decode(r io.Reader) (File, Summary, error) {
	hdr := make([]byte, headerSize)
	if _, err := io.ReadFull(r, hdr); err != nil || [8]byte(hdr[:8]) != magic {
		return File{}, Summary{}, fmt.Errorf("%w: bad header", ErrCorrupt)
	}
	f := File{Rev: binary.LittleEndian.Uint64(hdr[8:]), Hash: binary.LittleEndian.Uint64(hdr[16:])}
	size := binary.LittleEndian.Uint64(hdr[24:])
	if size > 1<<30 {
		return File{}, Summary{}, fmt.Errorf("%w: %d bytes is not a backup this cluster could have written", ErrCorrupt, size)
	}
	f.Data = make([]byte, size)
	if _, err := io.ReadFull(r, f.Data); err != nil {
		return File{}, Summary{}, fmt.Errorf("%w: short: %v", ErrCorrupt, err)
	}
	if extra, _ := r.Read(make([]byte, 1)); extra != 0 {
		return File{}, Summary{}, fmt.Errorf("%w: trailing bytes", ErrCorrupt)
	}
	if binary.LittleEndian.Uint32(hdr[32:]) != crc32.Checksum(f.Data, castagnoli) {
		return File{}, Summary{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	s, err := Check(f)
	return f, s, err
}

func Check(f File) (Summary, error) {
	st, err := kv.Restore(f.Data, 0)
	if err != nil {
		return Summary{}, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if st.Rev() != f.Rev || st.Hash() != f.Hash {
		return Summary{}, fmt.Errorf("%w: the state is at revision %d with hash %x, the header says %d and %x", ErrCorrupt, st.Rev(), st.Hash(), f.Rev, f.Hash)
	}
	kvs := st.Range("", 0)
	return Summary{Rev: f.Rev, Hash: f.Hash, Keys: len(kvs), Content: kv.ContentHash(kvs), Bytes: len(f.Data)}, nil
}

func Save(path string, f File) error {
	var buf bytes.Buffer
	if err := Encode(&buf, f); err != nil {
		return err
	}
	return vfs.WriteAtomic(vfs.OS{Dir: filepath.Dir(path)}, filepath.Base(path), buf.Bytes())
}

func Load(path string) (File, Summary, error) {
	in, err := os.Open(path)
	if err != nil {
		return File{}, Summary{}, err
	}
	defer in.Close()
	return Decode(in)
}
