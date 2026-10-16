package backup

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func sample(t *testing.T) File {
	t.Helper()
	s := kv.NewStore(0)
	for i, cmd := range []kv.Command{
		{Op: kv.OpPut, Key: "/config/api/limit", Value: []byte("10")},
		{Op: kv.OpPut, Key: "a", Value: []byte("1")},
		{Op: kv.OpLeaseGrant, TTL: 5000},
	} {
		s.Apply(raft.Entry{Term: 1, Index: uint64(i + 1), Data: kv.Encode(cmd)})
	}
	return File{Rev: s.Rev(), Hash: s.Hash(), Data: s.Snapshot()}
}

func TestABackupRoundTrips(t *testing.T) {
	f := sample(t)
	path := filepath.Join(t.TempDir(), "x.backup")
	if err := Save(path, f); err != nil {
		t.Fatal(err)
	}
	got, s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Rev != 3 || s.Keys != 2 || s.Content == 0 || !bytes.Equal(got.Data, f.Data) {
		t.Fatalf("%+v %+v", got, s)
	}
}

func TestEveryDamagedByteIsCaught(t *testing.T) {
	var buf bytes.Buffer
	Encode(&buf, sample(t))
	good := buf.Bytes()
	for i := range good {
		bad := append([]byte(nil), good...)
		bad[i] ^= 0x20
		if _, _, err := Decode(bytes.NewReader(bad)); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("flipping byte %d went unnoticed: %v", i, err)
		}
	}
	for _, cut := range []int{0, 10, headerSize, len(good) - 1} {
		if _, _, err := Decode(bytes.NewReader(good[:cut])); err == nil {
			t.Fatalf("a backup cut at %d bytes loaded", cut)
		}
	}
	if _, _, err := Decode(bytes.NewReader(append(append([]byte(nil), good...), 0))); err == nil {
		t.Fatal("a trailing byte went unnoticed")
	}
}

func TestAHeaderThatLiesAboutTheStateIsCaught(t *testing.T) {
	f := sample(t)
	f.Hash++
	var buf bytes.Buffer
	Encode(&buf, f)
	if _, _, err := Decode(&buf); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a wrong hash in the header: %v", err)
	}
}
