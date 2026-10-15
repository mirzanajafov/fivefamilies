package wal

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/vfs"
)

func saltSource(seed uint64) func() uint64 {
	rng := rand.New(rand.NewPCG(seed, 1))
	return rng.Uint64
}

func open(t *testing.T, fs vfs.FS, segBytes int64) (*Log, raft.Restore, Recovery) {
	t.Helper()
	l, st, rec, err := Open(fs, Options{SegmentBytes: segBytes, Salt: saltSource(1)})
	if err != nil {
		t.Fatal(err)
	}
	return l, st, rec
}

func entries(term uint64, from, to uint64) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Term: term, Index: i, Data: fmt.Appendf(nil, "t%d-%d", term, i)})
	}
	return out
}

func sameEntries(a, b []raft.Entry) bool {
	return slices.EqualFunc(a, b, func(x, y raft.Entry) bool {
		return x.Term == y.Term && x.Index == y.Index && x.Type == y.Type && string(x.Data) == string(y.Data)
	})
}

func mustWrite(t *testing.T, l *Log, hs raft.HardState, ents []raft.Entry, sync bool) {
	t.Helper()
	if err := l.Write(hs, ents); err != nil {
		t.Fatal(err)
	}
	if sync {
		if err := l.Sync(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	fs := vfs.NewMem()
	l, st, _ := open(t, fs, 0)
	if len(st.Entries) != 0 || !st.HardState.IsEmpty() {
		t.Fatalf("fresh log restored %+v", st)
	}
	mustWrite(t, l, raft.HardState{Term: 1, Vote: 2}, entries(1, 1, 3), true)
	mustWrite(t, l, raft.HardState{Term: 1, Vote: 2, Commit: 3}, nil, true)
	l.Close()
	_, st, rec := open(t, fs, 0)
	if st.HardState != (raft.HardState{Term: 1, Vote: 2, Commit: 3}) || !sameEntries(st.Entries, entries(1, 1, 3)) {
		t.Fatalf("restored %+v", st)
	}
	if rec.Records != 5 || rec.TornBytes != 0 {
		t.Fatalf("recovery %+v", rec)
	}
}

func TestALaterEntryReplacesTheSuffix(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 5), true)
	mustWrite(t, l, raft.HardState{Term: 2}, entries(2, 3, 4), true)
	l.Close()
	_, st, _ := open(t, fs, 0)
	want := append(entries(1, 1, 2), entries(2, 3, 4)...)
	if !sameEntries(st.Entries, want) {
		t.Fatalf("restored %v", st.Entries)
	}
}

func TestRotationAcrossSegments(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 256)
	for i := uint64(1); i <= 60; i++ {
		mustWrite(t, l, raft.HardState{Term: 1, Commit: i - 1}, entries(1, i, i), true)
	}
	mustWrite(t, l, raft.HardState{Term: 2}, entries(2, 40, 41), true)
	l.Close()
	names, _ := fs.List()
	if len(names) < 5 {
		t.Fatalf("only %d segments", len(names))
	}
	_, st, rec := open(t, fs, 256)
	want := append(entries(1, 1, 39), entries(2, 40, 41)...)
	if !sameEntries(st.Entries, want) || rec.Segments != len(names) {
		t.Fatalf("restored %d entries from %d segments", len(st.Entries), rec.Segments)
	}
	if st.HardState.Commit != 0 || st.HardState.Term != 2 {
		t.Fatalf("hard state %+v", st.HardState)
	}
}

type state struct {
	hs   raft.HardState
	ents []raft.Entry
}

func prefixes(base state, batch []raft.Entry, hs raft.HardState) []state {
	out := []state{base}
	cur := state{hs: base.hs, ents: slices.Clone(base.ents)}
	if !hs.IsEmpty() {
		cur.hs = hs
		out = append(out, state{hs: cur.hs, ents: slices.Clone(cur.ents)})
	}
	for _, e := range batch {
		cur.ents = append(cur.ents[:e.Index-1], e)
		out = append(out, state{hs: cur.hs, ents: slices.Clone(cur.ents)})
	}
	return out
}

func matchesOne(st raft.Restore, candidates []state) bool {
	for _, c := range candidates {
		hs := c.hs
		hs.Commit = min(hs.Commit, uint64(len(c.ents)))
		if st.HardState == hs && sameEntries(st.Entries, c.ents) {
			return true
		}
	}
	return false
}

func TestTornTailAtEveryByte(t *testing.T) {
	build := func() (*vfs.Mem, string, int) {
		fs := vfs.NewMem()
		l, _, _ := open(t, fs, 0)
		mustWrite(t, l, raft.HardState{Term: 1, Vote: 1, Commit: 2}, entries(1, 1, 4), true)
		names, _ := fs.List()
		synced, _ := fs.ReadFile(names[0])
		mustWrite(t, l, raft.HardState{Term: 2, Vote: 3, Commit: 4}, entries(2, 3, 6), false)
		return fs, names[0], len(synced)
	}
	fs, name, synced := build()
	full, _ := fs.ReadFile(name)
	base := state{hs: raft.HardState{Term: 1, Vote: 1, Commit: 2}, ents: entries(1, 1, 4)}
	candidates := prefixes(base, entries(2, 3, 6), raft.HardState{Term: 2, Vote: 3, Commit: 4})
	tornSeen := 0
	for cut := synced; cut <= len(full); cut++ {
		fs, name, _ := build()
		if err := fs.Truncate(name, int64(cut)); err != nil {
			t.Fatal(err)
		}
		_, st, rec, err := Open(fs, Options{Salt: saltSource(2)})
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		if !matchesOne(st, candidates) {
			t.Fatalf("cut at %d: restored %+v, which is not the synced state plus a prefix of the torn write", cut, st)
		}
		if rec.TornBytes > 0 {
			tornSeen++
		}
	}
	if tornSeen < (len(full)-synced)/2 {
		t.Fatalf("only %d of %d cuts reported a torn tail", tornSeen, len(full)-synced)
	}
}

func TestGarbageInTheTailIsCut(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 300 {
		fs := vfs.NewMem()
		l, _, _ := open(t, fs, 0)
		mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 3), true)
		names, _ := fs.List()
		synced, _ := fs.ReadFile(names[0])
		mustWrite(t, l, raft.HardState{Term: 2}, entries(2, 2, 5), false)
		full, _ := fs.ReadFile(names[0])
		pos := len(synced) + rng.IntN(len(full)-len(synced))
		fs.Truncate(names[0], int64(len(synced)))
		g, _ := fs.OpenAppend(names[0])
		tail := slices.Clone(full[len(synced):])
		tail[pos-len(synced)] ^= byte(1 + rng.IntN(255))
		g.Write(tail)
		_, st, _, err := Open(fs, Options{Salt: saltSource(2)})
		if err != nil {
			t.Fatalf("flipped byte %d: %v", pos, err)
		}
		candidates := prefixes(state{hs: raft.HardState{Term: 1}, ents: entries(1, 1, 3)}, entries(2, 2, 5), raft.HardState{Term: 2})
		if !matchesOne(st, candidates) {
			t.Fatalf("flipped byte %d: restored %+v", pos, st)
		}
	}
}

func TestCorruptionBeforeTheLastSegmentRefusesToOpen(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 128)
	for i := uint64(1); i <= 20; i++ {
		mustWrite(t, l, raft.HardState{Term: 1}, entries(1, i, i), true)
	}
	l.Close()
	names, _ := fs.List()
	first, _ := fs.ReadFile(names[0])
	first[len(first)-3] ^= 0xff
	fs.Truncate(names[0], 0)
	g, _ := fs.OpenAppend(names[0])
	g.Write(first)
	_, _, _, err := Open(fs, Options{Salt: saltSource(2)})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err %v, want ErrCorrupt", err)
	}
}

func TestALastSegmentWithABadHeaderIsDropped(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 64)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 2), true)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 3, 3), true)
	l.Close()
	names, _ := fs.List()
	lastName := names[len(names)-1]
	fs.Truncate(lastName, 10)
	l, st, rec := open(t, fs, 64)
	if !rec.DroppedEmpty || !sameEntries(st.Entries, entries(1, 1, 2)) {
		t.Fatalf("recovery %+v restored %d entries", rec, len(st.Entries))
	}
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 3, 4), true)
	l.Close()
	_, st, _ = open(t, fs, 64)
	if !sameEntries(st.Entries, entries(1, 1, 4)) {
		t.Fatalf("after writing again: %v", st.Entries)
	}
}

func TestAGapInTheEntriesIsCorruption(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 2), true)
	mustWrite(t, l, raft.HardState{}, entries(1, 5, 5), true)
	l.Close()
	if _, _, _, err := Open(fs, Options{Salt: saltSource(2)}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err %v", err)
	}
}

func TestCommitIsClampedToTheRecoveredLog(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1, Commit: 9}, entries(1, 1, 3), true)
	l.Close()
	_, st, _ := open(t, fs, 0)
	if st.HardState.Commit != 3 {
		t.Fatalf("commit %d", st.HardState.Commit)
	}
	if _, err := raft.New(raft.Config{ID: 1, Voters: []raft.NodeID{1}, ElectionTick: 10, HeartbeatTick: 1, Rand: rand.NewPCG(1, 1)}, st); err != nil {
		t.Fatalf("the core rejects what the log recovered: %v", err)
	}
}

type write struct {
	hs   raft.HardState
	ents []raft.Entry
}

func candidatesAfter(base state, writes []write) []state {
	out := []state{base}
	cur := state{hs: base.hs, ents: slices.Clone(base.ents)}
	for _, w := range writes {
		if !w.hs.IsEmpty() {
			cur.hs = w.hs
			out = append(out, state{hs: cur.hs, ents: slices.Clone(cur.ents)})
		}
		for _, e := range w.ents {
			cur.ents = append(cur.ents[:e.Index-1], e)
			out = append(out, state{hs: cur.hs, ents: slices.Clone(cur.ents)})
		}
	}
	return out
}

func TestCrashesKeepEverythingSynced(t *testing.T) {
	torn := 0
	for seed := range uint64(500) {
		rng := rand.New(rand.NewPCG(seed, 9))
		fs := vfs.NewMem()
		segBytes := int64(64 + rng.IntN(512))
		l, _, _ := open(t, fs, segBytes)
		var base state
		var pending []write
		term, next := uint64(1), uint64(1)
		for range 1 + rng.IntN(30) {
			if rng.IntN(5) == 0 {
				term++
			}
			from := next - min(uint64(rng.IntN(3)), next-1)
			w := write{hs: raft.HardState{Term: term, Commit: from - 1}, ents: entries(term, from, from+uint64(rng.IntN(4)))}
			if err := l.Write(w.hs, w.ents); err != nil {
				t.Fatal(err)
			}
			pending = append(pending, w)
			next = w.ents[len(w.ents)-1].Index + 1
			if rng.IntN(3) > 0 {
				if err := l.Sync(); err != nil {
					t.Fatal(err)
				}
				all := candidatesAfter(base, pending)
				base = all[len(all)-1]
				pending = nil
			}
		}
		if fs.Unsynced() > 0 {
			torn++
		}
		fs.Crash(rng)
		_, st, _, err := Open(fs, Options{SegmentBytes: segBytes, Salt: saltSource(seed)})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		if !matchesOne(st, candidatesAfter(base, pending)) {
			t.Fatalf("seed %d: restored %d entries (hs %+v), not the synced state plus a prefix of what was in flight", seed, len(st.Entries), st.HardState)
		}
	}
	if torn < 100 {
		t.Fatalf("only %d of 500 crashes had unsynced data", torn)
	}
}

func FuzzOpen(f *testing.F) {
	f.Add([]byte{}, uint8(0))
	f.Add([]byte{1, 2, 3}, uint8(5))
	f.Fuzz(func(t *testing.T, tail []byte, cut uint8) {
		fs := vfs.NewMem()
		l, _, _, err := Open(fs, Options{Salt: saltSource(1)})
		if err != nil {
			t.Fatal(err)
		}
		l.Write(raft.HardState{Term: 1, Commit: 2}, entries(1, 1, 3))
		l.Sync()
		l.Write(raft.HardState{Term: 2}, entries(2, 3, 4))
		names, _ := fs.List()
		data, _ := fs.ReadFile(names[0])
		keep := len(data) - int(cut)%len(data)
		fs.Truncate(names[0], int64(keep))
		g, _ := fs.OpenAppend(names[0])
		g.Write(tail)
		_, st, _, err := Open(fs, Options{Salt: saltSource(2)})
		if err != nil {
			t.Fatalf("garbage at the end of the only segment: %v", err)
		}
		if len(st.Entries) > 0 && st.Entries[0].Index != 1 {
			t.Fatalf("restored entries start at %d", st.Entries[0].Index)
		}
		for i, e := range st.Entries {
			if e.Index != uint64(i+1) {
				t.Fatalf("restored entry %d at position %d", e.Index, i)
			}
		}
	})
}
