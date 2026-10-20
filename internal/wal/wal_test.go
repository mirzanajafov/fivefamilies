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

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func appendTo(t *testing.T, fs *vfs.Mem, name string, data []byte) {
	t.Helper()
	f, err := fs.OpenAppend(name)
	must(t, err)
	_, err = f.Write(data)
	must(t, err)
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
	must(t, l.Close())
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
	must(t, l.Close())
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
	must(t, l.Close())
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
		tail := slices.Clone(full[len(synced):])
		tail[pos-len(synced)] ^= byte(1 + rng.IntN(255))
		must(t, fs.Truncate(names[0], int64(len(synced))))
		appendTo(t, fs, names[0], tail)
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
	must(t, l.Close())
	names, _ := fs.List()
	first, _ := fs.ReadFile(names[0])
	first[len(first)-3] ^= 0xff
	must(t, fs.Truncate(names[0], 0))
	appendTo(t, fs, names[0], first)
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
	must(t, l.Close())
	names, _ := fs.List()
	lastName := names[len(names)-1]
	must(t, fs.Truncate(lastName, 10))
	l, st, rec := open(t, fs, 64)
	if !rec.DroppedEmpty || !sameEntries(st.Entries, entries(1, 1, 2)) {
		t.Fatalf("recovery %+v restored %d entries", rec, len(st.Entries))
	}
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 3, 4), true)
	must(t, l.Close())
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
	must(t, l.Close())
	if _, _, _, err := Open(fs, Options{Salt: saltSource(2)}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err %v", err)
	}
}

func TestCommitIsClampedToTheRecoveredLog(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1, Commit: 9}, entries(1, 1, 3), true)
	must(t, l.Close())
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
		must(t, l.Write(raft.HardState{Term: 1, Commit: 2}, entries(1, 1, 3)))
		must(t, l.Sync())
		must(t, l.Write(raft.HardState{Term: 2}, entries(2, 3, 4)))
		names, _ := fs.List()
		data, _ := fs.ReadFile(names[0])
		keep := len(data) - int(cut)%len(data)
		must(t, fs.Truncate(names[0], int64(keep)))
		appendTo(t, fs, names[0], tail)
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

func snapAt(index, term uint64) *raft.Snapshot {
	return &raft.Snapshot{Index: index, Term: term}
}

func TestACompactionMarkerKeepsWhatFollows(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 10), true)
	if err := l.WriteBatch(Batch{HardState: raft.HardState{Term: 1, Commit: 6}, Snapshot: snapAt(6, 1)}); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, l, raft.HardState{}, entries(1, 11, 12), true)
	must(t, l.Close())
	_, st, _ := open(t, fs, 0)
	if st.Snapshot == nil || st.Snapshot.Index != 6 || !sameEntries(st.Entries, entries(1, 7, 12)) {
		t.Fatalf("snapshot %+v entries %d", st.Snapshot, len(st.Entries))
	}
}

func TestAnInstalledSnapshotDropsTheOldLog(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 10), true)
	if err := l.WriteBatch(Batch{HardState: raft.HardState{Term: 2, Commit: 8}, Snapshot: snapAt(8, 2), Reset: true, Entries: entries(2, 9, 9)}); err != nil {
		t.Fatal(err)
	}
	must(t, l.Sync())
	must(t, l.Close())
	_, st, _ := open(t, fs, 0)
	if st.Snapshot == nil || st.Snapshot.Index != 8 || st.Snapshot.Term != 2 || !sameEntries(st.Entries, entries(2, 9, 9)) {
		t.Fatalf("snapshot %+v entries %v", st.Snapshot, st.Entries)
	}
	if st.HardState.Commit != 8 {
		t.Fatalf("commit %d", st.HardState.Commit)
	}
}

func TestCompactRemovesOldSegmentsButKeepsTheTerm(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 200)
	mustWrite(t, l, raft.HardState{Term: 5, Vote: 2}, nil, true)
	for i := uint64(1); i <= 50; i++ {
		mustWrite(t, l, raft.HardState{}, entries(5, i, i), true)
	}
	before := l.Segments()
	if err := l.WriteBatch(Batch{HardState: raft.HardState{Term: 5, Vote: 2, Commit: 40}, Snapshot: snapAt(40, 5)}); err != nil {
		t.Fatal(err)
	}
	must(t, l.Sync())
	removed, err := l.Compact(40)
	if err != nil || removed == 0 || l.Segments() != before-removed {
		t.Fatalf("removed %d of %d segments: %v", removed, before, err)
	}
	must(t, l.Close())
	_, st, _ := open(t, fs, 200)
	if st.HardState.Term != 5 || st.HardState.Vote != 2 {
		t.Fatalf("hard state after compaction %+v", st.HardState)
	}
	if st.Snapshot == nil || st.Snapshot.Index != 40 || !sameEntries(st.Entries, entries(5, 41, 50)) {
		t.Fatalf("snapshot %+v, %d entries", st.Snapshot, len(st.Entries))
	}
}

func TestASnapshotRecordWithoutTheHardStateIsRefused(t *testing.T) {
	l, _, _ := open(t, vfs.NewMem(), 0)
	if err := l.WriteBatch(Batch{Snapshot: snapAt(1, 1)}); err == nil {
		t.Fatal("accepted a snapshot record without the hard state")
	}
}

func TestACompactionOutsideTheLogIsCorruption(t *testing.T) {
	fs := vfs.NewMem()
	l, _, _ := open(t, fs, 0)
	mustWrite(t, l, raft.HardState{Term: 1}, entries(1, 1, 3), true)
	must(t, l.WriteBatch(Batch{HardState: raft.HardState{Term: 1}, Snapshot: snapAt(9, 1)}))
	must(t, l.Sync())
	must(t, l.Close())
	if _, _, _, err := Open(fs, Options{Salt: saltSource(2)}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err %v", err)
	}
}

type model struct {
	hs   raft.HardState
	snap *raft.Snapshot
	ents []raft.Entry
}

func (m model) clone() model {
	return model{hs: m.hs, snap: m.snap, ents: slices.Clone(m.ents)}
}

func (m model) offset() uint64 {
	if m.snap == nil {
		return 0
	}
	return m.snap.Index
}

func (m *model) apply(b Batch, each func(model)) {
	if !b.HardState.IsEmpty() {
		m.hs = b.HardState
		each(*m)
	}
	if b.Snapshot != nil {
		if b.Reset {
			m.ents = nil
		} else {
			m.ents = slices.Clone(m.ents[b.Snapshot.Index-m.offset():])
		}
		m.snap = b.Snapshot
		each(*m)
	}
	for _, e := range b.Entries {
		m.ents = append(m.ents[:e.Index-m.offset()-1], e)
		each(*m)
	}
}

func (m model) matches(st raft.Restore) bool {
	hs := m.hs
	hs.Commit = max(min(hs.Commit, m.offset()+uint64(len(m.ents))), m.offset())
	if st.HardState != hs || !sameEntries(st.Entries, m.ents) {
		return false
	}
	if (st.Snapshot == nil) != (m.snap == nil) {
		return false
	}
	return st.Snapshot == nil || (st.Snapshot.Index == m.snap.Index && st.Snapshot.Term == m.snap.Term)
}

func TestCrashesWithSnapshotsAndCompaction(t *testing.T) {
	for seed := range uint64(1000) {
		rng := rand.New(rand.NewPCG(seed, 11))
		fs := vfs.NewMem()
		segBytes := int64(64 + rng.IntN(400))
		l, _, _ := open(t, fs, segBytes)
		var synced model
		var pending []Batch
		term, next := uint64(1), uint64(1)
		for range 1 + rng.IntN(40) {
			cur := synced.clone()
			for _, b := range pending {
				cur.apply(b, func(model) {})
			}
			var b Batch
			switch r := rng.IntN(10); {
			case r == 0:
				term++
				idx := cur.offset() + uint64(len(cur.ents)) + uint64(rng.IntN(5))
				b = Batch{HardState: raft.HardState{Term: term}, Snapshot: snapAt(idx, term), Reset: true}
				next = idx + 1
			case r == 1 && len(cur.ents) > 0:
				e := cur.ents[rng.IntN(len(cur.ents))]
				b = Batch{HardState: raft.HardState{Term: term}, Snapshot: snapAt(e.Index, e.Term)}
			default:
				if rng.IntN(5) == 0 {
					term++
				}
				from := next - min(uint64(rng.IntN(3)), next-1-cur.offset())
				b = Batch{HardState: raft.HardState{Term: term}, Entries: entries(term, from, from+uint64(rng.IntN(4)))}
				next = b.Entries[len(b.Entries)-1].Index + 1
			}
			if err := l.WriteBatch(b); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
			pending = append(pending, b)
			if rng.IntN(3) > 0 {
				must(t, l.Sync())
				for _, p := range pending {
					synced.apply(p, func(model) {})
				}
				pending = nil
				if synced.snap != nil && rng.IntN(2) == 0 {
					if _, err := l.Compact(synced.snap.Index); err != nil {
						t.Fatal(err)
					}
				}
			}
		}
		fs.Crash(rng)
		_, st, _, err := Open(fs, Options{SegmentBytes: segBytes, Salt: saltSource(seed)})
		if err != nil {
			t.Fatalf("seed %d: %v", seed, err)
		}
		ok := synced.matches(st)
		m := synced.clone()
		for _, b := range pending {
			m.apply(b, func(c model) { ok = ok || c.matches(st) })
		}
		if !ok {
			t.Fatalf("seed %d: restored snapshot %+v, %d entries, hs %+v: not the synced state plus a prefix of what was in flight", seed, st.Snapshot, len(st.Entries), st.HardState)
		}
	}
}
