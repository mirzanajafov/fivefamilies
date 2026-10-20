package watch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
)

func put(key string, rev uint64) kv.Event {
	return kv.Event{Type: kv.EventPut, KV: kv.KeyValue{Key: key, ModRev: rev}}
}

type sink struct {
	got  []Batch
	full bool
}

func (s *sink) send(b Batch) bool {
	if s.full {
		return false
	}
	s.got = append(s.got, b)
	return true
}

func (s *sink) keys() []string {
	var out []string
	for _, b := range s.got {
		for _, ev := range b.Events {
			out = append(out, fmt.Sprintf("%s@%d", ev.KV.Key, ev.KV.ModRev))
		}
	}
	return out
}

func feed(h *Hub, from, to uint64) {
	for rev := from; rev <= to; rev++ {
		h.Publish(rev, []kv.Event{put(fmt.Sprintf("/a/%d", rev), rev), put(fmt.Sprintf("/b/%d", rev), rev)})
	}
}

func TestAWatchReplaysFromItsRevisionAndThenFollows(t *testing.T) {
	h := New(100, 0, 0)
	feed(h, 1, 5)
	s := &sink{}
	if _, err := h.Watch("/a/", true, 3, s.send); err != nil {
		t.Fatal(err)
	}
	feed(h, 6, 7)
	want := []string{"/a/3@3", "/a/4@4", "/a/5@5", "/a/6@6", "/a/7@7"}
	if got := s.keys(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if s.got[0].Rev != 5 {
		t.Fatalf("the replay says it covers up to %d, want 5", s.got[0].Rev)
	}
}

func TestAWatchFromNowStartsWithAnEmptyBatchAtTheCurrentRevision(t *testing.T) {
	h := New(100, 0, 0)
	feed(h, 1, 4)
	s := &sink{}
	watch(t, h, "/a/1", false, s.send)
	if len(s.got) != 1 || s.got[0].Rev != 4 || len(s.got[0].Events) != 0 {
		t.Fatalf("first batch %+v: a client must learn where it starts even if nothing happens", s.got)
	}
}

func TestAWatchFromBeforeTheRingIsCompacted(t *testing.T) {
	h := New(4, 0, 0)
	feed(h, 1, 5)
	if h.Start() != 4 {
		t.Fatalf("start %d: past twice its size the ring keeps four events, revisions 4 and 5, and nothing older", h.Start())
	}
	var ce *CompactedError
	if _, err := h.Watch("/a/", true, 3, (&sink{}).send); !errors.As(err, &ce) || ce.Start != 4 {
		t.Fatalf("watch from 3: %v", err)
	}
	s := &sink{}
	if _, err := h.Watch("/a/", true, 4, s.send); err != nil || fmt.Sprint(s.keys()) != "[/a/4@4 /a/5@5]" {
		t.Fatalf("watch from 4: %v %v", s.keys(), err)
	}
}

func TestARingNeverKeepsHalfARevision(t *testing.T) {
	h := New(3, 0, 0)
	feed(h, 1, 4)
	if h.Start() != 4 || len(h.ring) != 2 {
		t.Fatalf("start %d, %d events: cutting to three events splits revision 3, so all of it goes", h.Start(), len(h.ring))
	}
}

func TestEndingEveryWatchTellsEachWatcher(t *testing.T) {
	h := New(100, 0, 0)
	a := &sink{}
	watch(t, h, "/a/", true, a.send)
	h.EndAll()
	if last := a.got[len(a.got)-1]; !last.Ended || h.Len() != 0 {
		t.Fatalf("last batch %+v, %d watchers left", last, h.Len())
	}
}

func TestASlowWatcherIsDropped(t *testing.T) {
	h := New(100, 0, 0)
	s := &sink{}
	watch(t, h, "/a/", true, s.send)
	s.full = true
	feed(h, 1, 1)
	if h.Len() != 0 {
		t.Fatal("a watcher that could not take a batch is still registered and has a hole in its stream")
	}
}

func TestProgressGoesOnlyToWatchersThatHaveNotHeardTheLatestRevision(t *testing.T) {
	h := New(100, 0, 0)
	a, b := &sink{}, &sink{}
	watch(t, h, "/a/", true, a.send)
	watch(t, h, "/c/", true, b.send)
	feed(h, 1, 2)
	h.Progress()
	if n := len(a.got); n != 3 {
		t.Fatalf("watcher a got %d batches, want the start and two event batches and no progress", n)
	}
	if last := b.got[len(b.got)-1]; !last.Progress || last.Rev != 2 {
		t.Fatalf("watcher b's last batch %+v, want progress at 2", last)
	}
	h.Progress()
	if len(b.got) != 2 {
		t.Fatal("progress was sent twice for the same revision")
	}
}

func TestInstallingASnapshotEndsEveryWatch(t *testing.T) {
	h := New(100, 0, 0)
	feed(h, 1, 2)
	s := &sink{}
	watch(t, h, "/a/", true, s.send)
	h.Reset(50)
	if last := s.got[len(s.got)-1]; last.Compacted != 51 || h.Len() != 0 {
		t.Fatalf("last batch %+v, %d watchers left: the watcher missed 3 to 50 and must start over", last, h.Len())
	}
	if _, err := h.Watch("/a/", true, 10, (&sink{}).send); err == nil {
		t.Fatal("a watch from before the snapshot was accepted")
	}
}

func watch(t *testing.T, h *Hub, key string, prefix bool, send func(Batch) bool) {
	t.Helper()
	if _, err := h.Watch(key, prefix, 0, send); err != nil {
		t.Fatal(err)
	}
}
