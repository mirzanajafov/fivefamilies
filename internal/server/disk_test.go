package server

import (
	"testing"
	"time"
)

func TestFsyncModesDoWhatTheySay(t *testing.T) {
	for _, mode := range []string{FsyncGroup, FsyncReady, FsyncEntry, FsyncOff} {
		t.Run(mode, func(t *testing.T) {
			c := newTestCluster(t, 1, func(cfg *Config) { cfg.FsyncMode = mode })
			c.leader(5 * time.Second)
			putMany(t, c, 10, 5)
			st := c.status(1)
			t.Logf("%s: %d entries in %d batches with %d fsyncs", mode, st.GetEntriesWritten(), st.GetWalBatches(), st.GetFsyncs())
			switch mode {
			case FsyncOff:
				if st.GetFsyncs() != 0 {
					t.Fatalf("%d fsyncs with fsync off", st.GetFsyncs())
				}
			case FsyncEntry:
				if st.GetFsyncs() < st.GetEntriesWritten() {
					t.Fatalf("%d fsyncs for %d entries", st.GetFsyncs(), st.GetEntriesWritten())
				}
			case FsyncReady:
				if st.GetFsyncs() > st.GetWalBatches() || st.GetFsyncs() < st.GetEntriesWritten()/2 {
					t.Fatalf("%d fsyncs, %d batches, %d entries", st.GetFsyncs(), st.GetWalBatches(), st.GetEntriesWritten())
				}
			case FsyncGroup:
				if st.GetFsyncs() == 0 || st.GetFsyncs() >= st.GetEntriesWritten() {
					t.Fatalf("%d fsyncs for %d entries from ten writers at once: nothing was grouped", st.GetFsyncs(), st.GetEntriesWritten())
				}
			}
		})
	}
}
