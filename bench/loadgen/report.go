package loadgen

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Counters struct {
	Fsyncs    uint64 `json:"fsyncs"`
	Batches   uint64 `json:"wal_batches"`
	Entries   uint64 `json:"entries"`
	Sent      uint64 `json:"msgs_sent"`
	Writes    uint64 `json:"writes"`
	Reads     uint64 `json:"reads"`
	Elections uint64 `json:"elections"`
}

func (c Counters) Sub(before Counters) Counters {
	return Counters{
		Fsyncs: c.Fsyncs - before.Fsyncs, Batches: c.Batches - before.Batches, Entries: c.Entries - before.Entries,
		Sent: c.Sent - before.Sent, Writes: c.Writes - before.Writes, Reads: c.Reads - before.Reads, Elections: c.Elections - before.Elections,
	}
}

type Report struct {
	Summary         Summary    `json:"summary"`
	Nodes           int        `json:"nodes"`
	ReadMode        string     `json:"read_mode"`
	PerNode         []Counters `json:"per_node_delta,omitempty"`
	FsyncsPerOp     float64    `json:"fsyncs_per_op"`
	MsgsPerOp       float64    `json:"msgs_per_op"`
	EntriesPerFsync float64    `json:"entries_per_fsync"`
	Elections       uint64     `json:"elections"`
}

type FailoverSample struct {
	Kind      string `json:"kind,omitempty"`
	Leader    int    `json:"leader,omitempty"`
	Term      uint64 `json:"term,omitempty"`
	Scheduled int64  `json:"scheduled_ns"`
	Done      int64  `json:"done_ns"`
	Node      int    `json:"node"`
	Ok        bool   `json:"ok"`
	Err       string `json:"error,omitempty"`
}

type Probe func(ctx context.Context) ([]Counters, error)

func Measure(ctx context.Context, t Target, cfg Config, preload bool, probe Probe) (Report, error) {
	if err := cfg.normalize(); err != nil {
		return Report{}, err
	}
	if preload {
		if err := writeEveryKey(ctx, t, cfg); err != nil {
			return Report{}, fmt.Errorf("preload: %w", err)
		}
	}
	type sampled struct {
		c   []Counters
		err error
	}
	before := make(chan sampled, 1)
	go func() {
		time.Sleep(cfg.Warmup)
		c, err := probe(ctx)
		before <- sampled{c, err}
	}()
	s, err := Run(ctx, t, cfg)
	if err != nil {
		return Report{}, err
	}
	b := <-before
	after, err := probe(ctx)
	if err := errors.Join(b.err, err); err != nil {
		return Report{}, fmt.Errorf("counters: %w", err)
	}
	r := Report{Summary: s, Nodes: len(after)}
	var fsyncs, sent, entries uint64
	for i := range after {
		d := after[i].Sub(b.c[i])
		r.PerNode = append(r.PerNode, d)
		fsyncs, sent, entries = fsyncs+d.Fsyncs, sent+d.Sent, entries+d.Entries
		r.Elections = max(r.Elections, d.Elections)
	}
	if ops := float64(s.All.N); ops > 0 {
		r.FsyncsPerOp, r.MsgsPerOp = float64(fsyncs)/ops, float64(sent)/ops
	}
	if fsyncs > 0 {
		r.EntriesPerFsync = float64(entries) / float64(fsyncs)
	}
	return r, nil
}

func writeEveryKey(ctx context.Context, t Target, cfg Config) error {
	value := valueOf(cfg.ValueBytes)
	for i := range cfg.Keys {
		op := Op{Key: key(i), Value: value}
		for try := 0; ; try++ {
			octx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			err := t.Do(octx, op)
			cancel()
			if err == nil {
				break
			}
			if try == 50 {
				return fmt.Errorf("writing %s: %w", op.Key, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	return nil
}
