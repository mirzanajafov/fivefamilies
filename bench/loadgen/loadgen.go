package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	Read  bool
	Key   string
	Value []byte
}

type Target interface {
	Do(ctx context.Context, op Op) error
}

type Config struct {
	Rate         float64
	Duration     time.Duration
	Warmup       time.Duration
	ReadFraction float64
	Keys         int
	ValueBytes   int
	MaxInflight  int
	Timeout      time.Duration
	Poisson      bool
	Seed         uint64
}

func (c *Config) normalize() error {
	switch {
	case !(c.Rate > 0):
		return errors.New("loadgen: rate must be positive")
	case c.Duration <= 0:
		return errors.New("loadgen: duration must be positive")
	case c.ReadFraction < 0 || c.ReadFraction > 1:
		return errors.New("loadgen: the read fraction is between 0 and 1")
	}
	if c.Keys <= 0 {
		c.Keys = 1000
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = 10000
	}
	if c.Timeout <= 0 {
		c.Timeout = 2 * time.Second
	}
	return nil
}

type Dist struct {
	N     int           `json:"n"`
	P50   time.Duration `json:"p50_ns"`
	P90   time.Duration `json:"p90_ns"`
	P99   time.Duration `json:"p99_ns"`
	P999  time.Duration `json:"p999_ns"`
	Max   time.Duration `json:"max_ns"`
	Mean  time.Duration `json:"mean_ns"`
	Error int           `json:"errors"`
}

type Summary struct {
	Config    Config         `json:"config"`
	Offered   float64        `json:"offered_per_s"`
	Achieved  float64        `json:"achieved_per_s"`
	Window    time.Duration  `json:"window_ns"`
	Reads     Dist           `json:"reads"`
	Writes    Dist           `json:"writes"`
	All       Dist           `json:"all"`
	Dropped   int            `json:"dropped"`
	Lag       Dist           `json:"lag"`
	Errors    map[string]int `json:"errors"`
	Valid     bool           `json:"valid"`
	Invalid   string         `json:"invalid,omitempty"`
	TimedOut  int            `json:"timed_out"`
	Scheduled int            `json:"scheduled"`
}

type record struct {
	read      bool
	scheduled time.Duration
	started   time.Duration
	ended     time.Duration
	err       string
}

const (
	spinWindow = 2 * time.Millisecond
	maxLagP99  = time.Millisecond
)

func Run(ctx context.Context, target Target, cfg Config) (Summary, error) {
	if err := cfg.normalize(); err != nil {
		return Summary{}, err
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, 0x6c6f6164))
	value := make([]byte, cfg.ValueBytes)
	for i := range value {
		value[i] = 'a' + byte(i%26)
	}
	interval := float64(time.Second) / cfg.Rate
	total := cfg.Warmup + cfg.Duration
	var (
		mu       sync.Mutex
		records  []record
		dropped  int
		inflight atomic.Int64
		wg       sync.WaitGroup
	)
	start := time.Now()
	since := func() time.Duration { return time.Since(start) }
	next := 0.0
	for ctx.Err() == nil {
		at := time.Duration(next)
		if at >= total {
			break
		}
		if cfg.Poisson {
			next += rng.ExpFloat64() * interval
		} else {
			next += interval
		}
		waitUntil(start, at)
		op := Op{Read: rng.Float64() < cfg.ReadFraction, Key: fmt.Sprintf("k%06d", rng.IntN(cfg.Keys))}
		if !op.Read {
			op.Value = value
		}
		measured := at >= cfg.Warmup
		if inflight.Load() >= int64(cfg.MaxInflight) {
			if measured {
				mu.Lock()
				dropped++
				mu.Unlock()
			}
			continue
		}
		inflight.Add(1)
		wg.Go(func() {
			defer inflight.Add(-1)
			started := since()
			octx, cancel := context.WithTimeout(ctx, cfg.Timeout)
			err := target.Do(octx, op)
			cancel()
			if !measured {
				return
			}
			r := record{read: op.Read, scheduled: at, started: started, ended: since()}
			if err != nil {
				r.err = errorClass(err)
			}
			mu.Lock()
			records = append(records, r)
			mu.Unlock()
		})
	}
	wg.Wait()
	return summarize(cfg, records, dropped), nil
}

func waitUntil(start time.Time, at time.Duration) {
	for {
		d := at - time.Since(start)
		if d <= 0 {
			return
		}
		if d > spinWindow {
			time.Sleep(d - spinWindow)
		}
	}
}

func errorClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	type coded interface{ Class() string }
	var c coded
	if errors.As(err, &c) {
		return c.Class()
	}
	return err.Error()
}

func dist(lat []time.Duration, errs int) Dist {
	d := Dist{N: len(lat), Error: errs}
	if len(lat) == 0 {
		return d
	}
	slices.Sort(lat)
	at := func(q float64) time.Duration {
		i := int(q * float64(len(lat)-1))
		return lat[i]
	}
	var sum time.Duration
	for _, l := range lat {
		sum += l
	}
	d.P50, d.P90, d.P99, d.P999, d.Max, d.Mean = at(0.5), at(0.9), at(0.99), at(0.999), lat[len(lat)-1], sum/time.Duration(len(lat))
	return d
}

func summarize(cfg Config, records []record, dropped int) Summary {
	s := Summary{Config: cfg, Offered: cfg.Rate, Window: cfg.Duration, Dropped: dropped, Errors: map[string]int{}, Scheduled: len(records) + dropped}
	var reads, writes, all, lags []time.Duration
	var readErrs, writeErrs int
	ok := 0
	for _, r := range records {
		lags = append(lags, r.started-r.scheduled)
		if r.err != "" {
			s.Errors[r.err]++
			if r.err == "timeout" {
				s.TimedOut++
			}
			if r.read {
				readErrs++
			} else {
				writeErrs++
			}
			continue
		}
		ok++
		l := r.ended - r.scheduled
		all = append(all, l)
		if r.read {
			reads = append(reads, l)
		} else {
			writes = append(writes, l)
		}
	}
	s.Reads, s.Writes, s.All, s.Lag = dist(reads, readErrs), dist(writes, writeErrs), dist(all, readErrs+writeErrs), dist(lags, 0)
	s.Achieved = float64(ok) / cfg.Duration.Seconds()
	s.Valid = true
	switch {
	case s.Lag.P99 > max(maxLagP99, s.All.P99/10):
		s.Valid, s.Invalid = false, fmt.Sprintf("the generator itself started requests late: p99 lag %v against a p99 latency of %v", s.Lag.P99, s.All.P99)
	case dropped > 0:
		s.Valid, s.Invalid = false, fmt.Sprintf("%d requests dropped at the in-flight limit", dropped)
	}
	return s
}

type Counters struct {
	Fsyncs, Batches, Entries, Sent, Writes, Reads, Elections uint64
}

type Report struct {
	Summary     Summary    `json:"summary"`
	Nodes       int        `json:"nodes"`
	ReadMode    string     `json:"read_mode"`
	PerNode     []Counters `json:"per_node_delta,omitempty"`
	FsyncsPerOp float64    `json:"fsyncs_per_op"`
	MsgsPerOp   float64    `json:"msgs_per_op"`
	EntriesPerF float64    `json:"entries_per_fsync"`
	Elections   uint64     `json:"elections"`
}
