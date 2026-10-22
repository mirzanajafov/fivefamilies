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
	Rate         float64       `json:"rate"`
	Duration     time.Duration `json:"duration_ns"`
	Warmup       time.Duration `json:"warmup_ns"`
	ReadFraction float64       `json:"read_fraction"`
	Keys         int           `json:"keys"`
	ValueBytes   int           `json:"value_bytes"`
	MaxInflight  int           `json:"max_inflight"`
	Timeout      time.Duration `json:"timeout_ns"`
	Poisson      bool          `json:"poisson"`
	Seed         uint64        `json:"seed"`
	Preload      bool          `json:"preload"`
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

func run(ctx context.Context, target Target, cfg Config) (Summary, error) {
	if err := cfg.normalize(); err != nil {
		return Summary{}, err
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, 0x6c6f6164))
	value := valueOf(cfg.ValueBytes)
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
		op := Op{Read: rng.Float64() < cfg.ReadFraction, Key: key(rng.IntN(cfg.Keys))}
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

func key(i int) string {
	return fmt.Sprintf("k%06d", i)
}

func valueOf(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = 'a' + byte(i%26)
	}
	return value
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

func Quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	pos := q * float64(len(sorted)-1)
	i := int(pos)
	if i == len(sorted)-1 {
		return sorted[i]
	}
	return sorted[i] + time.Duration((pos-float64(i))*float64(sorted[i+1]-sorted[i]))
}

func dist(lat []time.Duration, errs int) Dist {
	d := Dist{N: len(lat), Error: errs}
	if len(lat) == 0 {
		return d
	}
	slices.Sort(lat)
	var sum time.Duration
	for _, l := range lat {
		sum += l
	}
	d.P50, d.P90, d.P99, d.P999 = Quantile(lat, 0.5), Quantile(lat, 0.9), Quantile(lat, 0.99), Quantile(lat, 0.999)
	d.Max, d.Mean = lat[len(lat)-1], sum/time.Duration(len(lat))
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
