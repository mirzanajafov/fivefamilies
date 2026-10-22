package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

type result struct {
	Mode     string        `json:"mode"`
	Writers  int           `json:"writers"`
	Bytes    int           `json:"bytes"`
	Syncs    int           `json:"syncs"`
	PerSec   float64       `json:"syncs_per_s"`
	P50      time.Duration `json:"p50_ns"`
	P99      time.Duration `json:"p99_ns"`
	Max      time.Duration `json:"max_ns"`
	Duration time.Duration `json:"duration_ns"`
}

type writer struct {
	dir     string
	n       int
	mode    string
	segment int64
	f       *os.File
	off     int64
	files   int
}

func (w *writer) close() error {
	if w.f == nil {
		return nil
	}
	f := w.f
	w.f = nil
	return f.Close()
}

func (w *writer) open() error {
	if err := w.close(); err != nil {
		return err
	}
	w.files++
	name := filepath.Join(w.dir, fmt.Sprintf("w%d-%d.log", w.n, w.files))
	flags := os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	if strings.HasPrefix(w.mode, "append") {
		flags |= os.O_APPEND
	}
	f, err := os.OpenFile(name, flags, 0o600)
	if err != nil {
		return err
	}
	w.f, w.off = f, 0
	if strings.HasPrefix(w.mode, "prealloc") {
		if err := preallocate(f, w.segment); err != nil {
			return err
		}
		return f.Sync()
	}
	return nil
}

func (w *writer) write(buf []byte) error {
	if strings.HasPrefix(w.mode, "prealloc") {
		if w.off+int64(len(buf)) > w.segment {
			if err := w.open(); err != nil {
				return err
			}
		}
		_, err := w.f.WriteAt(buf, w.off)
		w.off += int64(len(buf))
		return err
	}
	_, err := w.f.Write(buf)
	return err
}

func (w *writer) sync() error {
	if strings.HasSuffix(w.mode, "fdatasync") {
		return datasync(w.f)
	}
	return w.f.Sync()
}

func run(dir, mode string, writers, size int, segment int64, d time.Duration) (result, error) {
	res := result{Mode: mode, Writers: writers, Bytes: size, Duration: d}
	if err := os.RemoveAll(dir); err != nil {
		return res, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return res, err
	}
	var mu sync.Mutex
	var lat []time.Duration
	var first error
	var wg sync.WaitGroup
	deadline := time.Now().Add(d)
	for n := range writers {
		wg.Go(func() {
			w := &writer{dir: dir, n: n, mode: mode, segment: segment}
			buf := make([]byte, size)
			for i := range buf {
				buf[i] = byte(i)
			}
			var mine []time.Duration
			err := w.open()
			for err == nil && time.Now().Before(deadline) {
				if err = w.write(buf); err != nil {
					break
				}
				t := time.Now()
				err = w.sync()
				mine = append(mine, time.Since(t))
			}
			err = errors.Join(err, w.close())
			mu.Lock()
			lat = append(lat, mine...)
			if err != nil && first == nil {
				first = err
			}
			mu.Unlock()
		})
	}
	wg.Wait()
	if first != nil {
		return res, first
	}
	slices.Sort(lat)
	res.Syncs = len(lat)
	res.PerSec = float64(len(lat)) / d.Seconds()
	if len(lat) > 0 {
		res.P50, res.P99, res.Max = loadgen.Quantile(lat, 0.5), loadgen.Quantile(lat, 0.99), lat[len(lat)-1]
	}
	return res, os.RemoveAll(dir)
}

func main() {
	dir := flag.String("dir", "disktest", "directory for the test files")
	modes := flag.String("modes", strings.Join(knownModes, ","), "modes to run, comma separated")
	writers := flag.String("writers", "1,5", "concurrent writers, each with its own file, comma separated")
	sizes := flag.String("bytes", "512,16384", "bytes written before each sync, comma separated")
	segment := flag.Int64("segment", 64<<20, "preallocated file size")
	d := flag.Duration("duration", 10*time.Second, "time per configuration")
	flag.Parse()
	writerCounts, err := parseList(*writers, parseCount)
	if err != nil {
		fmt.Fprintln(os.Stderr, "-writers:", err)
		os.Exit(2)
	}
	byteCounts, err := parseList(*sizes, parseCount)
	if err != nil {
		fmt.Fprintln(os.Stderr, "-bytes:", err)
		os.Exit(2)
	}
	modeList, err := parseList(*modes, parseMode)
	if err != nil {
		fmt.Fprintln(os.Stderr, "-modes:", err)
		os.Exit(2)
	}
	var out []result
	for _, mode := range modeList {
		for _, w := range writerCounts {
			for _, b := range byteCounts {
				r, err := run(filepath.Join(*dir, mode), mode, w, b, *segment, *d)
				if err != nil {
					fmt.Fprintln(os.Stderr, mode, err)
					os.Exit(1)
				}
				fmt.Fprintf(os.Stderr, "%-20s writers %d, %6d B: %7.0f syncs/s, p50 %v, p99 %v\n", mode, w, b, r.PerSec, r.P50, r.P99)
				out = append(out, r)
			}
		}
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(out); err != nil {
		fmt.Fprintln(os.Stderr, "writing the results:", err)
		os.Exit(1)
	}
}

var knownModes = []string{"append-fsync", "append-fdatasync", "prealloc-fsync", "prealloc-fdatasync"}

func parseList[T any](list string, parse func(string) (T, error)) ([]T, error) {
	var out []T
	for s := range strings.SplitSeq(list, ",") {
		v, err := parse(strings.TrimSpace(s))
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func parseMode(s string) (string, error) {
	if !slices.Contains(knownModes, s) {
		return "", fmt.Errorf("unknown mode %q", s)
	}
	return s, nil
}

func parseCount(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%q is not a positive number", s)
	}
	return n, nil
}
