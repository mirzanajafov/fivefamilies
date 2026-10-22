package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

type target struct {
	cli          *clientv3.Client
	serializable bool
}

func (t *target) Do(ctx context.Context, op loadgen.Op) error {
	if op.Read {
		var opts []clientv3.OpOption
		if t.serializable {
			opts = append(opts, clientv3.WithSerializable())
		}
		_, err := t.cli.Get(ctx, op.Key, opts...)
		return err
	}
	_, err := t.cli.Put(ctx, op.Key, string(op.Value))
	return err
}

func scrape(ctx context.Context, endpoint string) (loadgen.Counters, error) {
	var c loadgen.Counters
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+endpoint+"/metrics", nil)
	if err != nil {
		return c, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return c, err
	}
	defer func() { _ = resp.Body.Close() }()
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), " ")
		if !ok || strings.HasPrefix(name, "#") {
			continue
		}
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			continue
		}
		switch name {
		case "etcd_disk_wal_fsync_duration_seconds_count":
			c.Fsyncs = uint64(v)
		case "etcd_server_leader_changes_seen_total":
			c.Elections = uint64(v)
		case "etcd_server_proposals_committed_total":
			c.Entries = uint64(v)
		}
	}
	return c, sc.Err()
}

func scrapeAll(ctx context.Context, endpoints []string) ([]loadgen.Counters, error) {
	out := make([]loadgen.Counters, len(endpoints))
	for i, e := range endpoints {
		c, err := scrape(ctx, e)
		if err != nil {
			return nil, fmt.Errorf("metrics of %s: %w", e, err)
		}
		out[i] = c
	}
	return out, nil
}

func main() {
	endpoints := flag.String("endpoints", "e1:2379,e2:2379,e3:2379,e4:2379,e5:2379", "etcd client endpoints")
	only := flag.String("only", "", "send every request to this one endpoint (the leader) instead of all of them")
	rate := flag.Float64("rate", 1000, "operations per second, offered open-loop")
	duration := flag.Duration("duration", 20*time.Second, "measured time")
	warmup := flag.Duration("warmup", 5*time.Second, "time before measuring")
	reads := flag.Float64("reads", 0, "share of operations that are reads")
	readMode := flag.String("read-mode", "linearizable", "linearizable or serializable")
	keys := flag.Int("keys", 1000, "key space")
	valueBytes := flag.Int("value-bytes", 128, "value size")
	timeout := flag.Duration("timeout", 2*time.Second, "per-operation timeout")
	inflight := flag.Int("max-inflight", 10000, "operations in flight before the generator drops")
	seed := flag.Uint64("seed", 1, "seed")
	flag.Parse()

	all := strings.Split(*endpoints, ",")
	use := all
	if *only != "" {
		use = []string{*only}
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: use, DialTimeout: 5 * time.Second})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	t := &target{cli: cli, serializable: *readMode == "serializable"}
	cfg := loadgen.Config{Rate: *rate, Duration: *duration, Warmup: *warmup, ReadFraction: *reads, Keys: *keys, ValueBytes: *valueBytes,
		MaxInflight: *inflight, Timeout: *timeout, Seed: *seed, Preload: true}
	r, err := loadgen.Measure(context.Background(), t, cfg, func(ctx context.Context) ([]loadgen.Counters, error) { return scrapeAll(ctx, all) })
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r.ReadMode = *readMode
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, "writing the report:", err)
		os.Exit(1)
	}
}
