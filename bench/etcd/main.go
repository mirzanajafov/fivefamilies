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
	defer resp.Body.Close()
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
	defer cli.Close()
	t := &target{cli: cli, serializable: *readMode == "serializable"}
	ctx := context.Background()
	value := make([]byte, *valueBytes)
	for i := range *keys {
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := t.Do(pctx, loadgen.Op{Key: fmt.Sprintf("k%06d", i), Value: value})
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, "preload:", err)
			os.Exit(1)
		}
	}
	cfg := loadgen.Config{Rate: *rate, Duration: *duration, Warmup: *warmup, ReadFraction: *reads, Keys: *keys, ValueBytes: *valueBytes,
		MaxInflight: *inflight, Timeout: *timeout, Seed: *seed}
	type sampled struct {
		c   []loadgen.Counters
		err error
	}
	before := make(chan sampled, 1)
	go func() {
		time.Sleep(*warmup)
		c, err := scrapeAll(ctx, all)
		before <- sampled{c, err}
	}()
	s, err := loadgen.Run(ctx, t, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b := <-before
	after, aerr := scrapeAll(ctx, all)
	if b.err != nil || aerr != nil {
		fmt.Fprintln(os.Stderr, "metrics:", b.err, aerr)
		os.Exit(1)
	}
	r := loadgen.Report{Summary: s, Nodes: len(all), ReadMode: *readMode}
	var fsyncs, entries uint64
	for i := range after {
		d := loadgen.Counters{Fsyncs: after[i].Fsyncs - b.c[i].Fsyncs, Entries: after[i].Entries - b.c[i].Entries, Elections: after[i].Elections - b.c[i].Elections}
		r.PerNode = append(r.PerNode, d)
		fsyncs, entries = fsyncs+d.Fsyncs, entries+d.Entries
		r.Elections = max(r.Elections, d.Elections)
	}
	if ops := float64(s.All.N); ops > 0 {
		r.FsyncsPerOp = float64(fsyncs) / ops
	}
	if fsyncs > 0 {
		r.EntriesPerF = float64(entries) / float64(fsyncs)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, "writing the report:", err)
		os.Exit(1)
	}
}
