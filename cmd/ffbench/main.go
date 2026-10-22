package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
	"github.com/mirzanajafov/fivefamilies/client"
)

type target struct {
	kv       []pb.KVClient
	admin    []pb.AdminClient
	router   *client.Router
	next     atomic.Uint64
	mode     pb.ReadMode
	follower bool
}

type codeErr struct{ code string }

func (e codeErr) Error() string { return e.code }
func (e codeErr) Class() string { return e.code }

func (t *target) Do(ctx context.Context, op loadgen.Op) error {
	n := t.router.Target()
	toLeader := !op.Read || !t.follower
	if !toLeader {
		n = int(t.next.Add(1) % uint64(len(t.kv)))
	}
	var md metadata.MD
	var err error
	if op.Read {
		_, err = t.kv[n].Get(ctx, &pb.GetRequest{Key: op.Key, Mode: t.mode}, grpc.Header(&md))
	} else {
		_, err = t.kv[n].Put(ctx, &pb.PutRequest{Key: op.Key, Value: op.Value}, grpc.Header(&md))
	}
	if err == nil {
		return nil
	}
	if toLeader && status.Code(err) == codes.Unavailable {
		t.router.MoveOn(n, md)
	}
	return codeErr{status.Code(err).String()}
}

func (t *target) counters(ctx context.Context) ([]loadgen.Counters, error) {
	out := make([]loadgen.Counters, len(t.admin))
	for i, a := range t.admin {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		st, err := a.Status(cctx, &pb.StatusRequest{})
		cancel()
		if err != nil {
			return nil, fmt.Errorf("status of node %d: %w", i+1, err)
		}
		out[i] = loadgen.Counters{Fsyncs: st.GetFsyncs(), Batches: st.GetWalBatches(), Entries: st.GetEntriesWritten(), Sent: st.GetMsgsSent(),
			Writes: st.GetWrites(), Reads: st.GetReads(), Elections: st.GetElections()}
	}
	return out, nil
}

func main() {
	nodes := flag.String("nodes", "n1:3161,n2:3161,n3:3161,n4:3161,n5:3161", "node addresses, in ID order")
	rate := flag.Float64("rate", 1000, "operations per second, offered open-loop")
	duration := flag.Duration("duration", 20*time.Second, "measured time")
	warmup := flag.Duration("warmup", 5*time.Second, "time before measuring")
	reads := flag.Float64("reads", 0, "share of operations that are reads")
	readMode := flag.String("read-mode", "index", "index, lease, log or follower")
	keys := flag.Int("keys", 1000, "key space")
	valueBytes := flag.Int("value-bytes", 128, "value size")
	timeout := flag.Duration("timeout", 2*time.Second, "per-operation timeout")
	inflight := flag.Int("max-inflight", 10000, "operations in flight before the generator drops")
	poisson := flag.Bool("poisson", false, "Poisson arrivals instead of evenly spaced")
	seed := flag.Uint64("seed", 1, "seed")
	preload := flag.Bool("preload", true, "write every key once before the run")
	flag.Parse()

	t := &target{}
	switch *readMode {
	case "index":
		t.mode = pb.ReadMode_READ_MODE_INDEX
	case "lease":
		t.mode = pb.ReadMode_READ_MODE_LEASE
	case "log":
		t.mode = pb.ReadMode_READ_MODE_LOG
	case "follower":
		t.mode, t.follower = pb.ReadMode_READ_MODE_INDEX, true
	default:
		fmt.Fprintln(os.Stderr, "unknown read mode", *readMode)
		os.Exit(2)
	}
	addrs := strings.Split(*nodes, ",")
	t.router = client.NewRouter(addrs)
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		conn.Connect()
		t.kv = append(t.kv, pb.NewKVClient(conn))
		t.admin = append(t.admin, pb.NewAdminClient(conn))
	}
	ctx := context.Background()
	if *preload {
		value := make([]byte, *valueBytes)
		for i := range *keys {
			op := loadgen.Op{Key: fmt.Sprintf("k%06d", i), Value: value}
			for try := 0; ; try++ {
				octx, cancel := context.WithTimeout(ctx, *timeout)
				err := t.Do(octx, op)
				cancel()
				if err == nil {
					break
				}
				if try == 50 {
					fmt.Fprintln(os.Stderr, "preload:", err)
					os.Exit(1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}
	cfg := loadgen.Config{Rate: *rate, Duration: *duration, Warmup: *warmup, ReadFraction: *reads, Keys: *keys, ValueBytes: *valueBytes,
		MaxInflight: *inflight, Timeout: *timeout, Poisson: *poisson, Seed: *seed}
	type sampled struct {
		c   []loadgen.Counters
		err error
	}
	before := make(chan sampled, 1)
	go func() {
		time.Sleep(*warmup)
		c, err := t.counters(ctx)
		before <- sampled{c, err}
	}()
	s, err := loadgen.Run(ctx, t, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b := <-before
	after, aerr := t.counters(ctx)
	if b.err != nil || aerr != nil {
		fmt.Fprintln(os.Stderr, "counters:", b.err, aerr)
		os.Exit(1)
	}
	r := loadgen.Report{Summary: s, Nodes: len(t.kv), ReadMode: *readMode}
	var fsyncs, sent, entries uint64
	for i := range after {
		d := loadgen.Counters{
			Fsyncs: after[i].Fsyncs - b.c[i].Fsyncs, Batches: after[i].Batches - b.c[i].Batches, Entries: after[i].Entries - b.c[i].Entries,
			Sent: after[i].Sent - b.c[i].Sent, Writes: after[i].Writes - b.c[i].Writes, Reads: after[i].Reads - b.c[i].Reads,
			Elections: after[i].Elections - b.c[i].Elections,
		}
		r.PerNode = append(r.PerNode, d)
		fsyncs, sent, entries = fsyncs+d.Fsyncs, sent+d.Sent, entries+d.Entries
		r.Elections = max(r.Elections, d.Elections)
	}
	if ops := float64(s.All.N); ops > 0 {
		r.FsyncsPerOp, r.MsgsPerOp = float64(fsyncs)/ops, float64(sent)/ops
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
