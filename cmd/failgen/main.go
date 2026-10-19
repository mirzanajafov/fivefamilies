package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

type sample struct {
	Kind      string `json:"kind,omitempty"`
	Leader    int    `json:"leader,omitempty"`
	Term      uint64 `json:"term,omitempty"`
	Scheduled int64  `json:"scheduled_ns"`
	Done      int64  `json:"done_ns"`
	Node      int    `json:"node"`
	Ok        bool   `json:"ok"`
	Err       string `json:"error,omitempty"`
}

type router struct {
	clients []pb.KVClient
	admins  []pb.AdminClient
	leader  atomic.Int64
}

func (r *router) watch(ctx context.Context, start time.Time, record func(sample)) {
	lastLeader, lastTerm := 0, uint64(0)
	down := make([]bool, len(r.admins))
	for ctx.Err() == nil {
		for i, a := range r.admins {
			cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
			st, err := a.Status(cctx, &pb.StatusRequest{})
			cancel()
			now := int64(time.Since(start))
			if err != nil {
				if !down[i] {
					down[i] = true
					record(sample{Kind: "unreachable", Node: i + 1, Done: now})
				}
				continue
			}
			if down[i] {
				down[i] = false
				record(sample{Kind: "reachable", Node: i + 1, Done: now})
			}
			if st.GetState() == "leader" && (i+1 != lastLeader || st.GetTerm() != lastTerm) && st.GetTerm() >= lastTerm {
				lastLeader, lastTerm = i+1, st.GetTerm()
				record(sample{Kind: "leader", Leader: i + 1, Term: st.GetTerm(), Done: now})
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (r *router) put(ctx context.Context, key string, value []byte) (int, error) {
	n := int(r.leader.Load())
	var md metadata.MD
	_, err := r.clients[n].Put(ctx, &pb.PutRequest{Key: key, Value: value}, grpc.Header(&md))
	if err == nil {
		return n, nil
	}
	if hint := md.Get("ff-leader"); len(hint) > 0 {
		if id, perr := strconv.Atoi(hint[0]); perr == nil && id >= 1 && id <= len(r.clients) && id-1 != n {
			r.leader.CompareAndSwap(int64(n), int64(id-1))
			return n, err
		}
	}
	r.leader.CompareAndSwap(int64(n), int64((n+1)%len(r.clients)))
	return n, err
}

func main() {
	nodes := flag.String("nodes", "n1:3161,n2:3161,n3:3161,n4:3161,n5:3161", "node addresses, in ID order")
	rate := flag.Int("rate", 200, "writes per second")
	duration := flag.Duration("duration", 10*time.Second, "how long to write")
	timeout := flag.Duration("timeout", 300*time.Millisecond, "per-request timeout")
	out := flag.String("out", "", "file for the samples (JSON lines)")
	coldKV := flag.Bool("cold", false, "watch leadership over separate connections, so the write connections to followers stay cold until the leader dies")
	flag.Parse()

	r := &router{}
	for a := range strings.SplitSeq(*nodes, ",") {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer conn.Close()
		r.clients = append(r.clients, pb.NewKVClient(conn))
		adm := conn
		if *coldKV {
			adm, err = grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			defer adm.Close()
		}
		r.admins = append(r.admins, pb.NewAdminClient(adm))
	}
	interval := time.Second / time.Duration(*rate)
	start := time.Now()
	var mu sync.Mutex
	var samples []sample
	var wg sync.WaitGroup
	record := func(s sample) {
		mu.Lock()
		samples = append(samples, s)
		mu.Unlock()
	}
	wctx, stopWatch := context.WithCancel(context.Background())
	var ww sync.WaitGroup
	ww.Go(func() { r.watch(wctx, start, record) })
	for i := 0; ; i++ {
		at := start.Add(time.Duration(i) * interval)
		if at.Sub(start) >= *duration {
			break
		}
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			defer cancel()
			node, err := r.put(ctx, "failover", []byte(strconv.Itoa(i)))
			s := sample{Scheduled: int64(at.Sub(start)), Done: int64(time.Since(start)), Node: node + 1, Ok: err == nil}
			if err != nil {
				s.Err = status.Code(err).String()
			}
			record(s)
		})
	}
	wg.Wait()
	stopWatch()
	ww.Wait()
	if err := writeSamples(*out, samples); err != nil {
		fmt.Fprintln(os.Stderr, "writing the samples:", err)
		os.Exit(1)
	}
}

func writeSamples(path string, samples []sample) error {
	if path == "" {
		return encodeAll(os.Stdout, samples)
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := encodeAll(f, samples); err != nil {
		return errors.Join(err, f.Close())
	}
	return f.Close()
}

func encodeAll(w io.Writer, samples []sample) error {
	enc := json.NewEncoder(w)
	for _, s := range samples {
		if err := enc.Encode(s); err != nil {
			return err
		}
	}
	return nil
}
