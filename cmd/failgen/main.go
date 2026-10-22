package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
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

type generator struct {
	clients []pb.KVClient
	admins  []pb.AdminClient
	router  *client.Router
}

func (g *generator) watch(ctx context.Context, start time.Time, record func(sample)) {
	lastLeader, lastTerm := 0, uint64(0)
	down := make([]bool, len(g.admins))
	for ctx.Err() == nil {
		for i, a := range g.admins {
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
			if st.GetRole() == pb.Role_ROLE_LEADER && (i+1 != lastLeader || st.GetTerm() != lastTerm) && st.GetTerm() >= lastTerm {
				lastLeader, lastTerm = i+1, st.GetTerm()
				record(sample{Kind: "leader", Leader: i + 1, Term: st.GetTerm(), Done: now})
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (g *generator) put(ctx context.Context, key string, value []byte) (int, error) {
	n := g.router.Target()
	var md metadata.MD
	_, err := g.clients[n].Put(ctx, &pb.PutRequest{Key: key, Value: value}, grpc.Header(&md))
	if err != nil {
		g.router.MoveOn(n, md)
	}
	return n, err
}

func main() {
	nodes := flag.String("nodes", "n1:3161,n2:3161,n3:3161,n4:3161,n5:3161", "node addresses, in ID order")
	rate := flag.Int("rate", 200, "writes per second")
	duration := flag.Duration("duration", 10*time.Second, "how long to write")
	timeout := flag.Duration("timeout", 300*time.Millisecond, "per-request timeout")
	coldKV := flag.Bool("cold", false, "watch leadership over separate connections, so the write connections to followers stay cold until the leader dies")
	flag.Parse()

	addrs := strings.Split(*nodes, ",")
	g := &generator{router: client.NewRouter(addrs)}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		g.clients = append(g.clients, pb.NewKVClient(conn))
		adm := conn
		if *coldKV {
			adm, err = grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		g.admins = append(g.admins, pb.NewAdminClient(adm))
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
	ww.Go(func() { g.watch(wctx, start, record) })
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
			node, err := g.put(ctx, "failover", []byte(strconv.Itoa(i)))
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
	if err := encodeAll(os.Stdout, samples); err != nil {
		fmt.Fprintln(os.Stderr, "writing the samples:", err)
		os.Exit(1)
	}
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
