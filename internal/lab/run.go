package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
)

type Options struct {
	Seed         uint64        `json:"seed"`
	Duration     time.Duration `json:"duration_ns"`
	Settle       time.Duration `json:"settle_ns"`
	Workers      int           `json:"workers"`
	Keys         int           `json:"keys"`
	Read         string        `json:"read"`
	Faults       []FaultKind   `json:"faults"`
	FaultMean    time.Duration `json:"fault_mean_ns"`
	Bug          string        `json:"bug,omitempty"`
	Election     string        `json:"election"`
	Heartbeat    string        `json:"heartbeat"`
	Lease        string        `json:"lease"`
	OpTimeout    time.Duration `json:"op_timeout_ns"`
	CheckTimeout time.Duration `json:"check_timeout_ns"`
	Out          string        `json:"-"`
	Keep         bool          `json:"-"`
	Project      string        `json:"-"`
	File         string        `json:"-"`
}

type LeaderChange struct {
	At     time.Duration `json:"at_ns"`
	Leader uint64        `json:"leader"`
	Term   uint64        `json:"term"`
}

type Report struct {
	Options      Options        `json:"options"`
	Started      time.Time      `json:"started"`
	Ops          int            `json:"ops"`
	Unknown      int            `json:"unknown_writes"`
	FailedReads  int            `json:"failed_reads"`
	Faults       []FaultRecord  `json:"faults"`
	Leaders      []LeaderChange `json:"leaders"`
	Linearizable string         `json:"linearizable"`
	CheckTook    time.Duration  `json:"check_took_ns"`
	StatesEqual  bool           `json:"states_equal"`
	StateIndex   uint64         `json:"state_index"`
	Hashes       []string       `json:"hashes"`
	Problems     []string       `json:"problems,omitempty"`
}

func (r Report) OK() bool {
	return r.Linearizable == "Ok" && r.StatesEqual && len(r.Problems) == 0
}

type cluster struct {
	addrs []string
	conns []*grpc.ClientConn
}

func dialAdmin(addrs []string) (*cluster, error) {
	c := &cluster{addrs: addrs}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns = append(c.conns, conn)
	}
	return c, nil
}

func (c *cluster) close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *cluster) statuses(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return c.ask(ctx, timeout, &pb.StatusRequest{Hash: true})
}

func (c *cluster) progress(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return c.ask(ctx, timeout, &pb.StatusRequest{})
}

func (c *cluster) ask(ctx context.Context, timeout time.Duration, req *pb.StatusRequest) []*pb.StatusResponse {
	out := make([]*pb.StatusResponse, len(c.conns))
	var wg sync.WaitGroup
	for i, conn := range c.conns {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			st, err := pb.NewAdminClient(conn).Status(cctx, req)
			if err == nil {
				out[i] = st
			}
		})
	}
	wg.Wait()
	return out
}

func leaderOf(sts []*pb.StatusResponse) (int, uint64) {
	best, term := 0, uint64(0)
	for i, st := range sts {
		if st != nil && st.GetState() == "leader" && st.GetTerm() >= term {
			best, term = i+1, st.GetTerm()
		}
	}
	return best, term
}

func Run(ctx context.Context, opts Options) (Report, error) {
	rep := Report{Options: opts, Started: time.Now()}
	d := &Docker{Project: opts.Project, File: opts.File, Nodes: 5}
	env := []string{
		"FF_BUG=" + opts.Bug, "FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=" + opts.Lease,
	}
	if err := d.Up(ctx, env); err != nil {
		return rep, err
	}
	if !opts.Keep {
		defer d.Down(context.Background())
	}
	var addrs []string
	for i := 1; i <= d.Nodes; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:315%d", i))
	}
	adm, err := dialAdmin(addrs)
	if err != nil {
		return rep, err
	}
	defer adm.close()
	err = poll(ctx, 200*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		l, _ := leaderOf(adm.statuses(ctx, time.Second))
		return l > 0, nil
	})
	if err != nil {
		return rep, fmt.Errorf("no leader after starting: %w", err)
	}
	start := time.Now()
	mode := map[string]client.ReadMode{"index": client.ReadIndex, "lease": client.ReadLease, "log": client.ReadLog}[opts.Read]
	w := &workload{addrs: addrs, workers: opts.Workers, keys: opts.Keys, read: mode, opTimeout: opts.OpTimeout, seed: opts.Seed, start: start}
	var mu sync.Mutex
	current := 0
	pollCtx, stopPoll := context.WithCancel(ctx)
	var bg sync.WaitGroup
	bg.Go(func() {
		lastLeader, lastTerm := -1, uint64(0)
		for pollCtx.Err() == nil {
			l, t := leaderOf(adm.statuses(pollCtx, 300*time.Millisecond))
			mu.Lock()
			current = l
			if l != lastLeader || t != lastTerm {
				rep.Leaders = append(rep.Leaders, LeaderChange{At: time.Since(start), Leader: uint64(l), Term: t})
				lastLeader, lastTerm = l, t
			}
			mu.Unlock()
			wait(pollCtx, 200*time.Millisecond)
		}
	})
	stopWork := make(chan struct{})
	bg.Go(func() { w.run(stopWork) })
	nctx, stopFaults := context.WithTimeout(ctx, opts.Duration)
	nem := &nemesis{d: d, adm: adm, rng: rand.New(rand.NewPCG(opts.Seed, 7)), kinds: opts.Faults, mean: opts.FaultMean, start: start,
		leader: func() int { mu.Lock(); defer mu.Unlock(); return current }}
	if len(opts.Faults) > 0 {
		nem.run(nctx)
	} else {
		<-nctx.Done()
	}
	stopFaults()
	if err := d.Heal(context.Background()); err != nil {
		rep.Problems = append(rep.Problems, "heal: "+err.Error())
	}
	for i := 1; i <= d.Nodes; i++ {
		if msg, exited := d.Exited(context.Background(), i); exited {
			rep.Problems = append(rep.Problems, msg)
			d.Start(context.Background(), i)
		} else if !d.Running(context.Background(), i) {
			d.Unpause(context.Background(), i)
		}
	}
	rep.Faults = nem.log
	wait(ctx, opts.Settle)
	close(stopWork)
	stopPoll()
	bg.Wait()
	hist, unknown, failedReads := w.history()
	rep.Ops, rep.Unknown, rep.FailedReads = len(hist), unknown, failedReads
	v := check.Linearizable(hist, opts.CheckTimeout)
	rep.Linearizable, rep.CheckTook = string(v.Result), v.Took
	err = poll(ctx, 500*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		sts := adm.statuses(ctx, 2*time.Second)
		rep.Hashes = rep.Hashes[:0]
		equal := true
		for _, st := range sts {
			if st == nil {
				equal = false
				rep.Hashes = append(rep.Hashes, "unreachable")
				continue
			}
			rep.Hashes = append(rep.Hashes, fmt.Sprintf("%x@%d", st.GetStateHash(), st.GetHashIndex()))
			if sts[0] == nil || st.GetHashIndex() != sts[0].GetHashIndex() || st.GetStateHash() != sts[0].GetStateHash() {
				equal = false
			}
		}
		if equal {
			rep.StatesEqual, rep.StateIndex = true, sts[0].GetHashIndex()
		}
		return equal, nil
	})
	if err != nil {
		rep.Problems = append(rep.Problems, fmt.Sprintf("nodes did not converge to one state after the heal: %v", err))
	}
	if opts.Out != "" {
		if err := save(opts.Out, rep, w); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

func save(dir string, rep Report, w *workload) error {
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "report.json"), append(b, '\n'), 0o644); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "raw", "history.jsonl"))
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, op := range w.ops {
		if err := enc.Encode(op); err != nil {
			return err
		}
	}
	return nil
}
