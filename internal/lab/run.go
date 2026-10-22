package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

type Options struct {
	Seed         uint64                           `json:"seed"`
	Duration     time.Duration                    `json:"duration_ns"`
	Settle       time.Duration                    `json:"settle_ns"`
	Workers      int                              `json:"workers"`
	Keys         int                              `json:"keys"`
	Read         client.ReadMode                  `json:"read"`
	Faults       []FaultKind                      `json:"faults"`
	FaultMean    time.Duration                    `json:"fault_mean_ns"`
	Bug          string                           `json:"bug,omitempty"`
	Election     string                           `json:"election"`
	Heartbeat    string                           `json:"heartbeat"`
	Lease        string                           `json:"lease"`
	OpTimeout    time.Duration                    `json:"op_timeout_ns"`
	CheckTimeout time.Duration                    `json:"check_timeout_ns"`
	Out          string                           `json:"-"`
	Keep         bool                             `json:"-"`
	Project      string                           `json:"-"`
	File         string                           `json:"-"`
	Log          func(format string, args ...any) `json:"-"`
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

func (o Options) Validate() error {
	bugs, err := planted.Parse(o.Bug)
	if err != nil {
		return fmt.Errorf("-bug: %w", err)
	}
	if _, err := time.ParseDuration(o.Election); err != nil {
		return fmt.Errorf("-election: %w", err)
	}
	if _, err := time.ParseDuration(o.Heartbeat); err != nil {
		return fmt.Errorf("-heartbeat: %w", err)
	}
	lease, err := time.ParseDuration(o.Lease)
	switch {
	case err != nil:
		return fmt.Errorf("-lease: %w", err)
	case o.Read == client.ReadLease && lease <= 0:
		return errors.New("-read lease needs a -lease longer than zero")
	case bugs.Has(planted.Watches | planted.LocalKeepAlive):
		return errors.New("-bug: the lab runs no watchers or lease holders, so watch and lease bugs are never exercised")
	case bugs.Has(planted.FollowerRead) && o.Read == client.ReadLog:
		return errors.New("-bug followerread is never exercised when every read goes through the log")
	case bugs.Has(planted.EarlyConf) && !slices.Contains(o.Faults, FaultMember):
		return errors.New("-bug earlyconf is never exercised without member in -faults, since nothing else changes the membership")
	}
	return nil
}

func (r Report) OK() bool {
	return r.Linearizable == "Ok" && r.StatesEqual && len(r.Problems) == 0
}

func Run(ctx context.Context, opts Options) (Report, error) {
	rep := Report{Options: opts, Started: time.Now()}
	if err := opts.Validate(); err != nil {
		return rep, err
	}
	env := []string{"FF_BUG=" + opts.Bug, "FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=" + opts.Lease}
	c, err := startLab(ctx, opts.Project, opts.File, env)
	if err != nil {
		return rep, err
	}
	if opts.Keep {
		defer c.adm.Close()
	} else {
		defer c.stop(opts.Log)
	}
	err = rig.Poll(ctx, 200*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		l, _ := rig.LeaderOf(c.adm.Statuses(ctx, time.Second))
		return l > 0, nil
	})
	if err != nil {
		return rep, fmt.Errorf("no leader after starting: %w", err)
	}
	start := time.Now()
	leaders := watchLeaders(ctx, c.adm, start)
	w := &workload{addrs: c.addrs, workers: opts.Workers, keys: opts.Keys, read: opts.Read, opTimeout: opts.OpTimeout, seed: opts.Seed, start: start}
	stopWork := make(chan struct{})
	var work sync.WaitGroup
	work.Go(func() { w.run(stopWork) })
	rep.Faults = injectFaults(ctx, opts, c, start, leaders.current)
	rep.Problems = append(rep.Problems, restoreNodes(c.d)...)
	rig.Wait(ctx, opts.Settle)
	close(stopWork)
	rep.Leaders = leaders.stop()
	work.Wait()
	hist, unknown, failedReads := w.history()
	rep.Ops, rep.Unknown, rep.FailedReads = len(hist), unknown, failedReads
	v := check.Linearizable(hist, opts.CheckTimeout)
	rep.Linearizable, rep.CheckTook = string(v.Result), v.Took
	switch err := compareStates(ctx, c.adm, &rep); {
	case err == nil:
	case ctx.Err() != nil:
		rep.Problems = append(rep.Problems, fmt.Sprintf("the nodes had not converged when the run ended: %v", ctx.Err()))
	default:
		rep.Problems = append(rep.Problems, fmt.Sprintf("nodes did not converge to one state after the heal: %v", err))
	}
	if opts.Out != "" {
		if err := save(opts.Out, rep, w); err != nil {
			return rep, err
		}
	}
	return rep, nil
}

type leaderWatch struct {
	mu      sync.Mutex
	leader  int
	changes []LeaderChange
	cancel  context.CancelFunc
	done    chan struct{}
}

func watchLeaders(ctx context.Context, adm *rig.Admin, start time.Time) *leaderWatch {
	ctx, cancel := context.WithCancel(ctx)
	lw := &leaderWatch{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(lw.done)
		lastLeader, lastTerm := -1, uint64(0)
		for ctx.Err() == nil {
			l, t := rig.LeaderOf(adm.Statuses(ctx, 300*time.Millisecond))
			lw.mu.Lock()
			lw.leader = l
			if l != lastLeader || t != lastTerm {
				lw.changes = append(lw.changes, LeaderChange{At: time.Since(start), Leader: uint64(l), Term: t})
				lastLeader, lastTerm = l, t
			}
			lw.mu.Unlock()
			rig.Wait(ctx, 200*time.Millisecond)
		}
	}()
	return lw
}

func (lw *leaderWatch) current() int {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	return lw.leader
}

func (lw *leaderWatch) stop() []LeaderChange {
	lw.cancel()
	<-lw.done
	return lw.changes
}

func injectFaults(ctx context.Context, opts Options, c *labCluster, start time.Time, leader func() int) []FaultRecord {
	nctx, stop := context.WithTimeout(ctx, opts.Duration)
	defer stop()
	nem := &nemesis{d: c.d, adm: c.adm, rng: rand.New(rand.NewPCG(opts.Seed, 7)), kinds: opts.Faults, mean: opts.FaultMean, start: start, leader: leader}
	if len(opts.Faults) > 0 {
		nem.run(nctx)
	} else {
		<-nctx.Done()
	}
	return nem.log
}

func restoreNodes(d *Docker) []string {
	var problems []string
	if err := d.Heal(context.Background()); err != nil {
		problems = append(problems, "heal: "+err.Error())
	}
	for i := 1; i <= d.Nodes; i++ {
		if msg, exited := d.Exited(context.Background(), i); exited {
			problems = append(problems, msg)
			if err := d.Start(context.Background(), i); err != nil {
				problems = append(problems, fmt.Sprintf("restarting node %d: %v", i, err))
			}
		} else if !d.Running(context.Background(), i) {
			if err := d.Unpause(context.Background(), i); err != nil {
				problems = append(problems, fmt.Sprintf("unpausing node %d: %v", i, err))
			}
		}
	}
	return problems
}

func compareStates(ctx context.Context, adm *rig.Admin, rep *Report) error {
	return rig.Poll(ctx, 500*time.Millisecond, 30*time.Second, func(ctx context.Context) (bool, error) {
		sts := adm.Statuses(ctx, 2*time.Second)
		if ctx.Err() != nil && slices.Contains(sts, nil) {
			return false, nil
		}
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
}

func save(dir string, rep Report, w *workload) error {
	if err := rig.SaveJSON(dir, "report.json", rep); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, "raw", "history.jsonl"))
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, r := range w.ops {
		if err := enc.Encode(r); err != nil {
			return errors.Join(err, f.Close())
		}
	}
	return f.Close()
}
