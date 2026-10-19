package lab

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"time"
)

type FailoverOptions struct {
	Election  string        `json:"election"`
	Heartbeat string        `json:"heartbeat"`
	Lease     string        `json:"lease"`
	Trials    int           `json:"trials"`
	Rate      int           `json:"rate"`
	Timeout   time.Duration `json:"request_timeout_ns"`
	Cold      bool          `json:"cold_connections"`
	Before    time.Duration `json:"before_ns"`
	After     time.Duration `json:"after_ns"`
	Project   string        `json:"-"`
	File      string        `json:"-"`
}

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

type Trial struct {
	Killed      int            `json:"killed"`
	Requests    int            `json:"requests"`
	Failed      int            `json:"failed"`
	Unavailable time.Duration  `json:"unavailable_ns"`
	QuietGap    time.Duration  `json:"quiet_gap_ns"`
	Errors      map[string]int `json:"errors"`
	Detected    time.Duration  `json:"detected_ns"`
	NewLeader   time.Duration  `json:"new_leader_ns"`
	Election    time.Duration  `json:"election_ns"`
	Err         string         `json:"error,omitempty"`
}

type FailoverResult struct {
	Options FailoverOptions `json:"options"`
	Trials  []Trial         `json:"trials"`
	Median  time.Duration   `json:"median_ns"`
	Min     time.Duration   `json:"min_ns"`
	Max     time.Duration   `json:"max_ns"`
	Err     string          `json:"error,omitempty"`
}

func gaps(samples []sample, from, to int64) time.Duration {
	var done []int64
	for _, s := range samples {
		if s.Ok {
			done = append(done, s.Done)
		}
	}
	slices.Sort(done)
	var widest int64
	for i := 1; i < len(done); i++ {
		if done[i] < from || done[i-1] > to {
			continue
		}
		widest = max(widest, done[i]-done[i-1])
	}
	return time.Duration(widest)
}

func (d *Docker) generate(ctx context.Context, opts FailoverOptions) ([]sample, error) {
	network := d.Project + "_lab"
	args := []string{"run", "--rm", "--network", network, "--entrypoint", "failgen", "fivefamilies-lab-node",
		"-rate", fmt.Sprint(opts.Rate), "-duration", (opts.Before + opts.After).String(), "-timeout", opts.Timeout.String(), fmt.Sprintf("-cold=%v", opts.Cold)}
	c := exec.CommandContext(ctx, "docker", args...)
	out, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("generator: %w", err)
	}
	var samples []sample
	sc := bufio.NewScanner(strings.NewReader(string(out)))
	for sc.Scan() {
		var s sample
		if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
			return nil, err
		}
		samples = append(samples, s)
	}
	return samples, nil
}

func Failover(ctx context.Context, opts FailoverOptions, out string) (FailoverResult, error) {
	res, err := runFailover(ctx, opts)
	if err != nil {
		res.Err = err.Error()
	}
	if out != "" {
		if serr := SaveJSON(out, fmt.Sprintf("election-%s.json", opts.Election), res); serr != nil {
			return res, errors.Join(err, serr)
		}
	}
	return res, err
}

func runFailover(ctx context.Context, opts FailoverOptions) (FailoverResult, error) {
	res := FailoverResult{Options: opts}
	d := &Docker{Project: opts.Project, File: opts.File, Nodes: 5}
	env := []string{"FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=" + opts.Lease, "FF_BUG="}
	if err := d.Up(ctx, env); err != nil {
		return res, err
	}
	defer d.Down(context.Background())
	var addrs []string
	for i := 1; i <= d.Nodes; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:315%d", i))
	}
	adm, err := dialAdmin(addrs)
	if err != nil {
		return res, err
	}
	defer adm.close()
	var runErr error
	for t := 0; t < opts.Trials && ctx.Err() == nil; t++ {
		leader, err := steady(ctx, adm)
		if err != nil {
			runErr = err
			break
		}
		trial := Trial{Killed: leader}
		type genResult struct {
			samples []sample
			err     error
		}
		ch := make(chan genResult, 1)
		go func() {
			s, err := d.generate(ctx, opts)
			ch <- genResult{s, err}
		}()
		wait(ctx, opts.Before)
		if err := d.Kill(ctx, leader); err != nil {
			trial.Err = err.Error()
		}
		g := <-ch
		if err := d.Start(ctx, leader); err != nil && trial.Err == "" {
			trial.Err = err.Error()
		}
		if g.err != nil {
			trial.Err = g.err.Error()
		}
		trial.Errors = map[string]int{}
		var writes []sample
		for _, s := range g.samples {
			switch {
			case s.Kind == "unreachable" && s.Node == leader && trial.Detected == 0:
				trial.Detected = time.Duration(s.Done)
			case s.Kind == "leader" && s.Leader != leader && trial.Detected > 0 && trial.NewLeader == 0:
				trial.NewLeader = time.Duration(s.Done)
			}
			if s.Kind != "" {
				continue
			}
			writes = append(writes, s)
			trial.Requests++
			if !s.Ok {
				trial.Failed++
				trial.Errors[s.Err]++
			}
		}
		if trial.NewLeader > 0 {
			trial.Election = trial.NewLeader - trial.Detected
		}
		half := int64(opts.Before) / 2
		trial.QuietGap = gaps(writes, 0, half)
		trial.Unavailable = gaps(writes, half, int64(opts.Before+opts.After))
		res.Trials = append(res.Trials, trial)
	}
	var us []time.Duration
	for _, tr := range res.Trials {
		if tr.Err == "" {
			us = append(us, tr.Unavailable)
		}
	}
	if len(us) > 0 {
		slices.Sort(us)
		res.Median, res.Min, res.Max = us[len(us)/2], us[0], us[len(us)-1]
	}
	return res, cmp.Or(ctx.Err(), runErr)
}

func steady(ctx context.Context, adm *cluster) (int, error) {
	var leader int
	err := poll(ctx, 200*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		sts := adm.statuses(ctx, time.Second)
		l := agreedOn(sts)
		if l == 0 {
			return false, nil
		}
		for _, st := range sts {
			if st.GetCommit() != sts[l-1].GetCommit() {
				return false, nil
			}
		}
		leader = l
		return true, nil
	})
	if err != nil {
		return 0, fmt.Errorf("the cluster did not settle: %w", err)
	}
	return leader, nil
}
