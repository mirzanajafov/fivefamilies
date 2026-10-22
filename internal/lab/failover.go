package lab

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

type FailoverOptions struct {
	Election  string                           `json:"election"`
	Heartbeat string                           `json:"heartbeat"`
	Lease     string                           `json:"lease"`
	Trials    int                              `json:"trials"`
	Rate      int                              `json:"rate"`
	Timeout   time.Duration                    `json:"request_timeout_ns"`
	Cold      bool                             `json:"cold_connections"`
	Before    time.Duration                    `json:"before_ns"`
	After     time.Duration                    `json:"after_ns"`
	Project   string                           `json:"-"`
	File      string                           `json:"-"`
	Log       func(format string, args ...any) `json:"-"`
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

func gaps(samples []loadgen.FailoverSample, from, to int64) time.Duration {
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

func (d *Docker) generate(ctx context.Context, opts FailoverOptions) ([]loadgen.FailoverSample, error) {
	network := d.Name + "_lab"
	args := []string{"run", "--rm", "--network", network, "--entrypoint", "failgen", rig.LabImage,
		"-nodes", strings.Join(rig.NetworkAddrs(d.Nodes), ","),
		"-rate", fmt.Sprint(opts.Rate), "-duration", (opts.Before + opts.After).String(), "-timeout", opts.Timeout.String(), fmt.Sprintf("-cold=%v", opts.Cold)}
	out, err := rig.Docker(ctx, nil, args...)
	if err != nil {
		return nil, fmt.Errorf("generator: %w", err)
	}
	var samples []loadgen.FailoverSample
	dec := json.NewDecoder(strings.NewReader(out))
	for dec.More() {
		var s loadgen.FailoverSample
		if err := dec.Decode(&s); err != nil {
			return nil, err
		}
		samples = append(samples, s)
	}
	return samples, nil
}

func Failover(ctx context.Context, opts FailoverOptions, out string) (FailoverResult, error) {
	res, err := runFailover(ctx, opts)
	if err = cmp.Or(ctx.Err(), err); err != nil {
		res.Err = err.Error()
	}
	if out != "" {
		if serr := rig.SaveJSON(out, fmt.Sprintf("election-%s.json", opts.Election), res); serr != nil {
			return res, errors.Join(err, serr)
		}
	}
	return res, err
}

func runFailover(ctx context.Context, opts FailoverOptions) (FailoverResult, error) {
	res := FailoverResult{Options: opts}
	env := []string{"FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=" + opts.Lease, "FF_BUG="}
	c, err := startLab(ctx, opts.Project, opts.File, env)
	if err != nil {
		return res, err
	}
	defer c.stop(opts.Log)
	var runErr error
	for t := 0; t < opts.Trials && ctx.Err() == nil; t++ {
		leader, err := c.adm.Steady(ctx)
		if err != nil {
			runErr = err
			break
		}
		res.Trials = append(res.Trials, failoverTrial(ctx, c.d, opts, leader))
	}
	var us []time.Duration
	for _, tr := range res.Trials {
		if tr.Err == "" {
			us = append(us, tr.Unavailable)
		}
	}
	if len(us) > 0 {
		slices.Sort(us)
		res.Median, res.Min, res.Max = loadgen.Quantile(us, 0.5), us[0], us[len(us)-1]
	}
	return res, runErr
}

func failoverTrial(ctx context.Context, d *Docker, opts FailoverOptions, leader int) Trial {
	type genResult struct {
		samples []loadgen.FailoverSample
		err     error
	}
	ch := make(chan genResult, 1)
	go func() {
		s, err := d.generate(ctx, opts)
		ch <- genResult{s, err}
	}()
	rig.Wait(ctx, opts.Before)
	killErr := d.Kill(ctx, leader)
	g := <-ch
	startErr := d.Start(ctx, leader)
	trial := measureTrial(g.samples, leader, opts)
	if err := cmp.Or(g.err, killErr, startErr); err != nil {
		trial.Err = err.Error()
	}
	return trial
}

func measureTrial(samples []loadgen.FailoverSample, leader int, opts FailoverOptions) Trial {
	trial := Trial{Killed: leader, Errors: map[string]int{}}
	var writes []loadgen.FailoverSample
	for _, s := range samples {
		switch {
		case s.Kind == loadgen.SampleUnreachable && s.Node == leader && trial.Detected == 0:
			trial.Detected = time.Duration(s.Done)
		case s.Kind == loadgen.SampleLeader && s.Leader != leader && trial.Detected > 0 && trial.NewLeader == 0:
			trial.NewLeader = time.Duration(s.Done)
		}
		if s.Kind != loadgen.SampleWrite {
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
	return trial
}
