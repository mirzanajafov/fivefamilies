package lab

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
	"github.com/mirzanajafov/fivefamilies/client"
)

type RegistryOptions struct {
	Election  string                           `json:"election"`
	Heartbeat string                           `json:"heartbeat"`
	TTL       time.Duration                    `json:"ttl_ns"`
	Trials    int                              `json:"trials"`
	Seed      uint64                           `json:"seed"`
	Project   string                           `json:"-"`
	File      string                           `json:"-"`
	Log       func(format string, args ...any) `json:"-"`
}

type RegistryTrial struct {
	Variant  string        `json:"variant"`
	Instance string        `json:"instance"`
	Leader   int           `json:"leader_killed,omitempty"`
	Held     time.Duration `json:"held_ns"`
	Gone     time.Duration `json:"gone_ns"`
	Err      string        `json:"error,omitempty"`
}

type RegistryVariant struct {
	Median time.Duration `json:"median_ns"`
	Min    time.Duration `json:"min_ns"`
	Max    time.Duration `json:"max_ns"`
	N      int           `json:"n"`
}

type RegistryResult struct {
	Options  RegistryOptions            `json:"options"`
	Trials   []RegistryTrial            `json:"trials"`
	Variants map[string]RegistryVariant `json:"variants"`
	Err      string                     `json:"error,omitempty"`
}

type sighting struct {
	mu   sync.Mutex
	seen map[string]time.Time
	gone map[string]time.Time
}

func (s *sighting) follow(ch <-chan []client.Instance) {
	for list := range ch {
		now := time.Now()
		s.mu.Lock()
		present := map[string]bool{}
		for _, in := range list {
			present[in.Name] = true
			if _, ok := s.seen[in.Name]; !ok {
				s.seen[in.Name] = now
			}
		}
		for name := range s.seen {
			if _, done := s.gone[name]; !present[name] && !done {
				s.gone[name] = now
			}
		}
		s.mu.Unlock()
	}
}

func (s *sighting) when(name string, gone bool) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.seen
	if gone {
		m = s.gone
	}
	t, ok := m[name]
	return t, ok
}

func (s *sighting) await(ctx context.Context, name string, gone bool, limit time.Duration) (time.Time, error) {
	var at time.Time
	err := Poll(ctx, 5*time.Millisecond, limit, func(context.Context) (bool, error) {
		t, ok := s.when(name, gone)
		at = t
		return ok, nil
	})
	if err != nil {
		verb := "appear"
		if gone {
			verb = "disappear"
		}
		return time.Time{}, fmt.Errorf("%s did not %s: %w", name, verb, err)
	}
	return at, nil
}

func Registry(ctx context.Context, opts RegistryOptions, out string) (RegistryResult, error) {
	res, err := runRegistry(ctx, opts)
	if err = cmp.Or(ctx.Err(), err); err != nil {
		res.Err = err.Error()
	}
	if out != "" {
		if serr := SaveJSON(out, "registry.json", res); serr != nil {
			return res, errors.Join(err, serr)
		}
	}
	return res, err
}

func runRegistry(ctx context.Context, opts RegistryOptions) (RegistryResult, error) {
	res := RegistryResult{Options: opts, Variants: map[string]RegistryVariant{}}
	env := []string{"FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=0s", "FF_BUG="}
	c, err := startLab(ctx, opts.Project, opts.File, env)
	if err != nil {
		return res, err
	}
	defer c.stop(opts.Log, false)
	if _, err := Steady(ctx, c.adm); err != nil {
		return res, err
	}
	cl, err := client.New(c.addrs, client.Options{})
	if err != nil {
		return res, err
	}
	defer cl.Close()
	dctx, stopDiscover := context.WithCancel(ctx)
	defer stopDiscover()
	seen, err := cl.Discover(dctx, "bench")
	if err != nil {
		return res, err
	}
	sg := &sighting{seen: map[string]time.Time{}, gone: map[string]time.Time{}}
	go sg.follow(seen)
	r := &registryRun{d: c.d, sg: sg, opts: opts, rng: rand.New(rand.NewPCG(opts.Seed, 11))}
	var runErr error
	for t := 0; t < 2*opts.Trials && ctx.Err() == nil; t++ {
		leader, err := Steady(ctx, c.adm)
		if err != nil {
			runErr = err
			break
		}
		var tr RegistryTrial
		if tr, runErr = r.trial(ctx, t, leader); runErr != nil {
			break
		}
		res.Trials = append(res.Trials, tr)
	}
	byVariant := map[string][]time.Duration{}
	for _, tr := range res.Trials {
		if tr.Err == "" {
			byVariant[tr.Variant] = append(byVariant[tr.Variant], tr.Gone)
		}
	}
	for v, gs := range byVariant {
		slices.Sort(gs)
		res.Variants[v] = RegistryVariant{Median: loadgen.Quantile(gs, 0.5), Min: gs[0], Max: gs[len(gs)-1], N: len(gs)}
	}
	return res, runErr
}

type registryRun struct {
	d    *Docker
	sg   *sighting
	opts RegistryOptions
	rng  *rand.Rand
}

func (r *registryRun) trial(ctx context.Context, t, leader int) (RegistryTrial, error) {
	variant := "instance"
	if t%2 == 1 {
		variant = "instance+leader"
	}
	tr := RegistryTrial{Variant: variant, Instance: fmt.Sprintf("i%d", t)}
	var inside []string
	for i := 1; i <= r.d.Nodes; i++ {
		inside = append(inside, fmt.Sprintf("n%d:3161", i))
	}
	name := fmt.Sprintf("%s-reg-%d", r.d.Project, t)
	_, runErr := r.d.Cmd(ctx, nil, "run", "-d", "--name", name, "--network", r.d.Project+"_lab", "--entrypoint", "ffctl", "fivefamilies-lab-node",
		"-endpoints", strings.Join(inside, ","), "register", "bench", tr.Instance, "10.9.9.9:80", r.opts.TTL.String())
	if runErr == nil {
		if err := r.killAndTime(ctx, &tr, name, leader); err != nil {
			tr.Err = err.Error()
		}
	}
	if _, err := r.d.Cmd(context.Background(), nil, "rm", "-f", name); err != nil && tr.Err == "" {
		tr.Err = err.Error()
	}
	if tr.Leader > 0 {
		if err := r.d.Start(context.Background(), tr.Leader); err != nil && tr.Err == "" {
			tr.Err = err.Error()
		}
	}
	return tr, runErr
}

func (r *registryRun) killAndTime(ctx context.Context, tr *RegistryTrial, name string, leader int) error {
	appeared, err := r.sg.await(ctx, tr.Instance, false, 20*time.Second)
	if err != nil {
		return err
	}
	tr.Held = time.Duration(r.rng.Int64N(int64(r.opts.TTL))) + r.opts.TTL
	Wait(ctx, time.Until(appeared.Add(tr.Held)))
	killed := time.Now()
	var instanceErr, leaderErr error
	var wg sync.WaitGroup
	wg.Go(func() { _, instanceErr = r.d.Cmd(context.Background(), nil, "kill", name) })
	if tr.Variant == "instance+leader" {
		tr.Leader = leader
		wg.Go(func() { leaderErr = r.d.Kill(context.Background(), leader) })
	}
	wg.Wait()
	if err := errors.Join(instanceErr, leaderErr); err != nil {
		return err
	}
	gone, err := r.sg.await(ctx, tr.Instance, true, 10*r.opts.TTL+30*time.Second)
	if err != nil {
		return err
	}
	tr.Gone = gone.Sub(killed)
	return nil
}
