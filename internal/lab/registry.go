package lab

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
)

type RegistryOptions struct {
	Election  string        `json:"election"`
	Heartbeat string        `json:"heartbeat"`
	TTL       time.Duration `json:"ttl_ns"`
	Trials    int           `json:"trials"`
	Seed      uint64        `json:"seed"`
	Project   string        `json:"-"`
	File      string        `json:"-"`
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
	err := poll(ctx, 5*time.Millisecond, limit, func(context.Context) (bool, error) {
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
	res := RegistryResult{Options: opts, Variants: map[string]RegistryVariant{}}
	d := &Docker{Project: opts.Project, File: opts.File, Nodes: 5}
	env := []string{"FF_ELECTION=" + opts.Election, "FF_HEARTBEAT=" + opts.Heartbeat, "FF_LEASE=0s", "FF_BUG="}
	if err := d.Up(ctx, env); err != nil {
		return res, err
	}
	defer d.Down(context.Background())
	var addrs, inside []string
	for i := 1; i <= d.Nodes; i++ {
		addrs = append(addrs, fmt.Sprintf("127.0.0.1:315%d", i))
		inside = append(inside, fmt.Sprintf("n%d:3161", i))
	}
	adm, err := dialAdmin(addrs)
	if err != nil {
		return res, err
	}
	defer adm.close()
	if _, err := steady(ctx, adm); err != nil {
		return res, err
	}
	cl, err := client.New(addrs, client.Options{})
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
	rng := rand.New(rand.NewPCG(opts.Seed, 11))
	network := d.Project + "_lab"
	var runErr error
	for t := 0; t < 2*opts.Trials && ctx.Err() == nil; t++ {
		variant := "instance"
		if t%2 == 1 {
			variant = "instance+leader"
		}
		leader, err := steady(ctx, adm)
		if err != nil {
			runErr = err
			break
		}
		tr := RegistryTrial{Variant: variant, Instance: fmt.Sprintf("i%d", t)}
		name := fmt.Sprintf("%s-reg-%d", d.Project, t)
		if _, err := d.cmd(ctx, nil, "run", "-d", "--name", name, "--network", network, "--entrypoint", "ffctl", "fivefamilies-lab-node",
			"-endpoints", strings.Join(inside, ","), "register", "bench", tr.Instance, "10.9.9.9:80", opts.TTL.String()); err != nil {
			runErr = err
			break
		}
		appeared, err := sg.await(ctx, tr.Instance, false, 20*time.Second)
		if err != nil {
			tr.Err = err.Error()
		} else {
			tr.Held = time.Duration(rng.Int64N(int64(opts.TTL))) + opts.TTL
			wait(ctx, time.Until(appeared.Add(tr.Held)))
			killed := time.Now()
			var wg sync.WaitGroup
			wg.Go(func() { d.cmd(context.Background(), nil, "kill", name) })
			if variant == "instance+leader" {
				tr.Leader = leader
				wg.Go(func() { d.Kill(context.Background(), leader) })
			}
			wg.Wait()
			if gone, err := sg.await(ctx, tr.Instance, true, 10*opts.TTL+30*time.Second); err != nil {
				tr.Err = err.Error()
			} else {
				tr.Gone = gone.Sub(killed)
			}
		}
		d.cmd(context.Background(), nil, "rm", "-f", name)
		if tr.Leader > 0 {
			d.Start(context.Background(), tr.Leader)
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
		res.Variants[v] = RegistryVariant{Median: gs[len(gs)/2], Min: gs[0], Max: gs[len(gs)-1], N: len(gs)}
	}
	if out != "" {
		if err := SaveJSON(out, "registry.json", res); err != nil {
			return res, errors.Join(runErr, err)
		}
	}
	return res, runErr
}
