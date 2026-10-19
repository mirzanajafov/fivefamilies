package lab

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

type ProfileOptions struct {
	Nodes       int
	Flags       []string
	GenFlags    []string
	Rate        float64
	Seconds     int
	Trace       time.Duration
	Overload    float64
	OverloadFor time.Duration
	NodeCPUs    []string
	GenCPUs     string
	Out         string
	Log         func(format string, args ...any)
}

type ProfilePhase struct {
	Name     string         `json:"name"`
	Rate     float64        `json:"rate"`
	Leader   int            `json:"leader"`
	Follower int            `json:"follower"`
	Report   loadgen.Report `json:"report"`
	Err      string         `json:"error,omitempty"`
	Files    []string       `json:"files,omitempty"`
}

type ProfileReport struct {
	Nodes    int            `json:"nodes"`
	Flags    []string       `json:"node_flags"`
	GenFlags []string       `json:"generator_flags"`
	Phases   []ProfilePhase `json:"phases"`
}

func profileURL(node int, path string) string {
	return fmt.Sprintf("http://127.0.0.1:316%d/debug/pprof/%s", node, path)
}

func fetchProfile(ctx context.Context, url, file string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (b *BenchCluster) leader(ctx context.Context) (int, error) {
	adm, err := dialAdmin(b.addrs())
	if err != nil {
		return 0, err
	}
	defer adm.close()
	return steady(ctx, adm)
}

func (b *BenchCluster) phase(ctx context.Context, o ProfileOptions, name string, rate float64, seconds int) ProfilePhase {
	p := ProfilePhase{Name: name, Rate: rate}
	if err := b.Quiet(ctx); err != nil {
		p.Err = err.Error()
		return p
	}
	l, err := b.leader(ctx)
	if err != nil {
		p.Err = err.Error()
		return p
	}
	p.Leader, p.Follower = l, l%b.Nodes+1
	args := append([]string{"-rate", fmt.Sprint(rate), "-duration", fmt.Sprintf("%ds", seconds+4), "-warmup", "3s"}, o.GenFlags...)
	var wg sync.WaitGroup
	wg.Go(func() {
		r, err := b.Generate(ctx, o.GenCPUs, args...)
		p.Report = r
		if err != nil {
			p.Err = err.Error()
		}
	})
	if err := collect(ctx, o, &p, min(o.Seconds, seconds)); err != nil {
		p.Err = err.Error()
	}
	wg.Wait()
	return p
}

func collect(ctx context.Context, o ProfileOptions, p *ProfilePhase, seconds int) error {
	raw := filepath.Join(o.Out, "raw")
	if err := os.MkdirAll(raw, 0o755); err != nil {
		return err
	}
	if !wait(ctx, 4*time.Second) {
		return ctx.Err()
	}
	var mu sync.Mutex
	var fetches sync.WaitGroup
	get := func(node int, what, path string) {
		fetches.Go(func() {
			ext := ".pprof"
			if what == "trace" {
				ext = ".trace"
			}
			file := filepath.Join(raw, fmt.Sprintf("%s-%s-n%d%s", p.Name, what, node, ext))
			if err := fetchProfile(ctx, profileURL(node, path), file); err != nil {
				o.Log("%s: %s on n%d: %v", p.Name, what, node, err)
				return
			}
			mu.Lock()
			p.Files = append(p.Files, file)
			mu.Unlock()
		})
	}
	for _, n := range []int{p.Leader, p.Follower} {
		get(n, "cpu", fmt.Sprintf("profile?seconds=%d", seconds))
		if o.Trace > 0 {
			get(n, "trace", fmt.Sprintf("trace?seconds=%d", int(o.Trace/time.Second)))
		}
	}
	if wait(ctx, time.Duration(seconds)*time.Second/2) {
		get(p.Leader, "goroutine", "goroutine")
	}
	fetches.Wait()
	for _, n := range []int{p.Leader, p.Follower} {
		get(n, "mutex", "mutex")
		get(n, "block", "block")
		get(n, "heap", "heap")
	}
	fetches.Wait()
	slices.Sort(p.Files)
	return nil
}

func Profile(ctx context.Context, o ProfileOptions) (ProfileReport, error) {
	rep := ProfileReport{Nodes: o.Nodes, Flags: o.Flags, GenFlags: o.GenFlags}
	b := &BenchCluster{Nodes: o.Nodes, Flags: o.Flags, NodeCPUs: o.NodeCPUs, Profiles: true}
	o.Log("starting %d nodes", o.Nodes)
	if err := b.Up(ctx); err != nil {
		b.Down()
		return rep, err
	}
	defer b.Down()
	type step struct {
		name    string
		rate    float64
		seconds int
	}
	plan := []step{{"before", o.Rate, o.Seconds}}
	if o.Overload > 0 {
		plan = append(plan, step{"overload", o.Overload, int(o.OverloadFor / time.Second)}, step{"after", o.Rate, o.Seconds})
	}
	for _, step := range plan {
		o.Log("%s: %.0f/s", step.name, step.rate)
		p := b.phase(ctx, o, step.name, step.rate, step.seconds)
		s := p.Report.Summary
		o.Log("%s: achieved %.0f/s, p50 %v, p99 %v, %d errors, leader n%d %s", step.name, s.Achieved, s.All.P50, s.All.P99, s.All.Error, p.Leader, p.Err)
		rep.Phases = append(rep.Phases, p)
		if err := SaveJSON(o.Out, "profile.json", rep); err != nil {
			return rep, err
		}
		if ctx.Err() != nil {
			return rep, ctx.Err()
		}
	}
	return rep, nil
}
