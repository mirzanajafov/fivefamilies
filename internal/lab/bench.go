package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

const benchProject = "ffbench"

type BenchCluster struct {
	Nodes    int
	Flags    []string
	NodeCPUs []string
	Image    string
	Profiles bool
	dir      string
	d        *Docker
	gens     int
}

func (b *BenchCluster) addrs() []string {
	var out []string
	for i := 1; i <= b.Nodes; i++ {
		out = append(out, fmt.Sprintf("127.0.0.1:315%d", i))
	}
	return out
}

func (b *BenchCluster) inside() string {
	var out []string
	for i := 1; i <= b.Nodes; i++ {
		out = append(out, fmt.Sprintf("n%d:3161", i))
	}
	return strings.Join(out, ",")
}

func quote(args []string) string {
	q := make([]string, len(args))
	for i, a := range args {
		q[i] = fmt.Sprintf("%q", a)
	}
	return strings.Join(q, ", ")
}

func (b *BenchCluster) compose() string {
	var sb strings.Builder
	var cluster []string
	for i := 1; i <= b.Nodes; i++ {
		cluster = append(cluster, fmt.Sprintf("%d=n%d:3161", i, i))
	}
	sb.WriteString("services:\n")
	for i := 1; i <= b.Nodes; i++ {
		args := append([]string{"-id", fmt.Sprint(i), "-cluster", strings.Join(cluster, ","), "-data", "/data"}, b.Flags...)
		ports := fmt.Sprintf("\"127.0.0.1:315%d:3161\"", i)
		if b.Profiles {
			args = append(args, "-pprof", ":6060")
			ports += fmt.Sprintf(", \"127.0.0.1:316%d:6060\"", i)
		}
		fmt.Fprintf(&sb, "  n%d:\n    image: %s\n    restart: \"no\"\n    cpuset: %q\n    mem_limit: 1g\n    pids_limit: 512\n", i, b.Image, b.NodeCPUs[(i-1)%len(b.NodeCPUs)])
		fmt.Fprintf(&sb, "    command: [%s]\n    ports: [%s]\n    volumes: [\"n%d:/data\"]\n    networks: [bench]\n", quote(args), ports, i)
	}
	sb.WriteString("networks:\n  bench: {}\nvolumes:\n")
	for i := 1; i <= b.Nodes; i++ {
		fmt.Fprintf(&sb, "  n%d: {}\n", i)
	}
	return sb.String()
}

func (b *BenchCluster) Up(ctx context.Context) error {
	if b.Image == "" {
		b.Image = "fivefamilies-lab-node"
	}
	dir, err := os.MkdirTemp("", "ffbench")
	if err != nil {
		return err
	}
	b.dir = dir
	file := filepath.Join(dir, "compose.yml")
	if err := os.WriteFile(file, []byte(b.compose()), 0o644); err != nil {
		return err
	}
	b.d = &Docker{Project: benchProject, File: file, Nodes: b.Nodes}
	if err := b.d.compose(ctx, nil, "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	if err := b.d.compose(ctx, nil, "up", "-d"); err != nil {
		return err
	}
	adm, err := dialAdmin(b.addrs())
	if err != nil {
		return err
	}
	defer adm.close()
	_, err = steady(ctx, adm)
	return err
}

func (b *BenchCluster) Down() {
	if b.d != nil {
		b.d.compose(context.Background(), nil, "down", "-v", "--remove-orphans")
	}
	if b.dir != "" {
		os.RemoveAll(b.dir)
	}
}

func (b *BenchCluster) Generate(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error) {
	b.gens++
	name := fmt.Sprintf("%s-gen-%d", benchProject, b.gens)
	full := append([]string{"run", "--rm", "--name", name, "--network", benchProject + "_bench", "--cpuset-cpus", genCPUs, "--entrypoint", "ffbench", b.Image, "-nodes", b.inside()}, args...)
	out, err := b.d.cmd(ctx, nil, full...)
	var r loadgen.Report
	if err != nil {
		b.d.cmd(context.Background(), nil, "rm", "-f", name)
		return r, err
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		return r, fmt.Errorf("generator output: %w: %.200s", err, out)
	}
	return r, nil
}

type Step struct {
	Rate   float64        `json:"rate"`
	Report loadgen.Report `json:"report"`
	Err    string         `json:"error,omitempty"`
}

type Curve struct {
	Name      string   `json:"name"`
	Nodes     int      `json:"nodes"`
	NodeFlags []string `json:"node_flags"`
	GenFlags  []string `json:"generator_flags"`
	Steps     []Step   `json:"steps"`
	Round     int      `json:"round,omitempty"`
	Knee      float64  `json:"knee_per_s"`
	At50      *Step    `json:"at_50_percent,omitempty"`
	At80      *Step    `json:"at_80_percent,omitempty"`
}

type SweepOptions struct {
	Start    float64
	Floor    float64
	Factor   float64
	Max      float64
	Refine   int
	Duration time.Duration
	Warmup   time.Duration
	GenCPUs  string
	MaxP99   time.Duration
}

func saturated(s Step, maxP99 time.Duration) bool {
	sum := s.Report.Summary
	return s.Err != "" || !sum.Valid || sum.Achieved < 0.9*s.Rate || sum.All.P99 > maxP99 || float64(sum.All.Error) > 0.01*float64(sum.Scheduled)
}

type generator func(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error)

type quieter func(ctx context.Context) error

func (b *BenchCluster) Sweep(ctx context.Context, name string, opts SweepOptions, genFlags []string) Curve {
	return sweep(ctx, b.Generate, b.Quiet, Curve{Name: name, Nodes: b.Nodes, NodeFlags: b.Flags, GenFlags: genFlags}, opts)
}

func sweep(ctx context.Context, gen generator, quiet quieter, c Curve, opts SweepOptions) Curve {
	stopped := false
	run := func(rate float64) Step {
		s := Step{Rate: rate}
		if err := quiet(ctx); err != nil {
			s.Err, stopped = err.Error(), true
			return s
		}
		args := append([]string{"-rate", fmt.Sprint(rate), "-duration", opts.Duration.String(), "-warmup", opts.Warmup.String()}, c.GenFlags...)
		r, err := gen(ctx, opts.GenCPUs, args...)
		s.Report = r
		if err != nil {
			s.Err = err.Error()
		}
		return s
	}
	going := func() bool { return !stopped && ctx.Err() == nil }
	var lo, hi float64
	try := func(rate float64) bool {
		s := run(rate)
		c.Steps = append(c.Steps, s)
		if saturated(s, opts.MaxP99) {
			hi = rate
			return false
		}
		lo, c.Knee = rate, s.Report.Summary.Achieved
		return true
	}
	for rate := opts.Start; rate <= opts.Max && going() && try(rate); rate *= opts.Factor {
	}
	for rate := opts.Start / opts.Factor; lo == 0 && rate >= opts.Floor && going(); rate /= opts.Factor {
		try(rate)
	}
	for i := 0; i < opts.Refine && lo > 0 && hi > lo && going(); i++ {
		try(math.Round((lo + hi) / 2))
	}
	if lo > 0 && going() {
		s50 := run(math.Round(0.5 * lo))
		s80 := run(math.Round(0.8 * lo))
		c.At50, c.At80 = &s50, &s80
	}
	return c
}

func settle(ctx context.Context, probe func(context.Context) (uint64, bool)) error {
	deadline := time.Now().Add(2 * time.Minute)
	var last uint64
	was := false
	for time.Now().Before(deadline) && ctx.Err() == nil {
		at, idle := probe(ctx)
		if idle && was && at == last {
			return nil
		}
		last, was = at, idle
		wait(ctx, 500*time.Millisecond)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return fmt.Errorf("the cluster was still working through its backlog after 2 minutes")
}

func (b *BenchCluster) Quiet(ctx context.Context) error {
	adm, err := dialAdmin(b.addrs())
	if err != nil {
		return err
	}
	defer adm.close()
	return settle(ctx, func(ctx context.Context) (uint64, bool) {
		sts := adm.progress(ctx, time.Second)
		l, _ := leaderOf(sts)
		if l == 0 {
			return 0, false
		}
		commit := sts[l-1].GetCommit()
		if sts[l-1].GetLastIndex() != commit {
			return commit, false
		}
		for _, st := range sts {
			if st == nil || st.GetApplied() != commit {
				return commit, false
			}
		}
		return commit, true
	})
}

func SaveJSON(dir, name string, v any) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, name), append(b, '\n'), 0o644)
}
