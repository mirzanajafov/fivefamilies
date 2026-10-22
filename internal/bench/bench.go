package bench

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

const benchProject = "ffbench"

type service struct {
	name    string
	image   string
	cpuset  string
	env     []string
	command []string
	ports   []string
}

func composeFile(services []service) string {
	var sb strings.Builder
	sb.WriteString("services:\n")
	for _, s := range services {
		fmt.Fprintf(&sb, "  %s:\n    image: %s\n    restart: \"no\"\n    cpuset: %q\n    mem_limit: 1g\n    pids_limit: 512\n", s.name, s.image, s.cpuset)
		if len(s.env) > 0 {
			fmt.Fprintf(&sb, "    environment: [%s]\n", quote(s.env))
		}
		fmt.Fprintf(&sb, "    command: [%s]\n", quote(s.command))
		if len(s.ports) > 0 {
			fmt.Fprintf(&sb, "    ports: [%s]\n", quote(s.ports))
		}
		fmt.Fprintf(&sb, "    volumes: [\"%s:/data\"]\n    networks: [bench]\n", s.name)
	}
	sb.WriteString("networks:\n  bench: {}\nvolumes:\n")
	for _, s := range services {
		fmt.Fprintf(&sb, "  %s: {}\n", s.name)
	}
	return sb.String()
}

func quote(args []string) string {
	q := make([]string, len(args))
	for i, a := range args {
		q[i] = fmt.Sprintf("%q", a)
	}
	return strings.Join(q, ", ")
}

type composeCluster struct {
	dir     string
	project *rig.Project
	gens    int
}

func (c *composeCluster) start(ctx context.Context, project string, services []service) error {
	dir, err := os.MkdirTemp("", project)
	if err != nil {
		return err
	}
	c.dir = dir
	file := filepath.Join(dir, "compose.yml")
	if err := os.WriteFile(file, []byte(composeFile(services)), 0o644); err != nil {
		return err
	}
	c.project = &rig.Project{Name: project, File: file}
	return c.project.Recreate(ctx, nil)
}

func (c *composeCluster) Down() error {
	var errs []error
	if c.project != nil {
		errs = append(errs, c.project.Down(context.Background()))
	}
	if c.dir != "" {
		errs = append(errs, os.RemoveAll(c.dir))
	}
	return errors.Join(errs...)
}

func (c *composeCluster) runGenerator(ctx context.Context, genCPUs string, command, args []string) (loadgen.Report, error) {
	c.gens++
	name := fmt.Sprintf("%s-gen-%d", c.project.Name, c.gens)
	run := append([]string{"run", "--rm", "--name", name, "--network", c.project.Name + "_bench", "--cpuset-cpus", genCPUs}, command...)
	out, err := rig.Docker(ctx, nil, append(run, args...)...)
	if err != nil {
		_, rmErr := rig.Docker(context.Background(), nil, "rm", "-f", name)
		return loadgen.Report{}, errors.Join(err, rmErr)
	}
	return decodeReport(out)
}

var errUnreadableReport = errors.New("bench: the generator's report can't be read (an image older than the harness?)")

func decodeReport(out string) (loadgen.Report, error) {
	var r loadgen.Report
	dec := json.NewDecoder(strings.NewReader(out))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return r, fmt.Errorf("%w: %w: %.200s", errUnreadableReport, err, out)
	}
	if dec.More() {
		return r, fmt.Errorf("%w: more output after the report: %.200s", errUnreadableReport, out)
	}
	return r, nil
}

type ffCluster struct {
	Nodes    int
	Flags    []string
	NodeCPUs []string
	Image    string
	Profiles bool
	adm      *rig.Admin
	composeCluster
}

func (b *ffCluster) services() []service {
	network, host := rig.NetworkAddrs(b.Nodes), rig.HostAddrs(b.Nodes)
	var cluster []string
	for i, addr := range network {
		cluster = append(cluster, fmt.Sprintf("%d=%s", i+1, addr))
	}
	var out []service
	for i := 1; i <= b.Nodes; i++ {
		s := service{
			name: fmt.Sprintf("n%d", i), image: b.Image, cpuset: b.NodeCPUs[(i-1)%len(b.NodeCPUs)],
			command: append([]string{"-id", fmt.Sprint(i), "-cluster", strings.Join(cluster, ","), "-data", "/data"}, b.Flags...),
			ports:   []string{fmt.Sprintf("%s:%d", host[i-1], rig.NodePort)},
		}
		if b.Profiles {
			s.command = append(s.command, "-pprof", fmt.Sprintf(":%d", pprofPort))
			s.ports = append(s.ports, fmt.Sprintf("%s:%d", pprofAddr(i), pprofPort))
		}
		out = append(out, s)
	}
	return out
}

func (b *ffCluster) Up(ctx context.Context) error {
	if b.Image == "" {
		b.Image = "fivefamilies-lab-node"
	}
	if err := b.start(ctx, benchProject, b.services()); err != nil {
		return err
	}
	var err error
	if b.adm, err = rig.DialAdmin(rig.HostAddrs(b.Nodes)); err != nil {
		return err
	}
	_, err = b.adm.Steady(ctx)
	return err
}

func (b *ffCluster) Down() error {
	if b.adm != nil {
		b.adm.Close()
	}
	return b.composeCluster.Down()
}

func (b *ffCluster) Generate(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error) {
	return b.runGenerator(ctx, genCPUs, []string{"--entrypoint", "ffbench", b.Image, "-nodes", strings.Join(rig.NetworkAddrs(b.Nodes), ",")}, args)
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

type sweepOptions struct {
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

type sweepable interface {
	Generate(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error)
	Quiet(ctx context.Context) error
}

func sweep(ctx context.Context, sys sweepable, c Curve, opts sweepOptions) (Curve, error) {
	stopped := false
	var broken error
	run := func(rate float64) Step {
		s := Step{Rate: rate}
		if err := sys.Quiet(ctx); err != nil {
			s.Err, stopped = err.Error(), true
			return s
		}
		args := append([]string{"-rate", fmt.Sprint(rate), "-duration", opts.Duration.String(), "-warmup", opts.Warmup.String()}, c.GenFlags...)
		r, err := sys.Generate(ctx, opts.GenCPUs, args...)
		s.Report = r
		if err != nil {
			s.Err = err.Error()
		}
		if errors.Is(err, errUnreadableReport) {
			broken, stopped = err, true
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
	for rate := opts.Start; rate <= opts.Max && going(); rate *= opts.Factor {
		if !try(rate) {
			break
		}
	}
	for rate := opts.Start / opts.Factor; lo == 0 && rate >= opts.Floor && going(); rate /= opts.Factor {
		try(rate)
	}
	for i := 0; i < opts.Refine && lo > 0 && hi > lo && going(); i++ {
		try(math.Round((lo + hi) / 2))
	}
	if lo > 0 && going() {
		s50 := run(math.Round(0.5 * lo))
		c.At50 = &s50
	}
	if c.At50 != nil && going() {
		s80 := run(math.Round(0.8 * lo))
		c.At80 = &s80
	}
	return c, broken
}

func settle(ctx context.Context, probe func(context.Context) (uint64, bool)) error {
	var last uint64
	was := false
	err := rig.Poll(ctx, 500*time.Millisecond, 2*time.Minute, func(ctx context.Context) (bool, error) {
		at, idle := probe(ctx)
		quiet := idle && was && at == last
		last, was = at, idle
		return quiet, nil
	})
	if errors.Is(err, rig.ErrGaveUp) {
		return fmt.Errorf("the cluster was still working through its backlog: %w", err)
	}
	return err
}

func (b *ffCluster) Quiet(ctx context.Context) error {
	return settle(ctx, func(ctx context.Context) (uint64, bool) {
		sts := b.adm.Progress(ctx, time.Second)
		l, _ := rig.LeaderOf(sts)
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
