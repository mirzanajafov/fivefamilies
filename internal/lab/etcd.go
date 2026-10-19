package lab

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mirzanajafov/fivefamilies/bench/loadgen"
)

const (
	EtcdImage    = "gcr.io/etcd-development/etcd:v3.6.15"
	EtcdGenImage = "fivefamilies-etcdgen"
	etcdProject  = "ffetcd"
)

type EtcdCluster struct {
	Nodes    int
	NodeCPUs []string
	Procs    int
	dir      string
	d        *Docker
	gens     int
}

func (e *EtcdCluster) endpoints() []string {
	var out []string
	for i := 1; i <= e.Nodes; i++ {
		out = append(out, fmt.Sprintf("e%d:2379", i))
	}
	return out
}

func (e *EtcdCluster) compose() string {
	var initial []string
	for i := 1; i <= e.Nodes; i++ {
		initial = append(initial, fmt.Sprintf("e%d=http://e%d:2380", i, i))
	}
	var sb strings.Builder
	sb.WriteString("services:\n")
	for i := 1; i <= e.Nodes; i++ {
		args := []string{"/usr/local/bin/etcd", "--name", fmt.Sprintf("e%d", i), "--data-dir", "/data",
			"--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", fmt.Sprintf("http://e%d:2379", i),
			"--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", fmt.Sprintf("http://e%d:2380", i),
			"--initial-cluster", strings.Join(initial, ","), "--initial-cluster-state", "new",
			"--heartbeat-interval", "100", "--election-timeout", "1000", "--logger", "zap", "--log-level", "warn"}
		fmt.Fprintf(&sb, "  e%d:\n    image: %s\n    restart: \"no\"\n    cpuset: %q\n    mem_limit: 1g\n    pids_limit: 512\n", i, EtcdImage, e.NodeCPUs[(i-1)%len(e.NodeCPUs)])
		if e.Procs > 0 {
			fmt.Fprintf(&sb, "    environment: [\"GOMAXPROCS=%d\"]\n", e.Procs)
		}
		fmt.Fprintf(&sb, "    command: [%s]\n    volumes: [\"e%d:/data\"]\n    networks: [bench]\n", quote(args), i)
	}
	sb.WriteString("networks:\n  bench: {}\nvolumes:\n")
	for i := 1; i <= e.Nodes; i++ {
		fmt.Fprintf(&sb, "  e%d: {}\n", i)
	}
	return sb.String()
}

func (e *EtcdCluster) Up(ctx context.Context) error {
	dir, err := os.MkdirTemp("", "ffetcd")
	if err != nil {
		return err
	}
	e.dir = dir
	file := filepath.Join(dir, "compose.yml")
	if err := os.WriteFile(file, []byte(e.compose()), 0o644); err != nil {
		return err
	}
	e.d = &Docker{Project: etcdProject, File: file, Nodes: e.Nodes}
	if err := e.d.compose(ctx, nil, "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	if err := e.d.compose(ctx, nil, "up", "-d"); err != nil {
		return err
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := e.Leader(ctx); err == nil {
			return nil
		}
		wait(ctx, 500*time.Millisecond)
	}
	return fmt.Errorf("etcd did not elect a leader within 60 s")
}

type etcdStatus struct {
	Endpoint string `json:"Endpoint"`
	Status   struct {
		Header struct {
			MemberID json.Number `json:"member_id"`
		} `json:"header"`
		Leader       json.Number `json:"leader"`
		Index        uint64      `json:"raftIndex"`
		AppliedIndex uint64      `json:"raftAppliedIndex"`
	} `json:"Status"`
}

func (e *EtcdCluster) statuses(ctx context.Context) ([]etcdStatus, error) {
	out, err := e.d.cmd(ctx, nil, "exec", e.d.container("e1"), "etcdctl", "--endpoints", strings.Join(e.endpoints(), ","), "endpoint", "status", "-w", "json")
	if err != nil {
		return nil, err
	}
	var sts []etcdStatus
	if err := json.Unmarshal([]byte(out), &sts); err != nil {
		return nil, err
	}
	return sts, nil
}

func (e *EtcdCluster) Quiet(ctx context.Context) error {
	return settle(ctx, func(ctx context.Context) (uint64, bool) {
		sts, err := e.statuses(ctx)
		if err != nil || len(sts) != e.Nodes {
			return 0, false
		}
		var top uint64
		for _, st := range sts {
			top = max(top, st.Status.Index)
		}
		for _, st := range sts {
			if st.Status.AppliedIndex != top {
				return top, false
			}
		}
		return top, true
	})
}

func (e *EtcdCluster) Leader(ctx context.Context) (string, error) {
	sts, err := e.statuses(ctx)
	if err != nil {
		return "", err
	}
	for _, st := range sts {
		if st.Status.Leader != "" && st.Status.Leader == st.Status.Header.MemberID {
			return strings.TrimPrefix(st.Endpoint, "http://"), nil
		}
	}
	return "", fmt.Errorf("no member reports itself as leader")
}

func (e *EtcdCluster) Down() {
	if e.d != nil {
		e.d.compose(context.Background(), nil, "down", "-v", "--remove-orphans")
	}
	if e.dir != "" {
		os.RemoveAll(e.dir)
	}
}

func (e *EtcdCluster) Generate(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error) {
	e.gens++
	name := fmt.Sprintf("%s-gen-%d", etcdProject, e.gens)
	full := append([]string{"run", "--rm", "--name", name, "--network", etcdProject + "_bench", "--cpuset-cpus", genCPUs, EtcdGenImage,
		"-endpoints", strings.Join(e.endpoints(), ",")}, args...)
	out, err := e.d.cmd(ctx, nil, full...)
	var r loadgen.Report
	if err != nil {
		e.d.cmd(context.Background(), nil, "rm", "-f", name)
		return r, err
	}
	if err := json.Unmarshal([]byte(out), &r); err != nil {
		return r, fmt.Errorf("etcd generator output: %w: %.200s", err, out)
	}
	return r, nil
}

func E5(ctx context.Context, o ExperimentOptions) ([]Curve, error) {
	type spec struct {
		name   string
		etcd   bool
		nodes  int
		gen    []string
		node   []string
		leader bool
		procs  int
	}
	writes := []string{"-reads", "0", "-value-bytes", "128"}
	specs := []spec{
		{"fivefamilies, 3 nodes, writes", false, 3, writes, withBase(), false, 0},
		{"etcd, 3 nodes, writes", true, 3, writes, nil, false, 0},
		{"fivefamilies, 5 nodes, writes", false, 5, writes, withBase(), false, 0},
		{"etcd, 5 nodes, writes", true, 5, writes, nil, false, 0},
		{"etcd, 5 nodes, writes, client on the leader only", true, 5, writes, nil, true, 0},
		{"etcd, 5 nodes, writes, two Go processors", true, 5, writes, nil, false, 2},
		{"fivefamilies, 5 nodes, ReadIndex reads 0.95", false, 5, []string{"-reads", "0.95", "-read-mode", "index"}, withBase(), false, 0},
		{"etcd, 5 nodes, linearizable reads 0.95", true, 5, []string{"-reads", "0.95", "-read-mode", "linearizable"}, nil, false, 0},
		{"fivefamilies, 5 nodes, local reads (unsafe) 0.95", false, 5, []string{"-reads", "0.95", "-read-mode", "follower"}, withBase("-bug", "followerread"), false, 0},
		{"etcd, 5 nodes, serializable reads 0.95", true, 5, []string{"-reads", "0.95", "-read-mode", "serializable"}, nil, false, 0},
	}
	if o.Quick {
		specs = specs[1:2]
	}
	var curves []Curve
	for round, sp := range rounds(o, specs, func(sp spec) string { return sp.name }) {
		o.Log("%s, round %d: starting", sp.name, round)
		var gen generator
		var quiet quieter
		var down func()
		c := Curve{Name: sp.name, Nodes: sp.nodes, NodeFlags: sp.node, GenFlags: sp.gen}
		if sp.etcd {
			ec := &EtcdCluster{Nodes: sp.nodes, NodeCPUs: o.NodeCPUs, Procs: sp.procs}
			if err := ec.Up(ctx); err != nil {
				ec.Down()
				return curves, fmt.Errorf("%s: %w", sp.name, err)
			}
			gen, quiet, down = ec.Generate, ec.Quiet, ec.Down
			if sp.leader {
				l, err := ec.Leader(ctx)
				if err != nil {
					ec.Down()
					return curves, err
				}
				c.GenFlags = append([]string{"-only", l}, sp.gen...)
			}
		} else {
			b := &BenchCluster{Nodes: sp.nodes, Flags: sp.node, NodeCPUs: o.NodeCPUs}
			if err := b.Up(ctx); err != nil {
				b.Down()
				return curves, fmt.Errorf("%s: %w", sp.name, err)
			}
			gen, quiet, down = b.Generate, b.Quiet, b.Down
		}
		c = sweep(ctx, gen, quiet, c, o.sweep())
		c.Round = round
		down()
		o.Log("%s, round %d: knee %.0f/s over %d steps", sp.name, round, c.Knee, len(c.Steps))
		curves = append(curves, c)
		if err := SaveJSON(o.Out, "e5.json", curves); err != nil {
			return curves, err
		}
		if ctx.Err() != nil {
			return curves, ctx.Err()
		}
	}
	return curves, nil
}
