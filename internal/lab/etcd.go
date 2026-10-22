package lab

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	composeCluster
}

func (e *EtcdCluster) endpoints() []string {
	var out []string
	for i := 1; i <= e.Nodes; i++ {
		out = append(out, fmt.Sprintf("e%d:2379", i))
	}
	return out
}

func (e *EtcdCluster) services() []service {
	var initial []string
	for i := 1; i <= e.Nodes; i++ {
		initial = append(initial, fmt.Sprintf("e%d=http://e%d:2380", i, i))
	}
	var out []service
	for i := 1; i <= e.Nodes; i++ {
		s := service{
			name: fmt.Sprintf("e%d", i), image: EtcdImage, cpuset: e.NodeCPUs[(i-1)%len(e.NodeCPUs)],
			command: []string{"/usr/local/bin/etcd", "--name", fmt.Sprintf("e%d", i), "--data-dir", "/data",
				"--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", fmt.Sprintf("http://e%d:2379", i),
				"--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", fmt.Sprintf("http://e%d:2380", i),
				"--initial-cluster", strings.Join(initial, ","), "--initial-cluster-state", "new",
				"--heartbeat-interval", "100", "--election-timeout", "1000", "--logger", "zap", "--log-level", "warn"},
		}
		if e.Procs > 0 {
			s.env = []string{fmt.Sprintf("GOMAXPROCS=%d", e.Procs)}
		}
		out = append(out, s)
	}
	return out
}

func (e *EtcdCluster) Up(ctx context.Context) error {
	if err := e.up(ctx, etcdProject, e.services()); err != nil {
		return err
	}
	var last error
	err := poll(ctx, 500*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, probeErr := e.Leader(pctx)
		cancel()
		if probeErr != nil && ctx.Err() == nil {
			last = probeErr
		}
		return probeErr == nil, nil
	})
	if errors.Is(err, errGaveUp) && last != nil {
		err = fmt.Errorf("%w, the last probe said: %w", err, last)
	}
	if err != nil {
		return fmt.Errorf("etcd did not elect a leader: %w", err)
	}
	return nil
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

func (e *EtcdCluster) Generate(ctx context.Context, genCPUs string, args ...string) (loadgen.Report, error) {
	return e.generate(ctx, genCPUs, []string{EtcdGenImage, "-endpoints", strings.Join(e.endpoints(), ",")}, args)
}
