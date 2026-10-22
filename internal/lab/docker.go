package lab

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

type Docker struct {
	rig.Project
	Nodes int
	ips   map[int]string
}

func (d *Docker) Up(ctx context.Context, env []string) error {
	if err := d.Recreate(ctx, env); err != nil {
		return err
	}
	d.ips = map[int]string{}
	for i := 1; i <= d.Nodes; i++ {
		ip, err := rig.Docker(ctx, nil, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", d.Container(fmt.Sprintf("n%dnet", i)))
		if err != nil {
			return err
		}
		d.ips[i] = ip
	}
	return nil
}

func (d *Docker) Kill(ctx context.Context, n int) error {
	_, err := rig.Docker(ctx, nil, "kill", "-s", "KILL", d.Container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Start(ctx context.Context, n int) error {
	_, err := rig.Docker(ctx, nil, "start", d.Container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Pause(ctx context.Context, n int) error {
	_, err := rig.Docker(ctx, nil, "pause", d.Container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Unpause(ctx context.Context, n int) error {
	_, err := rig.Docker(ctx, nil, "unpause", d.Container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) shell(ctx context.Context, n int, script string) error {
	_, err := rig.Docker(ctx, nil, "exec", d.Container(fmt.Sprintf("n%dnet", n)), "sh", "-c", script)
	return err
}

func (d *Docker) Block(ctx context.Context, from, to []int) error {
	for _, a := range from {
		var rules []string
		for _, b := range to {
			rules = append(rules,
				fmt.Sprintf("iptables -A OUTPUT -d %s -j DROP", d.ips[b]),
				fmt.Sprintf("iptables -A INPUT -s %s -j DROP", d.ips[b]))
		}
		if err := d.shell(ctx, a, strings.Join(rules, " && ")); err != nil {
			return err
		}
	}
	return nil
}

func (d *Docker) BlockOneWay(ctx context.Context, from, to int) error {
	return d.shell(ctx, from, fmt.Sprintf("iptables -A OUTPUT -d %s -j DROP", d.ips[to]))
}

func (d *Docker) Netem(ctx context.Context, n int, spec string) error {
	return d.shell(ctx, n, "tc qdisc replace dev eth0 root netem "+spec)
}

func (d *Docker) Heal(ctx context.Context) error {
	for i := 1; i <= d.Nodes; i++ {
		if err := d.shell(ctx, i, "iptables -F INPUT && iptables -F OUTPUT && (tc qdisc del dev eth0 root 2>/dev/null || true)"); err != nil {
			return err
		}
	}
	return nil
}

func (d *Docker) Running(ctx context.Context, n int) bool {
	out, err := rig.Docker(ctx, nil, "inspect", "-f", "{{.State.Running}} {{.State.Paused}}", d.Container(fmt.Sprintf("n%d", n)))
	return err == nil && out == "true false"
}

func (d *Docker) Exited(ctx context.Context, n int) (string, bool) {
	name := d.Container(fmt.Sprintf("n%d", n))
	out, err := rig.Docker(ctx, nil, "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", name)
	if err != nil || strings.HasPrefix(out, "true") {
		return "", false
	}
	logs, _ := rig.Docker(ctx, nil, "logs", "--tail", "2", name)
	return fmt.Sprintf("node %d is not running (state %q): %s", n, out, logs), true
}

type labCluster struct {
	d     *Docker
	adm   *rig.Admin
	addrs []string
}

func startLab(ctx context.Context, project, file string, env []string) (*labCluster, error) {
	d := &Docker{Project: rig.Project{Name: project, File: file}, Nodes: 5}
	addrs := rig.HostAddrs(d.Nodes)
	adm, err := rig.DialAdmin(addrs)
	if err != nil {
		return nil, err
	}
	if err := d.Up(ctx, env); err != nil {
		adm.Close()
		return nil, errors.Join(err, d.Down(context.Background()))
	}
	return &labCluster{d: d, adm: adm, addrs: addrs}, nil
}

func (c *labCluster) stop(log func(format string, args ...any)) {
	c.adm.Close()
	if err := c.d.Down(context.Background()); err != nil {
		log("tearing down the cluster: %v", err)
	}
}
