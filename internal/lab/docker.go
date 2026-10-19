package lab

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type Docker struct {
	Project string
	File    string
	Nodes   int
	ips     map[int]string
}

func (d *Docker) cmd(ctx context.Context, env []string, args ...string) (string, error) {
	c := exec.CommandContext(ctx, "docker", args...)
	if env != nil {
		c.Env = append(c.Environ(), env...)
	}
	var out, errb bytes.Buffer
	c.Stdout, c.Stderr = &out, &errb
	if err := c.Run(); err != nil {
		return out.String(), fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

func (d *Docker) compose(ctx context.Context, env []string, args ...string) error {
	_, err := d.cmd(ctx, env, append([]string{"compose", "-p", d.Project, "-f", d.File}, args...)...)
	return err
}

func (d *Docker) container(svc string) string {
	return d.Project + "-" + svc + "-1"
}

func (d *Docker) Up(ctx context.Context, env []string) error {
	if err := d.compose(ctx, env, "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	if err := d.compose(ctx, env, "up", "-d"); err != nil {
		return err
	}
	d.ips = map[int]string{}
	for i := 1; i <= d.Nodes; i++ {
		ip, err := d.cmd(ctx, nil, "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", d.container(fmt.Sprintf("n%dnet", i)))
		if err != nil {
			return err
		}
		d.ips[i] = ip
	}
	return nil
}

func (d *Docker) Down(ctx context.Context) error {
	return d.compose(ctx, nil, "down", "-v", "--remove-orphans")
}

func (d *Docker) Kill(ctx context.Context, n int) error {
	_, err := d.cmd(ctx, nil, "kill", "-s", "KILL", d.container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Start(ctx context.Context, n int) error {
	_, err := d.cmd(ctx, nil, "start", d.container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Pause(ctx context.Context, n int) error {
	_, err := d.cmd(ctx, nil, "pause", d.container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) Unpause(ctx context.Context, n int) error {
	_, err := d.cmd(ctx, nil, "unpause", d.container(fmt.Sprintf("n%d", n)))
	return err
}

func (d *Docker) shell(ctx context.Context, n int, script string) error {
	_, err := d.cmd(ctx, nil, "exec", d.container(fmt.Sprintf("n%dnet", n)), "sh", "-c", script)
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
	out, err := d.cmd(ctx, nil, "inspect", "-f", "{{.State.Running}} {{.State.Paused}}", d.container(fmt.Sprintf("n%d", n)))
	return err == nil && out == "true false"
}

func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func poll(ctx context.Context, every, limit time.Duration, done func(context.Context) (bool, error)) error {
	lctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		if ok, err := done(lctx); ok || err != nil {
			return err
		}
		if !wait(lctx, every) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("gave up after %v", limit)
		}
	}
}

func (d *Docker) Exited(ctx context.Context, n int) (string, bool) {
	name := d.container(fmt.Sprintf("n%d", n))
	out, err := d.cmd(ctx, nil, "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", name)
	if err != nil || strings.HasPrefix(out, "true") {
		return "", false
	}
	logs, _ := d.cmd(ctx, nil, "logs", "--tail", "2", name)
	return fmt.Sprintf("node %d is not running (state %q): %s", n, out, logs), true
}
