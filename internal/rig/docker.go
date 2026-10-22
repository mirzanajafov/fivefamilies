package rig

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

const (
	NodePort     = 3161
	hostPortBase = 3150
)

type Project struct {
	Name string
	File string
}

func Docker(ctx context.Context, env []string, args ...string) (string, error) {
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

func (p Project) Compose(ctx context.Context, env []string, args ...string) error {
	_, err := Docker(ctx, env, append([]string{"compose", "-p", p.Name, "-f", p.File}, args...)...)
	return err
}

func (p Project) Recreate(ctx context.Context, env []string) error {
	if err := p.Compose(ctx, env, "down", "-v", "--remove-orphans"); err != nil {
		return err
	}
	return p.Compose(ctx, env, "up", "-d")
}

func (p Project) Container(svc string) string {
	return p.Name + "-" + svc + "-1"
}

func (p Project) Down(ctx context.Context) error {
	return p.Compose(ctx, nil, "down", "-v", "--remove-orphans")
}

func HostAddrs(nodes int) []string {
	var out []string
	for i := 1; i <= nodes; i++ {
		out = append(out, fmt.Sprintf("127.0.0.1:%d", hostPortBase+i))
	}
	return out
}

func NetworkAddrs(nodes int) []string {
	var out []string
	for i := 1; i <= nodes; i++ {
		out = append(out, fmt.Sprintf("n%d:%d", i, NodePort))
	}
	return out
}
