package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/supervisor"
)

type chaosRequest struct {
	Action  string `json:"action"`
	Node    int    `json:"node"`
	Seconds int    `json:"seconds"`
}

var actions = []string{"kill", "pause", "isolate", "stall-disk", "step-down"}

type failure int

const (
	badRequest failure = iota + 1
	unsupported
	conflict
	limited
	notStarted
)

func (f failure) status() int {
	switch f {
	case badRequest:
		return http.StatusBadRequest
	case unsupported:
		return http.StatusNotImplemented
	case conflict:
		return http.StatusConflict
	case limited:
		return http.StatusTooManyRequests
	case notStarted:
		return http.StatusBadGateway
	}
	return http.StatusInternalServerError
}

type refusal struct {
	failure failure
	msg     string
}

func refuse(f failure, format string, args ...any) *refusal {
	return &refusal{failure: f, msg: fmt.Sprintf(format, args...)}
}

type limiter struct {
	per    int
	window time.Duration
	byAddr map[string][]time.Time
}

func (l *limiter) full(addr string, now time.Time) bool {
	for a, times := range l.byAddr {
		if times = slices.DeleteFunc(times, func(t time.Time) bool { return now.Sub(t) > l.window }); len(times) == 0 {
			delete(l.byAddr, a)
		} else {
			l.byAddr[a] = times
		}
	}
	return len(l.byAddr[addr]) >= l.per
}

func (l *limiter) add(addr string, at time.Time) {
	if l.byAddr == nil {
		l.byAddr = map[string][]time.Time{}
	}
	l.byAddr[addr] = append(l.byAddr[addr], at)
}

func (c *Controller) serveChaos(w http.ResponseWriter, r *http.Request) {
	var req chaosRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
		return
	}
	if req.Action == "slow-disk" {
		req.Action = "stall-disk"
	}
	f, no := c.chaos(r.Context(), req, c.admin(r), c.clientAddr(r))
	if no != nil {
		writeJSON(w, no.failure.status(), map[string]string{"error": no.msg})
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (c *Controller) chaos(ctx context.Context, req chaosRequest, admin bool, addr string) (*Fault, *refusal) {
	d, no := c.validate(req, admin)
	if no != nil {
		return nil, no
	}
	f, no := c.admit(req.Action, req.Node, d, admin, addr)
	if no != nil {
		return nil, no
	}
	err := c.inject(ctx, f.Action, f.Node, f.Duration)
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.cfg.Logger.Warn("chaos", "action", f.Action, "node", f.Node, "err", err)
		if c.fault == f {
			c.fault = nil
		}
		return nil, refuse(notStarted, "%s on node %d could not be started", f.Action, f.Node)
	}
	if !admin {
		c.perAddress.add(addr, f.Started)
	}
	c.event("fault", f.Node, 0, fmt.Sprintf("%s on node %d for %v (%s)", f.Action, f.Node, f.Duration, f.By))
	return f.clone(), nil
}

func (c *Controller) validate(req chaosRequest, admin bool) (time.Duration, *refusal) {
	limit := c.cfg.VisitorMax
	if admin {
		limit = supervisor.MaxFault
	}
	d := time.Duration(req.Seconds) * time.Second
	switch {
	case !slices.Contains(actions, req.Action):
		return 0, refuse(badRequest, "unknown action %q", req.Action)
	case req.Action == "step-down":
		return 0, nil
	case req.Node < 1 || req.Node > len(c.cfg.Nodes):
		return 0, refuse(badRequest, "node must be between 1 and %d", len(c.cfg.Nodes))
	case d <= 0 || d > limit:
		return 0, refuse(badRequest, "a fault lasts between 1 and %d seconds", int(limit/time.Second))
	case (req.Action == "kill" || req.Action == "pause") && len(c.cfg.Supervisors) == 0:
		return 0, refuse(unsupported, "%s needs the supervisors, which this cluster runs without", req.Action)
	}
	return d, nil
}

func (c *Controller) admit(action string, node int, d time.Duration, admin bool, addr string) (*Fault, *refusal) {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.fault != nil:
		return nil, refuse(conflict, "one fault at a time: %s on node %d is still running", c.fault.Action, c.fault.Node)
	case action == "step-down" && c.leader == 0:
		return nil, refuse(conflict, "there is no leader to step down")
	}
	if !admin {
		if wait := c.cooldownLeftLocked(now); wait > 0 {
			return nil, refuse(limited, "the Commission is still settling, so try again in %d s", int(math.Ceil(wait.Seconds())))
		}
		if c.perAddress.full(addr, now) {
			return nil, refuse(limited, "%d faults per %v from one address", c.cfg.PerAddress, c.cfg.PerWindow)
		}
	}
	if action == "step-down" {
		node = c.leader
	}
	c.faults++
	f := &Fault{ID: c.faults, Action: action, Node: node, Duration: d, Started: now, Ends: now.Add(d), By: "visitor", OldLeader: c.leader, OldTerm: c.term}
	if admin {
		f.By = "admin"
	}
	f.OnLeader = f.Node == c.leader && action != "stall-disk"
	c.fault = f
	return f, nil
}

func (c *Controller) cooldownLeftLocked(now time.Time) time.Duration {
	if c.last == nil {
		return 0
	}
	return c.last.Ends.Add(c.cfg.Cooldown).Sub(now)
}

func (c *Controller) supervisor(ctx context.Context, node int, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.cfg.Supervisors[node-1]+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.ChaosToken)
	resp, err := c.cfg.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("supervisor of node %d: %w", node, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("supervisor of node %d: %s", node, resp.Status)
	}
	return nil
}

func (c *Controller) inject(ctx context.Context, action string, node int, d time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	ms := d.Milliseconds()
	switch action {
	case "kill", "pause":
		return c.supervisor(ctx, node, fmt.Sprintf("/%s?ms=%d", action, ms))
	case "isolate":
		_, err := c.admins[node-1].Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_ISOLATE, DurationMs: uint64(ms)})
		return err
	case "stall-disk":
		_, err := c.admins[node-1].Chaos(ctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_STALL_DISK, DurationMs: uint64(ms)})
		return err
	case "step-down":
		to := node%len(c.cfg.Nodes) + 1
		_, err := c.admins[node-1].TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(to)})
		return err
	}
	return fmt.Errorf("unknown action %q", action)
}

func (c *Controller) healAll(ctx context.Context) {
	for i := range c.cfg.Nodes {
		hctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if len(c.cfg.Supervisors) > 0 {
			if err := c.supervisor(hctx, i+1, "/heal"); err != nil {
				c.cfg.Logger.Warn("heal", "node", i+1, "err", err)
			}
		}
		if _, err := c.admins[i].Chaos(hctx, &pb.ChaosRequest{Action: pb.ChaosAction_CHAOS_ACTION_HEAL}); err != nil {
			c.cfg.Logger.Warn("heal", "node", i+1, "err", err)
		}
		cancel()
	}
	c.mu.Lock()
	if c.fault != nil {
		c.fault.Ends = time.Now()
	}
	c.mu.Unlock()
}
