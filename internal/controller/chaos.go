package controller

import (
	"context"
	"encoding/json"
	"errors"
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

type refusal int

const (
	refusedInvalid refusal = iota + 1
	refusedUnsupported
	refusedBusy
	refusedLimited
	refusedFailed
)

var refusalStatus = map[refusal]int{
	refusedInvalid:     http.StatusBadRequest,
	refusedUnsupported: http.StatusNotImplemented,
	refusedBusy:        http.StatusConflict,
	refusedLimited:     http.StatusTooManyRequests,
	refusedFailed:      http.StatusBadGateway,
}

type chaosError struct {
	refusal refusal
	msg     string
}

func (e *chaosError) Error() string { return e.msg }

func refuse(r refusal, format string, args ...any) error {
	return &chaosError{refusal: r, msg: fmt.Sprintf(format, args...)}
}

type limiter struct {
	per    int
	window time.Duration
	byAddr map[string][]time.Time
}

func (l *limiter) allow(addr string, now time.Time) bool {
	for a, times := range l.byAddr {
		if times = slices.DeleteFunc(times, func(t time.Time) bool { return now.Sub(t) > l.window }); len(times) == 0 {
			delete(l.byAddr, a)
		} else {
			l.byAddr[a] = times
		}
	}
	if len(l.byAddr[addr]) >= l.per {
		return false
	}
	l.byAddr[addr] = append(l.byAddr[addr], now)
	return true
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
	f, err := c.chaos(r.Context(), req, c.admin(r), c.clientAddr(r))
	var ce *chaosError
	switch {
	case errors.As(err, &ce):
		writeJSON(w, refusalStatus[ce.refusal], map[string]string{"error": ce.msg})
	case err != nil:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "the fault could not be started"})
	default:
		writeJSON(w, http.StatusOK, f)
	}
}

func (c *Controller) chaos(ctx context.Context, req chaosRequest, admin bool, addr string) (*Fault, error) {
	if err := c.check(req, admin); err != nil {
		return nil, err
	}
	f, err := c.admit(req, admin, addr, time.Now())
	if err != nil {
		return nil, err
	}
	if err := c.inject(ctx, f.Action, f.Node, f.Duration, f.OldLeader); err != nil {
		c.cfg.Logger.Warn("chaos", "action", f.Action, "node", f.Node, "err", err)
		c.mu.Lock()
		c.fault = nil
		c.mu.Unlock()
		return nil, refuse(refusedFailed, "%s on node %d could not be started", f.Action, f.Node)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.event("fault", f.Node, 0, fmt.Sprintf("%s on node %d for %v (%s)", f.Action, f.Node, f.Duration, f.By))
	out := *f
	return &out, nil
}

func (c *Controller) check(req chaosRequest, admin bool) error {
	limit := c.cfg.VisitorMax
	if admin {
		limit = supervisor.MaxFault
	}
	d := time.Duration(req.Seconds) * time.Second
	switch {
	case !slices.Contains(actions, req.Action):
		return refuse(refusedInvalid, "unknown action %q", req.Action)
	case req.Action != "step-down" && (req.Node < 1 || req.Node > len(c.cfg.Nodes)):
		return refuse(refusedInvalid, "node must be between 1 and %d", len(c.cfg.Nodes))
	case req.Action != "step-down" && (d <= 0 || d > limit):
		return refuse(refusedInvalid, "a fault lasts between 1 and %d seconds", int(limit/time.Second))
	case (req.Action == "kill" || req.Action == "pause") && len(c.cfg.Supervisors) == 0:
		return refuse(refusedUnsupported, "%s needs the supervisors, which this cluster runs without", req.Action)
	}
	return nil
}

func (c *Controller) admit(req chaosRequest, admin bool, addr string, now time.Time) (*Fault, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fault != nil {
		return nil, refuse(refusedBusy, "one fault at a time: %s on node %d is still running", c.fault.Action, c.fault.Node)
	}
	if !admin {
		if wait := c.cooldownLeftLocked(now); wait > 0 {
			return nil, refuse(refusedLimited, "the Commission is still settling, so try again in %d s", int(math.Ceil(wait.Seconds())))
		}
		if !c.visitors.allow(addr, now) {
			return nil, refuse(refusedLimited, "%d faults per %v from one address", c.cfg.PerAddress, c.cfg.PerWindow)
		}
	}
	d := time.Duration(req.Seconds) * time.Second
	c.faults++
	f := &Fault{ID: c.faults, Action: req.Action, Node: req.Node, Duration: d, Started: now, Ends: now.Add(d), By: "visitor", OldLeader: c.leader, OldTerm: c.term}
	if admin {
		f.By = "admin"
	}
	if req.Action == "step-down" {
		f.Node, f.Duration, f.Ends = c.leader, 0, now
	}
	f.OnLeader = f.Node == c.leader && req.Action != "stall-disk"
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

func (c *Controller) inject(ctx context.Context, action string, node int, d time.Duration, leader int) error {
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
		if leader == 0 {
			return fmt.Errorf("there is no leader to step down")
		}
		to := leader%len(c.cfg.Nodes) + 1
		_, err := c.admins[leader-1].TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(to)})
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
