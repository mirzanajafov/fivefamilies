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
)

type chaosRequest struct {
	Action  string `json:"action"`
	Node    int    `json:"node"`
	Seconds int    `json:"seconds"`
}

var actions = []string{"kill", "pause", "isolate", "slow-disk", "step-down"}

func (c *Controller) serveChaos(w http.ResponseWriter, r *http.Request) {
	var req chaosRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad request"})
		return
	}
	admin := c.admin(r)
	addr := c.clientAddr(r)
	f, code, err := c.chaos(r.Context(), req, admin, addr)
	if err != nil {
		writeJSON(w, code, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (c *Controller) chaos(ctx context.Context, req chaosRequest, admin bool, addr string) (*Fault, int, error) {
	limit := c.cfg.VisitorMax
	if admin {
		limit = time.Minute
	}
	d := time.Duration(req.Seconds) * time.Second
	switch {
	case !slices.Contains(actions, req.Action):
		return nil, http.StatusBadRequest, fmt.Errorf("unknown action %q", req.Action)
	case req.Action != "step-down" && (req.Node < 1 || req.Node > len(c.cfg.Nodes)):
		return nil, http.StatusBadRequest, fmt.Errorf("node must be between 1 and %d", len(c.cfg.Nodes))
	case req.Action != "step-down" && (d <= 0 || d > limit):
		return nil, http.StatusBadRequest, fmt.Errorf("a fault lasts between 1 and %d seconds", int(limit/time.Second))
	case (req.Action == "kill" || req.Action == "pause") && len(c.cfg.Supervisors) == 0:
		return nil, http.StatusNotImplemented, fmt.Errorf("%s needs the supervisors, which this cluster runs without", req.Action)
	}
	now := time.Now()
	c.mu.Lock()
	if c.fault != nil {
		c.mu.Unlock()
		return nil, http.StatusConflict, fmt.Errorf("one fault at a time: %s on node %d is still running", c.fault.Action, c.fault.Node)
	}
	if !admin {
		if wait := c.cooldownLeftLocked(now); wait > 0 {
			c.mu.Unlock()
			return nil, http.StatusTooManyRequests, fmt.Errorf("the Commission is still settling, so try again in %d s", int(math.Ceil(wait.Seconds())))
		}
		recent := slices.DeleteFunc(c.byAddr[addr], func(t time.Time) bool { return now.Sub(t) > c.cfg.PerWindow })
		if len(recent) >= c.cfg.PerAddress {
			c.byAddr[addr] = recent
			c.mu.Unlock()
			return nil, http.StatusTooManyRequests, fmt.Errorf("%d faults per %v from one address", c.cfg.PerAddress, c.cfg.PerWindow)
		}
		c.byAddr[addr] = append(recent, now)
	}
	leader, term := c.leader, c.term
	c.faults++
	f := &Fault{ID: c.faults, Action: req.Action, Node: req.Node, Duration: d, Started: now, Ends: now.Add(d), OldLeader: leader, OldTerm: term}
	if admin {
		f.By = "admin"
	} else {
		f.By = "visitor"
	}
	if req.Action == "step-down" {
		f.Node, f.Duration, f.Ends = leader, 0, now
	}
	f.OnLeader = f.Node == leader && req.Action != "slow-disk"
	c.fault = f
	c.mu.Unlock()
	if err := c.inject(ctx, req.Action, f.Node, d, leader); err != nil {
		c.mu.Lock()
		c.fault = nil
		c.mu.Unlock()
		return nil, http.StatusBadGateway, err
	}
	c.mu.Lock()
	c.event("fault", f.Node, 0, fmt.Sprintf("%s on node %d for %v (%s)", f.Action, f.Node, d, f.By))
	out := *f
	c.mu.Unlock()
	return &out, http.StatusOK, nil
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
		_, err := c.admins[node-1].Chaos(ctx, &pb.ChaosRequest{Action: "isolate", DurationMs: uint64(ms)})
		return err
	case "slow-disk":
		_, err := c.admins[node-1].Chaos(ctx, &pb.ChaosRequest{Action: "stall-disk", DurationMs: uint64(ms)})
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
		if _, err := c.admins[i].Chaos(hctx, &pb.ChaosRequest{Action: "heal"}); err != nil {
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
