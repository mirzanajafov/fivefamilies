package controller

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

type Config struct {
	Nodes       []string
	Supervisors []string
	ChaosToken  string
	AdminToken  string
	Poll        time.Duration
	IdlePoll    time.Duration
	DownAfter   time.Duration
	Window      time.Duration
	Idle        time.Duration
	Clients     int
	TrustedHops int
	VisitorMax  time.Duration
	Cooldown    time.Duration
	PerAddress  int
	PerWindow   time.Duration
	HTTP        *http.Client
	Logger      *slog.Logger
}

func (c *Config) defaults() error {
	if len(c.Nodes) == 0 {
		return errors.New("controller: no nodes")
	}
	if len(c.Supervisors) != 0 && len(c.Supervisors) != len(c.Nodes) {
		return errors.New("controller: one supervisor address per node, or none")
	}
	if c.Poll <= 0 {
		c.Poll = 100 * time.Millisecond
	}
	if c.IdlePoll <= 0 {
		c.IdlePoll = time.Second
	}
	if c.DownAfter <= 0 {
		c.DownAfter = time.Second
	}
	if c.Window <= 0 {
		c.Window = 10 * time.Second
	}
	if c.Idle <= 0 {
		c.Idle = time.Minute
	}
	if c.Clients <= 0 {
		c.Clients = 3
	}
	if c.VisitorMax <= 0 {
		c.VisitorMax = 15 * time.Second
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 10 * time.Second
	}
	if c.PerAddress <= 0 {
		c.PerAddress = 6
	}
	if c.PerWindow <= 0 {
		c.PerWindow = 10 * time.Minute
	}
	if c.HTTP == nil {
		c.HTTP = &http.Client{Timeout: 5 * time.Second}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

type Controller struct {
	cfg     Config
	conns   []*grpc.ClientConn
	admins  []pb.AdminClient
	start   time.Time
	load    loadMeter
	viewers audience

	mu         sync.Mutex
	nodes      []NodeView
	silent     []time.Time
	prev       []*pb.StatusResponse
	prevAt     time.Time
	leader     int
	term       uint64
	voters     string
	events     []Event
	windows    []Window
	fault      *Fault
	last       *Fault
	faults     int
	perAddress limiter
}

func New(cfg Config) (*Controller, error) {
	if err := cfg.defaults(); err != nil {
		return nil, err
	}
	c := &Controller{cfg: cfg, start: time.Now(), perAddress: limiter{per: cfg.PerAddress, window: cfg.PerWindow}}
	for _, a := range cfg.Nodes {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		c.conns = append(c.conns, conn)
		c.admins = append(c.admins, pb.NewAdminClient(conn))
	}
	c.nodes = make([]NodeView, len(cfg.Nodes))
	c.silent = make([]time.Time, len(cfg.Nodes))
	for i := range c.nodes {
		c.nodes[i].ID = i + 1
	}
	return c, nil
}

func (c *Controller) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *Controller) Run(ctx context.Context) {
	c.healAll(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { c.poll(ctx) })
	wg.Go(func() { c.drive(ctx) })
	wg.Wait()
}

func (c *Controller) event(kind string, node int, term uint64, text string) {
	c.events = append(c.events, Event{At: time.Now(), Kind: kind, Node: node, Term: term, Text: text})
	if len(c.events) > 200 {
		c.events = c.events[len(c.events)-200:]
	}
}

func (c *Controller) Snapshot() Snapshot {
	active, rate := c.load.view()
	watching := c.viewers.count()
	c.mu.Lock()
	defer c.mu.Unlock()
	s := Snapshot{
		At:      time.Now(),
		Nodes:   append([]NodeView(nil), c.nodes...),
		Leader:  c.leader,
		Term:    c.term,
		Events:  append([]Event{}, c.events[max(0, len(c.events)-50):]...),
		Windows: append([]Window{}, c.windows[max(0, len(c.windows)-20):]...),
		Load:    Load{Active: active, OpsPerS: rate, Viewers: watching},
		Limits:  Limits{MaxSeconds: int(c.cfg.VisitorMax / time.Second), CooldownSeconds: int(c.cfg.Cooldown / time.Second)},
	}
	if c.fault != nil {
		s.Fault = c.fault.clone()
	}
	if c.last != nil {
		s.Last = c.last.clone()
		s.Limits.NextAllowed = c.last.Ends.Add(c.cfg.Cooldown)
	}
	return s
}

func (c *Controller) clientAddr(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if c.cfg.TrustedHops > 0 {
		var hops []string
		for _, h := range r.Header.Values("X-Forwarded-For") {
			for p := range strings.SplitSeq(h, ",") {
				if p = strings.TrimSpace(p); p != "" {
					hops = append(hops, p)
				}
			}
		}
		if len(hops) >= c.cfg.TrustedHops {
			return hops[len(hops)-c.cfg.TrustedHops]
		}
	}
	return host
}

func (c *Controller) admin(r *http.Request) bool {
	got := r.Header.Get("Authorization")
	return c.cfg.AdminToken != "" && subtle.ConstantTimeCompare([]byte(got), []byte("Bearer "+c.cfg.AdminToken)) == 1
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /api/snapshot", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, c.Snapshot()) })
	mux.HandleFunc("GET /api/events", c.serveEvents)
	mux.HandleFunc("POST /api/chaos", c.serveChaos)
	mux.HandleFunc("GET /api/admin", func(w http.ResponseWriter, r *http.Request) {
		if !c.admin(r) {
			writeJSON(w, http.StatusForbidden, map[string]bool{"admin": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]bool{"admin": true})
	})
	mux.HandleFunc("POST /api/heal", func(w http.ResponseWriter, r *http.Request) {
		if !c.admin(r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": "healing early needs the admin token"})
			return
		}
		c.healAll(r.Context())
		writeJSON(w, http.StatusOK, c.Snapshot())
	})
	return mux
}

func (c *Controller) serveEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	c.viewers.join()
	defer c.viewers.leave()
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		b, _ := json.Marshal(c.Snapshot())
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-t.C:
		}
	}
}
