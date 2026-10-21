package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/server"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
)

type fakeSupervisor struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeSupervisor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer chaos-token" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.calls = append(f.calls, r.Method+" "+r.URL.RequestURI())
	_, _ = w.Write([]byte("{}"))
}

func (f *fakeSupervisor) seen() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type harness struct {
	t     *testing.T
	c     *Controller
	sups  []*fakeSupervisor
	api   *httptest.Server
	stop  context.CancelFunc
	nodes []string
}

func newHarness(t *testing.T, n int, tweak func(*Config)) *harness {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	addrs := map[raft.NodeID]string{}
	lns := map[raft.NodeID]net.Listener{}
	var nodes []string
	for i := 1; i <= n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addrs[raft.NodeID(i)], lns[raft.NodeID(i)] = ln.Addr().String(), ln
		nodes = append(nodes, ln.Addr().String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for id, ln := range lns {
		srv, err := server.New(server.Config{ID: id, Nodes: addrs, DataDir: filepath.Join(t.TempDir(), fmt.Sprint(id)), Tick: 5 * time.Millisecond,
			HeartbeatTick: 4, ElectionTick: 30, Chaos: true, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer(transport.ServerOptions()...)
		srv.Register(g)
		go func() {
			if err := g.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Error(err)
			}
		}()
		wg.Go(func() {
			if err := srv.Run(ctx); err != nil {
				t.Errorf("node %d: %v", id, err)
			}
		})
		t.Cleanup(g.Stop)
	}
	h := &harness{t: t, nodes: nodes}
	var sups []string
	for range n {
		f := &fakeSupervisor{}
		s := httptest.NewServer(f)
		t.Cleanup(s.Close)
		h.sups = append(h.sups, f)
		sups = append(sups, s.URL)
	}
	cfg := Config{Nodes: nodes, Supervisors: sups, ChaosToken: "chaos-token", AdminToken: "admin-token", Window: 500 * time.Millisecond,
		Idle: time.Second, Cooldown: 300 * time.Millisecond, TrustedHops: 1, Logger: logger}
	if tweak != nil {
		tweak(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.c = c
	h.api = httptest.NewServer(c.Handler())
	cctx, ccancel := context.WithCancel(ctx)
	var cw sync.WaitGroup
	cw.Go(func() { c.Run(cctx) })
	h.stop = cancel
	t.Cleanup(func() {
		ccancel()
		cw.Wait()
		h.api.Close()
		c.Close()
		cancel()
		wg.Wait()
	})
	return h
}

func (h *harness) waitFor(what string, within time.Duration, ok func(Snapshot) bool) Snapshot {
	h.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s := h.c.Snapshot(); ok(s) {
			return s
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("never: %s; last snapshot %+v", what, h.c.Snapshot())
	return Snapshot{}
}

func (h *harness) chaos(body string, from string, admin bool) (int, string) {
	h.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.api.URL+"/api/chaos", strings.NewReader(body))
	req.Header.Set("X-Forwarded-For", from)
	if admin {
		req.Header.Set("Authorization", "Bearer admin-token")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	if err := errors.Join(err, resp.Body.Close()); err != nil {
		h.t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}

func (h *harness) watch(ctx context.Context) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, h.api.URL+"/api/events", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func TestTheControllerFollowsTheClusterAndChecksTheDemoWhileSomeoneWatches(t *testing.T) {
	h := newHarness(t, 3, nil)
	s := h.waitFor("a leader", 5*time.Second, func(s Snapshot) bool { return s.Leader != 0 })
	if s.Load.Active {
		t.Fatal("the demo clients run with nobody watching")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go h.watch(ctx)
	h.waitFor("a viewer and running demo clients", 5*time.Second, func(s Snapshot) bool { return s.Load.Viewers == 1 && s.Load.Active })
	s = h.waitFor("two checked windows", 15*time.Second, func(s Snapshot) bool { return len(s.Windows) >= 2 })
	for _, w := range s.Windows {
		if w.Result != string(porcupine.Ok) || w.Ops == 0 {
			t.Fatalf("window %+v", w)
		}
	}
	found := false
	for _, e := range s.Events {
		found = found || e.Kind == "succession"
	}
	if !found {
		t.Fatalf("no succession in %+v", s.Events)
	}
	cancel()
	h.waitFor("the demo clients stop once the last viewer is gone for the idle time", 10*time.Second, func(s Snapshot) bool { return !s.Load.Active && s.Load.Viewers == 0 })
}

func TestVisitorsGetBoundedChaos(t *testing.T) {
	h := newHarness(t, 3, func(c *Config) { c.PerAddress = 2 })
	h.waitFor("a leader", 5*time.Second, func(s Snapshot) bool { return s.Leader != 0 })
	if code, _ := h.chaos(`{"action":"kill","node":1,"seconds":16}`, "1.1.1.1", false); code != http.StatusBadRequest {
		t.Fatalf("a 16 s kill from a visitor: %d", code)
	}
	if code, _ := h.chaos(`{"action":"melt","node":1,"seconds":1}`, "1.1.1.1", false); code != http.StatusBadRequest {
		t.Fatalf("an unknown action: %d", code)
	}
	if code, body := h.chaos(`{"action":"kill","node":2,"seconds":1}`, "1.1.1.1", false); code != http.StatusOK {
		t.Fatalf("a valid kill: %d %s", code, body)
	}
	if got := h.sups[1].seen(); len(got) == 0 || got[len(got)-1] != "POST /kill?ms=1000" {
		t.Fatalf("supervisor 2 saw %v", got)
	}
	if code, _ := h.chaos(`{"action":"pause","node":3,"seconds":1}`, "2.2.2.2", false); code != http.StatusConflict {
		t.Fatalf("a second fault while one runs: %d", code)
	}
	h.waitFor("the fault to close", 10*time.Second, func(s Snapshot) bool { return s.Fault == nil && s.Last != nil })
	if code, body := h.chaos(`{"action":"slow-disk","node":3,"seconds":1}`, "1.1.1.1", false); code != http.StatusOK || !strings.Contains(body, `"action":"stall-disk"`) {
		t.Fatalf("the second fault from one address, by the name the previous dashboard sends: %d %s", code, body)
	}
	h.waitFor("the fault to close", 10*time.Second, func(s Snapshot) bool { return s.Fault == nil && s.Last.ID == 2 })
	if code, _ := h.chaos(`{"action":"stall-disk","node":3,"seconds":1}`, "9.9.9.9, 1.1.1.1", false); code != http.StatusTooManyRequests {
		t.Fatalf("a third fault from the same address behind the proxy: %d", code)
	}
	h.c.mu.Lock()
	h.c.last.Ends = time.Now()
	h.c.mu.Unlock()
	if code, body := h.chaos(`{"action":"stall-disk","node":3,"seconds":1}`, "3.3.3.3", false); code != http.StatusTooManyRequests || !strings.Contains(body, "settling, so try again in 1 s") {
		t.Fatalf("a fault right after the last one ended: %d %s", code, body)
	}
	if code, body := h.chaos(`{"action":"stall-disk","node":3,"seconds":30}`, "1.1.1.1", true); code != http.StatusOK {
		t.Fatalf("the admin is not limited: %d %s", code, body)
	}
}

func TestIsolatingTheLeaderShowsTheFailover(t *testing.T) {
	h := newHarness(t, 3, nil)
	s := h.waitFor("a leader", 5*time.Second, func(s Snapshot) bool { return s.Leader != 0 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go h.watch(ctx)
	h.waitFor("demo writes", 5*time.Second, func(s Snapshot) bool { return s.Load.Active })
	time.Sleep(time.Second)
	if code, body := h.chaos(fmt.Sprintf(`{"action":"isolate","node":%d,"seconds":2}`, s.Leader), "1.1.1.1", false); code != http.StatusOK {
		t.Fatalf("%d %s", code, body)
	}
	s = h.waitFor("the fault to close with a measured failover", 20*time.Second, func(s Snapshot) bool { return s.Fault == nil && s.Last != nil })
	f := s.Last.Failover
	if !s.Last.OnLeader || f == nil || f.NewLeader == 0 || f.NewLeader == s.Last.OldLeader || f.Election <= 0 || f.Gap <= 0 || f.Gap > 5*time.Second {
		t.Fatalf("failover %+v of fault %+v", f, s.Last)
	}
	t.Logf("isolating the leader: a new leader after %v, the widest gap between acknowledged writes %v", f.Election.Round(time.Millisecond), f.Gap.Round(time.Millisecond))
	s = h.waitFor("a window checked after the fault", 20*time.Second, func(s Snapshot) bool {
		for _, w := range s.Windows {
			if w.From.After(s.Last.Started) {
				return true
			}
		}
		return false
	})
	for _, w := range s.Windows {
		if w.Result == string(porcupine.Illegal) {
			t.Fatalf("window %+v", w)
		}
	}
}

func TestStepDownHandsLeadershipOn(t *testing.T) {
	h := newHarness(t, 3, nil)
	s := h.waitFor("a leader", 5*time.Second, func(s Snapshot) bool { return s.Leader != 0 })
	old := s.Leader
	if code, body := h.chaos(`{"action":"step-down"}`, "1.1.1.1", false); code != http.StatusOK {
		t.Fatalf("%d %s", code, body)
	}
	h.waitFor("another leader", 5*time.Second, func(s Snapshot) bool { return s.Leader != 0 && s.Leader != old })
}

func TestAWindowWithAStaleReadIsCaught(t *testing.T) {
	c := &Controller{}
	initial := map[string]check.Output{"ledger/a": {Found: true, Value: "old"}}
	ops := []porcupine.Operation{
		{ClientId: 0, Input: check.Input{Kind: check.Put, Key: "ledger/a", Value: "new"}, Call: 10, Output: check.Output{Ok: true}, Return: 20},
		{ClientId: 1, Input: check.Input{Kind: check.Get, Key: "ledger/a"}, Call: 30, Output: check.Output{Found: true, Value: "old"}, Return: 40},
	}
	c.checkWindow(1, time.Now(), ops, initial, false)
	ops[1].Output = check.Output{Found: true, Value: "new"}
	c.checkWindow(2, time.Now(), ops, initial, false)
	ops[1].Call, ops[1].Return = 1, 5
	ops[1].Output = check.Output{Found: true, Value: "old"}
	c.checkWindow(3, time.Now(), ops, initial, false)
	c.checkWindow(4, time.Now(), ops, nil, true)
	want := []string{string(porcupine.Illegal), string(porcupine.Ok), string(porcupine.Ok), "unchecked"}
	for i, w := range c.windows {
		if w.Result != want[i] {
			t.Fatalf("window %d: %s, want %s", i+1, w.Result, want[i])
		}
	}
	b, err := json.Marshal(c.windows)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "unknown") {
		t.Fatal("the unchecked window does not say why")
	}
}

func TestTheAdminCheck(t *testing.T) {
	c := &Controller{cfg: Config{AdminToken: "admin-token"}}
	h := c.Handler()
	for token, want := range map[string]int{"": http.StatusForbidden, "Bearer nope": http.StatusForbidden, "Bearer admin-token": http.StatusOK} {
		req := httptest.NewRequest(http.MethodGet, "/api/admin", nil)
		if token != "" {
			req.Header.Set("Authorization", token)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != want {
			t.Fatalf("%q: %d, want %d", token, w.Code, want)
		}
	}
}

func TestThePollerSlowsDownWithNobodyWatching(t *testing.T) {
	c := &Controller{cfg: Config{Poll: 100 * time.Millisecond, IdlePoll: time.Second}}
	if got := c.pollEvery(); got != time.Second {
		t.Fatalf("idle: %v", got)
	}
	c.viewers = 1
	if got := c.pollEvery(); got != 100*time.Millisecond {
		t.Fatalf("watched: %v", got)
	}
	c.viewers, c.fault = 0, &Fault{}
	if got := c.pollEvery(); got != 100*time.Millisecond {
		t.Fatalf("during a fault: %v", got)
	}
}

func TestAFreshControllerSendsEmptyListsTheDashboardCanRead(t *testing.T) {
	c, err := New(Config{Nodes: []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	b, err := json.Marshal(c.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, list := range []string{`"events":[]`, `"windows":[]`} {
		if !bytes.Contains(b, []byte(list)) {
			t.Errorf("the snapshot %s has no %s", b, list)
		}
	}
}

func TestANodeIsShownDownOnlyAfterItHasBeenSilentForAWhile(t *testing.T) {
	c := &Controller{cfg: Config{DownAfter: time.Second}, nodes: make([]NodeView, 2), silent: make([]time.Time, 2)}
	leader := &pb.StatusResponse{Role: pb.Role_ROLE_LEADER, Term: 1, Leader: 1}
	follower := &pb.StatusResponse{Role: pb.Role_ROLE_FOLLOWER, Term: 1, Leader: 1}
	t0 := time.Now()
	downs := func() (n int) {
		for _, e := range c.events {
			if e.Kind == "down" {
				n++
			}
		}
		return n
	}
	c.observe([]*pb.StatusResponse{leader, follower}, t0)
	for _, ms := range []int{300, 900, 1200} {
		c.observe([]*pb.StatusResponse{leader, nil}, t0.Add(time.Duration(ms)*time.Millisecond))
		if !c.nodes[1].Up || downs() != 0 {
			t.Fatalf("after %d ms of missed polls the node shows as down", ms-300)
		}
	}
	c.observe([]*pb.StatusResponse{leader, nil}, t0.Add(1300*time.Millisecond))
	if c.nodes[1].Up || downs() != 1 {
		t.Fatalf("a node silent for a second still shows as up (events %+v)", c.events)
	}
	c.observe([]*pb.StatusResponse{leader, follower}, t0.Add(1400*time.Millisecond))
	if !c.nodes[1].Up || c.events[len(c.events)-1].Kind != "up" {
		t.Fatalf("the node came back but shows %+v", c.nodes[1])
	}
	c.observe([]*pb.StatusResponse{leader, nil}, t0.Add(1500*time.Millisecond))
	if !c.nodes[1].Up {
		t.Fatal("one missed poll after coming back shows the node as down")
	}
}
