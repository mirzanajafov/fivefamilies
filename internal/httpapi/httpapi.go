package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
)

const (
	defaultWait = 30 * time.Second
	maxWait     = 5 * time.Minute
	defaultTTL  = 10 * time.Second
	callTimeout = 5 * time.Second
	maxBody     = 1 << 10
)

type Registry interface {
	Lookup(ctx context.Context, service string, index uint64, wait time.Duration) ([]client.Instance, uint64, error)
	RegisterOnce(ctx context.Context, service string, inst client.Instance, ttl time.Duration) (client.Lease, error)
	KeepAliveOnce(ctx context.Context, id uint64) (time.Duration, error)
	Revoke(ctx context.Context, id uint64) error
}

type duration time.Duration

func (d duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`a duration is a string like "10s"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return errors.New(`a duration is a string like "10s"`)
	}
	*d = duration(v)
	return nil
}

type instance struct {
	Name string            `json:"name"`
	Addr string            `json:"addr"`
	Meta map[string]string `json:"meta,omitempty"`
}

type instances struct {
	Index     uint64     `json:"index"`
	Instances []instance `json:"instances"`
}

type registration struct {
	Addr string            `json:"addr"`
	Meta map[string]string `json:"meta"`
	TTL  *duration         `json:"ttl"`
}

type granted struct {
	Lease uint64   `json:"lease"`
	TTL   duration `json:"ttl"`
}

type renewed struct {
	TTL duration `json:"ttl"`
}

type nodeStatus struct {
	ID     uint64 `json:"id"`
	Role   string `json:"role"`
	Leader uint64 `json:"leader"`
	Term   uint64 `json:"term"`
}

type failure struct {
	Error  string `json:"error"`
	Reason string `json:"reason,omitempty"`
}

type api struct {
	reg      Registry
	status   func() *pb.StatusResponse
	stopping context.Context
}

type Server struct {
	http *http.Server
	stop context.CancelFunc
}

func NewServer(reg Registry, statusOf func() *pb.StatusResponse) *Server {
	stopping, stop := context.WithCancel(context.Background())
	a := &api{reg: reg, status: statusOf, stopping: stopping}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, failure{Error: r.Method + " " + r.URL.Path + " is not part of the API"})
	})
	mux.HandleFunc("GET /v1/registry/{service}", a.lookup)
	mux.HandleFunc("PUT /v1/registry/{service}/{instance}", a.register)
	mux.HandleFunc("POST /v1/leases/{id}/keepalive", a.keepAlive)
	mux.HandleFunc("DELETE /v1/leases/{id}", a.revoke)
	mux.HandleFunc("GET /v1/status", a.nodeStatus)
	return &Server{
		http: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      maxWait + 2*callTimeout,
			IdleTimeout:       2 * time.Minute,
		},
		stop: stop,
	}
}

func (s *Server) Handler() http.Handler { return s.http.Handler }

func (s *Server) Serve(ln net.Listener) error { return s.http.Serve(ln) }

func (s *Server) Stop() error {
	s.stop()
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout+time.Second)
	defer cancel()
	if err := s.http.Shutdown(ctx); err != nil {
		return errors.Join(err, s.http.Close())
	}
	return nil
}

func waitParams(r *http.Request) (uint64, time.Duration, error) {
	q := r.URL.Query()
	var index uint64
	if s := q.Get("index"); s != "" {
		var err error
		if index, err = strconv.ParseUint(s, 10, 64); err != nil {
			return 0, 0, errors.New("index must be a revision")
		}
	}
	wait := defaultWait
	if s := q.Get("wait"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return 0, 0, errors.New(`wait must be a duration like "30s"`)
		}
		wait = min(d, maxWait)
	}
	return index, wait, nil
}

func finishing(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.Context()), callTimeout)
}

func (a *api) lookup(w http.ResponseWriter, r *http.Request) {
	index, wait, err := waitParams(r)
	if err != nil {
		badRequest(w, err)
		return
	}
	timeout := callTimeout
	if index > 0 {
		timeout += wait
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	if index > 0 {
		stop := context.AfterFunc(a.stopping, cancel)
		defer stop()
	}
	list, rev, err := a.reg.Lookup(ctx, r.PathValue("service"), index, wait)
	if err != nil {
		fail(w, err)
		return
	}
	out := instances{Index: rev, Instances: make([]instance, len(list))}
	for i, in := range list {
		out.Instances[i] = instance(in)
	}
	writeJSON(w, http.StatusOK, out)
}

func readRegistration(w http.ResponseWriter, r *http.Request) (registration, error) {
	var req registration
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		return req, fmt.Errorf("a registration is at most %d bytes", maxBody)
	}
	if err != nil {
		return req, fmt.Errorf("reading the registration: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return req, fmt.Errorf("the body must be one JSON registration: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return req, errors.New("the body must hold one JSON registration and nothing after it")
	}
	if req.TTL == nil {
		req.TTL = new(duration(defaultTTL))
	}
	return req, nil
}

func (a *api) register(w http.ResponseWriter, r *http.Request) {
	req, err := readRegistration(w, r)
	if err != nil {
		badRequest(w, err)
		return
	}
	ttl := time.Duration(*req.TTL)
	ctx, cancel := context.WithTimeout(r.Context(), callTimeout)
	defer cancel()
	l, err := a.reg.RegisterOnce(ctx, r.PathValue("service"), client.Instance{Name: r.PathValue("instance"), Addr: req.Addr, Meta: req.Meta}, ttl)
	if errors.Is(err, client.ErrLeaseNotFound) {
		unavailable(w, failure{Error: "the lease ran out before the instance was put on it: " + err.Error()})
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, granted{Lease: l.ID, TTL: duration(l.TTL)})
}

func leaseID(r *http.Request) (uint64, error) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("a lease id is a positive number")
	}
	return id, nil
}

func (a *api) keepAlive(w http.ResponseWriter, r *http.Request) {
	id, err := leaseID(r)
	if err != nil {
		badRequest(w, err)
		return
	}
	ctx, cancel := finishing(r)
	defer cancel()
	ttl, err := a.reg.KeepAliveOnce(ctx, id)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, renewed{TTL: duration(ttl)})
}

func (a *api) revoke(w http.ResponseWriter, r *http.Request) {
	id, err := leaseID(r)
	if err != nil {
		badRequest(w, err)
		return
	}
	ctx, cancel := finishing(r)
	defer cancel()
	if err := a.reg.Revoke(ctx, id); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *api) nodeStatus(w http.ResponseWriter, _ *http.Request) {
	st := a.status()
	writeJSON(w, http.StatusOK, nodeStatus{ID: st.GetId(), Role: client.RoleName(st.GetRole()), Leader: st.GetLeader(), Term: st.GetTerm()})
}

func reason(r pb.ErrorReason) string {
	return strings.TrimPrefix(r.String(), "ERROR_REASON_")
}

func fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, client.ErrInvalid):
		badRequest(w, err)
	case errors.Is(err, client.ErrLeaseNotFound):
		writeJSON(w, http.StatusNotFound, failure{Error: err.Error(), Reason: reason(pb.ErrorReason_ERROR_REASON_LEASE_NOT_FOUND)})
	case errors.Is(err, client.ErrTooManyLeases):
		unavailable(w, failure{Error: err.Error(), Reason: reason(pb.ErrorReason_ERROR_REASON_TOO_MANY_LEASES)})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), errors.Is(err, client.ErrSessionExpired):
		unavailable(w, failure{Error: err.Error()})
	default:
		writeJSON(w, http.StatusInternalServerError, failure{Error: err.Error()})
	}
}

func badRequest(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusBadRequest, failure{Error: err.Error()})
}

func unavailable(w http.ResponseWriter, f failure) {
	w.Header().Set("Retry-After", "1")
	writeJSON(w, http.StatusServiceUnavailable, f)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
