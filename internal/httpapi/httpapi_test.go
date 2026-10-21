package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
)

type exchange struct {
	Name    string `json:"name"`
	Request struct {
		Method string          `json:"method"`
		Path   string          `json:"path"`
		Body   json.RawMessage `json:"body"`
	} `json:"request"`
	Response struct {
		Status  int               `json:"status"`
		Headers map[string]string `json:"headers"`
		Body    json.RawMessage   `json:"body"`
	} `json:"response"`
}

type fake struct {
	called    string
	instances []client.Instance
	index     uint64
	lease     client.Lease
	ttl       time.Duration
	err       error
}

func (f *fake) Lookup(_ context.Context, service string, index uint64, wait time.Duration) ([]client.Instance, uint64, error) {
	f.called = fmt.Sprintf("Lookup(%s, %d, %v)", service, index, wait)
	return f.instances, f.index, f.err
}

func (f *fake) RegisterOnce(_ context.Context, service string, inst client.Instance, ttl time.Duration) (client.Lease, error) {
	f.called = fmt.Sprintf("RegisterOnce(%s, %v, %v)", service, inst, ttl)
	return f.lease, f.err
}

func (f *fake) KeepAliveOnce(_ context.Context, id uint64) (time.Duration, error) {
	f.called = fmt.Sprintf("KeepAliveOnce(%d)", id)
	return f.ttl, f.err
}

func (f *fake) Revoke(_ context.Context, id uint64) error {
	f.called = fmt.Sprintf("Revoke(%d)", id)
	return f.err
}

func versioned(name, addr, version string) client.Instance {
	return client.Instance{Name: name, Addr: addr, Meta: map[string]string{"version": version}}
}

func decode(t *testing.T, raw []byte) any {
	t.Helper()
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	if m, ok := v.(map[string]any); ok {
		if text, ok := m["error"].(string); ok && text != "" {
			m["error"] = "some text"
		}
	}
	return v
}

func TestTheHandlersAnswerEveryExampleOfTheContract(t *testing.T) {
	raw, err := os.ReadFile("testdata/contract-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var exchanges []exchange
	if err := json.Unmarshal(raw, &exchanges); err != nil {
		t.Fatal(err)
	}
	dims := []client.Instance{versioned("c-137", "toxiproxy:18080", "v1"), versioned("d-99", "toxiproxy:18082", "v2")}
	cases := map[string]struct {
		call string
		fake fake
	}{
		"list a service":                     {"Lookup(dimensions, 0, 30s)", fake{instances: dims, index: 57}},
		"wait for a change after an index":   {"Lookup(dimensions, 57, 30s)", fake{instances: append(dims, client.Instance{Name: "e-7", Addr: "dim-e:8080"}), index: 64}},
		"a service with no instances":        {"Lookup(billing, 0, 30s)", fake{instances: []client.Instance{}, index: 12}},
		"the cluster cannot answer in time":  {"Lookup(dimensions, 64, 30s)", fake{err: context.DeadlineExceeded}},
		"register an instance":               {"RegisterOnce(dimensions, e-7=dim-e:8080 (version=v1), 10s)", fake{lease: client.Lease{ID: 63, TTL: 10 * time.Second}}},
		"register under a name with a slash": {"RegisterOnce(dimensions, e/7=dim-e:8080 (version=v1), 10s)", fake{err: fmt.Errorf("%w: instance name %q must be non-empty and without a slash", client.ErrInvalid, "e/7")}},
		"renew a lease":                      {"KeepAliveOnce(63)", fake{ttl: 10 * time.Second}},
		"renew a lease that is gone":         {"KeepAliveOnce(63)", fake{err: client.ErrLeaseNotFound}},
		"deregister":                         {"Revoke(63)", fake{}},
		"this node's view":                   {"", fake{}},
	}
	if len(exchanges) != len(cases) {
		t.Fatalf("the contract has %d examples and the test %d cases", len(exchanges), len(cases))
	}
	status := func() *pb.StatusResponse {
		return &pb.StatusResponse{Id: 3, Role: pb.Role_ROLE_LEADER, Leader: 3, Term: 7}
	}
	for _, ex := range exchanges {
		t.Run(ex.Name, func(t *testing.T) {
			c, ok := cases[ex.Name]
			if !ok {
				t.Fatalf("no case for the example %q", ex.Name)
			}
			f := c.fake
			rec := httptest.NewRecorder()
			NewServer(&f, status).Handler.ServeHTTP(rec, httptest.NewRequest(ex.Request.Method, ex.Request.Path, bytes.NewReader(ex.Request.Body)))
			if f.called != c.call {
				t.Errorf("called %q, want %q", f.called, c.call)
			}
			if rec.Code != ex.Response.Status {
				t.Errorf("status %d, want %d", rec.Code, ex.Response.Status)
			}
			for k, v := range ex.Response.Headers {
				if got := rec.Header().Get(k); got != v {
					t.Errorf("%s: %q, want %q", k, got, v)
				}
			}
			if got, want := decode(t, rec.Body.Bytes()), decode(t, ex.Response.Body); !reflect.DeepEqual(got, want) {
				t.Errorf("body %s, want %s", rec.Body.Bytes(), ex.Response.Body)
			}
		})
	}
}

func TestBadRequestsNeverReachTheRegistry(t *testing.T) {
	for _, target := range []string{
		"GET /v1/registry/dimensions?index=abc",
		"GET /v1/registry/dimensions?index=-1",
		"GET /v1/registry/dimensions?index=5&wait=soon",
		"GET /v1/registry/dimensions?index=5&wait=-1s",
		"POST /v1/leases/0/keepalive",
		"POST /v1/leases/x/keepalive",
		"DELETE /v1/leases/-3",
	} {
		var method, path string
		if _, err := fmt.Sscan(target, &method, &path); err != nil {
			t.Fatal(err)
		}
		f := fake{}
		rec := httptest.NewRecorder()
		NewServer(&f, nil).Handler.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
		if rec.Code != 400 || f.called != "" {
			t.Errorf("%s: %d, called %q", target, rec.Code, f.called)
		}
	}
	for _, body := range []string{
		`{"addr": "a:1", "ttl": "soon"}`,
		`{"addr": "a:1", "port": 80}`,
		`{"addr": "a:1", "meta": {"weight": 2}}`,
		`{"addr": "a:1", "meta": {"pad": "` + string(bytes.Repeat([]byte("x"), maxBody)) + `"}}`,
		`not json`,
	} {
		f := fake{}
		rec := httptest.NewRecorder()
		NewServer(&f, nil).Handler.ServeHTTP(rec, httptest.NewRequest("PUT", "/v1/registry/dimensions/e-7", bytes.NewReader([]byte(body))))
		if rec.Code != 400 || f.called != "" {
			t.Errorf("%.40s: %d, called %q", body, rec.Code, f.called)
		}
	}
}

func TestAWaitIsCappedAndATTLDefaults(t *testing.T) {
	f := fake{instances: []client.Instance{}}
	NewServer(&f, nil).Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/v1/registry/dimensions?index=5&wait=1h", nil))
	if f.called != "Lookup(dimensions, 5, 5m0s)" {
		t.Errorf("called %q", f.called)
	}
	NewServer(&f, nil).Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("PUT", "/v1/registry/dimensions/e-7", bytes.NewReader([]byte(`{"addr": "a:1"}`))))
	if f.called != "RegisterOnce(dimensions, e-7=a:1, 10s)" {
		t.Errorf("called %q", f.called)
	}
}

type slowKeepAlive struct {
	fake
	ended chan error
}

func (s *slowKeepAlive) KeepAliveOnce(ctx context.Context, _ uint64) (time.Duration, error) {
	select {
	case <-ctx.Done():
	case <-time.After(300 * time.Millisecond):
	}
	s.ended <- ctx.Err()
	return time.Second, nil
}

func TestACallWhoseCallerWentAwayStillFinishesInTheCluster(t *testing.T) {
	reg := &slowKeepAlive{ended: make(chan error, 1)}
	srv := httptest.NewServer(NewServer(reg, nil).Handler)
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/leases/63/keepalive", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := http.DefaultClient.Do(req); err == nil {
		t.Fatal("the caller got an answer before its deadline")
	}
	if err := <-reg.ended; err != nil {
		t.Fatalf("the call stopped with %v when its caller went away", err)
	}
}
