package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

func TestRoleNamesAreTheOnesTheDashboardShows(t *testing.T) {
	for role, want := range map[pb.Role]string{
		pb.Role_ROLE_UNSPECIFIED:   "",
		pb.Role_ROLE_FOLLOWER:      "follower",
		pb.Role_ROLE_PRE_CANDIDATE: "precandidate",
		pb.Role_ROLE_CANDIDATE:     "candidate",
		pb.Role_ROLE_LEADER:        "leader",
	} {
		if got := RoleName(role); got != want {
			t.Errorf("%v is shown as %q, want %q", role, got, want)
		}
	}
}

func TestOnlyKnownReadModesParse(t *testing.T) {
	for s, want := range map[string]ReadMode{"index": ReadIndex, "lease": ReadLease, "log": ReadLog} {
		var got ReadMode
		if err := got.UnmarshalText([]byte(s)); err != nil || got != want {
			t.Errorf("%q parsed as %s: %v", s, got, err)
		}
		if text, _ := want.MarshalText(); string(text) != s {
			t.Errorf("%s writes itself as %q", want, text)
		}
	}
	for _, s := range []string{"", "leases", "Index"} {
		var got ReadMode
		if err := got.UnmarshalText([]byte(s)); err == nil {
			t.Errorf("%q was accepted", s)
		}
	}
}

func unreachable(t *testing.T) (*Client, context.Context) {
	t.Helper()
	c, err := New([]string{"127.0.0.1:1"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return c, ctx
}

func TestALeaseWithoutAPositiveTTLIsRefusedBeforeAnythingIsSent(t *testing.T) {
	c, ctx := unreachable(t)
	for _, ttl := range []time.Duration{0, -5 * time.Second} {
		if _, err := c.Grant(ctx, ttl); !errors.Is(err, ErrInvalid) {
			t.Errorf("a %v lease: %v, want ErrInvalid", ttl, err)
		}
	}
}

func TestAnInstanceOverTheLimitAsStoredIsRefusedBeforeAnythingIsSent(t *testing.T) {
	c, ctx := unreachable(t)
	escaped := Instance{Name: "e-7", Addr: "a:1", Meta: map[string]string{"v": strings.Repeat("<", 200)}}
	if _, err := c.RegisterOnce(ctx, "dimensions", escaped, time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatalf("200 bytes that are stored as 1,200: %v, want ErrInvalid", err)
	}
}

func TestANameLongerThanTheLimitIsRefusedBeforeAnythingIsSent(t *testing.T) {
	c, ctx := unreachable(t)
	long := strings.Repeat("n", maxNameLen+1)
	if _, _, err := c.Lookup(ctx, long, 0, time.Second); !errors.Is(err, ErrInvalid) {
		t.Errorf("looking up a %d-byte service: %v, want ErrInvalid", len(long), err)
	}
	if _, err := c.RegisterOnce(ctx, "dimensions", Instance{Name: long, Addr: "a:1"}, time.Second); !errors.Is(err, ErrInvalid) {
		t.Errorf("registering a %d-byte instance: %v, want ErrInvalid", len(long), err)
	}
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := c.Lookup(done, strings.Repeat("n", maxNameLen), 0, time.Second); errors.Is(err, ErrInvalid) {
		t.Errorf("a name at the limit was refused: %v", err)
	}
}

type refusingLeases struct {
	pb.UnimplementedKVServer
	pb.UnimplementedLeaseServer
	refusal error
}

func (refusingLeases) OpenSession(context.Context, *pb.OpenSessionRequest) (*pb.OpenSessionResponse, error) {
	return &pb.OpenSessionResponse{Client: 1}, nil
}

func (r refusingLeases) Grant(context.Context, *pb.LeaseGrantRequest) (*pb.LeaseGrantResponse, error) {
	return nil, r.refusal
}

func refusal(t *testing.T, code codes.Code, reason pb.ErrorReason) error {
	t.Helper()
	st, err := status.New(code, "refused").WithDetails(&errdetails.ErrorInfo{Reason: reason.String(), Domain: errorDomain})
	if err != nil {
		t.Fatal(err)
	}
	return st.Err()
}

func TestTheServersRefusalsComeBackAsTheClientsErrors(t *testing.T) {
	for want, refused := range map[error]error{
		ErrInvalid:       status.Error(codes.InvalidArgument, "a lease needs a TTL between 1 ms and 24h0m0s"),
		ErrTooManyLeases: refusal(t, codes.FailedPrecondition, pb.ErrorReason_ERROR_REASON_TOO_MANY_LEASES),
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		g := grpc.NewServer()
		fake := refusingLeases{refusal: refused}
		pb.RegisterKVServer(g, fake)
		pb.RegisterLeaseServer(g, fake)
		go func() { _ = g.Serve(ln) }()
		c, err := New([]string{ln.Addr().String()}, Options{})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := c.Grant(ctx, 48*time.Hour); !errors.Is(err, want) {
			t.Errorf("%v came back as %v, want %v", refused, err, want)
		}
		cancel()
		_ = c.Close()
		g.Stop()
	}
}
