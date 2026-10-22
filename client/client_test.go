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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/route"
)

func TestRoleNamesAreTheOnesTheDashboardShows(t *testing.T) {
	for role, want := range map[pb.Role]string{
		pb.Role_ROLE_UNSPECIFIED:   "",
		pb.Role_ROLE_FOLLOWER:      "follower",
		pb.Role_ROLE_PRE_CANDIDATE: "precandidate",
		pb.Role_ROLE_CANDIDATE:     "candidate",
		pb.Role_ROLE_LEADER:        "leader",
	} {
		t.Run(role.String(), func(t *testing.T) {
			if got := RoleName(role); got != want {
				t.Errorf("%v is shown as %q, want %q", role, got, want)
			}
		})
	}
}

func TestOnlyKnownReadModesParse(t *testing.T) {
	for s, want := range map[string]ReadMode{"index": ReadIndex, "lease": ReadLease, "log": ReadLog} {
		t.Run(s, func(t *testing.T) {
			var got ReadMode
			if err := got.UnmarshalText([]byte(s)); err != nil || got != want {
				t.Errorf("%q parsed as %s: %v", s, got, err)
			}
			if text, _ := want.MarshalText(); string(text) != s {
				t.Errorf("%s writes itself as %q", want, text)
			}
		})
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
	t.Cleanup(func() { c.Close() })
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

func TestANameThatIsNotAPathSegmentOfAtMost128BytesIsRefusedBeforeAnythingIsSent(t *testing.T) {
	c, ctx := unreachable(t)
	long := strings.Repeat("n", maxNameLen+1)
	if _, _, err := c.Lookup(ctx, long, 0, time.Second); !errors.Is(err, ErrInvalid) {
		t.Errorf("looking up a %d-byte service: %v, want ErrInvalid", len(long), err)
	}
	for _, name := range []string{long, "e-\xff", ".", ".."} {
		if _, err := c.RegisterOnce(ctx, "dimensions", Instance{Name: name, Addr: "a:1"}, time.Second); !errors.Is(err, ErrInvalid) {
			t.Errorf("registering %q: %v, want ErrInvalid", name, err)
		}
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
		t.Run(want.Error(), func(t *testing.T) {
			fake := refusingLeases{refusal: refused}
			addr := serve(t, func(g *grpc.Server) {
				pb.RegisterKVServer(g, fake)
				pb.RegisterLeaseServer(g, fake)
			})
			c, err := New([]string{addr}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.Grant(ctx, 48*time.Hour); !errors.Is(err, want) {
				t.Errorf("%v came back as %v, want %v", refused, err, want)
			}
		})
	}
}

type fakeNode struct {
	pb.UnimplementedKVServer
	pb.UnimplementedWatchServer
	addr    string
	hint    string
	refusal error
	hang    bool
	watched chan<- string
}

func (f *fakeNode) refuse(ctx context.Context) error {
	if f.hint != "" {
		if err := grpc.SetHeader(ctx, metadata.Pairs(route.LeaderHeader, f.hint)); err != nil {
			return err
		}
	}
	return f.refusal
}

func (f *fakeNode) OpenSession(ctx context.Context, _ *pb.OpenSessionRequest) (*pb.OpenSessionResponse, error) {
	if f.hang {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if f.refusal != nil {
		return nil, f.refuse(ctx)
	}
	return &pb.OpenSessionResponse{Client: 1}, nil
}

func (f *fakeNode) Put(ctx context.Context, _ *pb.PutRequest) (*pb.PutResponse, error) {
	if f.refusal != nil {
		return nil, f.refuse(ctx)
	}
	return &pb.PutResponse{Rev: 1}, nil
}

func (f *fakeNode) Watch(_ *pb.WatchRequest, stream grpc.ServerStreamingServer[pb.WatchResponse]) error {
	f.watched <- f.addr
	<-stream.Context().Done()
	return stream.Context().Err()
}

func serve(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	register(g)
	go func() {
		if err := g.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}()
	t.Cleanup(g.Stop)
	return ln.Addr().String()
}

func serveFake(t *testing.T, node *fakeNode) {
	t.Helper()
	node.addr = serve(t, func(g *grpc.Server) {
		pb.RegisterKVServer(g, node)
		pb.RegisterWatchServer(g, node)
	})
}

func TestANodeThatAnsweredKeepsTheClientsWatches(t *testing.T) {
	notLeader := refusal(t, codes.Unavailable, pb.ErrorReason_ERROR_REASON_NOT_LEADER)
	for _, tc := range []struct {
		name    string
		hint    func(leader string) string
		refusal error
	}{
		{"not the leader, with a hint to the leader", func(leader string) string { return leader }, notLeader},
		{"not the leader, with a hint to an address the client doesn't know", func(string) string { return "fivefamilies-n2:3161" }, notLeader},
		{"unavailable, with no hint", func(string) string { return "" }, status.Error(codes.Unavailable, "the entry was lost")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			watched := make(chan string, 2)
			leader := &fakeNode{watched: watched}
			serveFake(t, leader)
			follower := &fakeNode{hint: tc.hint(leader.addr), refusal: tc.refusal, watched: watched}
			serveFake(t, follower)
			c, err := New([]string{follower.addr, leader.addr}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { c.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.Put(ctx, "k", []byte("v")); err != nil {
				t.Fatal(err)
			}
			c.Watch(ctx, "k", false, 0)
			select {
			case addr := <-watched:
				if addr != follower.addr {
					t.Fatalf("the watch went to the leader %s, want the follower %s, which answered", addr, follower.addr)
				}
			case <-ctx.Done():
				t.Fatal("no node got the watch")
			}
		})
	}
}

func TestANodeACallFoundSilentLosesTheClientsWatches(t *testing.T) {
	watched := make(chan string, 2)
	silent := &fakeNode{hang: true, watched: watched}
	serveFake(t, silent)
	leader := &fakeNode{watched: watched}
	serveFake(t, leader)
	c, err := New([]string{silent.addr, leader.addr}, Options{AttemptTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Put(ctx, "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	c.Watch(ctx, "k", false, 0)
	select {
	case addr := <-watched:
		if addr != leader.addr {
			t.Fatalf("the watch went to %s, want %s: the other node never answered a call", addr, leader.addr)
		}
	case <-ctx.Done():
		t.Fatal("no node got the watch")
	}
}
