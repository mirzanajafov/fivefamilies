package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mirzanajafov/fivefamilies/client"
)

func waitGone(t *testing.T, ctx context.Context, cl *client.Client, key string, within time.Duration) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < within {
		if _, found, err := cl.Get(ctx, key, client.ReadIndex); err == nil && !found {
			return time.Since(start)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s still there after %v", key, within)
	return 0
}

func TestLeasesOverTheWire(t *testing.T) {
	c := newTestCluster(t, 3, nil)
	c.leader(5 * time.Second)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	l, err := cl.Grant(ctx, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if min := 30 * 5 * time.Millisecond * 3 / 2; l.TTL != min {
		t.Fatalf("TTL %v, want it raised to one and a half election timeouts (%v)", l.TTL, min)
	}
	if _, err := cl.Put(ctx, "/registry/api/1", []byte("a"), client.WithLease(l.ID)); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, "/registry/api/2", []byte("b"), client.WithLease(l.ID+1000)); status.Code(err) != codes.NotFound {
		t.Fatalf("a put on a lease that doesn't exist: %v", err)
	}
	kctx, stopKeeping := context.WithCancel(ctx)
	lost := cl.KeepAlive(kctx, l)
	time.Sleep(4 * l.TTL)
	old := c.leader(time.Second)
	c.stop(old)
	time.Sleep(4 * l.TTL)
	kv, found, err := cl.Get(ctx, "/registry/api/1", client.ReadIndex)
	if err != nil || !found || kv.Lease != l.ID {
		t.Fatalf("a kept-alive key across a failover: %+v %v %v", kv, found, err)
	}
	select {
	case err := <-lost:
		t.Fatalf("keepalive gave up: %v", err)
	default:
	}
	remaining, ttl, keys, err := cl.TimeToLive(ctx, l.ID)
	if err != nil || ttl != l.TTL || keys != 1 || remaining <= 0 || remaining > ttl+2*5*time.Millisecond {
		t.Fatalf("time to live: %v of %v, %d keys, %v", remaining, ttl, keys, err)
	}
	stopKeeping()
	took := waitGone(t, ctx, cl, "/registry/api/1", 10*l.TTL)
	t.Logf("the key went %v after the last keepalive stopped (TTL %v)", took, l.TTL)
	if _, err := cl.KeepAliveOnce(ctx, l.ID); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("keepalive on an expired lease: %v", err)
	}

	l2, err := cl.Grant(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Put(ctx, "/config/api/limit", []byte("10"), client.WithLease(l2.ID)); err != nil {
		t.Fatal(err)
	}
	if err := cl.Revoke(ctx, l2.ID); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := cl.Get(ctx, "/config/api/limit", client.ReadIndex); found {
		t.Fatal("a revoked lease left its key")
	}
	if err := cl.Revoke(ctx, l2.ID); !errors.Is(err, client.ErrLeaseNotFound) {
		t.Fatalf("revoking twice: %v", err)
	}
}
