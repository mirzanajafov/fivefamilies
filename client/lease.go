package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

var (
	ErrLeaseNotFound = errors.New("client: the lease does not exist or has expired")
	ErrTooManyLeases = errors.New("client: the cluster holds as many leases as it allows")
)

type Lease struct {
	ID  uint64
	TTL time.Duration
}

func leaseErr(err error) error {
	switch Reason(err) {
	case pb.ErrorReason_ERROR_REASON_LEASE_NOT_FOUND:
		return ErrLeaseNotFound
	case pb.ErrorReason_ERROR_REASON_TOO_MANY_LEASES:
		return ErrTooManyLeases
	}
	return err
}

func (c *Client) Grant(ctx context.Context, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("%w: a lease needs a positive TTL, got %v", ErrInvalid, ttl)
	}
	res, err := write(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, session, seq uint64, opts ...grpc.CallOption) (*pb.LeaseGrantResponse, error) {
		return pb.NewLeaseClient(cc).Grant(ctx, &pb.LeaseGrantRequest{Client: session, Seq: seq, TtlMs: uint64(ttl / time.Millisecond)}, opts...)
	})
	if err != nil {
		return Lease{}, leaseErr(err)
	}
	return Lease{ID: res.GetId(), TTL: time.Duration(res.GetTtlMs()) * time.Millisecond}, nil
}

func (c *Client) Revoke(ctx context.Context, id uint64) error {
	call := func(ctx context.Context, cc grpc.ClientConnInterface, session, seq uint64, opts ...grpc.CallOption) (*pb.LeaseRevokeResponse, error) {
		return pb.NewLeaseClient(cc).Revoke(ctx, &pb.LeaseRevokeRequest{Client: session, Seq: seq, Id: id}, opts...)
	}
	_, err := write(ctx, c, call)
	if errors.Is(err, ErrSessionExpired) {
		_, err = write(ctx, c, call)
	}
	return leaseErr(err)
}

func (c *Client) KeepAliveOnce(ctx context.Context, id uint64) (time.Duration, error) {
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.LeaseKeepAliveResponse, error) {
		return pb.NewLeaseClient(cc).KeepAlive(ctx, &pb.LeaseKeepAliveRequest{Id: id}, opts...)
	})
	if err != nil {
		return 0, leaseErr(err)
	}
	return time.Duration(res.GetTtlMs()) * time.Millisecond, nil
}

func (c *Client) KeepAlive(ctx context.Context, l Lease) <-chan error {
	out := make(chan error, 1)
	go func() {
		defer close(out)
		every := max(l.TTL/3, 10*time.Millisecond)
		for {
			kctx, cancel := context.WithTimeout(ctx, every)
			_, err := c.KeepAliveOnce(kctx, l.ID)
			cancel()
			switch {
			case ctx.Err() != nil:
				return
			case errors.Is(err, ErrLeaseNotFound):
				out <- err
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(every):
			}
		}
	}()
	return out
}

func (c *Client) TimeToLive(ctx context.Context, id uint64) (remaining, ttl time.Duration, keys uint64, err error) {
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.LeaseTimeToLiveResponse, error) {
		return pb.NewLeaseClient(cc).TimeToLive(ctx, &pb.LeaseTimeToLiveRequest{Id: id}, opts...)
	})
	if err != nil {
		return 0, 0, 0, leaseErr(err)
	}
	return time.Duration(res.GetRemainingMs()) * time.Millisecond, time.Duration(res.GetTtlMs()) * time.Millisecond, res.GetKeys(), nil
}
