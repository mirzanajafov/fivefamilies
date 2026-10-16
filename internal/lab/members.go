package lab

import (
	"context"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

func (c *cluster) admin(n int) pb.AdminClient {
	return pb.NewAdminClient(c.conns[n-1])
}

func (c *cluster) leader(ctx context.Context) (int, *pb.StatusResponse) {
	sts := c.statuses(ctx, time.Second)
	l, _ := leaderOf(sts)
	if l == 0 {
		return 0, nil
	}
	return l, sts[l-1]
}

func (c *cluster) settleMember(ctx context.Context, node int, want pb.MemberChange, done func(*pb.StatusResponse) bool, limit time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	var last error
	for {
		if l, st := c.leader(ctx); l > 0 {
			if done(st) {
				return nil
			}
			change := want
			if want == pb.MemberChange_MEMBER_CHANGE_PROMOTE && !slices.Contains(st.GetLearners(), uint64(node)) {
				change = pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER
			}
			cctx, ccancel := context.WithTimeout(ctx, 3*time.Second)
			_, err := c.admin(l).ChangeMembers(cctx, &pb.ChangeMembersRequest{Change: change, Node: uint64(node)})
			ccancel()
			if err != nil {
				last = err
				if code := status.Code(err); code != codes.Unavailable && code != codes.FailedPrecondition && code != codes.DeadlineExceeded {
					return err
				}
			}
		}
		if !wait(ctx, 100*time.Millisecond) {
			if last == nil {
				last = ctx.Err()
			}
			return fmt.Errorf("membership of node %d did not settle within %v: %w", node, limit, last)
		}
	}
}

func (c *cluster) removeVoter(ctx context.Context, node int) error {
	return c.settleMember(ctx, node, pb.MemberChange_MEMBER_CHANGE_REMOVE, func(st *pb.StatusResponse) bool {
		return !slices.Contains(st.GetVoters(), uint64(node)) && !slices.Contains(st.GetLearners(), uint64(node))
	}, 10*time.Second)
}

func (c *cluster) restoreVoter(ctx context.Context, node int) error {
	return c.settleMember(ctx, node, pb.MemberChange_MEMBER_CHANGE_PROMOTE, func(st *pb.StatusResponse) bool {
		return slices.Contains(st.GetVoters(), uint64(node))
	}, 30*time.Second)
}

func (c *cluster) transfer(ctx context.Context, from, to int) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := c.admin(from).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(to)})
	return err
}
