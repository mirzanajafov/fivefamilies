package lab

import (
	"context"
	"errors"
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

var errNoLeader = errors.New("lab: no node reports a leader")

func (c *cluster) settleMember(ctx context.Context, node int, want pb.MemberChange, done func(*pb.StatusResponse) bool, limit time.Duration) error {
	var last error
	err := poll(ctx, 100*time.Millisecond, limit, func(ctx context.Context) (bool, error) {
		l, st := c.leader(ctx)
		if l == 0 {
			if ctx.Err() == nil {
				last = errNoLeader
			}
			return false, nil
		}
		if done(st) {
			return true, nil
		}
		change := want
		if want == pb.MemberChange_MEMBER_CHANGE_PROMOTE && !slices.Contains(st.GetLearners(), uint64(node)) {
			change = pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER
		}
		cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := c.admin(l).ChangeMembers(cctx, &pb.ChangeMembersRequest{Change: change, Node: uint64(node)})
		cancel()
		if ctx.Err() != nil {
			return false, nil
		}
		switch status.Code(err) {
		case codes.OK:
			last = nil
		case codes.Unavailable, codes.FailedPrecondition, codes.DeadlineExceeded:
			last = err
		default:
			return false, err
		}
		return false, nil
	})
	if errors.Is(err, errGaveUp) && last != nil {
		err = fmt.Errorf("%w, the last answer was: %w", err, last)
	}
	if err != nil {
		return fmt.Errorf("membership of node %d did not settle: %w", node, err)
	}
	return nil
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

func (c *cluster) transfer(ctx context.Context, from, to int) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := c.admin(from).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(to)})
	return resp.GetLeader(), err
}
