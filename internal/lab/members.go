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
	"github.com/mirzanajafov/fivefamilies/internal/rig"
)

var errNoLeader = errors.New("lab: no node reports a leader")

func settleMember(ctx context.Context, adm *rig.Admin, node int, want pb.MemberChange, done func(*pb.StatusResponse) bool, limit time.Duration) error {
	var last error
	err := rig.Poll(ctx, 100*time.Millisecond, limit, func(ctx context.Context) (bool, error) {
		l, st := adm.Leader(ctx)
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
		_, err := adm.Client(l).ChangeMembers(cctx, &pb.ChangeMembersRequest{Change: change, Node: uint64(node)})
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
	if errors.Is(err, rig.ErrGaveUp) && last != nil {
		err = fmt.Errorf("%w, the last answer was: %w", err, last)
	}
	if err != nil {
		return fmt.Errorf("membership of node %d did not settle: %w", node, err)
	}
	return nil
}

func removeVoter(ctx context.Context, adm *rig.Admin, node int) error {
	return settleMember(ctx, adm, node, pb.MemberChange_MEMBER_CHANGE_REMOVE, func(st *pb.StatusResponse) bool {
		return !slices.Contains(st.GetVoters(), uint64(node)) && !slices.Contains(st.GetLearners(), uint64(node))
	}, 10*time.Second)
}

func restoreVoter(ctx context.Context, adm *rig.Admin, node int) error {
	return settleMember(ctx, adm, node, pb.MemberChange_MEMBER_CHANGE_PROMOTE, func(st *pb.StatusResponse) bool {
		return slices.Contains(st.GetVoters(), uint64(node))
	}, 30*time.Second)
}

func transfer(ctx context.Context, adm *rig.Admin, from, to int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := adm.Client(from).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(to)})
	return int(resp.GetLeader()), err
}
