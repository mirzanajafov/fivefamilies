package rig

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

type Admin struct {
	conns []*grpc.ClientConn
}

func DialAdmin(addrs []string) (*Admin, error) {
	a := &Admin{}
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			a.Close()
			return nil, err
		}
		a.conns = append(a.conns, conn)
	}
	return a, nil
}

func (a *Admin) Close() {
	for _, conn := range a.conns {
		conn.Close()
	}
}

func (a *Admin) Client(n int) pb.AdminClient {
	return pb.NewAdminClient(a.conns[n-1])
}

func (a *Admin) Statuses(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return a.ask(ctx, timeout, &pb.StatusRequest{Hash: true})
}

func (a *Admin) Progress(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return a.ask(ctx, timeout, &pb.StatusRequest{})
}

func (a *Admin) ask(ctx context.Context, timeout time.Duration, req *pb.StatusRequest) []*pb.StatusResponse {
	out := make([]*pb.StatusResponse, len(a.conns))
	var wg sync.WaitGroup
	for i, conn := range a.conns {
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			st, err := pb.NewAdminClient(conn).Status(cctx, req)
			if err == nil {
				out[i] = st
			}
		})
	}
	wg.Wait()
	return out
}

func LeaderOf(sts []*pb.StatusResponse) (int, uint64) {
	best, term := 0, uint64(0)
	for i, st := range sts {
		if st != nil && st.GetRole() == pb.Role_ROLE_LEADER && st.GetTerm() >= term {
			best, term = i+1, st.GetTerm()
		}
	}
	return best, term
}

func agreedOn(sts []*pb.StatusResponse) int {
	l, _ := LeaderOf(sts)
	if l == 0 {
		return 0
	}
	for _, st := range sts {
		if st == nil || st.GetLeader() != uint64(l) {
			return 0
		}
	}
	return l
}

func (a *Admin) Leader(ctx context.Context) (int, *pb.StatusResponse) {
	sts := a.Statuses(ctx, time.Second)
	l, _ := LeaderOf(sts)
	if l == 0 {
		return 0, nil
	}
	return l, sts[l-1]
}

func (a *Admin) Agreed(ctx context.Context) (int, error) {
	var leader int
	err := Poll(ctx, 100*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		leader = agreedOn(a.Statuses(ctx, time.Second))
		return leader > 0, nil
	})
	if err != nil {
		return 0, fmt.Errorf("the nodes did not agree on a leader: %w", err)
	}
	return leader, nil
}

func (a *Admin) Steady(ctx context.Context) (int, error) {
	var leader int
	err := Poll(ctx, 200*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		sts := a.Statuses(ctx, time.Second)
		l := agreedOn(sts)
		if l == 0 {
			return false, nil
		}
		for _, st := range sts {
			if st.GetCommit() != sts[l-1].GetCommit() {
				return false, nil
			}
		}
		leader = l
		return true, nil
	})
	if err != nil {
		return 0, fmt.Errorf("the cluster did not settle: %w", err)
	}
	return leader, nil
}
