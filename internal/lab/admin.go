package lab

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
	addrs []string
	conns []*grpc.ClientConn
}

func DialAdmin(addrs []string) (*Admin, error) {
	c := &Admin{addrs: addrs}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns = append(c.conns, conn)
	}
	return c, nil
}

func (c *Admin) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
}

func (c *Admin) Statuses(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return c.ask(ctx, timeout, &pb.StatusRequest{Hash: true})
}

func (c *Admin) Progress(ctx context.Context, timeout time.Duration) []*pb.StatusResponse {
	return c.ask(ctx, timeout, &pb.StatusRequest{})
}

func (c *Admin) ask(ctx context.Context, timeout time.Duration, req *pb.StatusRequest) []*pb.StatusResponse {
	out := make([]*pb.StatusResponse, len(c.conns))
	var wg sync.WaitGroup
	for i, conn := range c.conns {
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

func AgreedOn(sts []*pb.StatusResponse) int {
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

func Steady(ctx context.Context, adm *Admin) (int, error) {
	var leader int
	err := Poll(ctx, 200*time.Millisecond, 60*time.Second, func(ctx context.Context) (bool, error) {
		sts := adm.Statuses(ctx, time.Second)
		l := AgreedOn(sts)
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
