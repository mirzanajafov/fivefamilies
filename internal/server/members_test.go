package server

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

func (c *testCluster) admin(id raft.NodeID) pb.AdminClient {
	c.t.Helper()
	conn, err := grpc.NewClient(c.nodes[id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.t.Fatal(err)
	}
	c.t.Cleanup(func() { conn.Close() })
	return pb.NewAdminClient(conn)
}

func (c *testCluster) change(ctx context.Context, change pb.MemberChange, node raft.NodeID) *pb.ChangeMembersResponse {
	c.t.Helper()
	for {
		lead := c.leader(5 * time.Second)
		resp, err := c.admin(lead).ChangeMembers(ctx, &pb.ChangeMembersRequest{Change: change, Node: uint64(node)})
		if err == nil {
			return resp
		}
		if code := status.Code(err); code != codes.Unavailable && code != codes.FailedPrecondition {
			c.t.Fatalf("%v %d: %v", change, node, err)
		}
		if ctx.Err() != nil {
			c.t.Fatalf("%v %d: %v", change, node, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func uint64s(ids ...raft.NodeID) []uint64 {
	out := make([]uint64, len(ids))
	for i, id := range ids {
		out[i] = uint64(id)
	}
	return out
}

func TestMembersChangeAndLeadershipMovesOverTheWire(t *testing.T) {
	c := newTestCluster(t, 4, func(cfg *Config) { cfg.Voters = []raft.NodeID{1, 2, 3} })
	old := c.leader(5 * time.Second)
	cl, err := client.New(c.addrs(), client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range 30 {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	if st := c.status(4); len(st.GetVoters()) != 3 || st.GetRole() != pb.Role_ROLE_FOLLOWER || st.GetTerm() != 0 {
		t.Fatalf("the spare node should wait quietly: %v", st)
	}
	resp := c.change(ctx, pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER, 4)
	if !slices.Equal(resp.GetLearners(), uint64s(4)) {
		t.Fatalf("after adding a learner: %v", resp)
	}
	resp = c.change(ctx, pb.MemberChange_MEMBER_CHANGE_PROMOTE, 4)
	if !slices.Equal(resp.GetVoters(), uint64s(1, 2, 3, 4)) || len(resp.GetLearners()) != 0 {
		t.Fatalf("after the promotion: %v", resp)
	}
	if _, err := c.admin(old).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: 4}); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	if lead := c.leader(time.Second); lead != 4 {
		t.Fatalf("leader %d after the transfer", lead)
	}
	if _, err := c.admin(4).ChangeMembers(ctx, &pb.ChangeMembersRequest{Change: pb.MemberChange_MEMBER_CHANGE_REMOVE, Node: 4}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("the leader removing itself: %v", err)
	}
	resp = c.change(ctx, pb.MemberChange_MEMBER_CHANGE_REMOVE, old)
	want := slices.DeleteFunc(uint64s(1, 2, 3, 4), func(id uint64) bool { return id == uint64(old) })
	if !slices.Equal(resp.GetVoters(), want) {
		t.Fatalf("after removing %d: %v", old, resp)
	}
	c.stop(old)
	for i := range 30 {
		if _, err := cl.Put(ctx, fmt.Sprintf("k%d", i), []byte("again")); err != nil {
			t.Fatalf("write %d without the removed node: %v", i, err)
		}
	}
	kv, found, err := cl.Get(ctx, "k29", client.ReadIndex)
	if err != nil || !found || string(kv.Value) != "again" {
		t.Fatalf("read after the changes: %q %v %v", kv.Value, found, err)
	}
	c.stop(4)
	c.start(4)
	if st := c.status(4); !slices.Equal(st.GetVoters(), want) {
		t.Fatalf("node 4 forgot the configuration across a restart: %v", st.GetVoters())
	}
}

func TestATransferEndsWhenANodeOtherThanTheTargetTakesOver(t *testing.T) {
	c := newTestCluster(t, 5, func(cfg *Config) { cfg.Chaos = true })
	old := c.leader(5 * time.Second)
	target := old%5 + 1
	chaos(t, c, target, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
	admin := c.admin(old)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := admin.TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(target)})
		done <- err
	}()
	for c.running[old].srv.Status().GetTransferee() != uint64(target) {
		if ctx.Err() != nil {
			t.Fatal("the transfer never started")
		}
		time.Sleep(time.Millisecond)
	}
	chaos(t, c, old, pb.ChaosAction_CHAOS_ACTION_PAUSE, time.Second)
	start := time.Now()
	err := <-done
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("the transfer ended with %v after %v, want FailedPrecondition once another node leads", err, time.Since(start).Round(time.Millisecond))
	}
	if lead := c.leader(5 * time.Second); lead == old || lead == target {
		t.Fatalf("node %d leads, want one of the nodes that were neither paused nor isolated", lead)
	}
}
