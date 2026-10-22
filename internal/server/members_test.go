package server

import (
	"context"
	"fmt"
	"slices"
	"strings"
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
	for attempt := 1; ; attempt++ {
		moved, err := c.admin(c.leader(time.Second)).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: 4})
		if err == nil && moved.GetLeader() == 4 {
			break
		}
		if code := status.Code(err); attempt == 5 || (err != nil && code != codes.FailedPrecondition && code != codes.Unavailable) {
			t.Fatalf("transfer %d to node 4: leader %d, %v", attempt, moved.GetLeader(), err)
		}
		t.Logf("transfer %d to node 4 left node %d leading (%v), so it goes again", attempt, moved.GetLeader(), err)
		time.Sleep(50 * time.Millisecond)
	}
	if lead := c.leader(time.Second); lead != 4 {
		t.Fatalf("node %d leads after the transfer, want 4", lead)
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

func TestATransferTheTargetNeverTakesUpEndsWithAnErrorNamingIt(t *testing.T) {
	c := newTestCluster(t, 3, func(cfg *Config) { cfg.Chaos = true })
	var err error
	for range 3 {
		old := c.leader(5 * time.Second)
		target := old%3 + 1
		chaos(t, c, target, pb.ChaosAction_CHAOS_ACTION_ISOLATE, time.Minute)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = c.admin(old).TransferLeadership(ctx, &pb.TransferLeadershipRequest{To: uint64(target)})
		cancel()
		chaos(t, c, target, pb.ChaosAction_CHAOS_ACTION_HEAL, 0)
		s := status.Convert(err)
		if s.Code() == codes.FailedPrecondition && strings.Contains(s.Message(), fmt.Sprintf("node %d did not take over", target)) {
			return
		}
		if err != nil {
			break
		}
		t.Logf("node %d took over while node %d was cut off, so the test goes again", c.leader(5*time.Second), target)
	}
	t.Fatalf("a transfer to an isolated node ended with %v, want FailedPrecondition naming it", err)
}
