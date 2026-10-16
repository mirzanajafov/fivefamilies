package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: ffctl [-endpoints a,b,c] [-read index|lease|log] command
  put KEY VALUE [LEASE]
  get KEY
  del KEY
  cas KEY OLD NEW        (OLD "-" means the key must not exist)
  range PREFIX
  status
  member add|promote|remove ID
  lease grant TTL | revoke ID | keepalive ID | ttl ID
  transfer ID`)
	os.Exit(2)
}

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:3151,127.0.0.1:3152,127.0.0.1:3153", "node addresses")
	read := flag.String("read", "index", "read path: index, lease or log")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
	}
	addrs := strings.Split(*endpoints, ",")
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	switch {
	case args[0] == "status":
		status(ctx, addrs)
		return
	case args[0] == "member" && len(args) == 3:
		member(ctx, addrs, args[1], args[2])
		return
	case args[0] == "transfer" && len(args) == 2:
		transfer(ctx, addrs, args[1])
		return
	}
	c, err := client.New(addrs, client.Options{})
	if err != nil {
		fail(err)
	}
	defer c.Close()
	mode := map[string]client.ReadMode{"index": client.ReadIndex, "lease": client.ReadLease, "log": client.ReadLog}[*read]
	switch {
	case args[0] == "put" && (len(args) == 3 || len(args) == 4):
		var opts []client.WriteOption
		if len(args) == 4 {
			opts = append(opts, client.WithLease(nodeID(args[3])))
		}
		rev, err := c.Put(ctx, args[1], []byte(args[2]), opts...)
		check(err)
		fmt.Println("rev", rev)
	case args[0] == "lease" && len(args) == 3:
		lease(ctx, c, args[1], args[2])
	case args[0] == "get" && len(args) == 2:
		kv, found, err := c.Get(ctx, args[1], mode)
		check(err)
		if !found {
			fmt.Println("(missing)")
			return
		}
		fmt.Printf("%s mod_rev=%d version=%d\n", kv.Value, kv.ModRev, kv.Version)
	case args[0] == "del" && len(args) == 2:
		deleted, err := c.Delete(ctx, args[1])
		check(err)
		fmt.Println("deleted", deleted)
	case args[0] == "cas" && len(args) == 4:
		exp := client.Expect{Value: []byte(args[2]), Missing: args[2] == "-"}
		ok, cur, err := c.CompareAndSwap(ctx, args[1], exp, []byte(args[3]))
		check(err)
		fmt.Println("swapped", ok, "current", strconv.Quote(string(cur.Value)))
	case args[0] == "range" && len(args) == 2:
		kvs, err := c.Range(ctx, args[1], 0)
		check(err)
		for _, kv := range kvs {
			fmt.Printf("%s = %s\n", kv.Key, kv.Value)
		}
	default:
		usage()
	}
}

func status(ctx context.Context, addrs []string) {
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("%s: %v\n", addr, err)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := pb.NewAdminClient(conn).Status(cctx, &pb.StatusRequest{})
		cancel()
		conn.Close()
		if err != nil {
			fmt.Printf("%s: %v\n", addr, err)
			continue
		}
		fmt.Printf("%s: node %d %s term %d leader %d commit %d applied %d log %d..%d snapshot %d hash %x@%d voters %v learners %v\n",
			addr, st.GetId(), st.GetState(), st.GetTerm(), st.GetLeader(), st.GetCommit(), st.GetApplied(),
			st.GetFirstIndex(), st.GetLastIndex(), st.GetSnapshotIndex(), st.GetStateHash(), st.GetHashIndex(), st.GetVoters(), st.GetLearners())
	}
}

func onLeader(ctx context.Context, addrs []string, call func(pb.AdminClient) error) {
	var last error
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			last = err
			continue
		}
		err = call(pb.NewAdminClient(conn))
		conn.Close()
		if err == nil {
			return
		}
		last = fmt.Errorf("%s: %w", addr, err)
		if s, ok := grpcstatus.FromError(err); !ok || s.Code() != codes.Unavailable || s.Message() != "not the leader" {
			break
		}
	}
	fail(last)
}

func nodeID(s string) uint64 {
	id, err := strconv.ParseUint(s, 10, 64)
	if err != nil || id == 0 {
		fail(fmt.Errorf("bad node id %q", s))
	}
	return id
}

func member(ctx context.Context, addrs []string, op, id string) {
	change, ok := map[string]pb.MemberChange{
		"add":     pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER,
		"promote": pb.MemberChange_MEMBER_CHANGE_PROMOTE,
		"remove":  pb.MemberChange_MEMBER_CHANGE_REMOVE,
	}[op]
	if !ok {
		usage()
	}
	req := &pb.ChangeMembersRequest{Change: change, Node: nodeID(id)}
	onLeader(ctx, addrs, func(c pb.AdminClient) error {
		resp, err := c.ChangeMembers(ctx, req)
		if err == nil {
			fmt.Printf("committed, applied %d: voters %v learners %v\n", resp.GetIndex(), resp.GetVoters(), resp.GetLearners())
		}
		return err
	})
}

func transfer(ctx context.Context, addrs []string, id string) {
	req := &pb.TransferLeadershipRequest{To: nodeID(id)}
	onLeader(ctx, addrs, func(c pb.AdminClient) error {
		start := time.Now()
		resp, err := c.TransferLeadership(ctx, req)
		if err == nil {
			fmt.Printf("node %d leads term %d after %v\n", resp.GetLeader(), resp.GetTerm(), time.Since(start).Round(time.Millisecond))
		}
		return err
	})
}

func lease(ctx context.Context, c *client.Client, op, arg string) {
	switch op {
	case "grant":
		ttl, err := time.ParseDuration(arg)
		check(err)
		l, err := c.Grant(ctx, ttl)
		check(err)
		fmt.Println("lease", l.ID, "ttl", l.TTL)
	case "revoke":
		check(c.Revoke(ctx, nodeID(arg)))
		fmt.Println("revoked")
	case "keepalive":
		ttl, err := c.KeepAliveOnce(ctx, nodeID(arg))
		check(err)
		fmt.Println("kept alive for", ttl)
	case "ttl":
		remaining, ttl, keys, err := c.TimeToLive(ctx, nodeID(arg))
		check(err)
		fmt.Printf("%v left of %v, %d keys\n", remaining.Round(time.Millisecond), ttl, keys)
	default:
		usage()
	}
}

func check(err error) {
	if err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
