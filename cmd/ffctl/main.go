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
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
)

func usage() {
	fmt.Fprintln(os.Stderr, `usage: ffctl [-endpoints a,b,c] [-read index|lease|log] command
  put KEY VALUE
  get KEY
  del KEY
  cas KEY OLD NEW        (OLD "-" means the key must not exist)
  range PREFIX
  status`)
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
	if args[0] == "status" {
		status(ctx, addrs)
		return
	}
	c, err := client.New(addrs, client.Options{})
	if err != nil {
		fail(err)
	}
	defer c.Close()
	mode := map[string]client.ReadMode{"index": client.ReadIndex, "lease": client.ReadLease, "log": client.ReadLog}[*read]
	switch {
	case args[0] == "put" && len(args) == 3:
		rev, err := c.Put(ctx, args[1], []byte(args[2]))
		check(err)
		fmt.Println("rev", rev)
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
		fmt.Printf("%s: node %d %s term %d leader %d commit %d applied %d log %d..%d snapshot %d hash %x@%d\n",
			addr, st.GetId(), st.GetState(), st.GetTerm(), st.GetLeader(), st.GetCommit(), st.GetApplied(),
			st.GetFirstIndex(), st.GetLastIndex(), st.GetSnapshotIndex(), st.GetStateHash(), st.GetHashIndex())
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
