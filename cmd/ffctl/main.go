package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
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
  watch PREFIX [REV]     (runs until interrupted)
  register SERVICE INSTANCE ADDR [TTL]   (keeps the instance registered until interrupted)
  discover SERVICE       (prints the instances on every change, until interrupted)
  config set SERVICE KEY VALUE | get SERVICE KEY | watch SERVICE
  backup FILE            (a consistent copy of the state, checked before it is kept)
  backup-check FILE
  content-hash           (key count and a hash of every key, to compare with a backup)
  transfer ID`)
	os.Exit(2)
}

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:3151,127.0.0.1:3152,127.0.0.1:3153", "node addresses")
	var mode client.ReadMode
	flag.TextVar(&mode, "read", client.ReadIndex, "read path: index, lease or log")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = usage
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
	case args[0] == "backup" && len(args) == 2:
		takeBackup(ctx, addrs, args[1])
		return
	case args[0] == "backup-check" && len(args) == 2:
		_, s, err := backup.Load(args[1])
		check(err)
		fmt.Printf("revision %d, %d keys, %d bytes, state %x, content %x\n", s.Rev, s.Keys, s.Bytes, s.Hash, s.Content)
		return
	}
	c, err := client.New(addrs, client.Options{})
	if err != nil {
		fail(err)
	}
	defer c.Close()
	switch {
	case args[0] == "put" && (len(args) == 3 || len(args) == 4):
		var opts []client.WriteOption
		if len(args) == 4 {
			opts = append(opts, client.WithLease(nodeID(args[3])))
		}
		rev, err := c.Put(ctx, args[1], []byte(args[2]), opts...)
		check(err)
		fmt.Println("rev", rev)
	case args[0] == "register" && (len(args) == 4 || len(args) == 5):
		ttl := 10 * time.Second
		if len(args) == 5 {
			var err error
			ttl, err = time.ParseDuration(args[4])
			check(err)
		}
		rctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		reg, err := c.Register(ctx, args[1], client.Instance{Name: args[2], Addr: args[3]}, ttl)
		check(err)
		fmt.Println("registered with lease", reg.Lease())
		<-rctx.Done()
		dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		check(c.Deregister(dctx, reg))
		fmt.Println("deregistered")
	case args[0] == "discover" && len(args) == 2:
		dctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		seen, err := c.Discover(dctx, args[1])
		check(err)
		for list := range seen {
			fmt.Println(time.Now().Format("15:04:05.000"), list)
		}
	case args[0] == "config" && len(args) >= 3:
		config(ctx, c, args[1:])
	case args[0] == "watch" && (len(args) == 2 || len(args) == 3):
		var from uint64
		if len(args) == 3 {
			from = nodeID(args[2])
		}
		wctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		watchPrefix(wctx, c, args[1], from)
	case args[0] == "content-hash" && len(args) == 1:
		kvs, err := c.Range(ctx, "", 0)
		check(err)
		contentHash(kvs)
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
		st, err := pb.NewAdminClient(conn).Status(cctx, &pb.StatusRequest{Hash: true})
		cancel()
		conn.Close()
		if err != nil {
			fmt.Printf("%s: %v\n", addr, err)
			continue
		}
		fmt.Printf("%s: node %d %s term %d leader %d commit %d applied %d log %d..%d snapshot %d hash %x@%d voters %v learners %v\n",
			addr, st.GetId(), client.RoleName(st.GetRole()), st.GetTerm(), st.GetLeader(), st.GetCommit(), st.GetApplied(),
			st.GetFirstIndex(), st.GetLastIndex(), st.GetSnapshotIndex(), st.GetStateHash(), st.GetHashIndex(), st.GetVoters(), st.GetLearners())
	}
}

func onLeader(addrs []string, call func(pb.AdminClient) error) {
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
		if client.Reason(err) != pb.ErrorReason_ERROR_REASON_NOT_LEADER {
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
	onLeader(addrs, func(c pb.AdminClient) error {
		resp, err := c.ChangeMembers(ctx, req)
		if err == nil {
			fmt.Printf("committed, applied %d: voters %v learners %v\n", resp.GetIndex(), resp.GetVoters(), resp.GetLearners())
		}
		return err
	})
}

func transfer(ctx context.Context, addrs []string, id string) {
	req := &pb.TransferLeadershipRequest{To: nodeID(id)}
	onLeader(addrs, func(c pb.AdminClient) error {
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

func watchPrefix(ctx context.Context, c *client.Client, prefix string, from uint64) {
	for r := range c.Watch(ctx, prefix, true, from) {
		if r.CompactRev != 0 {
			fail(fmt.Errorf("revisions before %d are no longer kept; read the keys again and watch from there", r.CompactRev))
		}
		for _, ev := range r.Events {
			if ev.Type == client.EventDelete {
				fmt.Println(ev.KV.ModRev, "delete", ev.KV.Key)
				continue
			}
			fmt.Println(ev.KV.ModRev, "put", ev.KV.Key, strconv.Quote(string(ev.KV.Value)))
		}
	}
}

func config(ctx context.Context, c *client.Client, args []string) {
	switch {
	case args[0] == "set" && len(args) == 4:
		rev, err := c.SetConfig(ctx, args[1], args[2], args[3])
		check(err)
		fmt.Println("rev", rev)
	case args[0] == "get" && len(args) == 3:
		v, found, err := c.GetConfig(ctx, args[1], args[2])
		check(err)
		if !found {
			fmt.Println("(missing)")
			return
		}
		fmt.Println(v)
	case args[0] == "watch" && len(args) == 2:
		wctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		cfg, err := c.WatchConfig(wctx, args[1])
		check(err)
		for m := range cfg {
			fmt.Println(time.Now().Format("15:04:05.000"), m)
		}
	default:
		usage()
	}
}

func takeBackup(ctx context.Context, addrs []string, path string) {
	var f backup.File
	onLeader(addrs, func(c pb.AdminClient) error {
		stream, err := c.Backup(ctx, &pb.BackupRequest{})
		if err != nil {
			return err
		}
		f = backup.File{}
		for first := true; ; first = false {
			chunk, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			if first {
				f.Rev, f.Hash, f.Data = chunk.GetRev(), chunk.GetHash(), make([]byte, 0, chunk.GetSize())
			}
			f.Data = append(f.Data, chunk.GetData()...)
		}
	})
	sum, err := backup.Check(f)
	check(err)
	check(backup.Save(path, f))
	fmt.Println("saved", path, "at revision", sum.Rev, "with", sum.Keys, "keys and content", fmt.Sprintf("%x", sum.Content))
}

func contentHash(kvs []client.KeyValue) {
	out := make([]kv.KeyValue, len(kvs))
	for i, k := range kvs {
		out[i] = kv.KeyValue{Key: k.Key, Value: k.Value, CreateRev: k.CreateRev, ModRev: k.ModRev, Version: k.Version, Lease: k.Lease}
	}
	fmt.Printf("%d keys, content %x\n", len(out), kv.ContentHash(out))
}
