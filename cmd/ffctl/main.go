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
}

var (
	errUsage       = errors.New("ffctl: bad command line")
	errNotPositive = errors.New("not a positive number")
)

type target struct {
	addrs []string
	mode  client.ReadMode
	c     *client.Client
}

type command struct {
	minArgs, maxArgs int
	needsClient      bool
	run              func(ctx context.Context, t target, args []string) error
}

var commands = map[string]command{
	"put":          {minArgs: 2, maxArgs: 3, needsClient: true, run: put},
	"get":          {minArgs: 1, maxArgs: 1, needsClient: true, run: get},
	"del":          {minArgs: 1, maxArgs: 1, needsClient: true, run: del},
	"cas":          {minArgs: 3, maxArgs: 3, needsClient: true, run: cas},
	"range":        {minArgs: 1, maxArgs: 1, needsClient: true, run: rangePrefix},
	"status":       {minArgs: 0, maxArgs: 0, run: status},
	"member":       {minArgs: 2, maxArgs: 2, run: member},
	"lease":        {minArgs: 2, maxArgs: 2, needsClient: true, run: lease},
	"watch":        {minArgs: 1, maxArgs: 2, needsClient: true, run: watch},
	"register":     {minArgs: 3, maxArgs: 4, needsClient: true, run: register},
	"discover":     {minArgs: 1, maxArgs: 1, needsClient: true, run: discover},
	"config":       {minArgs: 2, maxArgs: 4, needsClient: true, run: config},
	"backup":       {minArgs: 1, maxArgs: 1, run: takeBackup},
	"backup-check": {minArgs: 1, maxArgs: 1, run: backupCheck},
	"content-hash": {minArgs: 0, maxArgs: 0, needsClient: true, run: contentHash},
	"transfer":     {minArgs: 1, maxArgs: 1, run: transfer},
}

func main() {
	endpoints := flag.String("endpoints", "127.0.0.1:3151,127.0.0.1:3152,127.0.0.1:3153", "node addresses")
	var mode client.ReadMode
	flag.TextVar(&mode, "read", client.ReadIndex, "read path: index, lease or log")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout")
	flag.Usage = usage
	flag.Parse()
	err := run(target{addrs: strings.Split(*endpoints, ","), mode: mode}, *timeout, flag.Args())
	switch {
	case errors.Is(err, errUsage):
		usage()
		os.Exit(2)
	case err != nil:
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(t target, timeout time.Duration, args []string) error {
	if len(args) == 0 {
		return errUsage
	}
	cmd, ok := commands[args[0]]
	if !ok || len(args)-1 < cmd.minArgs || len(args)-1 > cmd.maxArgs {
		return errUsage
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if cmd.needsClient {
		c, err := client.New(t.addrs, client.Options{})
		if err != nil {
			return err
		}
		defer c.Close()
		t.c = c
	}
	return cmd.run(ctx, t, args[1:])
}

func positive(what, s string) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("%s %q: %w", what, s, errNotPositive)
	}
	return n, nil
}

func put(ctx context.Context, t target, args []string) error {
	var opts []client.WriteOption
	if len(args) == 3 {
		id, err := positive("lease id", args[2])
		if err != nil {
			return err
		}
		opts = append(opts, client.WithLease(id))
	}
	rev, err := t.c.Put(ctx, args[0], []byte(args[1]), opts...)
	if err != nil {
		return err
	}
	fmt.Println("rev", rev)
	return nil
}

func get(ctx context.Context, t target, args []string) error {
	kv, found, err := t.c.Get(ctx, args[0], t.mode)
	switch {
	case err != nil:
		return err
	case !found:
		fmt.Println("(missing)")
	default:
		fmt.Printf("%s mod_rev=%d version=%d\n", kv.Value, kv.ModRev, kv.Version)
	}
	return nil
}

func del(ctx context.Context, t target, args []string) error {
	deleted, err := t.c.Delete(ctx, args[0])
	if err != nil {
		return err
	}
	fmt.Println("deleted", deleted)
	return nil
}

func cas(ctx context.Context, t target, args []string) error {
	exp := client.Expect{Value: []byte(args[1]), Missing: args[1] == "-"}
	ok, cur, err := t.c.CompareAndSwap(ctx, args[0], exp, []byte(args[2]))
	if err != nil {
		return err
	}
	fmt.Println("swapped", ok, "current", strconv.Quote(string(cur.Value)))
	return nil
}

func rangePrefix(ctx context.Context, t target, args []string) error {
	kvs, err := t.c.Range(ctx, args[0], 0)
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		fmt.Printf("%s = %s\n", kv.Key, kv.Value)
	}
	return nil
}

func status(ctx context.Context, t target, _ []string) error {
	for _, addr := range t.addrs {
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
	return nil
}

func onLeader(addrs []string, call func(pb.AdminClient) error) error {
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
			return nil
		}
		last = fmt.Errorf("%s: %w", addr, err)
		if client.Reason(err) != pb.ErrorReason_ERROR_REASON_NOT_LEADER {
			break
		}
	}
	return last
}

func member(ctx context.Context, t target, args []string) error {
	change, ok := map[string]pb.MemberChange{
		"add":     pb.MemberChange_MEMBER_CHANGE_ADD_LEARNER,
		"promote": pb.MemberChange_MEMBER_CHANGE_PROMOTE,
		"remove":  pb.MemberChange_MEMBER_CHANGE_REMOVE,
	}[args[0]]
	if !ok {
		return errUsage
	}
	id, err := positive("node id", args[1])
	if err != nil {
		return err
	}
	req := &pb.ChangeMembersRequest{Change: change, Node: id}
	return onLeader(t.addrs, func(c pb.AdminClient) error {
		resp, err := c.ChangeMembers(ctx, req)
		if err == nil {
			fmt.Printf("committed, applied %d: voters %v learners %v\n", resp.GetIndex(), resp.GetVoters(), resp.GetLearners())
		}
		return err
	})
}

func transfer(ctx context.Context, t target, args []string) error {
	id, err := positive("node id", args[0])
	if err != nil {
		return err
	}
	req := &pb.TransferLeadershipRequest{To: id}
	return onLeader(t.addrs, func(c pb.AdminClient) error {
		start := time.Now()
		resp, err := c.TransferLeadership(ctx, req)
		if err != nil {
			return err
		}
		fmt.Printf("node %d leads term %d after %v\n", resp.GetLeader(), resp.GetTerm(), time.Since(start).Round(time.Millisecond))
		if resp.GetLeader() != req.GetTo() {
			return fmt.Errorf("node %d took over instead of %d", resp.GetLeader(), req.GetTo())
		}
		return nil
	})
}

func lease(ctx context.Context, t target, args []string) error {
	switch args[0] {
	case "grant":
		ttl, err := time.ParseDuration(args[1])
		if err != nil {
			return err
		}
		l, err := t.c.Grant(ctx, ttl)
		if err != nil {
			return err
		}
		fmt.Println("lease", l.ID, "ttl", l.TTL)
	case "revoke":
		id, err := positive("lease id", args[1])
		if err != nil {
			return err
		}
		if err := t.c.Revoke(ctx, id); err != nil {
			return err
		}
		fmt.Println("revoked")
	case "keepalive":
		id, err := positive("lease id", args[1])
		if err != nil {
			return err
		}
		ttl, err := t.c.KeepAliveOnce(ctx, id)
		if err != nil {
			return err
		}
		fmt.Println("kept alive for", ttl)
	case "ttl":
		id, err := positive("lease id", args[1])
		if err != nil {
			return err
		}
		remaining, ttl, keys, err := t.c.TimeToLive(ctx, id)
		if err != nil {
			return err
		}
		fmt.Printf("%v left of %v, %d keys\n", remaining.Round(time.Millisecond), ttl, keys)
	default:
		return errUsage
	}
	return nil
}

func watch(_ context.Context, t target, args []string) error {
	var from uint64
	if len(args) == 2 {
		var err error
		if from, err = positive("revision", args[1]); err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	for r := range t.c.Watch(ctx, args[0], true, from) {
		if r.CompactRev != 0 {
			return fmt.Errorf("revisions before %d are no longer kept; read the keys again and watch from there", r.CompactRev)
		}
		for _, ev := range r.Events {
			if ev.Type == client.EventDelete {
				fmt.Println(ev.KV.ModRev, "delete", ev.KV.Key)
				continue
			}
			fmt.Println(ev.KV.ModRev, "put", ev.KV.Key, strconv.Quote(string(ev.KV.Value)))
		}
	}
	return nil
}

func register(ctx context.Context, t target, args []string) error {
	ttl := 10 * time.Second
	if len(args) == 4 {
		var err error
		if ttl, err = time.ParseDuration(args[3]); err != nil {
			return err
		}
	}
	rctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reg, err := t.c.Register(ctx, args[0], client.Instance{Name: args[1], Addr: args[2]}, ttl)
	if err != nil {
		return err
	}
	fmt.Println("registered with lease", reg.Lease())
	<-rctx.Done()
	dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := t.c.Deregister(dctx, reg); err != nil {
		return err
	}
	fmt.Println("deregistered")
	return nil
}

func discover(_ context.Context, t target, args []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	seen, err := t.c.Discover(ctx, args[0])
	if err != nil {
		return err
	}
	for list := range seen {
		fmt.Println(time.Now().Format("15:04:05.000"), list)
	}
	return nil
}

func config(ctx context.Context, t target, args []string) error {
	switch {
	case args[0] == "set" && len(args) == 4:
		rev, err := t.c.SetConfig(ctx, args[1], args[2], args[3])
		if err != nil {
			return err
		}
		fmt.Println("rev", rev)
	case args[0] == "get" && len(args) == 3:
		v, found, err := t.c.GetConfig(ctx, args[1], args[2])
		switch {
		case err != nil:
			return err
		case !found:
			fmt.Println("(missing)")
		default:
			fmt.Println(v)
		}
	case args[0] == "watch" && len(args) == 2:
		wctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		cfg, err := t.c.WatchConfig(wctx, args[1])
		if err != nil {
			return err
		}
		for m := range cfg {
			fmt.Println(time.Now().Format("15:04:05.000"), m)
		}
	default:
		return errUsage
	}
	return nil
}

func takeBackup(ctx context.Context, t target, args []string) error {
	var f backup.File
	err := onLeader(t.addrs, func(c pb.AdminClient) error {
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
	if err != nil {
		return err
	}
	sum, err := backup.Check(f)
	if err != nil {
		return err
	}
	if err := backup.Save(args[0], f); err != nil {
		return err
	}
	fmt.Println("saved", args[0], "at revision", sum.Rev, "with", sum.Keys, "keys and content", fmt.Sprintf("%x", sum.Content))
	return nil
}

func backupCheck(_ context.Context, _ target, args []string) error {
	_, s, err := backup.Load(args[0])
	if err != nil {
		return err
	}
	fmt.Printf("revision %d, %d keys, %d bytes, state %x, content %x\n", s.Rev, s.Keys, s.Bytes, s.Hash, s.Content)
	return nil
}

func contentHash(ctx context.Context, t target, _ []string) error {
	kvs, err := t.c.Range(ctx, "", 0)
	if err != nil {
		return err
	}
	out := make([]kv.KeyValue, len(kvs))
	for i, k := range kvs {
		out[i] = kv.KeyValue{Key: k.Key, Value: k.Value, CreateRev: k.CreateRev, ModRev: k.ModRev, Version: k.Version, Lease: k.Lease}
	}
	fmt.Printf("%d keys, content %x\n", len(out), kv.ContentHash(out))
	return nil
}
