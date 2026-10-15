package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/server"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
)

var plantable string

func parseCluster(s string) (map[raft.NodeID]string, error) {
	nodes := map[raft.NodeID]string{}
	for part := range strings.SplitSeq(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return nil, fmt.Errorf("cluster entry %q is not id=host:port", part)
		}
		n, err := strconv.ParseUint(id, 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("cluster entry %q has a bad id", part)
		}
		nodes[raft.NodeID(n)] = addr
	}
	return nodes, nil
}

func ticks(d, tick time.Duration) int {
	return int((d + tick - 1) / tick)
}

func main() {
	id := flag.Uint64("id", 0, "this node's ID")
	cluster := flag.String("cluster", "", "every node as id=host:port, comma separated")
	listen := flag.String("listen", "", "address to listen on (default: this node's port from -cluster, on all interfaces)")
	data := flag.String("data", "data", "data directory")
	tick := flag.Duration("tick", 10*time.Millisecond, "tick interval")
	heartbeat := flag.Duration("heartbeat", 100*time.Millisecond, "heartbeat interval")
	election := flag.Duration("election", time.Second, "minimum election timeout")
	lease := flag.Duration("lease", 0, "leader lease for lease reads (0 turns them off)")
	snapEvery := flag.Uint64("snapshot-every", 10000, "applied entries between snapshots")
	retain := flag.Uint64("retain", 5000, "entries kept behind a snapshot for slow followers")
	segment := flag.Int64("segment-bytes", 64<<20, "log segment size")
	bugs := flag.String("bug", "", "planted bugs (only in builds made for testing)")
	level := flag.String("log-level", "info", "debug, info, warn or error")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*level)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	nodes, err := parseCluster(*cluster)
	if err != nil {
		logger.Error("bad -cluster", "err", err)
		os.Exit(2)
	}
	planted, err := raft.ParseBugs(*bugs)
	if err != nil || (planted != 0 && plantable != "yes") {
		logger.Error("planted bugs are only available in test builds", "bug", *bugs, "err", err)
		os.Exit(2)
	}
	addr := *listen
	if addr == "" {
		_, port, err := net.SplitHostPort(nodes[raft.NodeID(*id)])
		if err != nil {
			logger.Error("no address for this node in -cluster", "id", *id)
			os.Exit(2)
		}
		addr = ":" + port
	}
	cfg := server.Config{
		ID:            raft.NodeID(*id),
		Nodes:         nodes,
		DataDir:       *data,
		Tick:          *tick,
		HeartbeatTick: ticks(*heartbeat, *tick),
		ElectionTick:  ticks(*election, *tick),
		LeaseTicks:    ticks(*lease, *tick),
		SnapshotEvery: *snapEvery,
		Retain:        *retain,
		SegmentBytes:  *segment,
		Bugs:          planted,
		Logger:        logger,
	}
	srv, err := server.New(cfg)
	if err != nil {
		logger.Error("starting", "err", err)
		os.Exit(1)
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("listening", "addr", addr, "err", err)
		os.Exit(1)
	}
	g := grpc.NewServer(transport.ServerOptions()...)
	srv.Register(g)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := g.Serve(ln); err != nil {
			logger.Error("serving", "err", err)
		}
	}()
	logger.Info("node started", "id", *id, "addr", addr, "nodes", len(nodes), "election", *election, "lease", *lease)
	srv.Run(ctx)
	g.GracefulStop()
	logger.Info("node stopped")
}
