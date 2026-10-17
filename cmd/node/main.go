package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/server"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
)

var plantable string

func serveProfiles(addr string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	if err := http.ListenAndServe(addr, mux); err != nil {
		logger.Error("serving profiles", "addr", addr, "err", err)
	}
}

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

func parseIDs(s string) ([]raft.NodeID, error) {
	if s == "" {
		return nil, nil
	}
	var ids []raft.NodeID
	for part := range strings.SplitSeq(s, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(part), 10, 64)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("bad node id %q", part)
		}
		ids = append(ids, raft.NodeID(n))
	}
	return ids, nil
}

func ticks(d, tick time.Duration) int {
	return int((d + tick - 1) / tick)
}

func main() {
	id := flag.Uint64("id", 0, "this node's ID")
	cluster := flag.String("cluster", "", "every node as id=host:port, comma separated")
	voterList := flag.String("voters", "", "the voters a fresh cluster starts with (default: every node in -cluster); a node outside it waits to be added")
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
	fsync := flag.String("fsync", "group", "group (one fsync for everything queued), ready (one per write), entry (one per entry), or off (unsafe, test builds only)")
	syncStorage := flag.Bool("sync-storage", false, "wait for the disk inside the event loop, for measuring what asynchronous storage buys")
	writeFirst := flag.Bool("write-before-send", false, "the leader writes entries before sending them, instead of in parallel")
	maxInflight := flag.Int("max-inflight", 0, "append messages in flight per follower (1 turns pipelining off)")
	maxMsgBytes := flag.Int("max-msg-bytes", 0, "bytes of entries per append message (1 sends one entry per message)")
	batch := flag.Int("batch", 0, "events the loop takes before each Ready (default 256; 1 handles every event on its own)")
	maxPending := flag.Uint64("max-pending", 4096, "uncommitted entries a leader holds before it refuses new writes (0 never refuses)")
	restore := flag.String("restore", "", "start a new cluster from this backup file when the data directory is empty (every node needs the same file)")
	chaos := flag.Bool("chaos", false, "accept chaos requests (stall-disk, pause, isolate, crash) on the admin API")
	level := flag.String("log-level", "info", "debug, info, warn or error")
	profile := flag.String("pprof", "", "serve Go profiles on this address (off by default; for the lab, never published in production)")
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
	voters, err := parseIDs(*voterList)
	if err != nil {
		logger.Error("bad -voters", "err", err)
		os.Exit(2)
	}
	planted, err := raft.ParseBugs(*bugs)
	if err != nil || (planted != 0 && plantable != "yes") {
		logger.Error("planted bugs are only available in test builds", "bug", *bugs, "err", err)
		os.Exit(2)
	}
	if *fsync == server.FsyncOff && plantable != "yes" {
		logger.Error("turning fsync off is only available in test builds")
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
		ID:              raft.NodeID(*id),
		Nodes:           nodes,
		Voters:          voters,
		DataDir:         *data,
		Tick:            *tick,
		HeartbeatTick:   ticks(*heartbeat, *tick),
		ElectionTick:    ticks(*election, *tick),
		LeaseTicks:      ticks(*lease, *tick),
		SnapshotEvery:   *snapEvery,
		Retain:          *retain,
		SegmentBytes:    *segment,
		Bugs:            planted,
		Logger:          logger,
		FsyncMode:       *fsync,
		SyncStorage:     *syncStorage,
		WriteBeforeSend: *writeFirst,
		MaxInflight:     *maxInflight,
		MaxMsgBytes:     *maxMsgBytes,
		Batch:           *batch,
		MaxPending:      *maxPending,
		Chaos:           *chaos,
	}
	if *restore != "" {
		f, sum, err := backup.Load(*restore)
		if err != nil {
			logger.Error("reading the backup", "file", *restore, "err", err)
			os.Exit(1)
		}
		ids := voters
		if len(ids) == 0 {
			for id := range nodes {
				ids = append(ids, id)
			}
		}
		done, err := server.Restore(*data, ids, f)
		if err != nil {
			logger.Error("restoring", "err", err)
			os.Exit(1)
		}
		if done {
			logger.Info("restored a backup", "rev", sum.Rev, "keys", sum.Keys)
		} else {
			logger.Info("the data directory already holds a log; the backup is not used", "file", *restore)
		}
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
	if *profile != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(int(100 * time.Microsecond))
		go serveProfiles(*profile, logger)
	}
	logger.Info("node started", "id", *id, "addr", addr, "nodes", len(nodes), "election", *election, "lease", *lease)
	srv.Run(ctx)
	g.GracefulStop()
	logger.Info("node stopped")
}
