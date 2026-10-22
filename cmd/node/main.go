package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/backup"
	"github.com/mirzanajafov/fivefamilies/internal/httpapi"
	"github.com/mirzanajafov/fivefamilies/internal/planted"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
	"github.com/mirzanajafov/fivefamilies/internal/server"
	"github.com/mirzanajafov/fivefamilies/internal/transport"
)

var plantable string

func goProcs(asked, picked int) int {
	if asked > 0 {
		return asked
	}
	return max(2, picked)
}

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

func roundFrom(self raft.NodeID, nodes map[raft.NodeID]string) []string {
	ids := slices.Sorted(maps.Keys(nodes))
	at := slices.Index(ids, self)
	addrs := make([]string, 0, len(ids))
	for _, id := range slices.Concat(ids[at:], ids[:at]) {
		addrs = append(addrs, nodes[id])
	}
	return addrs
}

func serveAPI(addr string, self raft.NodeID, nodes map[raft.NodeID]string, srv *server.Server, logger *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	cl, err := client.New(roundFrom(self, nodes), client.Options{})
	if err != nil {
		return nil, errors.Join(err, ln.Close())
	}
	hs := httpapi.NewServer(cl, srv.Status)
	go func() {
		if err := hs.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serving the HTTP API", "addr", addr, "err", err)
		}
	}()
	return func() {
		if err := errors.Join(hs.Stop(), cl.Close()); err != nil {
			logger.Warn("stopping the HTTP API", "err", err)
		}
	}, nil
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

type nodeFlags struct {
	id                              uint64
	cluster, voters, listen, data   string
	tick, heartbeat, election       time.Duration
	lease                           time.Duration
	snapEvery, retain               uint64
	segment                         int64
	bugs, fsync                     string
	syncStorage, writeFirst, chaos  bool
	maxInflight, maxMsgBytes, batch int
	maxPending                      uint64
	restore                         string
	level                           slog.Level
	profile, http                   string
	procs                           int
}

func parseFlags() nodeFlags {
	var f nodeFlags
	flag.Uint64Var(&f.id, "id", 0, "this node's ID")
	flag.StringVar(&f.cluster, "cluster", "", "every node as id=host:port, comma separated")
	flag.StringVar(&f.voters, "voters", "", "the voters a fresh cluster starts with (default: every node in -cluster); a node outside it waits to be added")
	flag.StringVar(&f.listen, "listen", "", "address to listen on (default: this node's port from -cluster, on all interfaces)")
	flag.StringVar(&f.data, "data", "data", "data directory")
	flag.DurationVar(&f.tick, "tick", 10*time.Millisecond, "tick interval")
	flag.DurationVar(&f.heartbeat, "heartbeat", 100*time.Millisecond, "heartbeat interval")
	flag.DurationVar(&f.election, "election", time.Second, "minimum election timeout")
	flag.DurationVar(&f.lease, "lease", 0, "leader lease for lease reads (0 turns them off)")
	flag.Uint64Var(&f.snapEvery, "snapshot-every", 10000, "applied entries between snapshots")
	flag.Uint64Var(&f.retain, "retain", 5000, "entries kept behind a snapshot for slow followers")
	flag.Int64Var(&f.segment, "segment-bytes", 64<<20, "log segment size")
	flag.StringVar(&f.bugs, "bug", "", "planted bugs (only in builds made for testing)")
	flag.StringVar(&f.fsync, "fsync", "group", "group (one fsync for everything queued), ready (one per write), entry (one per entry), or off (unsafe, test builds only)")
	flag.BoolVar(&f.syncStorage, "sync-storage", false, "wait for the disk inside the event loop, for measuring what asynchronous storage buys")
	flag.BoolVar(&f.writeFirst, "write-before-send", false, "the leader writes entries before sending them, instead of in parallel")
	flag.IntVar(&f.maxInflight, "max-inflight", 0, "append messages in flight per follower (1 turns pipelining off)")
	flag.IntVar(&f.maxMsgBytes, "max-msg-bytes", 0, "bytes of entries per append message (1 sends one entry per message)")
	flag.IntVar(&f.batch, "batch", 0, "events the loop takes before each Ready (default 256; 1 handles every event on its own)")
	flag.Uint64Var(&f.maxPending, "max-pending", 4096, "uncommitted entries a leader holds before it refuses new writes (0 never refuses)")
	flag.StringVar(&f.restore, "restore", "", "start a new cluster from this backup file when the data directory is empty (every node needs the same file)")
	flag.BoolVar(&f.chaos, "chaos", false, "accept chaos requests (stall-disk, pause, isolate, heal) on the admin API")
	flag.TextVar(&f.level, "log-level", slog.LevelInfo, "debug, info, warn or error")
	flag.StringVar(&f.profile, "pprof", "", "serve Go profiles on this address (off by default; for the lab, never published in production)")
	flag.IntVar(&f.procs, "gomaxprocs", 0, "Go processors (default: what the runtime picks, but at least 2)")
	flag.StringVar(&f.http, "http", "", "serve the HTTP registry API on this address (off by default)")
	flag.Parse()
	return f
}

func (f nodeFlags) config(logger *slog.Logger) (server.Config, string, error) {
	nodes, err := parseCluster(f.cluster)
	if err != nil {
		return server.Config{}, "", fmt.Errorf("-cluster: %w", err)
	}
	voters, err := parseIDs(f.voters)
	if err != nil {
		return server.Config{}, "", fmt.Errorf("-voters: %w", err)
	}
	bugs, err := planted.Parse(f.bugs)
	switch {
	case err != nil:
		return server.Config{}, "", fmt.Errorf("-bug: %w", err)
	case bugs != 0 && plantable != "yes":
		return server.Config{}, "", fmt.Errorf("-bug %s: planted bugs are only available in test builds", f.bugs)
	case f.fsync == server.FsyncOff && plantable != "yes":
		return server.Config{}, "", errors.New("-fsync off is only available in test builds")
	}
	addr := f.listen
	if addr == "" {
		_, port, err := net.SplitHostPort(nodes[raft.NodeID(f.id)])
		if err != nil {
			return server.Config{}, "", fmt.Errorf("-cluster has no address for node %d", f.id)
		}
		addr = ":" + port
	}
	return server.Config{
		ID:              raft.NodeID(f.id),
		Nodes:           nodes,
		Voters:          voters,
		DataDir:         f.data,
		Tick:            f.tick,
		HeartbeatTick:   ticks(f.heartbeat, f.tick),
		ElectionTick:    ticks(f.election, f.tick),
		LeaseTicks:      ticks(f.lease, f.tick),
		SnapshotEvery:   f.snapEvery,
		Retain:          f.retain,
		SegmentBytes:    f.segment,
		Bugs:            bugs,
		Logger:          logger,
		FsyncMode:       f.fsync,
		SyncStorage:     f.syncStorage,
		WriteBeforeSend: f.writeFirst,
		MaxInflight:     f.maxInflight,
		MaxMsgBytes:     f.maxMsgBytes,
		Batch:           f.batch,
		MaxPending:      f.maxPending,
		Chaos:           f.chaos,
	}, addr, nil
}

func restoreBackup(file string, cfg server.Config) error {
	f, sum, err := backup.Load(file)
	if err != nil {
		return fmt.Errorf("reading the backup %s: %w", file, err)
	}
	ids := cfg.Voters
	if len(ids) == 0 {
		ids = slices.Collect(maps.Keys(cfg.Nodes))
	}
	done, err := server.Restore(cfg.DataDir, ids, f)
	if err != nil {
		return fmt.Errorf("restoring: %w", err)
	}
	if done {
		cfg.Logger.Info("restored a backup", "rev", sum.Rev, "keys", sum.Keys)
	} else {
		cfg.Logger.Info("the data directory already holds a log; the backup is not used", "file", file)
	}
	return nil
}

type node struct {
	srv      *server.Server
	grpc     *grpc.Server
	ln       net.Listener
	closeAPI func()
	logger   *slog.Logger
}

func start(f nodeFlags, cfg server.Config, addr string) (*node, error) {
	if f.restore != "" {
		if err := restoreBackup(f.restore, cfg); err != nil {
			return nil, err
		}
	}
	srv, err := server.New(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listening: %w", err)
	}
	n := &node{srv: srv, grpc: grpc.NewServer(transport.ServerOptions()...), ln: ln, closeAPI: func() {}, logger: cfg.Logger}
	if f.http != "" {
		if n.closeAPI, err = serveAPI(f.http, cfg.ID, cfg.Nodes, srv, cfg.Logger); err != nil {
			return nil, fmt.Errorf("serving the HTTP API: %w", err)
		}
	}
	srv.Register(n.grpc)
	return n, nil
}

func (n *node) serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		if err := n.grpc.Serve(n.ln); err != nil {
			n.logger.Error("serving", "err", err)
		}
	}()
	running, stopRunning := context.WithCancel(context.Background())
	go func() {
		<-ctx.Done()
		stop()
		n.closeAPI()
		stopRunning()
	}()
	err := n.srv.Run(running)
	stop()
	if err != nil {
		n.grpc.Stop()
		return err
	}
	n.grpc.GracefulStop()
	return nil
}

func main() {
	f := parseFlags()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: f.level}))
	cfg, addr, err := f.config(logger)
	if err != nil {
		logger.Error("bad flags", "err", err)
		os.Exit(2)
	}
	n, err := start(f, cfg, addr)
	if err != nil {
		logger.Error("starting", "err", err)
		os.Exit(1)
	}
	runtime.GOMAXPROCS(goProcs(f.procs, runtime.GOMAXPROCS(0)))
	if f.profile != "" {
		runtime.SetMutexProfileFraction(10)
		runtime.SetBlockProfileRate(int(100 * time.Microsecond))
		go serveProfiles(f.profile, logger)
	}
	logger.Info("node started", "id", f.id, "addr", addr, "http", f.http, "nodes", len(cfg.Nodes), "election", f.election, "lease", f.lease)
	if err := n.serve(); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(70)
	}
	logger.Info("node stopped")
}
