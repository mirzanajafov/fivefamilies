package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/controller"
)

func split(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

func main() {
	nodes := flag.String("nodes", "127.0.0.1:3151,127.0.0.1:3152,127.0.0.1:3153,127.0.0.1:3154,127.0.0.1:3155", "node addresses, in ID order")
	supervisors := flag.String("supervisors", "", "supervisor base URLs, in ID order (kill and pause need them)")
	listen := flag.String("listen", "127.0.0.1:3159", "address for the dashboard's API")
	hops := flag.Int("trusted-hops", 0, "X-Forwarded-For hops to trust (1 behind one proxy)")
	window := flag.Duration("window", 10*time.Second, "length of a linearizability window")
	idle := flag.Duration("idle", time.Minute, "how long the demo clients keep running after the last viewer leaves")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	c, err := controller.New(controller.Config{
		Nodes: split(*nodes), Supervisors: split(*supervisors), TrustedHops: *hops, Window: *window, Idle: *idle,
		AdminToken: os.Getenv("FF_ADMIN_TOKEN"), ChaosToken: os.Getenv("FF_CHAOS_TOKEN"), Logger: logger,
	})
	if err != nil {
		logger.Error("starting", "err", err)
		os.Exit(2)
	}
	defer c.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("listening", "err", err)
			stop()
		}
	}()
	logger.Info("controller started", "listen", *listen, "nodes", *nodes)
	c.Run(ctx)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
}
