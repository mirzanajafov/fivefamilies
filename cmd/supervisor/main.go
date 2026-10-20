package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/supervisor"
)

func main() {
	listen := flag.String("listen", ":3170", "address for the chaos endpoint")
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: supervisor [-listen addr] -- node [args...]")
		os.Exit(2)
	}
	token := os.Getenv("FF_CHAOS_TOKEN")
	s := supervisor.New(args)
	if err := s.Start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: *listen, Handler: supervisor.Handler(s, token), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "chaos endpoint:", err)
		}
	}()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, os.Interrupt)
	code := 0
	select {
	case sig := <-sigs:
		if err := s.Stop(sig); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for s.State().Running && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
	case err := <-s.Failed():
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	if err := srv.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "chaos endpoint:", err)
	}
	os.Exit(code)
}
