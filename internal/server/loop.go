package server

import (
	"context"
	"errors"
	"time"

	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
	"github.com/mirzanajafov/fivefamilies/internal/raft"
)

var ErrStopped = errors.New("server: the node is stopping")

func (s *Server) loop(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.Tick)
	defer ticker.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.catchUp()
			s.tendWatches()
		case m := <-s.inbound:
			s.catchUp()
			s.node.Receive(m)
		case f := <-s.calls:
			s.catchUp()
			f()
		case seq := <-s.written:
			s.catchUp()
			if err := s.node.WriteDone(seq); err != nil {
				return err
			}
		}
		if err := s.drain(); err != nil {
			return err
		}
		if err := s.node.Process(); err != nil {
			return err
		}
		if s.cfg.SyncStorage {
			if err := s.waitForDisk(ctx); err != nil {
				return err
			}
		}
		s.publish()
	}
	return nil
}

func (s *Server) catchUp() {
	now := time.Now()
	s.node.Advance(now.Sub(s.lastTick))
	s.lastTick = now
}

func (s *Server) drain() error {
	for range s.cfg.Batch - 1 {
		select {
		case m := <-s.inbound:
			s.node.Receive(m)
		case f := <-s.calls:
			f()
		case seq := <-s.written:
			if err := s.node.WriteDone(seq); err != nil {
				return err
			}
		default:
			return nil
		}
	}
	return nil
}

func (s *Server) waitForDisk(ctx context.Context) error {
	for s.node.PendingWrites() > 0 {
		select {
		case seq := <-s.written:
			if err := s.node.WriteDone(seq); err != nil {
				return err
			}
			if err := s.node.Process(); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func (s *Server) onLoop(ctx context.Context, f func()) error {
	_, err := s.run(ctx, func(done node.Done) {
		f()
		done(kv.Result{}, nil)
	})
	return err
}

type result struct {
	res kv.Result
	err error
}

func (s *Server) run(ctx context.Context, f func(done node.Done)) (kv.Result, error) {
	return s.call(ctx, false, f)
}

func (s *Server) admit(ctx context.Context, f func(done node.Done)) (kv.Result, error) {
	return s.call(ctx, s.cfg.MaxPending > 0, f)
}

func (s *Server) call(ctx context.Context, refuse bool, f func(done node.Done)) (kv.Result, error) {
	ch := make(chan result, 1)
	call := func() {
		if err := ctx.Err(); err != nil {
			ch <- result{err: err}
			return
		}
		f(func(res kv.Result, err error) { ch <- result{res, err} })
	}
	if refuse {
		select {
		case s.calls <- call:
		case <-s.stopped:
			return kv.Result{}, ErrStopped
		default:
			return kv.Result{}, raft.ErrBusy
		}
	} else {
		select {
		case s.calls <- call:
		case <-ctx.Done():
			return kv.Result{}, ctx.Err()
		case <-s.stopped:
			return kv.Result{}, ErrStopped
		}
	}
	select {
	case r := <-ch:
		return r.res, r.err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-s.stopped:
		select {
		case r := <-ch:
			return r.res, r.err
		default:
			return kv.Result{}, ErrStopped
		}
	}
}
