package server

import (
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/watch"
)

const watchBuffer = 256

type watchService struct {
	pb.UnimplementedWatchServer
	s *Server
}

func (s *Server) leaderless() bool {
	return s.node.LeaderlessTicks() > uint64(2*s.cfg.ElectionTick)
}

func (s *Server) tendWatches() {
	if s.sm.Hub.Len() == 0 {
		return
	}
	if s.leaderless() {
		s.sm.Hub.EndAll()
		return
	}
	if time.Since(s.lastWatch) >= s.cfg.WatchProgress {
		s.lastWatch = time.Now()
		s.sm.Hub.Progress()
	}
}

func toEvent(ev kv.Event) *pb.WatchEvent {
	t := pb.EventType_EVENT_TYPE_PUT
	if ev.Type == kv.EventDelete {
		t = pb.EventType_EVENT_TYPE_DELETE
	}
	return &pb.WatchEvent{Type: t, Kv: toKV(ev.KV)}
}

func (w *watchService) Watch(req *pb.WatchRequest, stream pb.Watch_WatchServer) error {
	ctx := stream.Context()
	ch := make(chan watch.Batch, watchBuffer)
	dropped := make(chan struct{})
	var once sync.Once
	send := func(b watch.Batch) bool {
		select {
		case ch <- b:
			return true
		default:
			once.Do(func() { close(dropped) })
			return false
		}
	}
	var id uint64
	var err error
	stale := false
	if lerr := w.s.onLoop(ctx, func() {
		if stale = w.s.leaderless(); !stale {
			id, err = w.s.sm.Hub.Watch(req.GetKey(), req.GetPrefix(), req.GetStartRev(), send)
		}
	}); lerr != nil {
		return fail(ctx, w.s, lerr)
	}
	if stale {
		return status.Error(codes.Unavailable, "this node has no leader; watch another node")
	}
	var ce *watch.CompactedError
	if errors.As(err, &ce) {
		return stream.Send(&pb.WatchResponse{CompactRev: ce.Start})
	}
	defer func() {
		select {
		case w.s.calls <- func() { w.s.sm.Hub.Cancel(id) }:
		case <-w.s.stopped:
		}
	}()
	beat := time.NewTicker(w.s.cfg.WatchProgress)
	defer beat.Stop()
	var rev uint64
	for {
		select {
		case b := <-ch:
			switch {
			case b.Ended:
				return status.Error(codes.Unavailable, "this node lost its leader; watch another node")
			case b.Compacted != 0:
				return stream.Send(&pb.WatchResponse{CompactRev: b.Compacted})
			}
			resp := &pb.WatchResponse{Rev: b.Rev, Progress: b.Progress}
			for _, ev := range b.Events {
				resp.Events = append(resp.Events, toEvent(ev))
			}
			if err := stream.Send(resp); err != nil {
				return err
			}
			rev = b.Rev
			beat.Reset(w.s.cfg.WatchProgress)
		case <-beat.C:
			if err := stream.Send(&pb.WatchResponse{Rev: rev, Progress: true}); err != nil {
				return err
			}
		case <-dropped:
			return status.Error(codes.ResourceExhausted, "the watch fell behind; resume from the last revision you saw")
		case <-ctx.Done():
			return nil
		case <-w.s.stopped:
			return fail(ctx, w.s, ErrStopped)
		}
	}
}
