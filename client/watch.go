package client

import (
	"context"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

const watchSilence = 5 * time.Second

type EventType int

const (
	EventPut EventType = iota + 1
	EventDelete
)

type Event struct {
	Type EventType
	KV   KeyValue
}

type WatchResponse struct {
	Events     []Event
	Rev        uint64
	Progress   bool
	CompactRev uint64
}

func (c *Client) Watch(ctx context.Context, key string, prefix bool, startRev uint64) <-chan WatchResponse {
	out := make(chan WatchResponse, 16)
	go func() {
		defer close(out)
		next := startRev
		backoff := c.opts.Backoff
		for ctx.Err() == nil {
			addr := c.watchTarget()
			got, end := c.watchOnce(ctx, addr, key, prefix, &next, out)
			if end == endCompacted || ctx.Err() != nil {
				return
			}
			if got {
				backoff = c.opts.Backoff
			}
			if end == endBroken {
				c.watchFailed(addr)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, 500*time.Millisecond)
		}
	}()
	return out
}

func (c *Client) watchTarget() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addrs[c.watchAt]
}

func (c *Client) watchFailed(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.addrs[c.watchAt] == addr {
		c.watchAt = (c.watchAt + 1) % len(c.addrs)
	}
}

type watchEnd int

const (
	endBroken watchEnd = iota
	endBehind
	endCompacted
)

func (c *Client) watchOnce(ctx context.Context, addr, key string, prefix bool, next *uint64, out chan<- WatchResponse) (bool, watchEnd) {
	cc, err := c.conn(addr)
	if err != nil {
		return false, endBroken
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	idle := time.AfterFunc(watchSilence, cancel)
	defer idle.Stop()
	stream, err := pb.NewWatchClient(cc).Watch(sctx, &pb.WatchRequest{Key: key, Prefix: prefix, StartRev: *next})
	if err != nil {
		return false, endBroken
	}
	got := false
	for {
		resp, err := stream.Recv()
		if status.Code(err) == codes.ResourceExhausted {
			return got, endBehind
		}
		if err != nil {
			return got, endBroken
		}
		idle.Stop()
		got = true
		r := WatchResponse{Rev: resp.GetRev(), Progress: resp.GetProgress(), CompactRev: resp.GetCompactRev()}
		for _, ev := range resp.GetEvents() {
			t := EventPut
			if ev.GetType() == pb.EventType_EVENT_TYPE_DELETE {
				t = EventDelete
			}
			r.Events = append(r.Events, Event{Type: t, KV: fromPB(ev.GetKv())})
		}
		if !r.Progress || r.Rev >= *next {
			select {
			case out <- r:
			case <-ctx.Done():
				return got, endBroken
			}
		}
		if r.CompactRev != 0 {
			return got, endCompacted
		}
		*next = r.Rev + 1
		idle.Reset(watchSilence)
	}
}
