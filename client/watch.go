package client

import (
	"context"
	"time"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

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
		c.mu.Lock()
		at := c.next
		c.mu.Unlock()
		backoff := c.opts.Backoff
		for ctx.Err() == nil {
			addr := c.addrs[at%len(c.addrs)]
			got, compacted := c.watchOnce(ctx, addr, key, prefix, &next, out)
			if compacted || ctx.Err() != nil {
				return
			}
			if got {
				backoff = c.opts.Backoff
			}
			at++
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

func (c *Client) watchOnce(ctx context.Context, addr, key string, prefix bool, next *uint64, out chan<- WatchResponse) (got, compacted bool) {
	cc, err := c.conn(addr)
	if err != nil {
		return false, false
	}
	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := pb.NewWatchClient(cc).Watch(sctx, &pb.WatchRequest{Key: key, Prefix: prefix, StartRev: *next})
	if err != nil {
		return false, false
	}
	for {
		resp, err := stream.Recv()
		if err != nil {
			return got, false
		}
		got = true
		r := WatchResponse{Rev: resp.GetRev(), Progress: resp.GetProgress(), CompactRev: resp.GetCompactRev()}
		for _, ev := range resp.GetEvents() {
			t := EventPut
			if ev.GetType() == pb.EventType_EVENT_TYPE_DELETE {
				t = EventDelete
			}
			r.Events = append(r.Events, Event{Type: t, KV: fromPB(ev.GetKv())})
		}
		select {
		case out <- r:
		case <-ctx.Done():
			return got, false
		}
		if r.CompactRev != 0 {
			return got, true
		}
		*next = r.Rev + 1
	}
}
