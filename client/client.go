package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "github.com/mirzanajafov/fivefamilies/api/fivefamiliesv1"
)

const headerLeaderAddr = "ff-leader-addr"

type ReadMode int

const (
	ReadIndex ReadMode = iota
	ReadLease
	ReadLog
)

func (m ReadMode) String() string {
	switch m {
	case ReadIndex:
		return "index"
	case ReadLease:
		return "lease"
	case ReadLog:
		return "log"
	}
	return fmt.Sprintf("ReadMode(%d)", int(m))
}

func (m ReadMode) MarshalText() ([]byte, error) {
	switch m {
	case ReadIndex, ReadLease, ReadLog:
		return []byte(m.String()), nil
	}
	return nil, fmt.Errorf("client: unknown read mode %d", int(m))
}

func (m *ReadMode) UnmarshalText(b []byte) error {
	for _, known := range []ReadMode{ReadIndex, ReadLease, ReadLog} {
		if string(b) == known.String() {
			*m = known
			return nil
		}
	}
	return fmt.Errorf("client: unknown read mode %q", b)
}

var ErrSessionExpired = errors.New("client: the cluster forgot this session; the last write's outcome is unknown")

type KeyValue struct {
	Key       string
	Value     []byte
	CreateRev uint64
	ModRev    uint64
	Version   uint64
	Lease     uint64
}

type WriteOption func(*writeOptions)

type writeOptions struct {
	lease uint64
}

func WithLease(id uint64) WriteOption {
	return func(o *writeOptions) { o.lease = id }
}

func writeOpts(opts []WriteOption) writeOptions {
	var o writeOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

type Options struct {
	AttemptTimeout time.Duration
	Backoff        time.Duration
}

type Client struct {
	opts    Options
	mu      sync.Mutex
	writeMu sync.Mutex
	addrs   []string
	conns   map[string]*grpc.ClientConn
	leader  string
	next    int
	session uint64
	seq     uint64
}

func New(addrs []string, opts Options) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("client: no addresses")
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 2 * time.Second
	}
	if opts.Backoff <= 0 {
		opts.Backoff = 20 * time.Millisecond
	}
	c := &Client{opts: opts, addrs: addrs, conns: map[string]*grpc.ClientConn{}, leader: addrs[0]}
	for _, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, err
		}
		conn.Connect()
		c.conns[a] = conn
	}
	return c, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var first error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *Client) conn(addr string) (grpc.ClientConnInterface, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	conn, ok := c.conns[addr]
	if !ok {
		var err error
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		c.conns[addr] = conn
	}
	return conn, nil
}

func (c *Client) target() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

func (c *Client) moveOn(failed string, md metadata.MD) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if hint := md.Get(headerLeaderAddr); len(hint) > 0 && hint[0] != failed && slices.Contains(c.addrs, hint[0]) {
		c.leader = hint[0]
		return
	}
	if c.leader == failed {
		c.next = (c.next + 1) % len(c.addrs)
		c.leader = c.addrs[c.next]
	}
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.ResourceExhausted:
		return true
	}
	return false
}

func do[T any](ctx context.Context, c *Client, call func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (T, error)) (T, error) {
	var zero T
	backoff := c.opts.Backoff
	for {
		addr := c.target()
		cc, err := c.conn(addr)
		if err != nil {
			return zero, err
		}
		actx, cancel := context.WithTimeout(ctx, c.opts.AttemptTimeout)
		var md metadata.MD
		out, err := call(actx, cc, grpc.Header(&md))
		cancel()
		if err == nil {
			return out, nil
		}
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		if !retryable(err) {
			if status.Code(err) == codes.FailedPrecondition && status.Convert(err).Message() == "session expired" {
				return zero, ErrSessionExpired
			}
			return zero, err
		}
		if status.Code(err) != codes.ResourceExhausted {
			c.moveOn(addr, md)
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
}

func (c *Client) nextSeq(ctx context.Context) (uint64, uint64, func(), error) {
	c.writeMu.Lock()
	c.mu.Lock()
	session := c.session
	c.mu.Unlock()
	if session == 0 {
		res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.RegisterResponse, error) {
			return pb.NewKVClient(cc).Register(ctx, &pb.RegisterRequest{}, opts...)
		})
		if err != nil {
			c.writeMu.Unlock()
			return 0, 0, nil, err
		}
		c.mu.Lock()
		c.session = res.GetClient()
		session = c.session
		c.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	return session, c.seq, c.writeMu.Unlock, nil
}

func fromPB(kv *pb.KeyValue) KeyValue {
	return KeyValue{Key: kv.GetKey(), Value: kv.GetValue(), CreateRev: kv.GetCreateRev(), ModRev: kv.GetModRev(), Version: kv.GetVersion(), Lease: kv.GetLease()}
}

func (c *Client) Put(ctx context.Context, key string, value []byte, opts ...WriteOption) (uint64, error) {
	o := writeOpts(opts)
	session, seq, done, err := c.nextSeq(ctx)
	if err != nil {
		return 0, err
	}
	defer done()
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.PutResponse, error) {
		return pb.NewKVClient(cc).Put(ctx, &pb.PutRequest{Client: session, Seq: seq, Key: key, Value: value, Lease: o.lease}, opts...)
	})
	return res.GetRev(), err
}

func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	session, seq, done, err := c.nextSeq(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
		return pb.NewKVClient(cc).Delete(ctx, &pb.DeleteRequest{Client: session, Seq: seq, Key: key}, opts...)
	})
	return res.GetDeleted(), err
}

type Expect struct {
	Value   []byte
	Missing bool
	ModRev  uint64
}

func (c *Client) CompareAndSwap(ctx context.Context, key string, expect Expect, value []byte, opts ...WriteOption) (bool, KeyValue, error) {
	o := writeOpts(opts)
	session, seq, done, err := c.nextSeq(ctx)
	if err != nil {
		return false, KeyValue{}, err
	}
	defer done()
	req := &pb.CompareAndSwapRequest{Client: session, Seq: seq, Key: key, Value: value, Lease: o.lease}
	switch {
	case expect.Missing:
		req.Expect = &pb.CompareAndSwapRequest_Missing{Missing: true}
	case expect.ModRev > 0:
		req.Expect = &pb.CompareAndSwapRequest_ModRev{ModRev: expect.ModRev}
	default:
		req.Expect = &pb.CompareAndSwapRequest_ValueIs{ValueIs: expect.Value}
	}
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.CompareAndSwapResponse, error) {
		return pb.NewKVClient(cc).CompareAndSwap(ctx, req, opts...)
	})
	if err != nil {
		return false, KeyValue{}, err
	}
	return res.GetSwapped(), fromPB(res.GetCurrent()), nil
}

func (c *Client) Get(ctx context.Context, key string, mode ReadMode) (KeyValue, bool, error) {
	var m pb.ReadMode
	switch mode {
	case ReadLease:
		m = pb.ReadMode_READ_MODE_LEASE
	case ReadLog:
		m = pb.ReadMode_READ_MODE_LOG
	default:
		m = pb.ReadMode_READ_MODE_INDEX
	}
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.GetResponse, error) {
		return pb.NewKVClient(cc).Get(ctx, &pb.GetRequest{Key: key, Mode: m}, opts...)
	})
	if err != nil {
		return KeyValue{}, false, err
	}
	return fromPB(res.GetKv()), res.GetFound(), nil
}

func (c *Client) Range(ctx context.Context, prefix string, limit uint64) ([]KeyValue, error) {
	kvs, _, err := c.RangeRev(ctx, prefix, limit)
	return kvs, err
}

func (c *Client) RangeRev(ctx context.Context, prefix string, limit uint64) ([]KeyValue, uint64, error) {
	res, err := do(ctx, c, func(ctx context.Context, cc grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.RangeResponse, error) {
		return pb.NewKVClient(cc).Range(ctx, &pb.RangeRequest{Prefix: prefix, Limit: limit}, opts...)
	})
	if err != nil {
		return nil, 0, err
	}
	out := make([]KeyValue, len(res.GetKvs()))
	for i, kv := range res.GetKvs() {
		out[i] = fromPB(kv)
	}
	return out, res.GetRev(), nil
}
