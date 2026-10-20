package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
)

var demoKeys = []string{"ledger/a", "ledger/b", "ledger/c"}

const opTimeout = 30 * time.Second

type demo struct {
	c         *Controller
	clients   []*client.Client
	mu        sync.Mutex
	cutting   bool
	busy      sync.WaitGroup
	gate      *sync.Cond
	ops       []porcupine.Operation
	unknown   bool
	inherited bool
	initial   map[string]check.Output
	from      time.Time
	index     int
}

func (c *Controller) now() int64 {
	return int64(time.Since(c.start))
}

func (c *Controller) drive(ctx context.Context) {
	var cancel context.CancelFunc
	var done chan struct{}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		c.mu.Lock()
		want := c.viewers > 0 || time.Since(c.lastSeen) < c.cfg.Idle
		c.mu.Unlock()
		switch {
		case want && cancel == nil:
			var lctx context.Context
			lctx, cancel = context.WithCancel(ctx)
			done = make(chan struct{})
			go func() {
				defer close(done)
				c.runDemo(lctx)
			}()
		case !want && cancel != nil:
			cancel()
			<-done
			cancel = nil
		}
		select {
		case <-ctx.Done():
			if cancel != nil {
				cancel()
				<-done
			}
			return
		case <-t.C:
		}
	}
}

func (c *Controller) setLoad(on bool) {
	c.mu.Lock()
	c.loadOn = on
	if !on {
		c.opsRate = 0
	}
	c.mu.Unlock()
}

func (c *Controller) runDemo(ctx context.Context) {
	c.setLoad(true)
	defer c.setLoad(false)
	d := &demo{c: c, initial: map[string]check.Output{}}
	d.gate = sync.NewCond(&d.mu)
	for range c.cfg.Clients + 1 {
		cl, err := client.New(c.cfg.Nodes, client.Options{AttemptTimeout: time.Second})
		if err != nil {
			c.cfg.Logger.Error("demo client", "err", err)
			return
		}
		defer cl.Close()
		d.clients = append(d.clients, cl)
	}
	if !d.cut(ctx, false) {
		return
	}
	var wg sync.WaitGroup
	for i := range c.cfg.Clients {
		wg.Go(func() { d.worker(ctx, i) })
	}
	t := time.NewTicker(c.cfg.Window)
	defer t.Stop()
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-t.C:
			d.cut(ctx, true)
		}
	}
	d.mu.Lock()
	d.cutting = false
	d.gate.Broadcast()
	d.mu.Unlock()
	wg.Wait()
}

func (d *demo) worker(ctx context.Context, id int) {
	rng := rand.New(rand.NewPCG(uint64(id), uint64(time.Now().UnixNano())))
	cl := d.clients[id]
	seq := 0
	last := map[string]string{}
	for ctx.Err() == nil {
		d.mu.Lock()
		for d.cutting && ctx.Err() == nil {
			d.gate.Wait()
		}
		d.busy.Add(1)
		d.mu.Unlock()
		key := demoKeys[rng.IntN(len(demoKeys))]
		var in check.Input
		switch r := rng.IntN(10); {
		case r < 4:
			seq++
			in = check.Input{Kind: check.Put, Key: key, Value: fmt.Sprintf("c%d-%d", id, seq)}
		case r < 8:
			in = check.Input{Kind: check.Get, Key: key}
		default:
			seq++
			v, seen := last[key]
			in = check.Input{Kind: check.CAS, Key: key, Value: fmt.Sprintf("c%d-%d", id, seq), Expect: v, ExpectMissing: !seen}
		}
		out, call, ret := d.do(ctx, cl, in)
		if in.Kind == check.Get && out.Found {
			last[key] = out.Value
		}
		d.record(id, in, out, call, ret)
		d.busy.Done()
		select {
		case <-ctx.Done():
		case <-time.After(time.Duration(80+rng.IntN(120)) * time.Millisecond):
		}
	}
}

func (d *demo) do(ctx context.Context, cl *client.Client, in check.Input) (check.Output, int64, int64) {
	call := d.c.now()
	octx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	var out check.Output
	var err error
	switch in.Kind {
	case check.Put:
		_, err = cl.Put(octx, in.Key, []byte(in.Value))
		out.Ok = err == nil
	case check.Get:
		var kv client.KeyValue
		kv, out.Found, err = cl.Get(octx, in.Key, client.ReadIndex)
		out.Value = string(kv.Value)
	case check.CAS:
		out.Ok, _, err = cl.CompareAndSwap(octx, in.Key, client.Expect{Value: []byte(in.Expect), Missing: in.ExpectMissing}, []byte(in.Value))
	}
	ret := d.c.now()
	if err != nil {
		if errors.Is(err, context.Canceled) && in.Kind == check.Get {
			return check.Output{Unknown: true}, call, ret
		}
		out = check.Output{Unknown: true}
		ret = -1
	}
	if err == nil && in.Kind != check.Get {
		d.c.mu.Lock()
		d.c.acks = append(d.c.acks, time.Now())
		if cut := len(d.c.acks) - 20000; cut > 0 {
			d.c.acks = d.c.acks[cut:]
		}
		d.c.mu.Unlock()
	}
	return out, call, ret
}

func (d *demo) record(id int, in check.Input, out check.Output, call, ret int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if out.Unknown && in.Kind != check.Get {
		d.unknown = true
	}
	if out.Unknown && in.Kind == check.Get {
		return
	}
	if ret < 0 {
		ret = 1 << 62
	}
	d.ops = append(d.ops, porcupine.Operation{ClientId: id, Input: in, Call: call, Output: out, Return: ret})
	d.c.mu.Lock()
	d.c.ops++
	d.c.mu.Unlock()
}

func (d *demo) cut(ctx context.Context, verify bool) bool {
	d.mu.Lock()
	d.cutting = true
	d.mu.Unlock()
	d.busy.Wait()
	reader := d.clients[len(d.clients)-1]
	finals := map[string]check.Output{}
	var finalOps []porcupine.Operation
	for _, key := range demoKeys {
		for ctx.Err() == nil {
			call := d.c.now()
			octx, cancel := context.WithTimeout(ctx, opTimeout)
			kv, found, err := reader.Get(octx, key, client.ReadLog)
			cancel()
			if err != nil {
				continue
			}
			out := check.Output{Found: found, Value: string(kv.Value)}
			finals[key] = out
			finalOps = append(finalOps, porcupine.Operation{ClientId: len(d.clients) - 1, Input: check.Input{Kind: check.Get, Key: key}, Call: call, Output: out, Return: d.c.now()})
			break
		}
	}
	if ctx.Err() != nil {
		return false
	}
	d.mu.Lock()
	d.ops = append(d.ops, finalOps...)
	ops, initial, unknown, from := d.ops, d.initial, d.unknown || d.inherited, d.from
	d.ops, d.initial, d.inherited, d.unknown, d.from = nil, finals, d.unknown, false, time.Now()
	d.index++
	index := d.index
	d.cutting = false
	d.gate.Broadcast()
	d.mu.Unlock()
	d.c.mu.Lock()
	if since := time.Since(d.c.opsAt).Seconds(); !d.c.opsAt.IsZero() && since > 0 {
		d.c.opsRate = float64(d.c.ops) / since
	}
	d.c.ops, d.c.opsAt = 0, time.Now()
	d.c.mu.Unlock()
	if verify {
		go d.c.checkWindow(index, from, ops, initial, unknown)
	}
	return true
}

func seedOps(initial map[string]check.Output, at int64) []porcupine.Operation {
	var out []porcupine.Operation
	for _, key := range demoKeys {
		o, ok := initial[key]
		in := check.Input{Kind: check.Delete, Key: key}
		res := check.Output{}
		if ok && o.Found {
			in = check.Input{Kind: check.Put, Key: key, Value: o.Value}
			res = check.Output{Ok: true}
		}
		out = append(out, porcupine.Operation{ClientId: -1, Input: in, Call: at - 2, Output: res, Return: at - 1})
	}
	return out
}

func (c *Controller) checkWindow(index int, from time.Time, ops []porcupine.Operation, initial map[string]check.Output, unknown bool) {
	w := Window{Index: index, From: from, To: time.Now(), Ops: len(ops)}
	switch {
	case unknown:
		w.Result, w.Note = "unchecked", "a write's outcome is unknown, so this window and the start of the next can't be pinned down"
	default:
		first := int64(1 << 62)
		for _, op := range ops {
			first = min(first, op.Call)
		}
		v := check.Linearizable(append(seedOps(initial, first), ops...), 10*time.Second)
		w.Result, w.Took = string(v.Result), v.Took
	}
	c.mu.Lock()
	c.windows = append(c.windows, w)
	if len(c.windows) > 100 {
		c.windows = c.windows[len(c.windows)-100:]
	}
	if w.Result == string(porcupine.Illegal) {
		c.event("violation", 0, 0, fmt.Sprintf("window %d is not linearizable", index))
	}
	c.mu.Unlock()
}
