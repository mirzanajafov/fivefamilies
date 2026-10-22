package sim

import (
	"errors"
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/kv"
	"github.com/mirzanajafov/fivefamilies/internal/node"
)

type simClient struct {
	idx         int
	session     uint64
	seq         uint64
	target      int
	attempt     uint64
	cmd         kv.Command
	op          int
	registering bool
	lastSeen    map[string]string
}

type clientReq struct {
	client  *simClient
	attempt uint64
	cmd     kv.Command
}

func (c *simClient) busy() bool {
	return c.registering || c.op >= 0
}

func (s *Sim) startClients() {
	for i := range s.sw.clients {
		c := &simClient{idx: i, op: -1, target: s.rng.IntN(len(s.nodes)), lastSeen: map[string]string{}}
		s.clients = append(s.clients, c)
		s.after(between(s.rng, 0, 50*time.Millisecond), func() {
			c.registering = true
			c.cmd = kv.Command{Op: kv.OpRegister}
			s.sendRequest(c)
		})
	}
}

func (s *Sim) clientsStopAt() time.Duration {
	return s.opts.Chaos + s.opts.Heal - 2*time.Second
}

func (s *Sim) nextOp(c *simClient) {
	if s.now >= s.clientsStopAt() || s.chk.failed() {
		return
	}
	key := fmt.Sprintf("k%d", s.rng.IntN(s.sw.keys))
	c.seq++
	value := fmt.Sprintf("c%d.%d", c.idx, c.seq)
	cmd := kv.Command{Client: c.session, Seq: c.seq, Key: key}
	in := check.Input{Key: key}
	switch r := s.rng.IntN(100); {
	case r < 40:
		cmd.Op, cmd.Client = kv.OpGet, 0
		in.Kind = check.Get
	case r < 70:
		cmd.Op, cmd.Value = kv.OpPut, []byte(value)
		in.Kind, in.Value = check.Put, value
	case r < 90:
		cmd.Op, cmd.Value = kv.OpCAS, []byte(value)
		in.Kind, in.Value = check.CAS, value
		if seen, ok := c.lastSeen[key]; ok && s.rng.IntN(10) > 0 {
			cmd.Expect, in.Expect = []byte(seen), seen
		} else if !ok {
			cmd.ExpectMissing, in.ExpectMissing = true, true
		} else {
			cmd.Expect, in.Expect = []byte("never"), "never"
		}
	default:
		cmd.Op = kv.OpDelete
		in.Kind = check.Delete
	}
	c.cmd = cmd
	c.op = len(s.history)
	s.history = append(s.history, check.Op{Client: c.idx, Input: in, Output: check.Output{Unknown: true}, Call: int64(s.now)})
	s.stats.ClientOps++
	s.sendRequest(c)
}

func (s *Sim) sendRequest(c *simClient) {
	if c.cmd.Op == kv.OpGet && s.sw.readMode == ReadFollower {
		c.target = s.rng.IntN(len(s.nodes))
	}
	c.attempt++
	a := c.attempt
	req := &clientReq{client: c, attempt: a, cmd: c.cmd}
	if !(s.net.loss > 0 && s.rng.Float64() < s.net.loss) {
		s.schedule(event{kind: evRequest, node: c.target, req: req}, s.net.delay(s.rng))
	}
	s.after(s.sw.timeout, func() {
		if c.attempt == a && c.busy() {
			s.stats.ClientRetries++
			c.target = s.rng.IntN(len(s.nodes))
			s.sendRequest(c)
		}
	})
}

func (s *Sim) handleRequest(sn *simNode, req *clientReq) {
	seq := req.cmd.Seq
	reply := func(res kv.Result, err error) {
		if s.net.loss > 0 && s.rng.Float64() < s.net.loss {
			return
		}
		s.after(s.net.delay(s.rng), func() { s.clientResponse(req.client, req.attempt, seq, res, err) })
	}
	if req.cmd.Op == kv.OpGet && s.sw.readMode != ReadLog {
		s.stats.LocalReads++
		if s.opts.Trace != nil {
			r := sn.n.Raft()
			s.tracef("n%d read %s for client %d attempt %d (state %v term %d commit %d applied %d)", sn.id, req.cmd.Key, req.client.idx, req.attempt, r.State(), r.Term(), r.Committed(), r.Applied())
		}
		sn.n.Read(req.cmd.Key, s.sw.readMode == ReadLease, func(res kv.Result, err error) {
			if s.opts.Trace != nil {
				r := sn.n.Raft()
				s.tracef("n%d answers read %s for client %d attempt %d: found %v %q rev %d err %v (state %v term %d)", sn.id, req.cmd.Key, req.client.idx, req.attempt, res.Found, res.KV.Value, res.Rev, err, r.State(), r.Term())
			}
			reply(res, err)
		})
		return
	}
	sn.n.Submit(req.cmd, reply)
}

func (s *Sim) clientResponse(c *simClient, attempt, seq uint64, res kv.Result, err error) {
	if !c.busy() || seq != c.cmd.Seq {
		return
	}
	if err == nil && res.Code != kv.OK {
		s.chk.report(CatClient, "client %d got %v for %v seq %d", c.idx, res.Code, c.cmd.Op, seq)
		return
	}
	if err != nil {
		if attempt != c.attempt {
			return
		}
		var nl *node.NotLeaderError
		if errors.As(err, &nl) && nl.Leader != 0 && int(nl.Leader)-1 != c.target {
			c.target = int(nl.Leader) - 1
			s.sendRequest(c)
			return
		}
		c.attempt++
		a := c.attempt
		s.after(between(s.rng, 10*time.Millisecond, 50*time.Millisecond), func() {
			if c.attempt == a && c.busy() {
				c.target = s.rng.IntN(len(s.nodes))
				s.sendRequest(c)
			}
		})
		return
	}
	if c.registering {
		c.registering = false
		c.session = res.Client
		s.after(exponential(s.rng, s.sw.think), func() { s.nextOp(c) })
		return
	}
	rec := &s.history[c.op]
	rec.Return = int64(s.now)
	switch c.cmd.Op {
	case kv.OpGet:
		rec.Output = check.Output{Found: res.Found, Value: string(res.KV.Value)}
		c.see(c.cmd.Key, res.Found, string(res.KV.Value))
	case kv.OpPut:
		rec.Output = check.Output{Ok: true}
		c.see(c.cmd.Key, true, rec.Input.Value)
	case kv.OpCAS:
		rec.Output = check.Output{Ok: res.Ok}
		if res.Ok {
			s.stats.SwapsWon++
			c.see(c.cmd.Key, true, rec.Input.Value)
		} else {
			c.see(c.cmd.Key, res.Found, string(res.KV.Value))
		}
	case kv.OpDelete:
		rec.Output = check.Output{Ok: res.Ok}
		c.see(c.cmd.Key, false, "")
	}
	c.op = -1
	s.after(exponential(s.rng, s.sw.think), func() { s.nextOp(c) })
}

func (c *simClient) see(key string, found bool, value string) {
	if found {
		c.lastSeen[key] = value
	} else {
		delete(c.lastSeen, key)
	}
}

func (s *Sim) porcupineHistory() []porcupine.Operation {
	hist, unknown := check.History(s.history)
	s.stats.Indeterminate += unknown
	return hist
}
