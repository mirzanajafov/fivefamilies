package lab

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
)

type Op struct {
	Client int          `json:"client"`
	Input  check.Input  `json:"input"`
	Output check.Output `json:"output"`
	Call   int64        `json:"call_ns"`
	Return int64        `json:"return_ns"`
	Err    string       `json:"error,omitempty"`
}

type workload struct {
	addrs     []string
	workers   int
	keys      int
	read      client.ReadMode
	opTimeout time.Duration
	seed      uint64
	start     time.Time
	mu        sync.Mutex
	ops       []Op
}

func (w *workload) now() int64 {
	return int64(time.Since(w.start))
}

func (w *workload) run(stop <-chan struct{}) {
	var wg sync.WaitGroup
	for id := range w.workers {
		wg.Go(func() { w.worker(id, stop) })
	}
	wg.Wait()
}

func (w *workload) worker(id int, stop <-chan struct{}) {
	rng := rand.New(rand.NewPCG(w.seed, uint64(id)))
	c, err := client.New(w.addrs, client.Options{AttemptTimeout: 500 * time.Millisecond})
	if err != nil {
		return
	}
	defer c.Close()
	seen := map[string]string{}
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		default:
		}
		key := fmt.Sprintf("k%d", rng.IntN(w.keys))
		value := fmt.Sprintf("w%d.%d", id, n)
		in := check.Input{Key: key}
		switch r := rng.IntN(100); {
		case r < 40:
			in.Kind = check.Get
		case r < 70:
			in.Kind, in.Value = check.Put, value
		case r < 90:
			in.Kind, in.Value = check.CAS, value
			if prev, ok := seen[key]; ok {
				in.Expect = prev
			} else {
				in.ExpectMissing = true
			}
		default:
			in.Kind = check.Delete
		}
		op := w.do(c, id, in)
		w.mu.Lock()
		w.ops = append(w.ops, op)
		w.mu.Unlock()
		if op.Err != "" {
			continue
		}
		switch {
		case in.Kind == check.Get && op.Output.Found:
			seen[key] = op.Output.Value
		case in.Kind == check.Put, in.Kind == check.CAS && op.Output.Ok:
			seen[key] = in.Value
		default:
			delete(seen, key)
		}
		time.Sleep(time.Duration(rng.IntN(20)) * time.Millisecond)
	}
}

func (w *workload) do(c *client.Client, id int, in check.Input) Op {
	ctx, cancel := context.WithTimeout(context.Background(), w.opTimeout)
	defer cancel()
	op := Op{Client: id, Input: in, Call: w.now()}
	var err error
	switch in.Kind {
	case check.Get:
		var kv client.KeyValue
		var found bool
		kv, found, err = c.Get(ctx, in.Key, w.read)
		op.Output = check.Output{Found: found, Value: string(kv.Value)}
	case check.Put:
		_, err = c.Put(ctx, in.Key, []byte(in.Value))
		op.Output = check.Output{Ok: true}
	case check.CAS:
		var ok bool
		ok, _, err = c.CompareAndSwap(ctx, in.Key, client.Expect{Value: []byte(in.Expect), Missing: in.ExpectMissing}, []byte(in.Value))
		op.Output = check.Output{Ok: ok}
	case check.Delete:
		var ok bool
		ok, err = c.Delete(ctx, in.Key)
		op.Output = check.Output{Ok: ok}
	}
	op.Return = w.now()
	if err != nil {
		op.Err = err.Error()
		op.Return = math.MaxInt64
		op.Output = check.Output{Unknown: true}
	}
	return op
}

func (w *workload) history() ([]porcupine.Operation, int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []porcupine.Operation
	unknown, failedReads := 0, 0
	for _, op := range w.ops {
		if op.Err != "" {
			if op.Input.Kind == check.Get {
				failedReads++
				continue
			}
			unknown++
		}
		out = append(out, porcupine.Operation{ClientId: op.Client, Input: op.Input, Output: op.Output, Call: op.Call, Return: op.Return})
	}
	return out, unknown, failedReads
}
