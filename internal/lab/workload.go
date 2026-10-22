package lab

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
	"github.com/mirzanajafov/fivefamilies/internal/check/live"
)

type workload struct {
	addrs     []string
	workers   int
	keys      int
	read      client.ReadMode
	opTimeout time.Duration
	seed      uint64
	start     time.Time
	mu        sync.Mutex
	ops       []check.Op
	errs      []string
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
		op, err := w.do(c, id, in)
		var msg string
		if err != nil {
			msg = err.Error()
		}
		w.mu.Lock()
		w.ops, w.errs = append(w.ops, op), append(w.errs, msg)
		w.mu.Unlock()
		if err != nil {
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

func (w *workload) do(c *client.Client, id int, in check.Input) (check.Op, error) {
	ctx, cancel := context.WithTimeout(context.Background(), w.opTimeout)
	defer cancel()
	call := w.now()
	out, err := live.Execute(ctx, c, in, w.read)
	return check.Op{Client: id, Input: in, Output: out, Call: call, Return: w.now()}, err
}

func (w *workload) history() ([]porcupine.Operation, int, int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	hist, unknown := check.History(w.ops)
	return hist, unknown, len(w.ops) - len(hist)
}
