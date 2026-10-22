package check

import (
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

const never = math.MaxInt64

func op(client int, call, ret int64, in Input, out Output) porcupine.Operation {
	return porcupine.Operation{ClientId: client, Call: call, Return: ret, Input: in, Output: out}
}

func put(k, v string) Input        { return Input{Kind: Put, Key: k, Value: v} }
func get(k string) Input           { return Input{Kind: Get, Key: k} }
func del(k string) Input           { return Input{Kind: Delete, Key: k} }
func cas(k, from, to string) Input { return Input{Kind: CAS, Key: k, Expect: from, Value: to} }
func create(k, to string) Input    { return Input{Kind: CAS, Key: k, ExpectMissing: true, Value: to} }
func value(v string) Output        { return Output{Found: true, Value: v} }
func ok(b bool) Output             { return Output{Ok: b} }
func unknown() Output              { return Output{Unknown: true} }
func missing() Output              { return Output{} }

func TestHandBuiltHistories(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
		ops  []porcupine.Operation
	}{
		{"sequential", true, []porcupine.Operation{
			op(0, 0, 1, put("k", "a"), ok(true)),
			op(0, 2, 3, get("k"), value("a")),
		}},
		{"stale read", false, []porcupine.Operation{
			op(0, 0, 1, put("k", "a"), ok(true)),
			op(0, 2, 3, put("k", "b"), ok(true)),
			op(1, 4, 5, get("k"), value("a")),
		}},
		{"read overlapping a write sees the old value", true, []porcupine.Operation{
			op(0, 0, 1, put("k", "a"), ok(true)),
			op(0, 2, 6, put("k", "b"), ok(true)),
			op(1, 3, 5, get("k"), value("a")),
		}},
		{"lost write", false, []porcupine.Operation{
			op(0, 0, 1, put("k", "a"), ok(true)),
			op(1, 2, 3, get("k"), missing()),
		}},
		{"write applied twice", false, []porcupine.Operation{
			op(0, 0, 1, create("k", "a"), ok(true)),
			op(1, 2, 3, del("k"), ok(true)),
			op(1, 4, 5, get("k"), value("a")),
		}},
		{"swap from the wrong value", false, []porcupine.Operation{
			op(0, 0, 1, put("k", "x"), ok(true)),
			op(1, 2, 3, cas("k", "y", "z"), ok(true)),
		}},
		{"failed swap that should have worked", false, []porcupine.Operation{
			op(0, 0, 1, put("k", "x"), ok(true)),
			op(1, 2, 3, cas("k", "x", "z"), ok(false)),
		}},
		{"two creates both succeed", false, []porcupine.Operation{
			op(0, 0, 3, create("k", "a"), ok(true)),
			op(1, 1, 4, create("k", "b"), ok(true)),
		}},
		{"indeterminate write seen", true, []porcupine.Operation{
			op(0, 0, never, put("k", "a"), unknown()),
			op(1, 5, 6, get("k"), value("a")),
		}},
		{"indeterminate write never seen", true, []porcupine.Operation{
			op(0, 0, never, put("k", "a"), unknown()),
			op(1, 5, 6, get("k"), missing()),
		}},
		{"indeterminate write seen, then gone", false, []porcupine.Operation{
			op(0, 0, never, put("k", "a"), unknown()),
			op(1, 2, 3, get("k"), value("a")),
			op(1, 4, 5, get("k"), missing()),
		}},
		{"indeterminate swap", true, []porcupine.Operation{
			op(2, 0, 1, put("k", "x"), ok(true)),
			op(0, 2, never, cas("k", "x", "y"), unknown()),
			op(1, 5, 6, get("k"), value("y")),
		}},
		{"keys are independent", true, []porcupine.Operation{
			op(0, 0, 1, put("a", "1"), ok(true)),
			op(1, 2, 3, put("b", "2"), ok(true)),
			op(2, 4, 5, get("a"), value("1")),
			op(2, 6, 7, get("b"), value("2")),
		}},
		{"stale read on one key among many", false, []porcupine.Operation{
			op(0, 0, 1, put("a", "1"), ok(true)),
			op(1, 2, 3, put("b", "2"), ok(true)),
			op(1, 4, 5, put("b", "3"), ok(true)),
			op(2, 6, 7, get("a"), value("1")),
			op(2, 8, 9, get("b"), value("2")),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := Linearizable(tc.ops, 10*time.Second)
			if v.Result == porcupine.Unknown {
				t.Fatal("timed out")
			}
			if v.OK() != tc.ok {
				t.Errorf("linearizable %v, want %v", v.OK(), tc.ok)
			}
		})
	}
}

func randomHistory(rng *rand.Rand, clients, perClient int) []porcupine.Operation {
	type cur struct {
		found bool
		value string
	}
	keys := []string{"a", "b", "c"}
	st := map[string]cur{}
	var ops []porcupine.Operation
	now := int64(0)
	for i := range clients * perClient {
		client := i % clients
		k := keys[rng.IntN(len(keys))]
		s := st[k]
		var in Input
		var out Output
		switch rng.IntN(4) {
		case 0:
			in = get(k)
			out = Output{Found: s.found, Value: s.value}
		case 1:
			in = put(k, fmt.Sprintf("v%d", i))
			out = ok(true)
			st[k] = cur{true, in.Value}
		case 2:
			in = del(k)
			out = ok(s.found)
			st[k] = cur{}
		default:
			in = cas(k, s.value, fmt.Sprintf("v%d", i))
			if rng.IntN(2) == 0 {
				in.Expect = "nope"
			}
			match := s.found && s.value == in.Expect
			out = ok(match)
			if match {
				st[k] = cur{true, in.Value}
			}
		}
		ops = append(ops, op(client, now, now+1, in, out))
		now += 2
	}
	return ops
}

func TestMutatedHistoriesAreCaught(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	caught, mutated := 0, 0
	for range 200 {
		h := randomHistory(rng, 4, 25)
		if v := Linearizable(h, 10*time.Second); !v.OK() {
			t.Fatalf("a sequential history from the model itself failed: %v", v.Result)
		}
		var reads []int
		for i, o := range h {
			in := o.Input.(Input)
			if in.Kind == Get {
				for j := i - 1; j >= 0; j-- {
					prev := h[j].Input.(Input)
					if prev.Key == in.Key && prev.Kind != Get {
						reads = append(reads, i)
						break
					}
				}
			}
		}
		if len(reads) == 0 {
			continue
		}
		i := reads[rng.IntN(len(reads))]
		out := h[i].Output.(Output)
		if out.Found {
			out.Value += "-stale"
		} else {
			out = value("from-nowhere")
		}
		h[i].Output = out
		mutated++
		if v := Linearizable(h, 10*time.Second); v.Result == porcupine.Illegal {
			caught++
		}
	}
	if mutated < 100 || caught != mutated {
		t.Fatalf("caught %d of %d mutated histories", caught, mutated)
	}
}
