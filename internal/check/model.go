package check

import (
	"fmt"
	"time"

	"github.com/anishathalye/porcupine"
)

type Kind uint8

const (
	Get Kind = iota + 1
	Put
	Delete
	CAS
)

type Input struct {
	Kind          Kind
	Key           string
	Value         string
	Expect        string
	ExpectMissing bool
}

type Output struct {
	Unknown bool
	Found   bool
	Value   string
	Ok      bool
}

type state struct {
	found bool
	value string
}

func casMatches(st state, in Input) bool {
	if in.ExpectMissing {
		return !st.found
	}
	return st.found && st.value == in.Expect
}

func step(s, i, o any) (bool, any) {
	st, in, out := s.(state), i.(Input), o.(Output)
	switch in.Kind {
	case Get:
		if out.Unknown {
			return true, st
		}
		return out.Found == st.found && (!st.found || out.Value == st.value), st
	case Put:
		return true, state{found: true, value: in.Value}
	case Delete:
		if !out.Unknown && out.Ok != st.found {
			return false, st
		}
		return true, state{}
	case CAS:
		match := casMatches(st, in)
		if !out.Unknown && out.Ok != match {
			return false, st
		}
		if match {
			return true, state{found: true, value: in.Value}
		}
		return true, st
	}
	return false, st
}

func partition(history []porcupine.Operation) [][]porcupine.Operation {
	index := map[string]int{}
	var parts [][]porcupine.Operation
	for _, op := range history {
		key := op.Input.(Input).Key
		i, ok := index[key]
		if !ok {
			i = len(parts)
			index[key] = i
			parts = append(parts, nil)
		}
		parts[i] = append(parts[i], op)
	}
	return parts
}

func describeOperation(i, o any) string {
	in, out := i.(Input), o.(Output)
	var res string
	switch {
	case out.Unknown:
		res = "?"
	case in.Kind == Get && !out.Found:
		res = "missing"
	case in.Kind == Get:
		res = fmt.Sprintf("%q", out.Value)
	default:
		res = fmt.Sprintf("%v", out.Ok)
	}
	switch in.Kind {
	case Get:
		return fmt.Sprintf("get(%s) -> %s", in.Key, res)
	case Put:
		return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
	case Delete:
		return fmt.Sprintf("delete(%s) -> %s", in.Key, res)
	case CAS:
		if in.ExpectMissing {
			return fmt.Sprintf("cas(%s, missing -> %q) -> %s", in.Key, in.Value, res)
		}
		return fmt.Sprintf("cas(%s, %q -> %q) -> %s", in.Key, in.Expect, in.Value, res)
	}
	return "?"
}

func describeState(s any) string {
	st := s.(state)
	if !st.found {
		return "missing"
	}
	return fmt.Sprintf("%q", st.value)
}

var KVModel = porcupine.Model{
	Partition:         partition,
	Init:              func() any { return state{} },
	Step:              step,
	Equal:             func(a, b any) bool { return a.(state) == b.(state) },
	DescribeOperation: describeOperation,
	DescribeState:     describeState,
}

type Verdict struct {
	Result porcupine.CheckResult
	Info   porcupine.LinearizationInfo
	Ops    int
	Took   time.Duration
}

func (v Verdict) OK() bool {
	return v.Result == porcupine.Ok
}

func Linearizable(history []porcupine.Operation, timeout time.Duration) Verdict {
	start := time.Now()
	res, info := porcupine.CheckOperationsVerbose(KVModel, history, timeout)
	return Verdict{Result: res, Info: info, Ops: len(history), Took: time.Since(start)}
}
