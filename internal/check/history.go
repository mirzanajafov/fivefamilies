package check

import (
	"math"

	"github.com/anishathalye/porcupine"
)

type Op struct {
	Client int    `json:"client"`
	Input  Input  `json:"input"`
	Output Output `json:"output"`
	Call   int64  `json:"call_ns"`
	Return int64  `json:"return_ns"`
}

func History(ops []Op) ([]porcupine.Operation, int) {
	out := make([]porcupine.Operation, 0, len(ops))
	unknown := 0
	for _, op := range ops {
		ret := op.Return
		if op.Output.Unknown {
			if op.Input.Kind == Get {
				continue
			}
			unknown++
			ret = math.MaxInt64
		}
		out = append(out, porcupine.Operation{ClientId: op.Client, Input: op.Input, Output: op.Output, Call: op.Call, Return: ret})
	}
	return out, unknown
}
