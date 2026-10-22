package live

import (
	"context"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
)

func Execute(ctx context.Context, c *client.Client, in check.Input, read client.ReadMode) (check.Output, error) {
	var out check.Output
	var err error
	switch in.Kind {
	case check.Get:
		var kv client.KeyValue
		kv, out.Found, err = c.Get(ctx, in.Key, read)
		out.Value = string(kv.Value)
	case check.Put:
		_, err = c.Put(ctx, in.Key, []byte(in.Value))
		out.Ok = true
	case check.CAS:
		out.Ok, _, err = c.CompareAndSwap(ctx, in.Key, client.Expect{Value: []byte(in.Expect), Missing: in.ExpectMissing}, []byte(in.Value))
	case check.Delete:
		out.Ok, err = c.Delete(ctx, in.Key)
	}
	if err != nil {
		return check.Output{Unknown: true}, err
	}
	return out, nil
}
