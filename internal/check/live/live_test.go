package live

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/mirzanajafov/fivefamilies/client"
	"github.com/mirzanajafov/fivefamilies/internal/check"
)

func TestAnOperationThatGetsNoAnswerHasAnUnknownOutcome(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := client.New([]string{addr}, client.Options{AttemptTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	for _, in := range []check.Input{
		{Kind: check.Get, Key: "k"},
		{Kind: check.Put, Key: "k", Value: "v"},
		{Kind: check.CAS, Key: "k", ExpectMissing: true, Value: "v"},
		{Kind: check.Delete, Key: "k"},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		out, err := Execute(ctx, c, in, client.ReadIndex)
		cancel()
		if err == nil || out != (check.Output{Unknown: true}) {
			t.Errorf("kind %d against a dead node: %+v, %v", in.Kind, out, err)
		}
	}
}
