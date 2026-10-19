package lab

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var errGaveUp = errors.New("gave up")

func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func poll(ctx context.Context, every, limit time.Duration, done func(context.Context) (bool, error)) error {
	lctx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		if ok, err := done(lctx); ok || err != nil {
			return err
		}
		if !wait(lctx, every) {
			if err := ctx.Err(); err != nil {
				return err
			}
			return fmt.Errorf("%w after %v", errGaveUp, limit)
		}
	}
}
