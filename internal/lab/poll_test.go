package lab

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPollingStopsAtOnceWhenItsContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := poll(ctx, time.Millisecond, time.Minute, func(context.Context) (bool, error) {
		calls++
		if calls == 3 {
			cancel()
		}
		return false, nil
	})
	if !errors.Is(err, context.Canceled) || calls != 3 {
		t.Fatalf("%d calls after cancelling at the third: %v", calls, err)
	}
}

func TestPollingGivesUpAfterItsLimit(t *testing.T) {
	err := poll(context.Background(), time.Millisecond, 20*time.Millisecond, func(context.Context) (bool, error) {
		return false, nil
	})
	if !errors.Is(err, errGaveUp) {
		t.Fatalf("polling past its limit returned %v", err)
	}
}

func TestEachCheckRunsWithinTheLimit(t *testing.T) {
	var deadline time.Time
	err := poll(context.Background(), time.Millisecond, time.Minute, func(ctx context.Context) (bool, error) {
		deadline, _ = ctx.Deadline()
		return true, nil
	})
	if err != nil || deadline.IsZero() || time.Until(deadline) > time.Minute {
		t.Fatalf("the check's deadline was %v: %v", deadline, err)
	}
}

func TestPollingReportsTheCallersOwnDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := poll(ctx, time.Millisecond, time.Minute, func(context.Context) (bool, error) {
		return false, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("polling past the caller's deadline returned %v", err)
	}
}

func TestPollingStopsOnTheFirstError(t *testing.T) {
	failed := errors.New("refused")
	calls := 0
	err := poll(context.Background(), time.Millisecond, time.Minute, func(context.Context) (bool, error) {
		calls++
		return false, failed
	})
	if !errors.Is(err, failed) || calls != 1 {
		t.Fatalf("%d calls: %v", calls, err)
	}
}
