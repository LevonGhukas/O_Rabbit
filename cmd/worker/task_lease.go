package main

import (
	"context"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
)

type leaseClock interface {
	Now() time.Time
	NewTimer(time.Duration) leaseTimer
}

type leaseTimer interface {
	C() <-chan time.Time
	Stop()
}

type realLeaseClock struct{}

type realLeaseTimer struct{ timer *time.Timer }

func (realLeaseClock) Now() time.Time { return time.Now() }

func (realLeaseClock) NewTimer(d time.Duration) leaseTimer {
	return realLeaseTimer{timer: time.NewTimer(d)}
}

func (t realLeaseTimer) C() <-chan time.Time { return t.timer.C }

func (t realLeaseTimer) Stop() { t.timer.Stop() }

func renewalDelay(now, deadline time.Time) time.Duration {
	remaining := deadline.Sub(now)
	interval := remaining / 3
	if interval > 10*time.Second {
		interval = 10 * time.Second
	}
	if interval < time.Second {
		interval = time.Second
	}
	if interval > remaining {
		interval = remaining
	}
	return interval
}

func retryableRenewalError(err error) bool {
	return isTransientRPCError(err)
}

func maintainTaskLeaseWithClock(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, cancel context.CancelFunc, lost chan<- error, clock leaseClock) {
	deadline := time.UnixMilli(t.LeaseDeadlineUnixMs)
	for {
		now := clock.Now()
		if !now.Before(deadline) {
			reportOwnershipLost(cancel, lost, context.DeadlineExceeded)
			return
		}
		delay := renewalDelay(now, deadline)
		timer := clock.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C():
			timer.Stop()
			if !clock.Now().Before(deadline) {
				reportOwnershipLost(cancel, lost, context.DeadlineExceeded)
				return
			}
			callCtx, done := context.WithTimeout(ctx, 3*time.Second)
			resp, err := cp.RenewTaskLease(callCtx, &grpcpb.RenewTaskLeaseRequest{WorkerId: workerID, TaskId: t.TaskId, AttemptId: t.AttemptId, FencingToken: t.FencingToken})
			done()
			if err == nil {
				deadline = time.UnixMilli(resp.LeaseDeadlineUnixMs)
				continue
			}
			if !retryableRenewalError(err) || !clock.Now().Before(deadline) {
				reportOwnershipLost(cancel, lost, err)
				return
			}
		}
	}
}

func reportOwnershipLost(cancel context.CancelFunc, lost chan<- error, err error) {
	select {
	case lost <- err:
	default:
	}
	cancel()
}
