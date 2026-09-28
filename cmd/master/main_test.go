package main

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

type liveCommitReconciler struct {
	mu     sync.Mutex
	status string
	calls  int
	called chan struct{}
}

func (r *liveCommitReconciler) ReconcileCommittingRuns(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.status == "COMMITTING" {
		r.status = "SUCCEEDED"
	}
	select {
	case r.called <- struct{}{}:
	default:
	}
	return nil
}

func (r *liveCommitReconciler) snapshot() (string, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.status, r.calls
}

func TestLiveCommittingReconciliationRecoversPostStartupRunAndRepeatsSafely(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reconciler := &liveCommitReconciler{status: "COMMITTING", called: make(chan struct{}, 4)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runCommittingReconciliationLoop(ctx, 5*time.Millisecond, 100*time.Millisecond, reconciler, nil)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-reconciler.called:
		case <-time.After(time.Second):
			t.Fatal("live committing-run reconciliation did not tick")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("live reconciliation loop ignored leader cancellation")
	}
	status, calls := reconciler.snapshot()
	if status != "SUCCEEDED" || calls < 2 {
		t.Fatalf("status=%s calls=%d", status, calls)
	}
}

func TestRunPeriodicSurvivesPanickingTick(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan int, 3)
	n := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		runPeriodic(ctx, slog.New(slog.NewTextHandler(io.Discard, nil)), "test", time.Millisecond, func() {
			n++
			ticks <- n
			if n == 1 {
				panic("first tick fails")
			}
		})
	}()
	for want := 1; want <= 3; want++ {
		select {
		case got := <-ticks:
			if got != want {
				t.Fatalf("tick=%d want %d", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("loop stopped after a panicking tick; saw %d ticks", want-1)
		}
	}
	cancel()
	<-done
}
