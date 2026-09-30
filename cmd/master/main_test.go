package main

import (
	"context"
	"errors"
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

// fakeServers simulates the HTTP and gRPC servers: each returns its result
// only after the servers are told to stop, like the real Serve functions.
type fakeServers struct {
	httpErr, grpcErr chan error
	stopped          chan struct{}
	drained          chan struct{}
}

func newFakeServers() *fakeServers {
	return &fakeServers{httpErr: make(chan error, 1), grpcErr: make(chan error, 1), stopped: make(chan struct{}), drained: make(chan struct{}, 2)}
}

func (f *fakeServers) stop() { close(f.stopped) }

// drainOnStop makes a server finish (with err) only once stop is called.
func (f *fakeServers) drainOnStop(ch chan error, err error) {
	go func() {
		<-f.stopped
		time.Sleep(20 * time.Millisecond)
		f.drained <- struct{}{}
		ch <- err
	}()
}

func TestAwaitShutdownExitCodes(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	serverErr := errors.New("listen tcp: address already in use")
	tests := []struct {
		name string
		// trigger ends the wait; it receives the signal and leadership cancel
		// functions and the fake servers.
		trigger func(cancelSignal, loseLeadership context.CancelFunc, f *fakeServers)
		want    int
	}{
		{name: "signal", want: exitOK, trigger: func(cancelSignal, _ context.CancelFunc, f *fakeServers) {
			cancelSignal()
			f.drainOnStop(f.httpErr, nil)
			f.drainOnStop(f.grpcErr, nil)
		}},
		{name: "leadership lost", want: exitFailure, trigger: func(_, loseLeadership context.CancelFunc, f *fakeServers) {
			loseLeadership()
			f.drainOnStop(f.httpErr, nil)
			f.drainOnStop(f.grpcErr, nil)
		}},
		{name: "http server error", want: exitFailure, trigger: func(_, _ context.CancelFunc, f *fakeServers) {
			f.drained <- struct{}{}
			f.httpErr <- serverErr
			f.drainOnStop(f.grpcErr, nil)
		}},
		{name: "grpc server stops unexpectedly", want: exitFailure, trigger: func(_, _ context.CancelFunc, f *fakeServers) {
			f.drained <- struct{}{}
			f.grpcErr <- nil
			f.drainOnStop(f.httpErr, nil)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			signalCtx, cancelSignal := context.WithCancel(context.Background())
			defer cancelSignal()
			leaderCtx, loseLeadership := context.WithCancel(signalCtx)
			defer loseLeadership()
			f := newFakeServers()
			tc.trigger(cancelSignal, loseLeadership, f)

			got := awaitShutdown(signalCtx, leaderCtx, f.stop, f.httpErr, f.grpcErr, quiet)
			if got != tc.want {
				t.Fatalf("exit code=%d want %d", got, tc.want)
			}
			// Both servers must have finished draining before returning.
			if len(f.drained) != 2 {
				t.Fatalf("awaitShutdown returned before both servers drained (%d/2)", len(f.drained))
			}
		})
	}
}

func TestRecoveryExitCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	if got := recoveryExitCode(ctx); got != exitFailure {
		t.Fatalf("recovery failure without a signal must exit %d, got %d", exitFailure, got)
	}
	cancel()
	if got := recoveryExitCode(ctx); got != exitOK {
		t.Fatalf("recovery interrupted by shutdown must exit %d, got %d", exitOK, got)
	}
}
