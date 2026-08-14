package main

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizePollInterval(t *testing.T) {
	if got := normalizePollInterval(0); got != 2*time.Second {
		t.Fatalf("normalizePollInterval(0) = %v, want %v", got, 2*time.Second)
	}
	if got := normalizePollInterval(-time.Second); got != 2*time.Second {
		t.Fatalf("normalizePollInterval(-1s) = %v, want %v", got, 2*time.Second)
	}
	if got := normalizePollInterval(350 * time.Millisecond); got != 350*time.Millisecond {
		t.Fatalf("normalizePollInterval(350ms) = %v, want %v", got, 350*time.Millisecond)
	}
}

func TestTaskPollingProtocolMismatchIsPermanent(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, "worker protocol version 4 is unsupported; accepted version=5")
	if !isPermanentTaskPollingError(err) {
		t.Fatal("protocol mismatch should stop task polling")
	}
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted} {
		if isPermanentTaskPollingError(status.Error(code, "temporary")) {
			t.Fatalf("%s should remain retryable", code)
		}
	}
}

func TestWaitForContextCancelsPromptly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if waitForContext(ctx, 5*time.Second) {
		t.Fatal("canceled wait reported completion")
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("canceled wait took %v", elapsed)
	}
}

func TestSuccessfulResultReportingRetriesTransientErrors(t *testing.T) {
	transient := []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted, codes.Internal}
	for _, code := range transient {
		t.Run(code.String(), func(t *testing.T) {
			var calls atomic.Int32
			var accepted atomic.Int32
			cp := fakeControlPlaneClient{reportTaskResult: func(context.Context, *grpcpb.ReportTaskResultRequest, ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
				if calls.Add(1) == 1 {
					return nil, status.Error(code, "temporary")
				}
				accepted.Add(1)
				return &grpcpb.ReportTaskResultResponse{Accepted: true}, nil
			}}
			err := reportResultWithRetryWindow(context.Background(), slog.Default(), cp, &grpcpb.ReportTaskResultRequest{Status: "SUCCEEDED"}, time.Second, 100*time.Millisecond)
			if err != nil || calls.Load() != 2 || accepted.Load() != 1 {
				t.Fatalf("calls=%d accepted=%d err=%v", calls.Load(), accepted.Load(), err)
			}
		})
	}
	t.Run("SQLite busy", func(t *testing.T) {
		var calls atomic.Int32
		var accepted atomic.Int32
		cp := fakeControlPlaneClient{reportTaskResult: func(context.Context, *grpcpb.ReportTaskResultRequest, ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
			if calls.Add(1) == 1 {
				return nil, errors.New("SQLITE_BUSY: database is locked")
			}
			accepted.Add(1)
			return &grpcpb.ReportTaskResultResponse{Accepted: true}, nil
		}}
		err := reportResultWithRetryWindow(context.Background(), slog.Default(), cp, &grpcpb.ReportTaskResultRequest{Status: "SUCCEEDED"}, time.Second, 100*time.Millisecond)
		if err != nil || calls.Load() != 2 || accepted.Load() != 1 {
			t.Fatalf("calls=%d accepted=%d err=%v", calls.Load(), accepted.Load(), err)
		}
	})
}

func TestSuccessfulResultReportingExhaustionSuppressesFailure(t *testing.T) {
	var calls atomic.Int32
	var statusesMu sync.Mutex
	var statuses []string
	cp := fakeControlPlaneClient{reportTaskResult: func(_ context.Context, req *grpcpb.ReportTaskResultRequest, _ ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
		calls.Add(1)
		statusesMu.Lock()
		statuses = append(statuses, req.Status)
		statusesMu.Unlock()
		return nil, status.Error(codes.Unavailable, "partition")
	}}
	err := reportResultWithRetryWindow(context.Background(), slog.Default(), cp, &grpcpb.ReportTaskResultRequest{Status: "SUCCEEDED"}, 250*time.Millisecond, 50*time.Millisecond)
	var transient *transientSuccessReportError
	if !errors.As(err, &transient) {
		t.Fatalf("err=%T %v", err, err)
	}
	if calls.Load() < 2 {
		t.Fatalf("calls=%d want retry", calls.Load())
	}
	statusesMu.Lock()
	defer statusesMu.Unlock()
	for _, got := range statuses {
		if got != "SUCCEEDED" {
			t.Fatalf("unexpected result report status=%q; statuses=%v", got, statuses)
		}
	}
}

func TestSuccessfulResultReportingStopsOnPermanentRejection(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied, codes.NotFound} {
		t.Run(code.String(), func(t *testing.T) {
			var calls atomic.Int32
			cp := fakeControlPlaneClient{reportTaskResult: func(context.Context, *grpcpb.ReportTaskResultRequest, ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
				calls.Add(1)
				return nil, status.Error(code, "permanent")
			}}
			err := reportResultWithRetryWindow(context.Background(), slog.Default(), cp, &grpcpb.ReportTaskResultRequest{Status: "SUCCEEDED"}, time.Second, 100*time.Millisecond)
			var transient *transientSuccessReportError
			if status.Code(err) != code || errors.As(err, &transient) || calls.Load() != 1 {
				t.Fatalf("calls=%d err=%T %v", calls.Load(), err, err)
			}
		})
	}
}

func TestSuccessfulResultReportingStopsOnContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	var calls atomic.Int32
	cp := fakeControlPlaneClient{reportTaskResult: func(ctx context.Context, _ *grpcpb.ReportTaskResultRequest, _ ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
		calls.Add(1)
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	done := make(chan error, 1)
	go func() {
		done <- reportResultWithRetryWindow(ctx, slog.Default(), cp, &grpcpb.ReportTaskResultRequest{Status: "SUCCEEDED"}, time.Second, time.Second)
	}()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%T %v", err, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d want 1", calls.Load())
	}
}

func TestRunScopedObjectPrefixes(t *testing.T) {
	one := buildAttemptRunPrefix("datasets/orders/", "run-1", "task-1", "attempt-1")
	two := buildAttemptRunPrefix("datasets/orders", "run-1", "task-1", "attempt-2")
	if one != two {
		t.Fatalf("run prefixes should be identical for same run: %q vs %q", one, two)
	}
	if !strings.HasSuffix(one, "/_runs/run-run-1") {
		t.Fatalf("prefix has unexpected format: %q", one)
	}
}

func TestArtifactFailureReportingIsSafeAndClassified(t *testing.T) {
	var captured *grpcpb.ReportTaskProgressRequest
	cp := fakeControlPlaneClient{reportTaskProgress: func(_ context.Context, req *grpcpb.ReportTaskProgressRequest, _ ...grpc.CallOption) (*grpcpb.ReportTaskProgressResponse, error) {
		captured = req
		return &grpcpb.ReportTaskProgressResponse{}, nil
	}}
	task := &grpcpb.TaskAssignment{TaskId: "task", RunId: "run", AttemptId: "attempt", AttemptNumber: 2, FencingToken: "do-not-persist"}
	reportArtifactFailureBestEffort(context.Background(), slog.Default(), cp, "worker", task, &artifact.Failure{Classification: artifact.FailureMultipartCompleteAmbiguous, Ambiguous: true, ReconciliationOK: true, VerificationMethod: artifact.VerificationPortable, ObjectKey: "safe/key", FileIndex: 3})
	if captured == nil {
		t.Fatal("failure event not reported")
	}
	if strings.Contains(captured.FieldsJson, task.FencingToken) || !strings.Contains(captured.FieldsJson, string(artifact.FailureMultipartCompleteAmbiguous)) || !strings.Contains(captured.FieldsJson, `"object_key":"safe/key"`) {
		t.Fatalf("fields=%s", captured.FieldsJson)
	}
}

type fakeLeaseTimer struct {
	clock   *fakeLeaseClock
	at      time.Time
	ch      chan time.Time
	stopped bool
}

func (t *fakeLeaseTimer) C() <-chan time.Time { return t.ch }
func (t *fakeLeaseTimer) Stop() {
	t.clock.mu.Lock()
	t.stopped = true
	t.clock.mu.Unlock()
}

type fakeLeaseClock struct {
	mu      sync.Mutex
	now     time.Time
	timers  []*fakeLeaseTimer
	changed chan struct{}
}

func newFakeLeaseClock(now time.Time) *fakeLeaseClock {
	return &fakeLeaseClock{now: now, changed: make(chan struct{}, 100)}
}

func (c *fakeLeaseClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeLeaseClock) NewTimer(d time.Duration) leaseTimer {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	timer := &fakeLeaseTimer{clock: c, at: c.now.Add(d), ch: ch}
	c.timers = append(c.timers, timer)
	c.changed <- struct{}{}
	return timer
}

func (c *fakeLeaseClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	keep := c.timers[:0]
	for _, timer := range c.timers {
		if timer.stopped {
			continue
		}
		if !timer.at.After(now) {
			timer.ch <- now
		} else {
			keep = append(keep, timer)
		}
	}
	c.timers = keep
	c.mu.Unlock()
}

func (c *fakeLeaseClock) PendingTimers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, timer := range c.timers {
		if !timer.stopped {
			n++
		}
	}
	return n
}

func (c *fakeLeaseClock) FireNext(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	if len(c.timers) == 0 {
		c.mu.Unlock()
		t.Fatal("no fake lease timer to fire")
	}
	var next time.Time
	for _, timer := range c.timers {
		if timer.stopped {
			continue
		}
		if next.IsZero() || timer.at.Before(next) {
			next = timer.at
		}
	}
	if next.IsZero() {
		c.mu.Unlock()
		t.Fatal("no active fake lease timer to fire")
	}
	for _, timer := range c.timers {
		if timer.stopped {
			continue
		}
		if timer.at.Before(next) {
			next = timer.at
		}
	}
	d := next.Sub(c.now)
	c.mu.Unlock()
	c.Advance(d)
}

func (c *fakeLeaseClock) waitTimer(t *testing.T) {
	t.Helper()
	select {
	case <-c.changed:
	case <-time.After(time.Second):
		t.Fatal("renewal loop did not schedule a timer")
	}
}

func testAssignment(now time.Time) *grpcpb.TaskAssignment {
	return &grpcpb.TaskAssignment{TaskId: "task-1", RunId: "run-1", AttemptId: "attempt-1", FencingToken: "secret-token", LeaseDeadlineUnixMs: now.Add(30 * time.Second).UnixMilli()}
}

func TestLongOperationRenewsWithoutProgressAndStopsCleanly(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	clock := newFakeLeaseClock(t0)
	var renewals atomic.Int32
	var results atomic.Int32
	cp := fakeControlPlaneClient{
		renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
			n := renewals.Add(1)
			_ = n
			return &grpcpb.RenewTaskLeaseResponse{LeaseDeadlineUnixMs: clock.Now().Add(30 * time.Second).UnixMilli()}, nil
		},
		reportTaskResult: func(context.Context, *grpcpb.ReportTaskResultRequest, ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
			results.Add(1)
			return &grpcpb.ReportTaskResultResponse{Accepted: true}, nil
		},
	}
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- executeTaskWithBody(context.Background(), cp, "worker-1", testAssignment(t0), clock, func(ctx context.Context) error {
			select {
			case <-finish:
				return reportResultWithRetry(ctx, slog.Default(), cp, &grpcpb.ReportTaskResultRequest{TaskId: "task-1", AttemptId: "attempt-1", FencingToken: "secret-token", Status: "SUCCEEDED"})
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	for i := 0; i < 4; i++ {
		clock.waitTimer(t)
		clock.FireNext(t)
	}
	close(finish)
	if err := <-done; err != nil {
		t.Fatalf("execute: %v", err)
	}
	if renewals.Load() != 4 || results.Load() != 1 {
		t.Fatalf("renewals=%d results=%d", renewals.Load(), results.Load())
	}
	before := renewals.Load()
	clock.Advance(time.Hour)
	if renewals.Load() != before {
		t.Fatalf("renewal continued after completion: %d -> %d", before, renewals.Load())
	}
	if clock.PendingTimers() != 0 {
		t.Fatalf("renewal timers leaked: %d", clock.PendingTimers())
	}
}

func TestWorkerShutdownStopsRenewalAndTimer(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	clock := newFakeLeaseClock(t0)
	var renewals atomic.Int32
	cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
		renewals.Add(1)
		return nil, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- executeTaskWithBody(ctx, cp, "worker", testAssignment(t0), clock, func(taskCtx context.Context) error { <-taskCtx.Done(); return taskCtx.Err() })
	}()
	clock.waitTimer(t)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown err=%v", err)
	}
	if renewals.Load() != 0 || clock.PendingTimers() != 0 {
		t.Fatalf("renewals=%d timers=%d", renewals.Load(), clock.PendingTimers())
	}
}

func TestTransientRenewalFailuresUseLastKnownDeadline(t *testing.T) {
	t.Run("recovers before deadline", func(t *testing.T) {
		t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
		clock := newFakeLeaseClock(t0)
		var calls atomic.Int32
		cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
			if calls.Add(1) <= 2 {
				return nil, status.Error(codes.Unavailable, "temporary")
			}
			return &grpcpb.RenewTaskLeaseResponse{LeaseDeadlineUnixMs: clock.Now().Add(30 * time.Second).UnixMilli()}, nil
		}}
		finish := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error {
				select {
				case <-finish:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
		}()
		for i := 0; i < 3; i++ {
			clock.waitTimer(t)
			clock.FireNext(t)
		}
		close(finish)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 3 {
			t.Fatalf("renew calls=%d", calls.Load())
		}
	})

	t.Run("deadline stops work", func(t *testing.T) {
		t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
		clock := newFakeLeaseClock(t0)
		var calls atomic.Int32
		var uploadStarted atomic.Bool
		var resultReported atomic.Bool
		cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
			calls.Add(1)
			return nil, status.Error(codes.Unavailable, "partition")
		}}
		done := make(chan error, 1)
		go func() {
			done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error {
				<-ctx.Done()
				if ctx.Err() == nil {
					uploadStarted.Store(true)
				}
				if ctx.Err() == nil {
					resultReported.Store(true)
				}
				return ctx.Err()
			})
		}()
		for i := 0; i < 2; i++ {
			clock.waitTimer(t)
			clock.FireNext(t)
		}
		clock.waitTimer(t)
		clock.Advance(30 * time.Second)
		err := <-done
		var lost *taskOwnershipLostError
		if !errors.As(err, &lost) {
			t.Fatalf("err=%T %v", err, err)
		}
		if uploadStarted.Load() || resultReported.Load() {
			t.Fatal("work continued after last known deadline")
		}
		if calls.Load() != 2 {
			t.Fatalf("renew calls=%d want 2; no RPC at deadline", calls.Load())
		}
	})
}

func TestDefinitiveRenewalRejectionsCancelImmediately(t *testing.T) {
	for _, code := range []codes.Code{codes.FailedPrecondition, codes.Canceled, codes.NotFound, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			clock := newFakeLeaseClock(t0)
			cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
				return nil, status.Error(code, "ownership rejected")
			}}
			done := make(chan error, 1)
			go func() {
				done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
			}()
			clock.waitTimer(t)
			clock.FireNext(t)
			var lost *taskOwnershipLostError
			if err := <-done; !errors.As(err, &lost) {
				t.Fatalf("err=%T %v", err, err)
			}
		})
	}
}

func TestConfirmedFencingSuppressesUploadAndResultBoundaries(t *testing.T) {
	stages := []string{"before extraction completion", "after parquet finalization", "before upload", "during upload", "after upload", "preparing result", "result in flight"}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			clock := newFakeLeaseClock(t0)
			var uploads, results atomic.Int32
			cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "fenced")
			}}
			reached := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error {
					close(reached)
					<-ctx.Done()
					if ctx.Err() == nil {
						uploads.Add(1)
					}
					if ctx.Err() == nil {
						results.Add(1)
					}
					return ctx.Err()
				})
			}()
			<-reached
			clock.waitTimer(t)
			clock.FireNext(t)
			var lost *taskOwnershipLostError
			if err := <-done; !errors.As(err, &lost) {
				t.Fatalf("err=%v", err)
			}
			if uploads.Load() != 0 || results.Load() != 0 {
				t.Fatalf("uploads=%d results=%d", uploads.Load(), results.Load())
			}
		})
	}
}

func TestConfirmedFencingCancelsInFlightUploadAndResultRPC(t *testing.T) {
	for _, stage := range []string{"upload", "result"} {
		t.Run(stage, func(t *testing.T) {
			t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
			clock := newFakeLeaseClock(t0)
			started := make(chan struct{})
			var canceled atomic.Bool
			cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
				return nil, status.Error(codes.FailedPrecondition, "fenced")
			}}
			if stage == "result" {
				cp.reportTaskResult = func(ctx context.Context, _ *grpcpb.ReportTaskResultRequest, _ ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
					close(started)
					<-ctx.Done()
					canceled.Store(true)
					return nil, ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() {
				done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error {
					if stage == "upload" {
						close(started)
						<-ctx.Done()
						canceled.Store(true)
						return ctx.Err()
					}
					return reportResultWithRetry(ctx, slog.Default(), cp, &grpcpb.ReportTaskResultRequest{TaskId: "task-1", AttemptId: "attempt-1", FencingToken: "secret-token", Status: "SUCCEEDED"})
				})
			}()
			<-started
			clock.waitTimer(t)
			clock.FireNext(t)
			var lost *taskOwnershipLostError
			if err := <-done; !errors.As(err, &lost) {
				t.Fatalf("err=%v", err)
			}
			if !canceled.Load() {
				t.Fatalf("in-flight %s did not observe cancellation", stage)
			}
		})
	}
}

func TestDurableUploadAfterFencingRemainsUnreported(t *testing.T) {
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	clock := newFakeLeaseClock(t0)
	uploadDurable := make(chan struct{})
	var results atomic.Int32
	cp := fakeControlPlaneClient{renewTaskLease: func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
		return nil, status.Error(codes.FailedPrecondition, "fenced")
	}}
	done := make(chan error, 1)
	go func() {
		done <- executeTaskWithBody(context.Background(), cp, "worker", testAssignment(t0), clock, func(ctx context.Context) error {
			close(uploadDurable) // attempt-scoped object became durable
			<-ctx.Done()
			if ctx.Err() == nil {
				results.Add(1)
			}
			return ctx.Err()
		})
	}()
	<-uploadDurable
	clock.waitTimer(t)
	clock.FireNext(t)
	var lost *taskOwnershipLostError
	if err := <-done; !errors.As(err, &lost) {
		t.Fatalf("err=%v", err)
	}
	if results.Load() != 0 {
		t.Fatal("durable stale upload was reported")
	}
}

func TestCheckTaskCancellationReturnsCanceledError(t *testing.T) {
	cp := fakeControlPlaneClient{
		reportTaskProgress: func(ctx context.Context, in *grpcpb.ReportTaskProgressRequest, opts ...grpc.CallOption) (*grpcpb.ReportTaskProgressResponse, error) {
			return nil, status.Error(codes.Canceled, "run canceled by client")
		},
	}

	err := checkTaskCancellation(context.Background(), slog.Default(), cp, "worker-1", &grpcpb.TaskAssignment{
		TaskId: "task-1",
		RunId:  "run-1",
	}, 10, 20, 30)
	cancelErr, ok := asTaskCanceledError(err)
	if !ok {
		t.Fatalf("expected taskCanceledError, got %T (%v)", err, err)
	}
	if !strings.Contains(cancelErr.Error(), "run canceled by client") {
		t.Fatalf("cancel error=%q want reason from server", cancelErr.Error())
	}
}

type fakeControlPlaneClient struct {
	reportTaskProgress func(context.Context, *grpcpb.ReportTaskProgressRequest, ...grpc.CallOption) (*grpcpb.ReportTaskProgressResponse, error)
	renewTaskLease     func(context.Context, *grpcpb.RenewTaskLeaseRequest, ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error)
	acquireUpload      func(context.Context, *grpcpb.AcquireUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.AcquireUploadCapacityResponse, error)
	releaseUpload      func(context.Context, *grpcpb.ReleaseUploadCapacityRequest, ...grpc.CallOption) (*grpcpb.ReleaseUploadCapacityResponse, error)
	reportTaskResult   func(context.Context, *grpcpb.ReportTaskResultRequest, ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error)
}

func (f fakeControlPlaneClient) RegisterWorker(context.Context, *grpcpb.RegisterWorkerRequest, ...grpc.CallOption) (*grpcpb.RegisterWorkerResponse, error) {
	panic("unexpected RegisterWorker call")
}

func (f fakeControlPlaneClient) Heartbeat(context.Context, *grpcpb.HeartbeatRequest, ...grpc.CallOption) (*grpcpb.HeartbeatResponse, error) {
	panic("unexpected Heartbeat call")
}

func (f fakeControlPlaneClient) RequestTask(context.Context, *grpcpb.RequestTaskRequest, ...grpc.CallOption) (*grpcpb.RequestTaskResponse, error) {
	panic("unexpected RequestTask call")
}

func (f fakeControlPlaneClient) RenewTaskLease(ctx context.Context, in *grpcpb.RenewTaskLeaseRequest, opts ...grpc.CallOption) (*grpcpb.RenewTaskLeaseResponse, error) {
	if f.renewTaskLease == nil {
		panic("unexpected RenewTaskLease call")
	}
	return f.renewTaskLease(ctx, in, opts...)
}

func (f fakeControlPlaneClient) AcquireUploadCapacity(ctx context.Context, in *grpcpb.AcquireUploadCapacityRequest, opts ...grpc.CallOption) (*grpcpb.AcquireUploadCapacityResponse, error) {
	if f.acquireUpload == nil {
		panic("unexpected AcquireUploadCapacity call")
	}
	return f.acquireUpload(ctx, in, opts...)
}

func (f fakeControlPlaneClient) ReleaseUploadCapacity(ctx context.Context, in *grpcpb.ReleaseUploadCapacityRequest, opts ...grpc.CallOption) (*grpcpb.ReleaseUploadCapacityResponse, error) {
	if f.releaseUpload == nil {
		panic("unexpected ReleaseUploadCapacity call")
	}
	return f.releaseUpload(ctx, in, opts...)
}

func (f fakeControlPlaneClient) ReportTaskProgress(ctx context.Context, in *grpcpb.ReportTaskProgressRequest, opts ...grpc.CallOption) (*grpcpb.ReportTaskProgressResponse, error) {
	if f.reportTaskProgress == nil {
		panic("unexpected ReportTaskProgress call")
	}
	return f.reportTaskProgress(ctx, in, opts...)
}

func (f fakeControlPlaneClient) ReportTaskResult(ctx context.Context, in *grpcpb.ReportTaskResultRequest, opts ...grpc.CallOption) (*grpcpb.ReportTaskResultResponse, error) {
	if f.reportTaskResult == nil {
		panic("unexpected ReportTaskResult call")
	}
	return f.reportTaskResult(ctx, in, opts...)
}
