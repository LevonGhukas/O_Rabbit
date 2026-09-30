package grpcapi

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
)

// blockingCommit is a commitRunFn that blocks until released and counts calls.
type blockingCommit struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	ctxErr  atomic.Value // error seen by the commit when it finished
}

func newBlockingCommit() *blockingCommit {
	return &blockingCommit{started: make(chan struct{}, 8), release: make(chan struct{})}
}

func (b *blockingCommit) run(ctx context.Context, _ string) error {
	b.calls.Add(1)
	b.started <- struct{}{}
	select {
	case <-b.release:
		b.ctxErr.Store(errNone{})
		return nil
	case <-ctx.Done():
		b.ctxErr.Store(ctx.Err())
		return ctx.Err()
	}
}

type errNone struct{}

func (errNone) Error() string { return "" }

// reportLastTask completes the run's only task, which moves it to COMMITTING.
func reportLastTask(t *testing.T, srv *Server, ctx context.Context, suffix string) *grpcpb.ReportTaskResultResponse {
	t.Helper()
	resp, err := srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId: "worker-1", TaskId: "task-" + suffix, RunId: "run-" + suffix,
		AttemptId: "attempt-task-" + suffix, FencingToken: "token-task-" + suffix, Status: "SUCCEEDED",
	})
	if err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}
	return resp
}

func newCommitClaimFixture(t *testing.T, suffix string) (*db.Store, *Server, *blockingCommit, *atomic.Int32) {
	t.Helper()
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, "run-"+suffix, "job-"+suffix, "task-"+suffix)
	assignGRPCTestAttempt(t, st, "task-"+suffix, "worker-1")
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	commit := newBlockingCommit()
	srv.commitRunFn = commit.run
	var completed atomic.Int32
	srv.completeRunCommitFn = func(context.Context, string) error { completed.Add(1); return nil }
	return st, srv, commit, &completed
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReportTaskResultReturnsWithoutWaitingForCommit(t *testing.T) {
	st, srv, commit, completed := newCommitClaimFixture(t, "async")

	// The worker's call context ends as soon as the RPC returns, as it does
	// when a real worker's per-attempt deadline expires.
	rpcCtx, cancelRPC := context.WithCancel(context.Background())
	start := time.Now()
	resp := reportLastTask(t, srv, rpcCtx, "async")
	cancelRPC()
	if elapsed := time.Since(start); elapsed > 2*time.Second || !resp.Accepted {
		t.Fatalf("RPC must return promptly (took %v, accepted=%v)", elapsed, resp.Accepted)
	}
	run, err := st.GetRun(context.Background(), "run-async")
	if err != nil || run.Status != "COMMITTING" {
		t.Fatalf("run status=%q err=%v, want COMMITTING", run.Status, err)
	}

	<-commit.started
	close(commit.release)
	waitFor(t, "commit completion", func() bool { return completed.Load() == 1 })
	if err, _ := commit.ctxErr.Load().(error); err != nil && err.Error() != "" {
		t.Fatalf("worker RPC cancellation must not cancel the commit: %v", err)
	}
}

func TestConcurrentCommitTriggersCommitOnce(t *testing.T) {
	_, srv, commit, completed := newCommitClaimFixture(t, "once")
	reportLastTask(t, srv, context.Background(), "once")
	<-commit.started // the RPC-launched committer holds the claim

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ { // reconciliation loop and HTTP recovery scans
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ReconcileCommittingRuns(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := commit.calls.Load(); n != 1 {
		t.Fatalf("commit ran %d times concurrently, want 1", n)
	}
	close(commit.release)
	waitFor(t, "commit completion", func() bool { return completed.Load() == 1 })
}

// stubLeader is a leadership guard whose work context the test controls; the
// master cancels that context on shutdown or loss of leadership.
type stubLeader struct{ ctx context.Context }

func (l stubLeader) Assert(context.Context) error { return nil }
func (l stubLeader) WorkContext() context.Context { return l.ctx }

func TestInterruptedCommitDoesNotConsumeAttempt(t *testing.T) {
	st, srv, commit, _ := newCommitClaimFixture(t, "interrupt")
	workCtx, stopWork := context.WithCancel(context.Background())
	defer stopWork()
	srv.SetLeadershipGuard(stubLeader{ctx: workCtx})

	reportLastTask(t, srv, context.Background(), "interrupt")
	<-commit.started
	stopWork() // shutdown or lost leadership mid-commit

	// The claim is released once the interrupted committer exits.
	waitFor(t, "claim release", func() bool {
		claimed, err := st.ClaimCommittingRun(context.Background(), "run-interrupt", "next", time.Now(), time.Minute)
		return err == nil && claimed
	})
	run, err := st.GetRun(context.Background(), "run-interrupt")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "COMMITTING" || run.CommitReconciliationAttempt != 0 || run.CommitReconciliationStatus == db.CommitReconciliationRetryRequired {
		t.Fatalf("interruption must leave the run COMMITTING without a failed attempt: status=%s reconciliation=%s attempts=%d", run.Status, run.CommitReconciliationStatus, run.CommitReconciliationAttempt)
	}
}
