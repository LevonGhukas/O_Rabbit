package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rejectingLeadership struct{}

func (rejectingLeadership) Assert(context.Context) error { return errors.New("lost") }

type cancellableLeadership struct {
	ctx context.Context
}

func (l cancellableLeadership) Assert(context.Context) error {
	if err := l.ctx.Err(); err != nil {
		return err
	}
	return nil
}

func (l cancellableLeadership) WorkContext() context.Context { return l.ctx }

type blockingLeadershipRegistrar struct {
	started chan struct{}
	done    chan struct{}
}

func (r *blockingLeadershipRegistrar) RegisterRun(ctx context.Context, _ icebergreg.RunRequest) (icebergreg.RunResult, error) {
	close(r.started)
	<-ctx.Done()
	close(r.done)
	return icebergreg.RunResult{}, ctx.Err()
}

type completingLeadershipRegistrar struct{}

func (completingLeadershipRegistrar) RegisterRun(_ context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	if req.BeforeExternalCommit != nil {
		if err := req.BeforeExternalCommit(); err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	receipt := req.CatalogReceipt
	if req.CatalogReceiptFactory != nil {
		var err error
		receipt, err = req.CatalogReceiptFactory()
		if err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	if req.CatalogCommitted != nil {
		if err := req.CatalogCommitted(receipt); err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	if req.IceStateWriting != nil {
		if err := req.IceStateWriting(); err != nil {
			return icebergreg.RunResult{}, err
		}
	}
	return icebergreg.RunResult{Objects: len(req.ExactArtifacts)}, nil
}

func TestStaleMasterRejectsWorkerMutations(t *testing.T) {
	srv := NewServer(nil, nil, nil, crypto.Key{}, time.Second, nil)
	srv.SetLeadershipGuard(rejectingLeadership{})
	if _, err := srv.RegisterWorker(context.Background(), &grpcpb.RegisterWorkerRequest{WorkerId: "w"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("register code=%s err=%v", status.Code(err), err)
	}
	if _, err := srv.RequestTask(context.Background(), &grpcpb.RequestTaskRequest{WorkerId: "w", ProtocolVersion: workerProtocolVersion}); status.Code(err) != codes.Unavailable {
		t.Fatalf("request code=%s err=%v", status.Code(err), err)
	}
	if _, err := srv.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("result code=%s err=%v", status.Code(err), err)
	}
}

func TestLeadershipLossCancelsInFlightCommitFinalization(t *testing.T) {
	st := openGRPCTestStore(t)
	createGRPCTestRunAndTask(t, st, "run-leader-commit", "job-leader-commit", "task-leader-commit", "PENDING")
	assignGRPCTestAttempt(t, st, "task-leader-commit", "worker")

	workCtx, loseLeadership := context.WithCancel(context.Background())
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.SetLeadershipGuard(cancellableLeadership{ctx: workCtx})
	started := make(chan struct{})
	srv.commitRunFn = func(ctx context.Context, _ string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	done := make(chan error, 1)
	go func() {
		_, err := srv.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{
			WorkerId:     "worker",
			TaskId:       "task-leader-commit",
			RunId:        "run-leader-commit",
			AttemptId:    "attempt-task-leader-commit",
			FencingToken: "token-task-leader-commit",
			Status:       "SUCCEEDED",
		})
		done <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("commit finalization did not start")
	}
	loseLeadership()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReportTaskResult: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("commit finalization ignored leadership cancellation")
	}

	run, err := st.GetRun(context.Background(), "run-leader-commit")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "COMMITTING" || run.CommitPhase == "COMPLETE" {
		t.Fatalf("stale leader advanced run: status=%s phase=%s", run.Status, run.CommitPhase)
	}
	events, err := st.ListEventsForRun(context.Background(), run.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Message == "run SUCCEEDED" || event.Message == "run committed" {
			t.Fatalf("stale leader published final completion event: %+v", event)
		}
	}

	freshCtx := context.Background()
	fresh := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	fresh.SetLeadershipGuard(cancellableLeadership{ctx: freshCtx})
	fresh.commitRunFn = func(ctx context.Context, runID string) error {
		intent, _ := json.Marshal(durableCommitIntent{CommitID: "fresh-commit", ManifestKey: "dataset/_commits/fresh.json", StateKey: "dataset/_state.json", Manifest: json.RawMessage(`{"schema_version":2,"artifacts":[]}`), ProposedState: json.RawMessage(`{}`)})
		if err := st.SaveCommitIntent(ctx, runID, "fresh-commit", intent); err != nil {
			return err
		}
		return st.SetCommitPhase(ctx, runID, "VERIFIED")
	}
	if err := fresh.ReconcileCommittingRuns(freshCtx); err != nil {
		t.Fatal(err)
	}
	run, err = st.GetRun(freshCtx, run.ID)
	if err != nil || run.Status != "SUCCEEDED" || run.CommitPhase != "COMPLETE" {
		t.Fatalf("fresh leader did not recover run: %+v err=%v", run, err)
	}
}

func TestLeadershipLossCancelsRegistrationAndLeavesItRecoverable(t *testing.T) {
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, "run-leader-registration", "job-leader-registration", "task-leader-registration")
	assignGRPCTestAttempt(t, st, "task-leader-registration", "worker")

	workCtx, loseLeadership := context.WithCancel(context.Background())
	registrar := &blockingLeadershipRegistrar{started: make(chan struct{}), done: make(chan struct{})}
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, registrar)
	srv.SetLeadershipGuard(cancellableLeadership{ctx: workCtx})
	srv.commitRunFn = func(ctx context.Context, runID string) error {
		return saveGRPCTestVerifiedEmptyIntent(ctx, st, runID, "exports/orders")
	}

	resp, err := srv.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{
		WorkerId:     "worker",
		TaskId:       "task-leader-registration",
		RunId:        "run-leader-registration",
		AttemptId:    "attempt-task-leader-registration",
		FencingToken: "token-task-leader-registration",
		Status:       "SUCCEEDED",
	})
	if err != nil || !resp.Accepted {
		t.Fatalf("ReportTaskResult accepted=%v err=%v", resp.Accepted, err)
	}
	select {
	case <-registrar.started:
	case <-time.After(time.Second):
		t.Fatal("registration did not start")
	}
	loseLeadership()
	select {
	case <-registrar.done:
	case <-time.After(time.Second):
		t.Fatal("registration ignored leadership cancellation")
	}

	registration, err := st.GetRegistrationForRun(context.Background(), "run-leader-registration")
	if err != nil {
		t.Fatal(err)
	}
	if registration.Status == db.RegistrationRegistered {
		t.Fatal("stale leader completed registration")
	}
	policy := db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute}
	expiredAt := time.Now().Add(time.Minute)
	if n, err := st.ExpireRegistrationAttempts(context.Background(), expiredAt, policy); err != nil || n != 1 {
		t.Fatalf("expire canceled registration n=%d err=%v", n, err)
	}
	registration, err = st.GetRegistrationForRun(context.Background(), "run-leader-registration")
	if err != nil {
		t.Fatal(err)
	}
	if registration.Status != db.RegistrationRetryRequired || registration.CurrentAttemptID != nil {
		t.Fatalf("registration is not recoverable: %+v", registration)
	}

	fresh := NewServer(nil, st, nil, crypto.Key{}, time.Second, completingLeadershipRegistrar{})
	fresh.SetLeadershipGuard(cancellableLeadership{ctx: context.Background()})
	fresh.nowFn = func() time.Time { return expiredAt.Add(2 * time.Second) }
	processed, err := fresh.ProcessRegistrationOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("fresh leader registration processed=%v err=%v", processed, err)
	}
	registration, err = st.GetRegistrationForRun(context.Background(), "run-leader-registration")
	if err != nil || registration.Status != db.RegistrationRegistered {
		t.Fatalf("fresh leader did not recover registration: %+v err=%v", registration, err)
	}
}
