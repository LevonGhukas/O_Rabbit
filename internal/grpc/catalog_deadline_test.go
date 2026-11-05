package grpcapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	grpcpb "github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
)

const (
	catalogAttemptDeadline = 200 * time.Millisecond
	catalogAttemptReturn   = 5 * time.Second
	catalogTimeoutMetric   = "orabbit_catalog_attempt_timeouts_total"
)

// hungInspector registers like hungRegistrar, then blocks catalog inspection
// until its context ends and returns whatever evidence it had gathered.
type hungInspector struct {
	hungRegistrar
	partial icebergreg.CatalogObservation
}

func (h *hungInspector) InspectCatalog(ctx context.Context, _ icebergreg.InspectionRequest) (icebergreg.CatalogObservation, error) {
	<-ctx.Done()
	return h.partial, ctx.Err()
}

// firstHangsRegistrar blocks its first registration until the context ends
// and completes every later one.
type firstHangsRegistrar struct {
	mu      sync.Mutex
	calls   int
	hungRun string
}

func (r *firstHangsRegistrar) RegisterRun(ctx context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	r.mu.Lock()
	r.calls++
	first := r.calls == 1
	if first {
		r.hungRun = req.RunID
	}
	r.mu.Unlock()
	if first {
		<-ctx.Done()
		return icebergreg.RunResult{}, ctx.Err()
	}
	return completingLeadershipRegistrar{}.RegisterRun(ctx, req)
}

func (r *firstHangsRegistrar) hung() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hungRun
}

// periodicLeaderContext is the long-lived, deadline-free context the master's
// periodic catalog loop passes to each attempt.
func periodicLeaderContext(t *testing.T, srv *Server) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv.SetLeadershipGuard(cancellableLeadership{ctx: ctx})
	return ctx
}

// processBounded runs one catalog attempt and fails the test if it outlives
// the attempt deadline by a wide margin.
func processBounded(t *testing.T, process func() (bool, error)) {
	t.Helper()
	type result struct {
		processed bool
		err       error
	}
	done := make(chan result, 1)
	go func() {
		processed, err := process()
		done <- result{processed, err}
	}()
	select {
	case res := <-done:
		if res.err != nil || !res.processed {
			t.Fatalf("catalog attempt processed=%v err=%v", res.processed, res.err)
		}
	case <-time.After(catalogAttemptReturn):
		t.Fatal("catalog attempt outlived its deadline")
	}
}

func assertCatalogSlotsFree(t *testing.T, srv *Server) {
	t.Helper()
	if n := len(srv.catalogWorkSlots); n != 0 {
		t.Fatalf("catalog work slots still held after the attempt: %d", n)
	}
}

func catalogTimeoutCount(t *testing.T, operation string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	if err := telemetry.WriteText(rec); err != nil {
		t.Fatal(err)
	}
	prefix := catalogTimeoutMetric + `{operation="` + operation + `"} `
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if value, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.ParseFloat(value, 64)
			if err != nil {
				t.Fatalf("parse %q: %v", line, err)
			}
			return n
		}
	}
	return 0
}

// reconcilingRegistration leaves a registration whose attempt timed out after
// the external commit began, so it waits for catalog reconciliation.
func reconcilingRegistration(t *testing.T, runID string, inspector *hungInspector) (*Server, *db.Store, context.Context, db.Registration) {
	t.Helper()
	stale := &hungRegistrar{started: make(chan struct{}, 1)}
	st, expiredAt := claimableHungRegistration(t, runID, stale)
	inspector.startExternal = true
	inspector.started = make(chan struct{}, 1)

	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, inspector)
	srv.SetCommitCatalogPolicy(CommitCatalogPolicy{RegistrationTimeout: catalogAttemptDeadline, ReconciliationTimeout: catalogAttemptDeadline})
	srv.nowFn = func() time.Time { return expiredAt.Add(2 * time.Second) }
	leaderCtx := periodicLeaderContext(t, srv)
	processBounded(t, func() (bool, error) { return srv.ProcessRegistrationOnce(leaderCtx) })

	reg, err := st.GetRegistrationForRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if reg.Status != db.RegistrationReconciling {
		t.Fatalf("setup: status=%s want %s", reg.Status, db.RegistrationReconciling)
	}
	return srv, st, leaderCtx, reg
}

func TestPeriodicReconciliationTimeoutReleasesSlotAndSchedulesRetry(t *testing.T) {
	cases := []struct {
		name    string
		partial icebergreg.CatalogObservation
	}{
		{"no evidence", icebergreg.CatalogObservation{}},
		// Evidence read under an expiring deadline is incomplete and must never
		// prove absence, or the registration would be replayed after a
		// possible catalog commit.
		{"partial evidence", icebergreg.CatalogObservation{
			Backend: "rest-go", TableExists: true, TableIdentifier: "mssql.orders",
			MetadataStart: "s3://bucket1/metadata/v1.json", MetadataEnd: "s3://bucket1/metadata/v1.json",
			CompleteHistory: true, SchemaCompatible: true, LocationCompatible: true,
		}},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runID := "run-reconcile-hung-" + string(rune('a'+i))
			srv, st, leaderCtx, reg := reconcilingRegistration(t, runID, &hungInspector{partial: tc.partial})
			before := catalogTimeoutCount(t, catalogOperationReconciliation)

			processBounded(t, func() (bool, error) { return srv.ProcessReconciliationOnce(leaderCtx) })

			after, err := st.GetRegistrationForRun(context.Background(), runID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != db.RegistrationReconciling {
				t.Fatalf("status=%s want %s", after.Status, db.RegistrationReconciling)
			}
			projection, err := st.GetReconciliationProjection(context.Background(), reg.ID)
			if err != nil {
				t.Fatal(err)
			}
			if projection.Status != "RETRY_REQUIRED" || projection.ErrorClass != reconciliationTimeoutClass || projection.Outcome != "" {
				t.Fatalf("projection=%+v want RETRY_REQUIRED/%s with no outcome", projection, reconciliationTimeoutClass)
			}
			assertCatalogSlotsFree(t, srv)
			if got := catalogTimeoutCount(t, catalogOperationReconciliation); got != before+1 {
				t.Fatalf("reconciliation timeouts=%v want %v", got, before+1)
			}
		})
	}
}

// queueGRPCTestRegistration commits a run whose registration is left PENDING:
// the committing server holds its only catalog slot, so its immediate
// registration launch is refused.
func queueGRPCTestRegistration(t *testing.T, st *db.Store, runID, table string) {
	t.Helper()
	createGRPCTestRegistrableJobRunAndTask(t, st, runID, "job-"+runID, "task-"+runID, grpcTestRegistrationConfig(t, table))
	assignGRPCTestAttempt(t, st, "task-"+runID, "worker")

	committer := NewServer(nil, st, nil, testCryptoKey, time.Second, &hungRegistrar{started: make(chan struct{}, 1)})
	committer.SetLeadershipGuard(cancellableLeadership{ctx: context.Background()})
	committer.SetCatalogWorkLimit(1)
	release, ok := committer.tryAcquireCatalogWork()
	if !ok {
		t.Fatal("setup: committer slot not acquired")
	}
	t.Cleanup(release)
	committer.commitRunFn = func(ctx context.Context, id string) error {
		return saveGRPCTestVerifiedEmptyIntent(ctx, st, id, "exports/orders")
	}
	if _, err := committer.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{
		WorkerId: "worker", TaskId: "task-" + runID, RunId: runID,
		AttemptId: "attempt-task-" + runID, FencingToken: "token-task-" + runID, Status: "SUCCEEDED",
	}); err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}
	deadline := time.Now().Add(catalogAttemptReturn)
	for time.Now().Before(deadline) {
		if reg, err := st.GetRegistrationForRun(context.Background(), runID); err == nil && reg.Status == db.RegistrationPending {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("setup: registration for %s never queued", runID)
}

func TestPeriodicRegistrationTimeoutLetsQueuedRegistrationProceed(t *testing.T) {
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableConnections(t, st)
	queueGRPCTestRegistration(t, st, "run-queue-a", "mssql.orders_a")
	queueGRPCTestRegistration(t, st, "run-queue-b", "mssql.orders_b")

	registrar := &firstHangsRegistrar{}
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, registrar)
	srv.SetCatalogWorkLimit(1)
	srv.SetCommitCatalogPolicy(CommitCatalogPolicy{RegistrationTimeout: catalogAttemptDeadline})
	leaderCtx := periodicLeaderContext(t, srv)
	before := catalogTimeoutCount(t, catalogOperationRegistration)

	processBounded(t, func() (bool, error) { return srv.ProcessRegistrationOnce(leaderCtx) })
	assertCatalogSlotsFree(t, srv)
	processBounded(t, func() (bool, error) { return srv.ProcessRegistrationOnce(leaderCtx) })

	hungRun := registrar.hung()
	nextRun := "run-queue-a"
	if hungRun == nextRun {
		nextRun = "run-queue-b"
	}
	hung, err := st.GetRegistrationForRun(context.Background(), hungRun)
	if err != nil {
		t.Fatal(err)
	}
	if hung.Status != db.RegistrationRetryRequired || hung.LastErrorClass != string(failure.FailureCatalogTimeout) {
		t.Fatalf("timed-out registration status=%s class=%s", hung.Status, hung.LastErrorClass)
	}
	next, err := st.GetRegistrationForRun(context.Background(), nextRun)
	if err != nil {
		t.Fatal(err)
	}
	if next.Status != db.RegistrationRegistered {
		t.Fatalf("queued registration status=%s want %s", next.Status, db.RegistrationRegistered)
	}
	assertCatalogSlotsFree(t, srv)
	if got := catalogTimeoutCount(t, catalogOperationRegistration); got != before+1 {
		t.Fatalf("registration timeouts=%v want %v", got, before+1)
	}
}

func TestClassifyRegistrationFailureTreatsOwnDeadlineAsTimeout(t *testing.T) {
	// A registrar may wrap the interruption in its own classified failure.
	wrapped := failure.NewFailure(failure.FailureCatalogUnavailable, true, true, context.DeadlineExceeded)
	cases := []struct {
		name            string
		externalStarted bool
		timedOut        bool
		want            failure.FailureClass
		wantRetryable   bool
	}{
		{"own deadline before external commit", false, true, failure.FailureCatalogTimeout, true},
		{"own deadline after external commit", true, true, failure.FailureExternalAmbiguous, false},
		{"no deadline keeps registrar class", false, false, failure.FailureCatalogUnavailable, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyRegistrationFailure(wrapped, tc.externalStarted, tc.timedOut)
			if got.Class != tc.want || got.Retryable != tc.wantRetryable {
				t.Fatalf("class=%s retryable=%v want %s/%v", got.Class, got.Retryable, tc.want, tc.wantRetryable)
			}
			if !errors.Is(got.Err, context.DeadlineExceeded) {
				t.Fatalf("original error lost: %v", got.Err)
			}
		})
	}
}

func TestImmediateRegistrationTimeoutIsCountedAndRetried(t *testing.T) {
	const runID = "run-immediate-hung"
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, runID, "job-"+runID, "task-"+runID)
	assignGRPCTestAttempt(t, st, "task-"+runID, "worker")
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, &hungRegistrar{started: make(chan struct{}, 1)})
	srv.SetCommitCatalogPolicy(CommitCatalogPolicy{RegistrationTimeout: catalogAttemptDeadline})
	periodicLeaderContext(t, srv)
	srv.commitRunFn = func(ctx context.Context, id string) error {
		return saveGRPCTestVerifiedEmptyIntent(ctx, st, id, "exports/orders")
	}
	before := catalogTimeoutCount(t, catalogOperationRegistration)

	// Committing the run launches its registration at once.
	if _, err := srv.ReportTaskResult(context.Background(), &grpcpb.ReportTaskResultRequest{
		WorkerId: "worker", TaskId: "task-" + runID, RunId: runID,
		AttemptId: "attempt-task-" + runID, FencingToken: "token-task-" + runID, Status: "SUCCEEDED",
	}); err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}

	deadline := time.Now().Add(catalogAttemptReturn)
	var reg db.Registration
	for time.Now().Before(deadline) {
		var err error
		reg, err = st.GetRegistrationForRun(context.Background(), runID)
		if err == nil && reg.Status == db.RegistrationRetryRequired && len(srv.catalogWorkSlots) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if reg.Status != db.RegistrationRetryRequired || reg.LastErrorClass != string(failure.FailureCatalogTimeout) {
		t.Fatalf("immediate registration status=%s class=%s", reg.Status, reg.LastErrorClass)
	}
	assertCatalogSlotsFree(t, srv)
	if got := catalogTimeoutCount(t, catalogOperationRegistration); got != before+1 {
		t.Fatalf("registration timeouts=%v want %v", got, before+1)
	}
}
