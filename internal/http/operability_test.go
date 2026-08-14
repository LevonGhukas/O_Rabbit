package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

type fakeCommitReconciler struct {
	calls int
}

func (f *fakeCommitReconciler) ReconcileCommittingRuns(context.Context) error {
	f.calls++
	return nil
}

func TestRunDiagnosisEndpointIsAuthenticatedAndRedacted(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, db.Run{ID: "http-diag", JobID: "job", DatasetKey: "http-diag-dataset", Status: "COMMITTING", CorrelationID: "corr", StartedAt: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "admin-token")
	h := srv.Handler()

	unauthorized := httptest.NewRecorder()
	h.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/runs/http-diag/diagnosis", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d", unauthorized.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/runs/http-diag/diagnosis", nil)
	req.Header.Set("Authorization", "Bearer admin-token")
	resp := httptest.NewRecorder()
	h.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	for _, want := range []string{`"run_id": "http-diag"`, `"status": "COMMITTING"`, `"suggested_next_action"`} {
		if !strings.Contains(resp.Body.String(), want) {
			t.Fatalf("missing %q in %s", want, resp.Body.String())
		}
	}
	for _, forbidden := range []string{"fencing_token", "commit_intent_json", "registration_config_json"} {
		if strings.Contains(resp.Body.String(), forbidden) {
			t.Fatalf("response exposed %q: %s", forbidden, resp.Body.String())
		}
	}
}

func TestRecoveryEndpointAcknowledgesQuarantineIdempotentlyAndAudits(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := st.CreateJob(ctx, db.Job{ID: "http-recover-job", Name: "http-recover-job", SourceConnectionID: "source", TargetConnectionID: "target", SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "table", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateRun(ctx, db.Run{ID: "http-recover", JobID: "http-recover-job", DatasetKey: "http-recover-dataset", Status: "RUNNING", CorrelationID: "corr", StartedAt: now.Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{ID: "http-recover-task", RunID: "http-recover", TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	policy := db.LeasePolicy{Duration: time.Minute, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
	assigned, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", now, policy, func() (string, error) { return "http-recover-attempt", nil }, func() (string, error) { return "http-recover-token", nil })
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if err := st.AbandonTaskAttemptWithPolicy(ctx, assigned.ID, assigned.AttemptID, "worker", "assignment failed", now, policy); err != nil {
		t.Fatal(err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "admin-token")
	h := srv.Handler()
	body := []byte(`{"action":"acknowledge_quarantine","reason":"source credentials repaired"}`)
	unauthorized := httptest.NewRecorder()
	h.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/api/runs/http-recover/recover", bytes.NewReader(body)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/api/runs/http-recover/recover", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer admin-token")
		req.Header.Set("X-Request-ID", "recover-request")
		resp := httptest.NewRecorder()
		h.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("attempt %d status=%d body=%s", i+1, resp.Code, resp.Body.String())
		}
	}
	audits, err := st.ListAuditRecords(ctx, 10)
	if err != nil || len(audits) != 2 || audits[0].ActorID != tokenFingerprint("admin-token") {
		t.Fatalf("audits=%+v err=%v", audits, err)
	}
	events, err := st.ListEventsForRun(ctx, "http-recover", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Message == "operator recovery requested" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("operator recovery events=%d want 1", count)
	}
}

func TestCommitRecoveryEndpointRechecksStateAndAudits(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.CreateRun(ctx, db.Run{ID: "http-commit-recover", JobID: "job", DatasetKey: "http-commit-recover-dataset", Status: "COMMITTING", CorrelationID: "corr", StartedAt: time.Now().UTC().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	reconciler := &fakeCommitReconciler{}
	srv := NewServer(nil, st, nil, crypto.Key{}, StatusInfo{}, "")
	srv.SetOperability(3, reconciler)
	resp := httptest.NewRecorder()
	srv.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/runs/http-commit-recover/recover", strings.NewReader(`{"action":"reconcile_commit","reason":"live scan is overdue"}`)))
	if resp.Code != http.StatusOK || reconciler.calls != 1 {
		t.Fatalf("status=%d calls=%d body=%s", resp.Code, reconciler.calls, resp.Body.String())
	}
	audits, err := st.ListAuditRecords(ctx, 10)
	if err != nil || len(audits) != 1 || audits[0].Action != "run.recovery.reconcile_commit" {
		t.Fatalf("audits=%+v err=%v", audits, err)
	}
}

func TestLifecycleMetricsUseOnlyBoundedLabels(t *testing.T) {
	body := renderLifecycleMetrics(db.LifecycleMetrics{
		RunsByStatus:                    map[string]int{"COMMITTING": 2, "run-user-input": 99},
		RunsByCommitPhase:               map[string]int{"INTENT": 1},
		TasksByStatus:                   map[string]int{"QUARANTINED": 3},
		RegistrationsByStatus:           map[string]int{"RETRY_REQUIRED": 1},
		RegistrationAttemptsByPhase:     map[string]int{"CATALOG_COMMITTED": 1},
		ReconciliationsByStatus:         map[string]int{"FAILED": 1},
		ReconciliationsByClassification: map[string]int{"unbounded-error-message": 5},
	})
	for _, want := range []string{
		`orabbit_runs{status="COMMITTING"} 2`,
		`orabbit_tasks_quarantined 3`,
		`orabbit_reconciliations_by_classification{classification="OTHER"} 5`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in metrics:\n%s", want, body)
		}
	}
	if strings.Contains(body, "run-user-input") || strings.Contains(body, "unbounded-error-message") {
		t.Fatalf("unbounded label leaked:\n%s", body)
	}
	if err := metricHasForbiddenLabel(body); err != nil {
		t.Fatal(err)
	}
}

func TestReadyRejectsExpiredDurableLeadershipDespiteCachedReadyState(t *testing.T) {
	srv := NewServer(nil, openTestStore(t), nil, crypto.Key{}, StatusInfo{}, "")
	srv.SetLeadershipGuard(fakeLeadership{status: db.Leadership{State: "LEADER", Ready: true}, err: errors.New("durable lease expired")})
	resp := httptest.NewRecorder()
	srv.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", resp.Code, resp.Body.String())
	}
	var body readinessResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Checks["leadership"] != "NOT_DURABLE_LEADER" {
		t.Fatalf("checks=%+v", body.Checks)
	}
}
