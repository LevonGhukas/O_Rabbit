package db

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRunDiagnosisIdentifiesLifecycleBlockersWithoutSecrets(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		seed   func(*testing.T, *Store) string
		assert func(*testing.T, RunDiagnosis)
	}{
		{
			name: "stuck committing",
			seed: func(t *testing.T, st *Store) string {
				_, err := st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_phase) VALUES('diag-committing','job','dataset-commit','COMMITTING','corr','2026-07-29T11:00:00Z','','STATE_VERIFIED')`)
				if err != nil {
					t.Fatal(err)
				}
				return "diag-committing"
			},
			assert: func(t *testing.T, got RunDiagnosis) {
				if got.Status != "COMMITTING" || got.CommitPhase != "STATE_VERIFIED" || got.CommitAgeSeconds != 3600 || !strings.Contains(got.SuggestedNextAction, "commit reconciliation") {
					t.Fatalf("diagnosis=%+v", got)
				}
			},
		},
		{
			name: "quarantined task",
			seed: func(t *testing.T, st *Store) string {
				createLeaseTestTask(t, st, "diag-quarantine")
				policy := LeasePolicy{Duration: time.Minute, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
				a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-secret-name", now.Add(-time.Minute), policy, fixedGenerator("diag-attempt"), fixedGenerator("do-not-expose-token"))
				if err != nil || !ok {
					t.Fatalf("assign ok=%v err=%v", ok, err)
				}
				if err := st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker-secret-name", "assignment failed at /Users/private/data.parquet", now, policy); err != nil {
					t.Fatal(err)
				}
				return a.RunID
			},
			assert: func(t *testing.T, got RunDiagnosis) {
				if !got.OperatorReview || got.TaskCounts["QUARANTINED"] != 1 || len(got.BlockingTasks) != 1 || got.BlockingTasks[0].RecentAttempts[0].FailureClass != "ASSIGNMENT_BUILD_FAILURE" {
					t.Fatalf("diagnosis=%+v", got)
				}
			},
		},
		{
			name: "registration blocked",
			seed: func(t *testing.T, st *Store) string {
				earlier := insertRegistrationFixture(t, st, "earlier-registration-run", "dataset-reg", 1, RegistrationPending)
				r := insertRegistrationFixture(t, st, "diag-registration-blocked", "dataset-reg", 2, RegistrationBlocked)
				if _, err := st.db.Exec(`UPDATE iceberg_registrations SET last_error_class='ORDERING_BLOCKED' WHERE id=?`, r.ID); err != nil {
					t.Fatal(err)
				}
				_ = earlier
				return r.RunID
			},
			assert: func(t *testing.T, got RunDiagnosis) {
				if got.Registration == nil || got.Registration.Status != RegistrationBlocked || got.Registration.BlockedBy == "" || !got.OperatorReview {
					t.Fatalf("diagnosis=%+v", got)
				}
			},
		},
		{
			name: "insufficient evidence",
			seed: func(t *testing.T, st *Store) string {
				r := insertRegistrationFixture(t, st, "diag-insufficient", "dataset-reconcile", 1, RegistrationReconciling)
				if _, err := st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='FAILED',reconciliation_outcome='INSUFFICIENT_EVIDENCE',reconciliation_error_class='CATALOG_HISTORY_INCOMPLETE' WHERE id=?`, r.ID); err != nil {
					t.Fatal(err)
				}
				return r.RunID
			},
			assert: func(t *testing.T, got RunDiagnosis) {
				if got.Registration == nil || got.Registration.Reconciliation.Outcome != "INSUFFICIENT_EVIDENCE" || !got.OperatorReview || !strings.Contains(got.SuggestedNextAction, "do not replay") {
					t.Fatalf("diagnosis=%+v", got)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			st := openTestStore(t)
			runID := tc.seed(t, st)
			got, err := st.DiagnoseRun(ctx, runID, now, 1)
			if err != nil {
				t.Fatal(err)
			}
			tc.assert(t, got)
			body, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"do-not-expose-token", "/Users/private", "data.parquet", "fencing_token", "partition_spec_json"} {
				if strings.Contains(string(body), secret) {
					t.Fatalf("diagnosis exposed %q: %s", secret, body)
				}
			}
		})
	}
}

func TestLifecycleRecoveryIsAuditedIdempotentAndReceiptSafe(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	r := insertRegistrationFixture(t, st, "recover-registration", "recover-dataset", 1, RegistrationRetryRequired)
	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET next_eligible_at='2099-01-01T00:00:00Z' WHERE id=?`, r.ID); err != nil {
		t.Fatal(err)
	}
	audit := AuditRecord{ActorType: "token", ActorID: "sha256:test", RequestID: "request-1"}
	first, err := st.RequestLifecycleRecovery(ctx, r.RunID, "registration_retry", "catalog outage resolved", audit)
	if err != nil || !first.Changed {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := st.RequestLifecycleRecovery(ctx, r.RunID, "registration_retry", "catalog outage resolved", audit)
	if err != nil || second.Changed {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	var next *string
	if err := st.db.QueryRow(`SELECT next_eligible_at FROM iceberg_registrations WHERE id=?`, r.ID).Scan(&next); err != nil || next != nil {
		t.Fatalf("next=%v err=%v", next, err)
	}
	audits, err := st.ListAuditRecords(ctx, 10)
	if err != nil || len(audits) != 2 || audits[0].Action != "run.recovery.registration_retry" {
		t.Fatalf("audits=%+v err=%v", audits, err)
	}
	events, err := st.ListEventsForRun(ctx, r.RunID, 100)
	if err != nil {
		t.Fatal(err)
	}
	recoveryEvents := 0
	for _, event := range events {
		if event.Message == "operator recovery requested" {
			recoveryEvents++
		}
	}
	if recoveryEvents != 1 {
		t.Fatalf("recovery events=%d want 1", recoveryEvents)
	}

	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET registered_snapshot_or_metadata_id='{"receipt":"durable"}',next_eligible_at='2099-01-01T00:00:00Z' WHERE id=?`, r.ID); err != nil {
		t.Fatal(err)
	}
	_, err = st.RequestLifecycleRecovery(ctx, r.RunID, "registration_retry", "try replay", audit)
	if !errors.Is(err, ErrRecoveryRefused) || !strings.Contains(err.Error(), "catalog replay forbidden") {
		t.Fatalf("receipt replay err=%v", err)
	}
}

func TestReconciliationRecoveryRequiresIdleRetryableState(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	r := insertRegistrationFixture(t, st, "recover-reconciliation", "recover-reconciliation-dataset", 1, RegistrationReconciling)
	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='RETRY_REQUIRED',reconciliation_next_eligible_at='2099-01-01T00:00:00Z' WHERE id=?`, r.ID); err != nil {
		t.Fatal(err)
	}
	audit := AuditRecord{ActorType: "token", ActorID: "sha256:test"}
	result, err := st.RequestLifecycleRecovery(ctx, r.RunID, "reconciliation_retry", "stable history access restored", audit)
	if err != nil || !result.Changed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	result, err = st.RequestLifecycleRecovery(ctx, r.RunID, "reconciliation_retry", "stable history access restored", audit)
	if err != nil || result.Changed {
		t.Fatalf("repeat result=%+v err=%v", result, err)
	}
	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='INSPECTING',current_reconciliation_attempt_id='active-attempt' WHERE id=?`, r.ID); err != nil {
		t.Fatal(err)
	}
	_, err = st.RequestLifecycleRecovery(ctx, r.RunID, "reconciliation_retry", "unsafe active retry", audit)
	if !errors.Is(err, ErrRecoveryRefused) {
		t.Fatalf("active reconciliation err=%v", err)
	}
}

func TestLifecycleMetricsSnapshotIsDeterministic(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	now := time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
	if _, err := st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_phase) VALUES
		('metrics-commit','job','metrics-d1','COMMITTING','c1','2026-07-29T11:30:00Z','','INTENT'),
		('metrics-failed','job','metrics-d2','FAILED','c2','2026-07-29T11:00:00Z','','')`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO tasks(id,run_id,task_index,partition_spec_json,status,rows_read,bytes_read,bytes_written,parquet_objects_json,attempt_count) VALUES
		('metrics-running','metrics-commit',1,'{}','RUNNING',0,0,0,'[]',1),
		('metrics-quarantine','metrics-failed',1,'{}','QUARANTINED',0,0,0,'[]',1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,failure_class,result_digest,created_at,updated_at) VALUES
		('metrics-attempt','metrics-running',1,'worker','metrics-secret-token','ACTIVE','2026-07-29T11:00:00Z','2026-07-29T11:59:00Z','2026-07-29T11:00:00Z','','','2026-07-29T11:00:00Z','2026-07-29T11:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	reg := insertRegistrationFixture(t, st, "metrics-registration", "metrics-d3", 1, RegistrationRetryRequired)
	if _, err := st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='FAILED',reconciliation_error_class='CATALOG_HISTORY_INCOMPLETE' WHERE id=?`, reg.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`UPDATE master_leadership SET instance_id='metrics-leader',epoch=7,status='ACTIVE',lease_deadline_ms=? WHERE leadership_name='master'`, now.Add(time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO master_leadership_history(id,instance_id,epoch,event_type,occurred_at_ms) VALUES('metrics-loss','old',6,'MASTER_LEADERSHIP_LOST',?)`, now.Add(-time.Minute).UnixMilli()); err != nil {
		t.Fatal(err)
	}

	got, err := st.LifecycleMetrics(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.RunsByStatus["COMMITTING"] != 1 || got.RunsByStatus["FAILED"] != 1 || got.OldestCommittingAgeSeconds != 1800 {
		t.Fatalf("run metrics=%+v", got)
	}
	if got.TasksByStatus["RUNNING"] != 1 || got.TasksByStatus["QUARANTINED"] != 1 || got.LeasedTasks != 1 || got.ExpiredActiveLeases != 1 {
		t.Fatalf("task metrics=%+v", got)
	}
	if got.RegistrationRetryRequired != 1 || got.ReconciliationsByClassification["CATALOG_HISTORY_INCOMPLETE"] != 1 {
		t.Fatalf("registration metrics=%+v", got)
	}
	if got.LeadershipActive != 1 || got.LeadershipEpoch != 7 || got.LeadershipRenewalFailures != 1 {
		t.Fatalf("leadership metrics=%+v", got)
	}
}
