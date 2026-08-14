package db

import (
	"context"
	"testing"
	"time"
)

func createCommittingRunForReconciliation(t *testing.T, st *Store, id string) {
	t.Helper()
	if err := st.CreateRun(context.Background(), Run{ID: id, JobID: "job", DatasetKey: "dataset-" + id, Status: "COMMITTING", CorrelationID: "corr", StartedAt: nowUTC()}); err != nil {
		t.Fatal(err)
	}
}

func TestDeterministicCommitFailureLeavesEligibilityScan(t *testing.T) {
	st := openTestStore(t)
	createCommittingRunForReconciliation(t, st, "terminal-commit")
	if err := st.RecordCommitReconciliationFailure(context.Background(), "terminal-commit", "DURABLE_INTEGRITY_CONFLICT", "manifest_hash mismatch", false, false, time.Now(), CommitReconciliationPolicy{}); err != nil {
		t.Fatal(err)
	}
	run, err := st.GetRun(context.Background(), "terminal-commit")
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "FAILED" || run.CommitReconciliationStatus != CommitReconciliationTerminal || run.FailureClass != "DURABLE_INTEGRITY_CONFLICT" || run.ErrorSummary == nil || *run.ErrorSummary != "manifest_hash mismatch" {
		t.Fatalf("run=%+v", run)
	}
	ids, err := st.ListCommittingRunIDs(context.Background())
	if err != nil || len(ids) != 0 {
		t.Fatalf("eligible=%v err=%v", ids, err)
	}
}

func TestTransientCommitFailureBacksOffAndExhaustsWithoutLosingCause(t *testing.T) {
	st := openTestStore(t)
	createCommittingRunForReconciliation(t, st, "transient-commit")
	policy := CommitReconciliationPolicy{MaxAttempts: 2, BackoffBase: time.Minute, BackoffMax: time.Minute}
	now := time.Now()
	if err := st.RecordCommitReconciliationFailure(context.Background(), "transient-commit", "TRANSIENT_STORAGE_CATALOG_FAILURE", "storage unavailable", true, false, now, policy); err != nil {
		t.Fatal(err)
	}
	run, _ := st.GetRun(context.Background(), "transient-commit")
	if run.Status != "COMMITTING" || run.CommitReconciliationStatus != CommitReconciliationRetryRequired || run.CommitReconciliationNextRetry == nil {
		t.Fatalf("first failure run=%+v", run)
	}
	ids, _ := st.ListCommittingRunIDs(context.Background())
	if len(ids) != 0 {
		t.Fatalf("backoff run was immediately eligible: %v", ids)
	}
	if err := st.RecordCommitReconciliationFailure(context.Background(), "transient-commit", "TRANSIENT_STORAGE_CATALOG_FAILURE", "storage unavailable", true, false, now.Add(time.Minute), policy); err != nil {
		t.Fatal(err)
	}
	run, _ = st.GetRun(context.Background(), "transient-commit")
	if run.Status != "FAILED" || run.CommitReconciliationStatus != CommitReconciliationTerminal || run.FailureClass != "TRANSIENT_STORAGE_CATALOG_FAILURE" || run.ErrorSummary == nil || *run.ErrorSummary != "storage unavailable" {
		t.Fatalf("exhausted run=%+v", run)
	}
}

func TestOperatorActionCommitFailureIsTerminalAndVisible(t *testing.T) {
	st := openTestStore(t)
	createCommittingRunForReconciliation(t, st, "operator-commit")
	if err := st.RecordCommitReconciliationFailure(context.Background(), "operator-commit", "OPERATOR_ACTION_REQUIRED", "endpoint incompatible", false, true, time.Now(), CommitReconciliationPolicy{}); err != nil {
		t.Fatal(err)
	}
	run, _ := st.GetRun(context.Background(), "operator-commit")
	if run.Status != "FAILED" || !run.OperatorActionRequired || run.CommitReconciliationStatus != CommitReconciliationActionRequired {
		t.Fatalf("run=%+v", run)
	}
}
