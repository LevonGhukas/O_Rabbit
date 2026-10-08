package db

import (
	"context"
	"strings"
	"testing"
)

// A run that fails because of a task must say which error stopped it, using
// the class and message of the first failed task's last attempt.
func TestTryFinalizeRunRecordsFirstTaskFailure(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	run := Run{ID: "run-fail", JobID: "job", DatasetKey: "dataset-fail", Status: "RUNNING", CorrelationID: "c", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{
		{ID: "task-ok", RunID: run.ID, TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "SUCCEEDED"},
		{ID: "task-bad", RunID: run.ID, TaskIndex: 2, PartitionSpec: []byte(`{}`), Status: "FAILED"},
	}); err != nil {
		t.Fatal(err)
	}
	now := nowUTC()
	if _, err := st.db.ExecContext(ctx, `UPDATE tasks SET error_message=?, finished_at=? WHERE id='task-bad'`,
		`open source reader: failed to connect to postgres://etl:s3cret@db:5432/app: password authentication failed`, now); err != nil {
		t.Fatal(err)
	}
	for i, class := range []string{"NETWORK_CONNECTION_FAILED", "AUTHENTICATION_FAILED"} {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,failure_class,created_at,updated_at)
			VALUES(?,?,?,?,?,'FAILED',?,?,?,?,?,?)`, "attempt-"+class, "task-bad", i+1, "w", "token-"+class, now, now, now, class, now, now); err != nil {
			t.Fatal(err)
		}
	}

	changed, status, err := st.TryFinalizeRun(ctx, run.ID)
	if err != nil || !changed || status != "FAILED" {
		t.Fatalf("changed=%v status=%q err=%v", changed, status, err)
	}
	got, err := st.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FailureClass != "AUTHENTICATION_FAILED" || got.FailurePhase != RunFailurePhaseExtract {
		t.Fatalf("failure_class=%q failure_phase=%q, want last attempt's AUTHENTICATION_FAILED in extract", got.FailureClass, got.FailurePhase)
	}
	summary := *got.ErrorSummary
	if !strings.HasPrefix(summary, "1 of 2 task(s) failed: ") || !strings.Contains(summary, "password authentication failed") {
		t.Fatalf("error_summary=%q must name the count and the task error", summary)
	}
	if strings.Contains(summary, "s3cret") {
		t.Fatalf("error_summary leaks a password: %q", summary)
	}
}

func TestRedactCredentials(t *testing.T) {
	for in, want := range map[string]string{
		"dial postgres://etl:s3cret@db:5432/app failed": "dial postgres://etl:***@db:5432/app failed",
		"mongodb://u:p%40ss@h/db":                       "mongodb://u:***@h/db",
		"no credentials in postgres://db:5432/app":      "no credentials in postgres://db:5432/app",
	} {
		if got := RedactCredentials(in); got != want {
			t.Errorf("RedactCredentials(%q) = %q, want %q", in, got, want)
		}
	}
}
