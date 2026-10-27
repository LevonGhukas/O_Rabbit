package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

func insertReconciliationRun(t *testing.T, st *Store, runID, status string) {
	t.Helper()
	if _, err := st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json) VALUES(?,'job','dataset',?,'corr','2026-07-29T11:00:00Z','')`, runID, status); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationV32CreatesSourceReconciliationsTable(t *testing.T) {
	ctx := context.Background()
	raw := openRawTestDB(t)
	if err := Migrate(ctx, raw); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := raw.QueryRowContext(ctx, `SELECT version FROM schema_migrations WHERE version=32`).Scan(&version); err != nil {
		t.Fatalf("v32 not recorded: %v", err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO source_reconciliations(run_id,task_id,method,consistency,observed_at,status) VALUES('r','t','count','none','2026-01-01T00:00:00Z','match')`); err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `INSERT INTO source_reconciliations(run_id,task_id,method,consistency,observed_at,status) VALUES('r','t','count','none','2026-01-01T00:00:00Z','match')`); err == nil {
		t.Fatal("expected primary key (run_id, task_id) conflict")
	}
}

func TestUpsertSourceReconciliationReplacesEarlierAttempt(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	rec := SourceReconciliation{RunID: "r1", TaskID: "t1", Method: SourceReconciliationMethodCount, Consistency: "immutable", ObservedAt: "2026-07-29T11:00:00Z",
		LowerBound: "10", LowerExclusive: true, UpperBound: "99", UpperInclusive: true, SourceRows: 5, OutputRows: 4, Status: SourceReconciliationMismatch, Detail: "first"}
	if err := st.UpsertSourceReconciliation(ctx, rec); err != nil {
		t.Fatal(err)
	}
	rec.Status, rec.SourceRows, rec.OutputRows, rec.Detail = SourceReconciliationMatch, 4, 4, "second"
	if err := st.UpsertSourceReconciliation(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := st.ListSourceReconciliations(ctx, "r1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Status != SourceReconciliationMatch || got[0].Detail != "second" || got[0].LowerBound != "10" || !got[0].LowerExclusive || !got[0].UpperInclusive {
		t.Fatalf("records=%+v", got)
	}
}

func TestRunDiagnosisIncludesSourceReconciliation(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	insertReconciliationRun(t, st, "diag-recon", "FAILED")
	for _, rec := range []SourceReconciliation{
		{RunID: "diag-recon", TaskID: "t1", Status: SourceReconciliationMatch, SourceRows: 3, OutputRows: 3},
		{RunID: "diag-recon", TaskID: "t2", Status: SourceReconciliationMismatch, SourceRows: 9, OutputRows: 3, Detail: "source has 6 more rows"},
		{RunID: "diag-recon", TaskID: "t3", Status: SourceReconciliationIndeterminate, SourceRows: 4, OutputRows: 3},
	} {
		rec.Method, rec.Consistency, rec.ObservedAt = SourceReconciliationMethodCount, "immutable", "2026-07-29T11:00:00Z"
		if err := st.UpsertSourceReconciliation(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}
	got, err := st.DiagnoseRun(ctx, "diag-recon", time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	sr := got.SourceReconciliation
	if sr == nil || sr.Method != SourceReconciliationMethodCount || sr.Consistency != "immutable" {
		t.Fatalf("summary=%+v", sr)
	}
	if sr.Counts[SourceReconciliationMatch] != 1 || sr.Counts[SourceReconciliationMismatch] != 1 || sr.Counts[SourceReconciliationIndeterminate] != 1 {
		t.Fatalf("counts=%v", sr.Counts)
	}
	if len(sr.Findings) != 2 {
		t.Fatalf("findings must list non-matching tasks only: %+v", sr.Findings)
	}
	if !got.OperatorReview || !strings.Contains(got.SuggestedNextAction, "source reconciliation") {
		t.Fatalf("next action=%q review=%v", got.SuggestedNextAction, got.OperatorReview)
	}
}

func TestRunDiagnosisOmitsSourceReconciliationWhenNotRecorded(t *testing.T) {
	st := openTestStore(t)
	insertReconciliationRun(t, st, "diag-none", "SUCCEEDED")
	got, err := st.DiagnoseRun(context.Background(), "diag-none", time.Now(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceReconciliation != nil {
		t.Fatalf("summary=%+v", got.SourceReconciliation)
	}
}
