package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

func TestRepresentativeIllegalTransitionsAreAtomic(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 24, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		run  func(*testing.T)
	}{
		{
			name: "stale success after ownership loss",
			run: func(t *testing.T) {
				st := openTestStore(t)
				createLeaseTestTask(t, st, "illegal-stale-success")
				policy := LeasePolicy{Duration: time.Second, MaxAttempts: 2, BackoffBase: time.Second, BackoffMax: time.Second}
				a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "old-worker", now, policy, fixedGenerator("old-attempt"), fixedGenerator("old-token"))
				if err != nil || !ok {
					t.Fatalf("assign ok=%v err=%v", ok, err)
				}
				if _, err := st.ExpireTaskAttempts(ctx, now.Add(2*time.Second), policy); err != nil {
					t.Fatal(err)
				}
				if _, _, _, err := st.CompleteTaskAttemptAt(ctx, "", a.ID, a.AttemptID, a.FencingToken, "old-worker", "SUCCEEDED", nil, []byte(`[{"key":"stale"}]`), 1, 1, 1, now.Add(3*time.Second)); !errors.Is(err, ErrAttemptFenced) {
					t.Fatalf("stale completion err=%v", err)
				}
				tasks, _ := st.ListTasksForRun(ctx, a.RunID)
				if len(tasks) != 1 || tasks[0].Status != "PENDING" || strings.Contains(string(tasks[0].ParquetObjects), "stale") {
					t.Fatalf("stale completion partially applied: %+v", tasks)
				}
			},
		},
		{
			name: "registration completion from wrong phase",
			run: func(t *testing.T) {
				st := openTestStore(t)
				r := insertRegistrationFixture(t, st, "illegal-registration-phase", "ds", 1, RegistrationPending)
				_, a, ok, err := st.ClaimRegistration(ctx, now, certPolicy())
				if err != nil || !ok {
					t.Fatalf("claim ok=%v err=%v", ok, err)
				}
				if err := st.CompleteRegistration(ctx, r.ID, a.ID, a.FencingToken, now); !errors.Is(err, ErrRegistrationFenced) {
					t.Fatalf("wrong-phase completion err=%v", err)
				}
				got, _ := st.GetRegistrationForRun(ctx, r.RunID)
				var phase, attemptStatus string
				if err := st.db.QueryRow(`SELECT phase,status FROM iceberg_registration_attempts WHERE id=?`, a.ID).Scan(&phase, &attemptStatus); err != nil {
					t.Fatal(err)
				}
				if got.Status != RegistrationRegistering || phase != "PREPARED" || attemptStatus != "ACTIVE" {
					t.Fatalf("wrong-phase completion partially applied: registration=%+v phase=%s attempt=%s", got, phase, attemptStatus)
				}
			},
		},
		{
			name: "retry after quarantine",
			run: func(t *testing.T) {
				st := openTestStore(t)
				createLeaseTestTask(t, st, "illegal-quarantine-retry")
				policy := LeasePolicy{Duration: time.Second, MaxAttempts: 1, BackoffBase: time.Second, BackoffMax: time.Second}
				a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker", now, policy, fixedGenerator("attempt"), fixedGenerator("token"))
				if err != nil || !ok {
					t.Fatalf("assign ok=%v err=%v", ok, err)
				}
				if err := st.AbandonTaskAttemptWithPolicy(ctx, a.ID, a.AttemptID, "worker", "build failed", now, policy); err != nil {
					t.Fatal(err)
				}
				if _, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "retry-worker", now.Add(time.Hour), policy, fixedGenerator("retry-attempt"), fixedGenerator("retry-token")); err != nil || ok {
					t.Fatalf("quarantined retry ok=%v err=%v", ok, err)
				}
				tasks, _ := st.ListTasksForRun(ctx, a.RunID)
				if len(tasks) != 1 || tasks[0].Status != "QUARANTINED" || tasks[0].AttemptCount != 1 {
					t.Fatalf("quarantine changed after retry: %+v", tasks)
				}
			},
		},
		{
			name: "catalog replay after durable receipt",
			run: func(t *testing.T) {
				st := openTestStore(t)
				r := insertRegistrationFixture(t, st, "illegal-catalog-replay", "ds", 1, RegistrationPending)
				_, a, ok, err := st.ClaimRegistration(ctx, now, certPolicy())
				if err != nil || !ok {
					t.Fatalf("claim ok=%v err=%v", ok, err)
				}
				if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
					t.Fatal(err)
				}
				receipt := `{"receipt":"durable"}`
				if err := st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, receipt, now); err != nil {
					t.Fatal(err)
				}
				if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now.Add(time.Second)); !errors.Is(err, ErrRegistrationFenced) {
					t.Fatalf("catalog replay err=%v", err)
				}
				got, _ := st.GetRegistrationForRun(ctx, r.RunID)
				var phase string
				if err := st.db.QueryRow(`SELECT phase FROM iceberg_registration_attempts WHERE id=?`, a.ID).Scan(&phase); err != nil {
					t.Fatal(err)
				}
				if got.Receipt != receipt || phase != "CATALOG_COMMITTED" {
					t.Fatalf("receipt boundary changed: registration=%+v phase=%s", got, phase)
				}
			},
		},
		{
			name: "stale leader mutation",
			run: func(t *testing.T) {
				oldStore, newStore := openSharedLeadershipStores(t)
				first, err := oldStore.AcquireLeadership(ctx, "old", time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := oldStore.ActivateLeadershipFence(ctx, "old", first.Epoch); err != nil {
					t.Fatal(err)
				}
				if _, err := newStore.db.ExecContext(ctx, `UPDATE master_leadership SET lease_deadline_ms=0 WHERE leadership_name='master'`); err != nil {
					t.Fatal(err)
				}
				second, err := newStore.AcquireLeadership(ctx, "new", time.Minute, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := newStore.ActivateLeadershipFence(ctx, "new", second.Epoch); err != nil {
					t.Fatal(err)
				}
				if err := oldStore.InsertEvent(ctx, Event{ID: "illegal-stale-event", TS: nowUTC(), Level: "INFO", Message: "stale"}); err == nil || !strings.Contains(err.Error(), "STALE_MASTER_MUTATION_REJECTED") {
					t.Fatalf("stale mutation err=%v", err)
				}
				var count int
				if err := newStore.db.QueryRow(`SELECT COUNT(*) FROM events WHERE id='illegal-stale-event'`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("stale event count=%d err=%v", count, err)
				}
			},
		},
		{
			name: "absence retry without complete evidence",
			run: func(t *testing.T) {
				op := icebergreg.OperationIdentity{RegistrationID: "reg", RunID: "run", CommitID: "commit", ArtifactSetDigest: "digest", ManifestKey: "manifest"}
				expected := []icebergreg.ExpectedFile{{Path: "s3://bucket/part.parquet", Size: 10, Records: 1}}
				obs := icebergreg.CatalogObservation{Backend: "rest-go", TableExists: true, TableIdentifier: "ns.table", MetadataStart: "meta-1", MetadataEnd: "meta-1", SchemaCompatible: true, LocationCompatible: true}
				decision, err := icebergreg.DecideReconciliation(op, expected, obs)
				if err != nil || decision.Outcome != icebergreg.OutcomeInsufficientEvidence || !decision.OperatorActionRequired {
					t.Fatalf("decision=%+v err=%v", decision, err)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, tc.run)
	}
}
