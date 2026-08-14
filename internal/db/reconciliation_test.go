package db

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReconciliationDurableClaimAndOutcomes(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 8, 0, 0, 0, time.UTC)
	t.Run("exact", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "rec-exact", "ds", 1, RegistrationReconciling)
		_, _ = st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='PENDING' WHERE id=?`, r.ID)
		got, a, ok, err := st.ClaimReconciliation(ctx, now, time.Second)
		if err != nil || !ok || got.ID != r.ID {
			t.Fatalf("%+v %+v %v %v", got, a, ok, err)
		}
		receipt, _ := ReconciliationReceipt(r, map[string]any{"snapshot_id": "7"}, now)
		if err := st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, "EXACTLY_COMMITTED", "evidence", "meta", "meta", "7", receipt, 2, 2, now, 2); err != nil {
			t.Fatal(err)
		}
		resolved, _ := st.GetRegistrationForRun(ctx, r.RunID)
		if resolved.Status != RegistrationRetryRequired || resolved.Receipt == "" {
			t.Fatalf("%+v", resolved)
		}
		if err := st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, "EXACTLY_COMMITTED", "e", "m", "m", "7", receipt, 2, 2, now, 2); !errors.Is(err, ErrRegistrationFenced) {
			t.Fatalf("stale err=%v", err)
		}
	})
	t.Run("absence-bounded", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "rec-absent", "ds", 1, RegistrationReconciling)
		_, _ = st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='PENDING',ambiguity_retry_count=2 WHERE id=?`, r.ID)
		_, a, _, _ := st.ClaimReconciliation(ctx, now, time.Second)
		if err := st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, "DEFINITELY_NOT_COMMITTED", "e", "m", "m", "", "", 0, 2, now, 2); err != nil {
			t.Fatal(err)
		}
		got, _ := st.GetRegistrationForRun(ctx, r.RunID)
		if got.Status != RegistrationQuarantined {
			t.Fatalf("%+v", got)
		}
	})
	t.Run("unavailable-retries-inspection", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "rec-down", "ds", 1, RegistrationReconciling)
		_, _ = st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='PENDING' WHERE id=?`, r.ID)
		_, a, _, _ := st.ClaimReconciliation(ctx, now, time.Second)
		if err := st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_OBSERVATION_UNAVAILABLE", "down", now, time.Second, 3); err != nil {
			t.Fatal(err)
		}
		p, _ := st.GetReconciliationProjection(ctx, r.ID)
		if p.Status != RegistrationRetryRequired || p.NextRetryAt == nil {
			t.Fatalf("%+v", p)
		}
	})
}
func TestReconciliationOrderingRemainsBlocked(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	_ = insertRegistrationFixture(t, st, "rec-first", "ds", 1, RegistrationReconciling)
	_ = insertRegistrationFixture(t, st, "rec-later", "ds", 2, RegistrationPending)
	_, _, ok, err := st.ClaimRegistration(ctx, time.Now(), certPolicy())
	if err != nil || ok {
		t.Fatalf("later claimed=%v err=%v", ok, err)
	}
}

func TestReconciliationLeaseExpiryAndCancellationFenceResults(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 9, 0, 0, 0, time.UTC)
	t.Run("expiry retries only observation", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "rec-expire", "ds", 1, RegistrationReconciling)
		_, _ = st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='PENDING' WHERE id=?`, r.ID)
		_, a, ok, err := st.ClaimReconciliation(ctx, now, time.Second)
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		if err := st.RenewReconciliationLease(ctx, r.ID, a.ID, a.FencingToken, now, 2*time.Second); err != nil {
			t.Fatal(err)
		}
		if n, err := st.ExpireReconciliationAttempts(ctx, now.Add(time.Second), time.Second, 5); err != nil || n != 0 {
			t.Fatalf("early expiry=%d err=%v", n, err)
		}
		if n, err := st.ExpireReconciliationAttempts(ctx, now.Add(3*time.Second), time.Second, 5); err != nil || n != 1 {
			t.Fatalf("expiry=%d err=%v", n, err)
		}
		if err := st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, "DEFINITELY_NOT_COMMITTED", "e", "m", "m", "", "", 0, 1, now, 2); !errors.Is(err, ErrRegistrationFenced) {
			t.Fatalf("stale decision err=%v", err)
		}
		p, _ := st.GetReconciliationProjection(ctx, r.ID)
		if p.Status != RegistrationRetryRequired {
			t.Fatalf("%+v", p)
		}
	})
	t.Run("cancel preserves ambiguity and ordering block", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "rec-cancel", "ds", 1, RegistrationReconciling)
		_ = insertRegistrationFixture(t, st, "rec-cancel-later", "ds", 2, RegistrationPending)
		_, _ = st.db.Exec(`UPDATE iceberg_registrations SET reconciliation_status='PENDING' WHERE id=?`, r.ID)
		_, a, _, _ := st.ClaimReconciliation(ctx, now, time.Minute)
		if err := st.CancelReconciliation(ctx, r.ID, now.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, "DEFINITELY_NOT_COMMITTED", "e", "m", "m", "", "", 0, 1, now, 2); !errors.Is(err, ErrRegistrationFenced) {
			t.Fatalf("stale result err=%v", err)
		}
		got, _ := st.GetRegistrationForRun(ctx, r.RunID)
		p, _ := st.GetReconciliationProjection(ctx, r.ID)
		if got.Status != RegistrationReconciling || p.Status != RegistrationCanceled || p.Outcome != "INSUFFICIENT_EVIDENCE" {
			t.Fatalf("registration=%+v projection=%+v", got, p)
		}
		if _, _, ok, err := st.ClaimRegistration(ctx, now.Add(2*time.Second), certPolicy()); err != nil || ok {
			t.Fatalf("later registration claimed=%v err=%v", ok, err)
		}
	})
}
