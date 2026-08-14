package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func certPolicy() RegistrationPolicy {
	return RegistrationPolicy{LeaseDuration: time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: 4 * time.Second}
}

func TestSchemaV2EmptyArtifactRepresentationsCreateCanonicalRegistration(t *testing.T) {
	ctx := context.Background()
	emptyDigest := sha256.Sum256(nil)
	wantDigest := hex.EncodeToString(emptyDigest[:])
	manifests := map[string]string{
		"absent":         `{"schema_version":2,"run_id":"RUN_ID","objects":[]}`,
		"null":           `{"schema_version":2,"run_id":"RUN_ID","objects":null,"artifacts":null}`,
		"empty":          `{"schema_version":2,"run_id":"RUN_ID","objects":[],"artifacts":[]}`,
		"legacy_objects": `{"schema_version":2,"run_id":"RUN_ID","objects_v2":[],"artifacts":[]}`,
	}
	for name, template := range manifests {
		t.Run(name, func(t *testing.T) {
			st := openTestStore(t)
			runID := "empty-" + name
			if err := st.CreateRun(ctx, Run{ID: runID, JobID: "job", DatasetKey: "dataset-" + name, Status: "SUCCEEDED", CorrelationID: "corr", StartedAt: nowUTC()}); err != nil {
				t.Fatal(err)
			}
			manifest := []byte(strings.ReplaceAll(template, "RUN_ID", runID))
			hash := sha256.Sum256(manifest)
			commitID := hex.EncodeToString(hash[:])
			intent, _ := json.Marshal(map[string]any{"manifest_key": "prefix/_commits/run-" + runID + ".json", "manifest": json.RawMessage(manifest)})
			tx, err := st.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := ensureRegistrationTx(ctx, tx, runID, "dataset-"+name, commitID, `{"enabled":true,"engine":"rest-go","table":"ns.tbl","uri":"http://catalog"}`, string(intent), nowUTC()); err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			registration, err := st.GetRegistrationForRun(ctx, runID)
			if err != nil {
				t.Fatal(err)
			}
			if registration.ArtifactSetDigest != wantDigest || registration.Status != RegistrationPending {
				t.Fatalf("registration=%+v want empty digest=%s", registration, wantDigest)
			}
		})
	}
}

func TestRegistrationCancellationMatrixAndFencing(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 2, 0, 0, 0, time.UTC)
	for _, status := range []string{RegistrationPending, RegistrationRetryRequired} {
		t.Run(status, func(t *testing.T) {
			st := openTestStore(t)
			r := insertRegistrationFixture(t, st, "cancel-"+status, "ds", 1, status)
			got, err := st.CancelRegistration(ctx, r.ID, now)
			if err != nil || got.Status != RegistrationCanceled {
				t.Fatalf("got=%+v err=%v", got, err)
			}
			_, _, ok, err := st.ClaimRegistration(ctx, now, certPolicy())
			if err != nil || ok {
				t.Fatalf("canceled work claimed ok=%v err=%v", ok, err)
			}
		})
	}
	t.Run("claimed-pre-external", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "cancel-active", "ds", 1, RegistrationPending)
		_, a, ok, err := st.ClaimRegistration(ctx, now, certPolicy())
		if err != nil || !ok {
			t.Fatal(err)
		}
		got, err := st.CancelRegistration(ctx, r.ID, now)
		if err != nil || got.Status != RegistrationCanceled {
			t.Fatalf("got=%+v err=%v", got, err)
		}
		if err := st.CompleteRegistration(ctx, r.ID, a.ID, a.FencingToken, now); !errors.Is(err, ErrRegistrationFenced) {
			t.Fatalf("stale completion err=%v", err)
		}
	})
	t.Run("post-boundary", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "cancel-post", "ds", 1, RegistrationPending)
		_, a, _, _ := st.ClaimRegistration(ctx, now, certPolicy())
		_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now)
		got, err := st.CancelRegistration(ctx, r.ID, now)
		if err != nil || got.Status != RegistrationReconciling {
			t.Fatalf("got=%+v err=%v", got, err)
		}
		projection, err := st.GetReconciliationProjection(ctx, r.ID)
		if err != nil || projection.Status != RegistrationPending {
			t.Fatalf("reconciliation=%+v err=%v", projection, err)
		}
	})
	t.Run("definite-success-too-late", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "cancel-receipt", "ds", 1, RegistrationPending)
		_, a, _, _ := st.ClaimRegistration(ctx, now, certPolicy())
		_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now)
		_ = st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"receipt":"definite"}`, now)
		_, err := st.CancelRegistration(ctx, r.ID, now)
		if !errors.Is(err, ErrRegistrationCancelTooLate) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("completed-too-late", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "cancel-done", "ds", 1, RegistrationRegistered)
		_, err := st.CancelRegistration(ctx, r.ID, now)
		if !errors.Is(err, ErrRegistrationCancelTooLate) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestRegistrationNoOpReceiptCompletesThroughFencedPhases(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 2, 30, 0, 0, time.UTC)
	st := openTestStore(t)
	r := insertRegistrationFixture(t, st, "noop-receipt", "ds", 1, RegistrationPending)
	_, a, ok, err := st.ClaimRegistration(ctx, now, certPolicy())
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if err := st.PersistCatalogNoOpReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"no_op":false}`, now); err == nil {
		t.Fatal("invalid no-op receipt was accepted")
	}
	receipt := `{"no_op":true,"no_op_reason":"ALL_ARTIFACTS_ALREADY_APPLIED","no_op_evidence_digest":"` + strings.Repeat("a", 64) + `"}`
	if err := st.PersistCatalogNoOpReceipt(ctx, r.ID, a.ID, a.FencingToken, receipt, now); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetRegistrationForRun(ctx, r.RunID)
	if err != nil || got.Status != RegistrationRegistering || got.Receipt != receipt {
		t.Fatalf("registration=%+v err=%v", got, err)
	}
	var phase string
	if err := st.db.QueryRowContext(ctx, `SELECT phase FROM iceberg_registration_attempts WHERE id = ?`, a.ID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != "CATALOG_COMMITTED" {
		t.Fatalf("phase = %q", phase)
	}
	if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_COMMITTED", "ICE_STATE_WRITING", now); err != nil {
		t.Fatal(err)
	}
	if err := st.CompleteRegistration(ctx, r.ID, a.ID, a.FencingToken, now); err != nil {
		t.Fatal(err)
	}
	got, err = st.GetRegistrationForRun(ctx, r.RunID)
	if err != nil || got.Status != RegistrationRegistered {
		t.Fatalf("registration=%+v err=%v", got, err)
	}
	if err := st.PersistCatalogNoOpReceipt(ctx, r.ID, a.ID, a.FencingToken, receipt, now); !errors.Is(err, ErrRegistrationFenced) {
		t.Fatalf("repeated stale persistence err=%v", err)
	}
	events, err := st.ListEventsForRun(ctx, r.RunID, 100)
	if err != nil {
		t.Fatal(err)
	}
	eventCount := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"event_type":"REGISTRATION_NOOP_VERIFIED"`) {
			eventCount++
		}
	}
	if eventCount != 1 {
		t.Fatalf("no-op events = %d, want 1", eventCount)
	}
}

func TestCrashBoundariesConvergeConservatively(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 3, 0, 0, 0, time.UTC)
	policy := certPolicy()
	t.Run("before-claim-survives", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "crash-pending", "ds", 1, RegistrationPending)
		got, _, ok, err := st.ClaimRegistration(ctx, now, policy)
		if err != nil || !ok || got.ID != r.ID {
			t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
		}
	})
	t.Run("claimed-pre-external-retries", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "crash-pre", "ds", 1, RegistrationPending)
		_, _, _, _ = st.ClaimRegistration(ctx, now, policy)
		n, err := st.ExpireRegistrationAttempts(ctx, now.Add(2*time.Second), policy)
		if err != nil || n != 1 {
			t.Fatal(err)
		}
		got, _ := st.GetRegistrationForRun(ctx, r.RunID)
		if got.Status != RegistrationRetryRequired {
			t.Fatalf("got=%+v", got)
		}
		n, _ = st.ExpireRegistrationAttempts(ctx, now.Add(3*time.Second), policy)
		if n != 0 {
			t.Fatalf("repeat expiry=%d", n)
		}
	})
	for _, phase := range []string{"EXTERNAL_COMMIT_STARTED", "ICE_STATE_WRITING"} {
		t.Run("post-"+phase, func(t *testing.T) {
			st := openTestStore(t)
			r := insertRegistrationFixture(t, st, "crash-"+phase, "ds", 1, RegistrationPending)
			_, a, _, _ := st.ClaimRegistration(ctx, now, policy)
			if phase == "ICE_STATE_WRITING" {
				_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now)
				_ = st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"receipt":"stable"}`, now)
				_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_COMMITTED", phase, now)
			} else {
				_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", phase, now)
			}
			_, _ = st.ExpireRegistrationAttempts(ctx, now.Add(2*time.Second), policy)
			got, _ := st.GetRegistrationForRun(ctx, r.RunID)
			want := RegistrationReconciling
			if phase == "ICE_STATE_WRITING" {
				want = RegistrationRetryRequired
			}
			if got.Status != want {
				t.Fatalf("got=%+v", got)
			}
			_, _, ok, _ := st.ClaimRegistration(ctx, now.Add(4*time.Second), policy)
			if phase == "EXTERNAL_COMMIT_STARTED" && ok {
				t.Fatal("reconciling work replayed")
			}
		})
	}
	t.Run("receipt-repair-no-replay", func(t *testing.T) {
		st := openTestStore(t)
		r := insertRegistrationFixture(t, st, "crash-receipt", "ds", 1, RegistrationPending)
		_, a, _, _ := st.ClaimRegistration(ctx, now, policy)
		_ = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now)
		_ = st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"receipt":"stable"}`, now)
		_ = st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ICE_STATE_WRITE_FAILED", "lost", true, false, now, policy)
		got, _ := st.GetRegistrationForRun(ctx, r.RunID)
		if got.Status != RegistrationRetryRequired || got.Receipt == "" {
			t.Fatalf("got=%+v", got)
		}
	})
}

func TestAmbiguousFailureBecomesClaimableReconciliation(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 3, 30, 0, 0, time.UTC)
	st := openTestStore(t)
	policy := certPolicy()
	r := insertRegistrationFixture(t, st, "ambiguous-claimable", "ds", 1, RegistrationPending)
	_, a, ok, err := st.ClaimRegistration(ctx, now, policy)
	if err != nil || !ok {
		t.Fatalf("registration claim ok=%v err=%v", ok, err)
	}
	if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
		t.Fatal(err)
	}
	if err := st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "UNKNOWN", "ambiguous", false, false, now, policy); err != nil {
		t.Fatal(err)
	}
	projection, err := st.GetReconciliationProjection(ctx, r.ID)
	if err != nil || projection.Status != RegistrationPending {
		t.Fatalf("reconciliation=%+v err=%v", projection, err)
	}
	claimed, _, ok, err := st.ClaimReconciliation(ctx, now.Add(time.Second), time.Second)
	if err != nil || !ok || claimed.ID != r.ID {
		t.Fatalf("reconciliation claim=%+v ok=%v err=%v", claimed, ok, err)
	}
}

func TestDatasetOrderingCompleteMatrix(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 4, 0, 0, 0, time.UTC)
	policy := certPolicy()
	for _, blocking := range []string{RegistrationRetryRequired, RegistrationReconciling, RegistrationFailed, RegistrationQuarantined, RegistrationCanceled} {
		t.Run(blocking, func(t *testing.T) {
			st := openTestStore(t)
			first := insertRegistrationFixture(t, st, "first-"+blocking, "ds", 1, blocking)
			later := insertRegistrationFixture(t, st, "later-"+blocking, "ds", 2, RegistrationPending)
			got, _, ok, err := st.ClaimRegistration(ctx, now, policy)
			if err != nil || (ok && got.ID == later.ID) {
				t.Fatalf("later bypassed %s got=%+v ok=%v err=%v", blocking, got, ok, err)
			}
			if blocking == RegistrationRetryRequired && (!ok || got.ID != first.ID) {
				t.Fatalf("oldest retry was not claimed got=%+v ok=%v", got, ok)
			}
		})
	}
	t.Run("different-datasets-and-targets", func(t *testing.T) {
		st := openTestStore(t)
		_ = insertRegistrationFixture(t, st, "blocked", "a", 1, RegistrationReconciling)
		r := insertRegistrationFixture(t, st, "independent", "b", 1, RegistrationPending)
		got, _, ok, err := st.ClaimRegistration(ctx, now, policy)
		if err != nil || !ok || got.ID != r.ID {
			t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
		}
	})
}

func TestLifecycleEventsAreBoundedAndSecretFree(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 5, 0, 0, 0, time.UTC)
	st := openTestStore(t)
	r := insertRegistrationFixture(t, st, "events", "ds", 1, RegistrationPending)
	_, a, _, _ := st.ClaimRegistration(ctx, now, certPolicy())
	_, _ = st.ExpireRegistrationAttempts(ctx, now.Add(2*time.Second), certPolicy())
	_, _ = st.ExpireRegistrationAttempts(ctx, now.Add(3*time.Second), certPolicy())
	var count int
	if err := st.db.QueryRow(`SELECT count(*) FROM events WHERE run_id=?`, r.RunID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("events=%d want assigned+expired", count)
	}
	rows, err := st.db.Query(`SELECT fields_json FROM events WHERE run_id=?`, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var fields string
		_ = rows.Scan(&fields)
		if strings.Contains(fields, a.FencingToken) || strings.Contains(strings.ToLower(fields), "secret") {
			t.Fatalf("unsafe event: %s", fields)
		}
	}
}

func TestLifecycleEventMatrix(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 23, 5, 30, 0, 0, time.UTC)
	st := openTestStore(t)
	policy := certPolicy()
	transition := func(run string, setup func(Registration, RegistrationAttempt) error) {
		r := insertRegistrationFixture(t, st, run, run, 1, RegistrationPending)
		_, a, ok, err := st.ClaimRegistration(ctx, now, policy)
		if err != nil || !ok {
			t.Fatalf("%s claim err=%v", run, err)
		}
		if err := setup(r, a); err != nil {
			t.Fatalf("%s: %v", run, err)
		}
	}
	transition("ev-retry", func(r Registration, a RegistrationAttempt) error {
		return st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_UNAVAILABLE", "down", true, true, now, policy)
	})
	transition("ev-reject", func(r Registration, a RegistrationAttempt) error {
		return st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "AUTHORIZATION_FAILED", "denied", false, true, now, policy)
	})
	transition("ev-reconcile", func(r Registration, a RegistrationAttempt) error {
		if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
			return err
		}
		return st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "UNKNOWN", "unknown", false, false, now, policy)
	})
	transition("ev-repair", func(r Registration, a RegistrationAttempt) error {
		if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
			return err
		}
		if err := st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"receipt":"ok"}`, now); err != nil {
			return err
		}
		return st.FailRegistrationAttempt(ctx, r.ID, a.ID, a.FencingToken, "ICE_STATE_WRITE_FAILED", "write", true, true, now, policy)
	})
	transition("ev-complete", func(r Registration, a RegistrationAttempt) error {
		if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
			return err
		}
		if err := st.PersistCatalogReceipt(ctx, r.ID, a.ID, a.FencingToken, `{"receipt":"ok"}`, now); err != nil {
			return err
		}
		if err := st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_COMMITTED", "ICE_STATE_WRITING", now); err != nil {
			return err
		}
		return st.CompleteRegistration(ctx, r.ID, a.ID, a.FencingToken, now)
	})
	rBlocked := insertRegistrationFixture(t, st, "ev-blocker", "shared", 1, RegistrationReconciling)
	_ = rBlocked
	_ = insertRegistrationFixture(t, st, "ev-blocked", "shared", 2, RegistrationPending)
	_, _, _, _ = st.ClaimRegistration(ctx, now, policy)
	stale := insertRegistrationFixture(t, st, "ev-stale", "stale", 1, RegistrationCanceled)
	_ = st.RecordRegistrationStaleResult(ctx, stale.ID, "old-attempt", "STALE_COMPLETION", now)
	rows, err := st.db.Query(`SELECT fields_json FROM events`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var raw string
		_ = rows.Scan(&raw)
		var fields map[string]any
		_ = json.Unmarshal([]byte(raw), &fields)
		if typ, _ := fields["event_type"].(string); typ != "" {
			seen[typ] = true
		}
	}
	for _, want := range []string{"REGISTRATION_ATTEMPT_ASSIGNED", "REGISTRATION_RETRY_SCHEDULED", "REGISTRATION_DEFINITELY_REJECTED", "REGISTRATION_RECONCILIATION_REQUIRED", "REGISTRATION_CATALOG_COMMITTED", "REGISTRATION_ICE_STATE_REPAIR_REQUIRED", "REGISTRATION_COMPLETED", "REGISTRATION_BLOCKED", "REGISTRATION_STALE_RESULT_REJECTED"} {
		if !seen[want] {
			t.Errorf("missing event %s; seen=%v", want, seen)
		}
	}
}

func TestReadinessProjectionEveryState(t *testing.T) {
	want := map[string]string{"": "DATA_COMMITTED", RegistrationPending: "CATALOG_PENDING", RegistrationRegistering: "CATALOG_REGISTERING", RegistrationRetryRequired: "CATALOG_RETRYING", RegistrationReconciling: "CATALOG_RECONCILING", RegistrationRegistered: "READY", RegistrationFailed: "CATALOG_FAILED", RegistrationQuarantined: "CATALOG_FAILED", RegistrationCanceled: "CATALOG_FAILED", RegistrationBlocked: "CATALOG_BLOCKED"}
	for state, readiness := range want {
		if got := RegistrationReadiness("SUCCEEDED", state); got != readiness {
			t.Errorf("%s got=%s want=%s", state, got, readiness)
		}
	}
}

func TestRunJSONSerializesCatalogProjectionEveryState(t *testing.T) {
	ctx := context.Background()
	for _, state := range []string{RegistrationPending, RegistrationRegistering, RegistrationRetryRequired, RegistrationReconciling, RegistrationRegistered, RegistrationFailed, RegistrationQuarantined, RegistrationCanceled, RegistrationBlocked} {
		t.Run(state, func(t *testing.T) {
			st := openTestStore(t)
			r := insertRegistrationFixture(t, st, "json-"+state, "ds", 1, state)
			_, _ = st.db.Exec(`UPDATE iceberg_registrations SET last_error_class='TEST_CLASS',next_eligible_at='2026-07-24T00:00:00Z',registered_snapshot_or_metadata_id='{"backend":"test"}' WHERE id=?`, r.ID)
			run, err := st.GetRun(ctx, r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			body, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			text := string(body)
			for _, field := range []string{`"data_status":"SUCCEEDED"`, `"catalog_status":"` + state + `"`, `"readiness":`, `"registration_id":`, `"registration_attempt":`, `"registration_error_class":"TEST_CLASS"`, `"registration_next_retry_at":`, `"catalog_receipt":`} {
				if !strings.Contains(text, field) {
					t.Errorf("missing %s in %s", field, text)
				}
			}
		})
	}
}

func TestHistoricalClassificationIsIdempotent(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	now := time.Date(2026, 7, 23, 6, 0, 0, 0, time.UTC)
	_, _ = st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES('hist-none','j','d','SUCCEEDED','c','2026-01-01T00:00:00Z','','','','COMPLETE')`)
	_, _ = st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES('hist-legacy','j','d','SUCCEEDED','c','2026-01-02T00:00:00Z','{"enabled":true,"engine":"rest-go","table":"n.t"}',?,'{"manifest_key":"m","manifest":{"schema_version":1,"run_id":"hist-legacy"}}','COMPLETE')`, strings.Repeat("a", 64))
	_, _ = st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES('hist-config','j','d','SUCCEEDED','c','2026-01-03T00:00:00Z','not-json','','','COMPLETE')`)
	manifest, _ := json.Marshal(map[string]any{"schema_version": 2, "run_id": "hist-safe", "artifacts": []map[string]any{{"object_key": "d/a.parquet"}}})
	digest := sha256.Sum256(manifest)
	commitID := hex.EncodeToString(digest[:])
	intent, _ := json.Marshal(map[string]any{"manifest_key": "d/_commits/safe.json", "manifest": json.RawMessage(manifest)})
	_, _ = st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES('hist-safe','j','d','SUCCEEDED','c','2026-01-04T00:00:00Z','{"enabled":true,"engine":"rest-go","table":"n.t"}',?,?,'COMPLETE')`, commitID, string(intent))
	registered := insertRegistrationFixture(t, st, "hist-registered", "other", 1, RegistrationRegistered)
	_, _ = st.db.Exec(`UPDATE iceberg_registrations SET registered_snapshot_or_metadata_id='{"receipt":"verified"}' WHERE id=?`, registered.ID)
	_ = insertRegistrationFixture(t, st, "hist-reconcile", "other", 2, RegistrationReconciling)
	first, err := st.ReconcileHistoricalRegistrations(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	second, err := st.ReconcileHistoricalRegistrations(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 6 || len(second) != 6 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	seen := map[string]bool{}
	for _, c := range first {
		seen[c.Classification] = true
	}
	for _, want := range []string{"NOT_CONFIGURED", "UNSUPPORTED_LEGACY_COMMIT", "CONFIGURATION_UNAVAILABLE", "SAFE_TO_ENQUEUE", "ALREADY_REGISTERED_VERIFIED", "REQUIRES_RECONCILIATION"} {
		if !seen[want] {
			t.Errorf("missing classification %s: %+v", want, first)
		}
	}
	var n int
	if err := st.db.QueryRow(`SELECT count(*) FROM events WHERE message='historical registration classified'`).Scan(&n); err != nil || n != 6 {
		t.Fatalf("events=%d err=%v", n, err)
	}
}

func TestOnlyOneCurrentAttemptConstraint(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	r := insertRegistrationFixture(t, st, "one-attempt", "ds", 1, RegistrationPending)
	_, a, _, _ := st.ClaimRegistration(ctx, time.Now(), certPolicy())
	_, err := st.db.Exec(`INSERT INTO iceberg_registration_attempts(id,registration_id,attempt_number,status,fencing_token,lease_deadline,last_renewed_at,phase,started_at,created_at,updated_at) VALUES('other',?,2,'ACTIVE','other-token','2099-01-01T00:00:00Z','2026-01-01T00:00:00Z','PREPARED','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`, r.ID)
	if err == nil {
		t.Fatal("second active attempt accepted")
	}
	if !errors.Is(st.CompleteRegistration(ctx, r.ID, "other", "bad", time.Now()), ErrRegistrationFenced) {
		t.Fatal("unowned completion accepted")
	}
	_ = a
}

var _ = sql.ErrNoRows
