package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func insertRegistrationFixture(t *testing.T, st *Store, runID, dataset string, seq int, status string) Registration {
	t.Helper()
	now := "2026-07-23T00:00:00Z"
	target := stableRegistrationID("target")
	id := stableRegistrationID(runID, target)
	_, err := st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES(?, 'job', ?, 'SUCCEEDED', ?, ?, '', ?, '{}', 'COMPLETE')`, runID, dataset, runID, now, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	_, err = st.db.Exec(`INSERT INTO iceberg_registrations(id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,? ,?,?,?)`, id, runID, dataset, seq, target, strings.Repeat("a", 64), "manifest", strings.Repeat("b", 64), "rest-go", status, now, now)
	if err != nil {
		t.Fatal(err)
	}
	r, err := st.GetRegistrationForRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCompleteRunCommitAtomicallyQueuesExactRegistration(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	runID := "run-reg-atomic"
	manifest := map[string]any{"schema_version": 2, "run_id": runID, "artifacts": []map[string]any{{"object_key": "d/a.parquet", "byte_size": 1, "sha256": string(make([]byte, 64)), "row_count": 0, "schema_fingerprint": string(make([]byte, 64)), "run_id": runID, "task_id": "t", "attempt_id": "a", "attempt_number": 1, "file_index": 0, "format_version": 1, "verification_method": "PORTABLE_FULL_SHA256", "verification_status": "VERIFIED"}}}
	mb, _ := json.Marshal(manifest)
	sum := sha256.Sum256(mb)
	cid := hex.EncodeToString(sum[:])
	intent, _ := json.Marshal(map[string]any{"commit_id": cid, "manifest_key": "d/_commits/run.json", "manifest": json.RawMessage(mb)})
	_, err := st.db.Exec(`INSERT INTO runs(id,job_id,dataset_key,status,correlation_id,started_at,registration_config_json,commit_id,commit_intent_json,commit_phase) VALUES(?, 'job','dataset','COMMITTING','c','2026-07-23T00:00:00Z',?,?,?,'VERIFIED')`, runID, `{"enabled":true,"engine":"rest-go","uri":"http://catalog","table":"ns.tbl"}`, cid, string(intent))
	if err != nil {
		t.Fatal(err)
	}
	if err = st.CompleteRunCommit(ctx, runID); err != nil {
		t.Fatal(err)
	}
	r, err := st.GetRegistrationForRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != RegistrationPending || r.CommitID != cid || r.ManifestKey != "d/_commits/run.json" {
		t.Fatalf("unexpected registration: %+v", r)
	}
	if err = st.CompleteRunCommit(ctx, runID); err == nil {
		t.Fatal("repeated completion should preserve the existing registration and reject run transition")
	}
	var n int
	if err = st.db.QueryRow(`SELECT count(*) FROM iceberg_registrations WHERE run_id=?`, runID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}

func TestRegistrationOrderingAndAmbiguousExpiry(t *testing.T) {
	ctx := context.Background()
	st := openTestStore(t)
	first := insertRegistrationFixture(t, st, "r1", "ds", 1, RegistrationRetryRequired)
	_ = insertRegistrationFixture(t, st, "r2", "ds", 2, RegistrationPending)
	now := time.Date(2026, 7, 23, 1, 0, 0, 0, time.UTC)
	policy := RegistrationPolicy{LeaseDuration: time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute}
	r, a, ok, err := st.ClaimRegistration(ctx, now, policy)
	if err != nil || !ok || r.ID != first.ID {
		t.Fatalf("claim=%+v ok=%v err=%v", r, ok, err)
	}
	if err = st.AdvanceRegistrationPhase(ctx, r.ID, a.ID, a.FencingToken, "PREPARED", "EXTERNAL_COMMIT_STARTED", now); err != nil {
		t.Fatal(err)
	}
	n, err := st.ExpireRegistrationAttempts(ctx, now.Add(2*time.Second), policy)
	if err != nil || n != 1 {
		t.Fatalf("expired=%d err=%v", n, err)
	}
	got, err := st.GetRegistrationForRun(ctx, "r1")
	if err != nil || got.Status != RegistrationReconciling {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	projection, err := st.GetReconciliationProjection(ctx, got.ID)
	if err != nil || projection.Status != RegistrationPending {
		t.Fatalf("reconciliation=%+v err=%v", projection, err)
	}
	claimed, _, ok, err := st.ClaimReconciliation(ctx, now.Add(3*time.Second), time.Second)
	if err != nil || !ok || claimed.ID != got.ID {
		t.Fatalf("reconciliation claim=%+v ok=%v err=%v", claimed, ok, err)
	}
	_, _, ok, err = st.ClaimRegistration(ctx, now.Add(3*time.Second), policy)
	if err != nil || ok {
		t.Fatalf("later registration bypassed reconciliation ok=%v err=%v", ok, err)
	}
}
