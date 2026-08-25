package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func canceledObjectFixture(t *testing.T, suffix string) (*Store, Task, time.Time, string) {
	t.Helper()
	st, task, now := multipartFixture(t, suffix)
	update := multipartUpdate(task, "PREPARED")
	if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
		t.Fatal(err)
	}
	update.Event = "CREATED"
	if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
		t.Fatal(err)
	}
	update.Event = "COMPLETING"
	if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
		t.Fatal(err)
	}
	update.Event = "COMPLETED"
	if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
		t.Fatal(err)
	}
	st.SetCanceledObjectRetention(time.Second)
	if changed, status, _, err := st.CancelRun(context.Background(), task.RunID, "operator canceled"); err != nil || !changed || status != "CANCELED" {
		t.Fatalf("cancel changed=%v status=%s err=%v", changed, status, err)
	}
	candidates, err := st.ListCanceledObjectCandidates(context.Background(), task.RunID)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	return st, task, now, candidates[0].ID
}

func TestCanceledObjectCandidateQuarantineAndDryRun(t *testing.T) {
	ctx := context.Background()
	st, task, _, candidateID := canceledObjectFixture(t, "canceled-object")
	
	deadline := time.Date(2030, 1, 2, 3, 4, 5, 1, time.UTC)
	if _, err := st.db.ExecContext(ctx, `UPDATE canceled_object_candidates SET quarantine_until=? WHERE id=?`, canceledObjectTimestamp(deadline), candidateID); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := st.ClaimCanceledObjectCleanup(ctx, deadline.Add(-time.Nanosecond), time.Minute); err != nil || ok {
		t.Fatalf("claimed before quarantine=%v err=%v", ok, err)
	}
	candidate, attempt, ok, err := st.ClaimCanceledObjectCleanup(ctx, deadline, time.Minute)
	if err != nil || !ok || candidate.ID != candidateID || candidate.AttemptID != task.AttemptID {
		t.Fatalf("candidate=%+v attempt=%+v ok=%v err=%v", candidate, attempt, ok, err)
	}
	if err := st.AuthorizeCanceledObjectDelete(ctx, candidate.ID, attempt.ID, attempt.FencingToken, "observation", "version-1", deadline); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishCanceledObjectCleanup(ctx, candidate.ID, attempt.ID, attempt.FencingToken, "DRY_RUN", "", deadline, time.Minute, 3, true); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetCanceledObjectCandidate(ctx, candidate.ID)
	if err != nil || got.Status != "QUARANTINED" || got.DryRunResult != "WOULD_DELETE" {
		t.Fatalf("candidate=%+v err=%v", got, err)
	}
}

func TestCanceledObjectReferenceAndStaleResultAreBlocked(t *testing.T) {
	ctx := context.Background()
	st, _, now, candidateID := canceledObjectFixture(t, "canceled-object-reference")
	initial, _ := st.GetCanceledObjectCandidate(ctx, candidateID)
	deadline, _ := time.Parse(time.RFC3339Nano, initial.QuarantineUntil)
	candidate, attempt, ok, err := st.ClaimCanceledObjectCleanup(ctx, deadline, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO task_artifacts(id,run_id,task_id,attempt_id,file_index,object_key,byte_size,row_count,sha256,schema_fingerprint,attempt_number,format_version,verification_status,verification_method,verified_at,created_at) SELECT 'late-reference',run_id,task_id,attempt_id,9,object_key,expected_size,0,expected_sha256,?,1,1,'VERIFIED','PORTABLE_FULL_SHA256',?,? FROM canceled_object_candidates WHERE id=?`, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), candidateID); err != nil {
		t.Fatal(err)
	}
	if err := st.AuthorizeCanceledObjectDelete(ctx, candidate.ID, attempt.ID, attempt.FencingToken, "observation", "", now.Add(3*time.Second)); !errors.Is(err, ErrCanceledObjectFenced) {
		t.Fatalf("late reference authorization err=%v", err)
	}
	if err := st.CancelCanceledObjectCleanup(ctx, candidate.ID, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := st.FinishCanceledObjectCleanup(ctx, candidate.ID, attempt.ID, attempt.FencingToken, "DELETED", "", now.Add(5*time.Second), time.Second, 3, false); !errors.Is(err, ErrCanceledObjectFenced) {
		t.Fatalf("stale result err=%v", err)
	}
	got, _ := st.GetCanceledObjectCandidate(ctx, candidate.ID)
	if got.Status != "CANCELED_CLEANUP" {
		t.Fatalf("status=%s", got.Status)
	}
}

func TestCanceledObjectCleanupExpiryIsExactAndIdempotent(t *testing.T) {
	ctx := context.Background()
	st, _, _, candidateID := canceledObjectFixture(t, "canceled-object-expiry")
	initial, err := st.GetCanceledObjectCandidate(ctx, candidateID)
	if err != nil {
		t.Fatal(err)
	}
	claimAt, err := time.Parse(time.RFC3339Nano, initial.QuarantineUntil)
	if err != nil {
		t.Fatal(err)
	}
	candidate, attempt, ok, err := st.ClaimCanceledObjectCleanup(ctx, claimAt, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	unrelatedID := candidate.ID + "-unrelated"
	if _, err := st.db.ExecContext(ctx, `
		INSERT INTO canceled_object_candidates(
			id,run_id,task_id,attempt_id,dataset_id,object_key,expected_size,expected_sha256,
			status,eligibility_reason,discovered_at,quarantine_until,created_at,updated_at
		)
		SELECT ?,run_id,task_id,attempt_id,dataset_id,object_key||'.unrelated',expected_size,expected_sha256,
		       'QUARANTINED',eligibility_reason,discovered_at,?,created_at,updated_at
		FROM canceled_object_candidates
		WHERE id=?`,
		unrelatedID, claimAt.Add(24*time.Hour).Format(time.RFC3339Nano), candidate.ID,
	); err != nil {
		t.Fatal(err)
	}
	unrelatedAttemptID := "cleanup-" + unrelatedID + "-1"
	unrelatedLease := claimAt.Add(24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := st.db.ExecContext(ctx, `
		UPDATE canceled_object_candidates
		SET status='DELETE_PENDING',delete_attempt_count=1,current_attempt_id=?
		WHERE id=?`,
		unrelatedAttemptID, unrelatedID,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `
		INSERT INTO canceled_object_cleanup_attempts(
			id,candidate_id,attempt_number,lease_deadline,status,fencing_token,started_at,created_at,updated_at
		)
		VALUES(?,?,1,?,'ACTIVE',?,?,?,?)`,
		unrelatedAttemptID, unrelatedID, unrelatedLease, "unrelated-expiry-token",
		claimAt.Format(time.RFC3339Nano), claimAt.Format(time.RFC3339Nano), claimAt.Format(time.RFC3339Nano),
	); err != nil {
		t.Fatal(err)
	}
	expireAt, err := time.Parse(time.RFC3339Nano, attempt.LeaseDeadline)
	if err != nil {
		t.Fatal(err)
	}
	expired, err := st.ExpireCanceledObjectCleanupAttempts(ctx, expireAt)
	if err != nil || expired != 1 {
		t.Fatalf("expired=%d err=%v", expired, err)
	}
	got, err := st.GetCanceledObjectCandidate(ctx, candidate.ID)
	if err != nil || got.Status != "DELETE_AMBIGUOUS" || got.CurrentAttemptID != "" || got.LastErrorClass != string(CleanupDeleteAmbiguous) {
		t.Fatalf("expired candidate=%+v err=%v", got, err)
	}
	var attemptStatus, finishedAt, errorClass string
	if err := st.db.QueryRowContext(ctx, `
		SELECT status,COALESCE(finished_at,''),error_class
		FROM canceled_object_cleanup_attempts
		WHERE id=?`,
		attempt.ID,
	).Scan(&attemptStatus, &finishedAt, &errorClass); err != nil {
		t.Fatal(err)
	}
	if attemptStatus != "EXPIRED" || finishedAt != expireAt.Format(time.RFC3339Nano) || errorClass != string(CleanupDeleteAmbiguous) {
		t.Fatalf("attempt status=%q finished_at=%q error_class=%q", attemptStatus, finishedAt, errorClass)
	}
	unrelated, err := st.GetCanceledObjectCandidate(ctx, unrelatedID)
	if err != nil || unrelated.Status != "DELETE_PENDING" || unrelated.CurrentAttemptID != unrelatedAttemptID || unrelated.LastErrorClass != "" {
		t.Fatalf("unrelated candidate=%+v err=%v", unrelated, err)
	}
	var unrelatedAttemptStatus string
	if err := st.db.QueryRowContext(ctx, `SELECT status FROM canceled_object_cleanup_attempts WHERE id=?`, unrelatedAttemptID).Scan(&unrelatedAttemptStatus); err != nil {
		t.Fatal(err)
	}
	if unrelatedAttemptStatus != "ACTIVE" {
		t.Fatalf("unrelated attempt status=%q", unrelatedAttemptStatus)
	}
	if repeated, err := st.ExpireCanceledObjectCleanupAttempts(ctx, expireAt.Add(time.Hour)); err != nil || repeated != 0 {
		t.Fatalf("repeated expiry=%d err=%v", repeated, err)
	}
}

func TestCanceledObjectCleanupExpiryRollsBackEveryIntermediateStage(t *testing.T) {
	for _, failAfter := range []string{"candidates", "attempts"} {
		t.Run(failAfter, func(t *testing.T) {
			ctx := context.Background()
			st, _, _, candidateID := canceledObjectFixture(t, "canceled-object-expiry-rollback-"+failAfter)
			initial, err := st.GetCanceledObjectCandidate(ctx, candidateID)
			if err != nil {
				t.Fatal(err)
			}
			claimAt, _ := time.Parse(time.RFC3339Nano, initial.QuarantineUntil)
			candidate, attempt, ok, err := st.ClaimCanceledObjectCleanup(ctx, claimAt, time.Minute)
			if err != nil || !ok {
				t.Fatalf("claim ok=%v err=%v", ok, err)
			}
			expireAt, _ := time.Parse(time.RFC3339Nano, attempt.LeaseDeadline)
			injected := errors.New("injected expiry failure")
			err = st.withTx(ctx, nil, func(tx *sql.Tx) error {
				_, expireErr := expireCanceledObjectCleanupAttemptsTx(ctx, tx, expireAt.Format(time.RFC3339Nano), func(stage string) error {
					if stage == failAfter {
						return injected
					}
					return nil
				})
				return expireErr
			})
			if !errors.Is(err, injected) {
				t.Fatalf("expiry error=%v", err)
			}
			got, err := st.GetCanceledObjectCandidate(ctx, candidate.ID)
			if err != nil || got.Status != "DELETE_PENDING" || got.CurrentAttemptID != attempt.ID || got.LastErrorClass != "" {
				t.Fatalf("candidate after rollback=%+v err=%v", got, err)
			}
			var attemptStatus, finishedAt, errorClass string
			if err := st.db.QueryRowContext(ctx, `
				SELECT status,COALESCE(finished_at,''),error_class
				FROM canceled_object_cleanup_attempts
				WHERE id=?`,
				attempt.ID,
			).Scan(&attemptStatus, &finishedAt, &errorClass); err != nil {
				t.Fatal(err)
			}
			if attemptStatus != "ACTIVE" || finishedAt != "" || errorClass != "" {
				t.Fatalf("attempt after rollback status=%q finished_at=%q error_class=%q", attemptStatus, finishedAt, errorClass)
			}
		})
	}
}

func TestCanceledObjectCleanupExpiryConcurrentScansAreSafe(t *testing.T) {
	ctx := context.Background()
	st, _, _, candidateID := canceledObjectFixture(t, "canceled-object-expiry-concurrent")
	initial, err := st.GetCanceledObjectCandidate(ctx, candidateID)
	if err != nil {
		t.Fatal(err)
	}
	claimAt, _ := time.Parse(time.RFC3339Nano, initial.QuarantineUntil)
	candidate, attempt, ok, err := st.ClaimCanceledObjectCleanup(ctx, claimAt, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim ok=%v err=%v", ok, err)
	}
	expireAt, _ := time.Parse(time.RFC3339Nano, attempt.LeaseDeadline)

	const scans = 8
	results := make(chan int64, scans)
	errs := make(chan error, scans)
	var wg sync.WaitGroup
	for i := 0; i < scans; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := st.ExpireCanceledObjectCleanupAttempts(ctx, expireAt)
			results <- n
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	var total int64
	for n := range results {
		total += n
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if total != 1 {
		t.Fatalf("total expired=%d, want 1", total)
	}
	got, err := st.GetCanceledObjectCandidate(ctx, candidate.ID)
	if err != nil || got.Status != "DELETE_AMBIGUOUS" || got.CurrentAttemptID != "" {
		t.Fatalf("candidate=%+v err=%v", got, err)
	}
	var active, expired int
	if err := st.db.QueryRowContext(ctx, `
		SELECT
			SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END),
			SUM(CASE WHEN status='EXPIRED' THEN 1 ELSE 0 END)
		FROM canceled_object_cleanup_attempts
		WHERE candidate_id=?`,
		candidate.ID,
	).Scan(&active, &expired); err != nil {
		t.Fatal(err)
	}
	if active != 0 || expired != 1 {
		t.Fatalf("active=%d expired=%d", active, expired)
	}
}

func TestRepeatedCancellationDoesNotDuplicateCandidates(t *testing.T) {
	ctx := context.Background()
	st, task, _, _ := canceledObjectFixture(t, "canceled-object-idempotent")
	if changed, status, _, err := st.CancelRun(ctx, task.RunID, "again"); err != nil || changed || status != "CANCELED" {
		t.Fatalf("repeat cancel changed=%v status=%s err=%v", changed, status, err)
	}
	candidates, err := st.ListCanceledObjectCandidates(ctx, task.RunID)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates=%d err=%v", len(candidates), err)
	}
}

func TestCanceledObjectStatusProjectionIsSafe(t *testing.T) {
	ctx := context.Background()
	st, task, _, candidateID := canceledObjectFixture(t, "canceled-object-status")
	run, err := st.GetRun(ctx, task.RunID)
	if err != nil || len(run.CanceledObjectCleanup) != 1 || run.CanceledObjectCleanup[0].ID != candidateID {
		t.Fatalf("cleanup=%+v err=%v", run.CanceledObjectCleanup, err)
	}
	body, err := json.Marshal(run)
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	if strings.Contains(text, `"expected_sha256"`) || strings.Contains(text, "fencing_token") {
		t.Fatalf("unsafe cleanup status: %s", text)
	}
	if !strings.Contains(text, `"status":"QUARANTINED"`) || !strings.Contains(text, `"object_key":`) {
		t.Fatalf("missing cleanup status: %s", text)
	}
}
