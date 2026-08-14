package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func multipartFixture(t *testing.T, suffix string) (*Store, Task, time.Time) {
	t.Helper()
	st := openTestStore(t)
	createLeaseTestTask(t, st, suffix)
	now := time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC)
	task, ok, err := st.AssignNextPendingTaskWithLease(context.Background(), "", "worker", now, LeasePolicy{Duration: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute}, fixedGenerator("attempt-"+suffix), fixedGenerator("token-"+suffix))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	return st, task, now
}

func multipartUpdate(task Task, event string) MultipartLifecycleUpdate {
	return MultipartLifecycleUpdate{Event: event, RunID: task.RunID, TaskID: task.ID, AttemptID: task.AttemptID, WorkerID: *task.WorkerID, FencingToken: task.FencingToken, FileIndex: 1, ObjectKey: "datasets/run/attempt/file.parquet", UploadID: "provider-upload", SHA256: strings.Repeat("a", 64), Size: 123}
}

func TestMultipartIntentLifecycleIsAttemptFenced(t *testing.T) {
	ctx := context.Background()
	st, task, now := multipartFixture(t, "multipart-life")
	prepared, err := st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "PREPARED"), now)
	if err != nil || prepared.Status != "PREPARED" {
		t.Fatalf("prepared=%+v err=%v", prepared, err)
	}
	duplicate, err := st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "PREPARED"), now)
	if err != nil || duplicate.ID != prepared.ID {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	wrong := multipartUpdate(task, "PREPARED")
	wrong.FencingToken = "wrong"
	if _, err := st.ApplyMultipartLifecycle(ctx, wrong, now); !errors.Is(err, ErrMultipartFenced) {
		t.Fatalf("wrong token err=%v", err)
	}
	created, err := st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "CREATED"), now.Add(time.Second))
	if err != nil || created.Status != "ACTIVE" || created.ProviderUploadID != "provider-upload" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	conflict := multipartUpdate(task, "CREATED")
	conflict.UploadID = "other"
	if _, err := st.ApplyMultipartLifecycle(ctx, conflict, now.Add(2*time.Second)); !errors.Is(err, ErrMultipartFenced) {
		t.Fatalf("conflicting upload err=%v", err)
	}
	if _, err := st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "COMPLETION_AMBIGUOUS"), now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.ClaimMultipartCleanup(ctx, now.Add(30*time.Second), time.Second, time.Minute); err != nil || ok {
		t.Fatalf("ambiguous completion claimed=%v err=%v", ok, err)
	}
}

func TestMultipartCleanupRequiresOwnershipLossAndGrace(t *testing.T) {
	ctx := context.Background()
	st, task, now := multipartFixture(t, "multipart-clean")
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "PREPARED"), now)
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "CREATED"), now.Add(time.Second))
	if _, ok, err := st.ClaimMultipartCleanup(ctx, now.Add(30*time.Second), 10*time.Second, time.Minute); err != nil || ok {
		t.Fatalf("active attempt claimed=%v err=%v", ok, err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE task_attempts SET status='EXPIRED',finished_at=? WHERE id=?`, now.Add(time.Minute).Format(time.RFC3339Nano), task.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.ClaimMultipartCleanup(ctx, now.Add(5*time.Second), 10*time.Second, time.Minute); err != nil || ok {
		t.Fatalf("before grace claimed=%v err=%v", ok, err)
	}
	claimed, ok, err := st.ClaimMultipartCleanup(ctx, now.Add(20*time.Second), 10*time.Second, time.Minute)
	if err != nil || !ok || claimed.Status != "ABORTING" {
		t.Fatalf("claimed=%+v ok=%v err=%v", claimed, ok, err)
	}
	if err := st.FinishMultipartCleanup(ctx, claimed.ID, "stale", "ABORTED", "", "", now, time.Second, 3); !errors.Is(err, ErrMultipartFenced) {
		t.Fatalf("stale cleanup result err=%v", err)
	}
	if err := st.FinishMultipartCleanup(ctx, claimed.ID, claimed.CleanupToken, "RETRY", "MULTIPART_ABORT_FAILED", "", now, time.Second, 3); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedOrAcceptedMultipartIsNeverCleanupEligible(t *testing.T) {
	ctx := context.Background()
	st, task, now := multipartFixture(t, "multipart-complete")
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "PREPARED"), now)
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "CREATED"), now)
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "COMPLETING"), now)
	_, _ = st.ApplyMultipartLifecycle(ctx, multipartUpdate(task, "COMPLETED"), now)
	_, _ = st.db.ExecContext(ctx, `UPDATE task_attempts SET status='EXPIRED' WHERE id=?`, task.AttemptID)
	if _, ok, err := st.ClaimMultipartCleanup(ctx, now.Add(time.Hour), time.Minute, time.Minute); err != nil || ok {
		t.Fatalf("completed upload claimed=%v err=%v", ok, err)
	}
}
