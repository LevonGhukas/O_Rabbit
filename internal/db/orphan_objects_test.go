package db

import (
	"context"
	"strings"
	"testing"
	"time"
)

// completedUploadFixture returns a store whose single attempt finished a
// multipart upload.
func completedUploadFixture(t *testing.T, suffix string) (*Store, Task, time.Time) {
	t.Helper()
	st, task, now := multipartFixture(t, suffix)
	for _, event := range []string{"PREPARED", "CREATED", "COMPLETING", "COMPLETED"} {
		if _, err := st.ApplyMultipartLifecycle(context.Background(), multipartUpdate(task, event), now); err != nil {
			t.Fatalf("%s: %v", event, err)
		}
	}
	st.SetCanceledObjectRetention(time.Second)
	return st, task, now
}

func TestDiscoverOrphanedObjectsQuarantinesSupersededAttemptUpload(t *testing.T) {
	ctx := context.Background()
	st, task, now := completedUploadFixture(t, "orphan-superseded")
	if _, err := st.db.ExecContext(ctx, `UPDATE task_attempts SET status='EXPIRED' WHERE id=?`, task.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE runs SET status='SUCCEEDED',commit_id=?,commit_phase='DONE' WHERE id=?`, strings.Repeat("c", 64), task.RunID); err != nil {
		t.Fatal(err)
	}
	n, err := st.DiscoverOrphanedObjects(ctx, now, 10)
	if err != nil || n != 1 {
		t.Fatalf("discovered=%d err=%v", n, err)
	}
	if again, err := st.DiscoverOrphanedObjects(ctx, now, 10); err != nil || again != 0 {
		t.Fatalf("rediscovered=%d err=%v", again, err)
	}
	c, _, ok, err := st.ClaimCanceledObjectCleanup(ctx, now.Add(time.Minute), time.Minute)
	if err != nil || !ok || c.EligibilityReason != "UNACCEPTED_ATTEMPT_COMPLETED_UPLOAD" {
		t.Fatalf("claim candidate=%+v ok=%v err=%v", c, ok, err)
	}
}

func TestDiscoverOrphanedObjectsQuarantinesUnpublishedFailedRunArtifacts(t *testing.T) {
	ctx := context.Background()
	st, task, now := completedUploadFixture(t, "orphan-failed")
	if _, err := st.db.ExecContext(ctx, `UPDATE task_attempts SET status='SUCCEEDED' WHERE id=?`, task.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO task_artifacts(id,run_id,task_id,attempt_id,file_index,object_key,byte_size,row_count,sha256,schema_fingerprint,attempt_number,format_version,verification_status,verification_method,verified_at,created_at) VALUES('art-1',?,?,?,1,'datasets/run/attempt/file.parquet',123,1,?,?,1,1,'VERIFIED','PORTABLE_FULL_SHA256',?,?)`,
		task.RunID, task.ID, task.AttemptID, strings.Repeat("a", 64), strings.Repeat("b", 64), FormatTimestamp(now), FormatTimestamp(now)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE runs SET status='FAILED',finished_at=? WHERE id=?`, FormatTimestamp(now), task.RunID); err != nil {
		t.Fatal(err)
	}
	n, err := st.DiscoverOrphanedObjects(ctx, now, 10)
	if err != nil || n != 1 {
		t.Fatalf("discovered=%d err=%v", n, err)
	}
	c, _, ok, err := st.ClaimCanceledObjectCleanup(ctx, now.Add(time.Minute), time.Minute)
	if err != nil || !ok || c.EligibilityReason != "UNPUBLISHED_RUN_ARTIFACT" {
		t.Fatalf("claim candidate=%+v ok=%v err=%v", c, ok, err)
	}
}

func TestDiscoverOrphanedObjectsSkipsRunsThatBeganCommitting(t *testing.T) {
	ctx := context.Background()
	st, task, now := completedUploadFixture(t, "orphan-committed")
	if _, err := st.db.ExecContext(ctx, `UPDATE task_attempts SET status='SUCCEEDED' WHERE id=?`, task.AttemptID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO task_artifacts(id,run_id,task_id,attempt_id,file_index,object_key,byte_size,row_count,sha256,schema_fingerprint,attempt_number,format_version,verification_status,verification_method,verified_at,created_at) VALUES('art-1',?,?,?,1,'datasets/run/attempt/file.parquet',123,1,?,?,1,1,'VERIFIED','PORTABLE_FULL_SHA256',?,?)`,
		task.RunID, task.ID, task.AttemptID, strings.Repeat("a", 64), strings.Repeat("b", 64), FormatTimestamp(now), FormatTimestamp(now)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `UPDATE runs SET status='FAILED',commit_id=?,commit_phase='PUBLISHING' WHERE id=?`, strings.Repeat("c", 64), task.RunID); err != nil {
		t.Fatal(err)
	}
	if n, err := st.DiscoverOrphanedObjects(ctx, now, 10); err != nil || n != 0 {
		t.Fatalf("discovered=%d err=%v", n, err)
	}
}

func TestDiscoverOrphanedObjectsWaitsForActiveAttempts(t *testing.T) {
	ctx := context.Background()
	st, task, now := completedUploadFixture(t, "orphan-active")
	if _, err := st.db.ExecContext(ctx, `UPDATE runs SET status='FAILED' WHERE id=?`, task.RunID); err != nil {
		t.Fatal(err)
	}
	if n, err := st.DiscoverOrphanedObjects(ctx, now, 10); err != nil || n != 0 {
		t.Fatalf("discovered=%d err=%v", n, err)
	}
}
