package grpcapi

import (
	"context"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

type fakeMultipartCleaner struct {
	finalExists bool
	finalErr    error
	uploads     []s3io.MultipartUploadInfo
	abortErr    error
	aborts      int
}

func (f *fakeMultipartCleaner) VerifyTrackedFinalObject(context.Context, string, int64, string, map[string]string) (bool, error) {
	return f.finalExists, f.finalErr
}
func (f *fakeMultipartCleaner) ListManagedMultipartUploads(context.Context, string, int) ([]s3io.MultipartUploadInfo, error) {
	return f.uploads, nil
}
func (f *fakeMultipartCleaner) AbortTrackedMultipart(context.Context, string, string) error {
	f.aborts++
	return f.abortErr
}
func (f *fakeMultipartCleaner) MultipartUploadExists(context.Context, string, string, string) (bool, error) {
	return f.abortErr != nil, nil
}

func TestMultipartCleanupExecutorAbortsOnlyAfterOwnershipLoss(t *testing.T) {
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, "run-multipart-cleanup", "job-multipart-cleanup", "task-multipart-cleanup")
	task := assignGRPCTestAttempt(t, st, "task-multipart-cleanup", "worker")
	now := time.Now().UTC()
	update := db.MultipartLifecycleUpdate{Event: "PREPARED", RunID: task.RunID, TaskID: task.ID, AttemptID: task.AttemptID, WorkerID: *task.WorkerID, FencingToken: task.FencingToken, FileIndex: 0, ObjectKey: "cert/multipart-cleanup/_runs/run-run-multipart-cleanup/part.parquet", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 10}
	if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
		t.Fatal(err)
	}
	cleaner := &fakeMultipartCleaner{uploads: []s3io.MultipartUploadInfo{{Key: update.ObjectKey, UploadID: "discovered"}}}
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.newMultipartCleanerFn = func(context.Context, s3io.Config) (multipartCleaner, error) { return cleaner, nil }
	srv.SetMultipartCleanupPolicy(time.Nanosecond, time.Second, 3)
	srv.nowFn = func() time.Time { return now.Add(30 * time.Second) }
	if processed, err := srv.ProcessMultipartCleanupOnce(context.Background()); err != nil || processed {
		t.Fatalf("active processed=%v err=%v", processed, err)
	}
	if cleaner.aborts != 0 {
		t.Fatal("active upload aborted")
	}
	if _, err := st.ExpireTaskAttempts(context.Background(), now.Add(2*time.Minute), db.LeasePolicy{Duration: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute}); err != nil {
		t.Fatal(err)
	}
	srv.nowFn = func() time.Time { return now.Add(3 * time.Minute) }
	processed, err := srv.ProcessMultipartCleanupOnce(context.Background())
	if err != nil || !processed || cleaner.aborts != 1 {
		t.Fatalf("processed=%v aborts=%d err=%v", processed, cleaner.aborts, err)
	}
}

func TestMultipartCleanupPreservesVerifiedFinalObject(t *testing.T) {
	st := openGRPCTestStore(t)
	createGRPCTestRegistrableRunAndTask(t, st, "run-multipart-final", "job-multipart-final", "task-multipart-final")
	task := assignGRPCTestAttempt(t, st, "task-multipart-final", "worker")
	now := time.Now().UTC()
	update := db.MultipartLifecycleUpdate{Event: "PREPARED", RunID: task.RunID, TaskID: task.ID, AttemptID: task.AttemptID, WorkerID: *task.WorkerID, FencingToken: task.FencingToken, FileIndex: 0, ObjectKey: "cert/multipart-final/_runs/run-run-multipart-final/part.parquet", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 10}
	_, _ = st.ApplyMultipartLifecycle(context.Background(), update, now)
	update.Event, update.UploadID = "CREATED", "upload"
	_, _ = st.ApplyMultipartLifecycle(context.Background(), update, now)
	_, _ = st.ExpireTaskAttempts(context.Background(), now.Add(2*time.Minute), db.LeasePolicy{Duration: time.Minute, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Minute})
	cleaner := &fakeMultipartCleaner{finalExists: true}
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.newMultipartCleanerFn = func(context.Context, s3io.Config) (multipartCleaner, error) { return cleaner, nil }
	srv.SetMultipartCleanupPolicy(time.Nanosecond, time.Second, 3)
	srv.nowFn = func() time.Time { return now.Add(3 * time.Minute) }
	if processed, err := srv.ProcessMultipartCleanupOnce(context.Background()); err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if cleaner.aborts != 0 {
		t.Fatal("verified final object aborted")
	}
}
