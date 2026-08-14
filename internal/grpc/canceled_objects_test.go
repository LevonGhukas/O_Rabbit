package grpcapi

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

type fakeCanceledObjectCleaner struct {
	observations []s3io.ExactObjectObservation
	observeErrs  []error
	deletes      []string
	deleteErr    error
}

func (f *fakeCanceledObjectCleaner) ObserveExactObject(context.Context, string, int64, string, map[string]string) (s3io.ExactObjectObservation, error) {
	var observation s3io.ExactObjectObservation
	var err error
	if len(f.observations) > 0 {
		observation = f.observations[0]
		f.observations = f.observations[1:]
	}
	if len(f.observeErrs) > 0 {
		err = f.observeErrs[0]
		f.observeErrs = f.observeErrs[1:]
	}
	return observation, err
}

func (f *fakeCanceledObjectCleaner) DeleteExactObject(_ context.Context, key, _ string) error {
	f.deletes = append(f.deletes, key)
	return f.deleteErr
}

func canceledObjectGRPCFixture(t *testing.T, suffix string) (*db.Store, time.Time, string) {
	t.Helper()
	st := openGRPCTestStore(t)
	runID, jobID, taskID := "run-"+suffix, "job-"+suffix, "task-"+suffix
	createGRPCTestRegistrableRunAndTaskWithSnapshot(t, st, runID, jobID, taskID, nil)
	task := assignGRPCTestAttempt(t, st, taskID, "worker")
	now := time.Now().UTC()
	update := db.MultipartLifecycleUpdate{Event: "PREPARED", RunID: task.RunID, TaskID: task.ID, AttemptID: task.AttemptID, WorkerID: *task.WorkerID, FencingToken: task.FencingToken, FileIndex: 0, ObjectKey: "cert/" + suffix + "/_runs/" + runID + "/part.parquet", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Size: 10}
	for _, event := range []string{"PREPARED", "CREATED", "COMPLETING", "COMPLETED"} {
		update.Event = event
		if event == "CREATED" {
			update.UploadID = "upload-" + suffix
		}
		if _, err := st.ApplyMultipartLifecycle(context.Background(), update, now); err != nil {
			t.Fatal(err)
		}
	}
	st.SetCanceledObjectRetention(time.Nanosecond)
	if _, _, _, err := st.CancelRun(context.Background(), runID, "cancel"); err != nil {
		t.Fatal(err)
	}
	candidates, err := st.ListCanceledObjectCandidates(context.Background(), runID)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	deadline, _ := time.Parse(time.RFC3339Nano, candidates[0].QuarantineUntil)
	return st, deadline, update.ObjectKey
}

func TestCanceledObjectCleanupDeletesExactObjectAfterRevalidation(t *testing.T) {
	st, deadline, key := canceledObjectGRPCFixture(t, "object-delete")
	cleaner := &fakeCanceledObjectCleaner{observations: []s3io.ExactObjectObservation{
		{Exists: true, Matches: true, Identity: "stable", VersionID: "v1"},
		{},
	}}
	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.newCanceledObjectCleanerFn = func(context.Context, s3io.Config) (canceledObjectCleaner, error) { return cleaner, nil }
	srv.SetCanceledObjectCleanupPolicy(time.Second, 3, false)
	srv.nowFn = func() time.Time { return deadline.Add(time.Second) }
	processed, err := srv.ProcessCanceledObjectCleanupOnce(context.Background())
	if err != nil || !processed || len(cleaner.deletes) != 1 || cleaner.deletes[0] != key {
		t.Fatalf("processed=%v deletes=%v err=%v", processed, cleaner.deletes, err)
	}
}

func TestCanceledObjectCleanupDryRunAndAmbiguousDelete(t *testing.T) {
	t.Run("dry-run", func(t *testing.T) {
		st, deadline, _ := canceledObjectGRPCFixture(t, "object-dry-run")
		cleaner := &fakeCanceledObjectCleaner{observations: []s3io.ExactObjectObservation{{Exists: true, Matches: true, Identity: "stable"}}}
		srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
		srv.newCanceledObjectCleanerFn = func(context.Context, s3io.Config) (canceledObjectCleaner, error) { return cleaner, nil }
		srv.SetCanceledObjectCleanupPolicy(time.Second, 3, true)
		srv.nowFn = func() time.Time { return deadline.Add(time.Second) }
		if processed, err := srv.ProcessCanceledObjectCleanupOnce(context.Background()); err != nil || !processed {
			t.Fatalf("processed=%v err=%v", processed, err)
		}
		if len(cleaner.deletes) != 0 {
			t.Fatalf("dry-run deletes=%v", cleaner.deletes)
		}
	})
	t.Run("ambiguous", func(t *testing.T) {
		st, deadline, _ := canceledObjectGRPCFixture(t, "object-ambiguous")
		cleaner := &fakeCanceledObjectCleaner{
			observations: []s3io.ExactObjectObservation{{Exists: true, Matches: true, Identity: "stable"}, {}},
			observeErrs:  []error{nil, errors.New("provider unavailable")},
			deleteErr:    errors.New("lost delete response"),
		}
		srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
		srv.newCanceledObjectCleanerFn = func(context.Context, s3io.Config) (canceledObjectCleaner, error) { return cleaner, nil }
		srv.SetCanceledObjectCleanupPolicy(time.Second, 3, false)
		srv.nowFn = func() time.Time { return deadline.Add(time.Second) }
		if processed, err := srv.ProcessCanceledObjectCleanupOnce(context.Background()); err != nil || !processed {
			t.Fatalf("processed=%v err=%v", processed, err)
		}
	})
}
