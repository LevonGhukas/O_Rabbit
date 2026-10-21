package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// firstHeadHook delegates to a commitObjectStore and runs hook once, on the
// first Head call, so a second commit pass can complete mid-way through the
// first.
type firstHeadHook struct {
	commitObjectStore
	fired atomic.Bool
	hook  func()
}

func (h *firstHeadHook) Head(ctx context.Context, key string) error {
	// Pass A shares this wrapper; a CAS (unlike sync.Once) lets its own Head
	// calls through instead of deadlocking on the running hook.
	if h.fired.CompareAndSwap(false, true) {
		h.hook()
	}
	return h.commitObjectStore.Head(ctx, key)
}

func assertNoCompletionPendingEvent(t *testing.T, st *db.Store, runID string) {
	t.Helper()
	events, err := st.ListEventsForRun(context.Background(), runID, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.Message == "run completion pending recovery" {
			t.Fatalf("unexpected completion-pending event: %+v", e)
		}
	}
}

func assertCleanCommit(t *testing.T, st *db.Store, runID string) {
	t.Helper()
	run, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "SUCCEEDED" || run.CommitPhase != "COMPLETE" || run.CommitReconciliationAttempt != 0 || run.FailureClass != "" {
		t.Fatalf("run status=%s phase=%s attempt=%d class=%q", run.Status, run.CommitPhase, run.CommitReconciliationAttempt, run.FailureClass)
	}
	assertNoCompletionPendingEvent(t, st, runID)
}

// A pass that loaded the run before another pass persisted the commit intent
// must not treat the other pass's state as an integrity conflict.
func TestStaleCommitPassAfterConcurrentCompletionSucceeds(t *testing.T) {
	f := newCommitFixture(t, "stale-pass")
	f.srv.completeRunCommitFn = f.st.CompleteRunCommit
	var passAErr error
	store := &firstHeadHook{commitObjectStore: f.objects}
	store.hook = func() { passAErr = f.srv.finalizeRunCommit(f.ctx, f.runID) }
	f.srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return store, nil }

	passBErr := f.finalize()

	if passAErr != nil {
		t.Fatalf("pass A: %v", passAErr)
	}
	if passBErr != nil {
		t.Fatalf("pass B: %v", passBErr)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	if n := f.objects.putCount(f.manifestKey); n != 1 {
		t.Fatalf("manifest puts=%d, want 1", n)
	}
	if n := f.objects.putCount(f.stateKey); n != 1 {
		t.Fatalf("state puts=%d, want 1", n)
	}
	assertCleanCommit(t, f.st, f.runID)
}

// A stale pass that finds state written by a pass still in progress (intent
// persisted, run still COMMITTING) must restart from the persisted intent.
func TestStaleCommitPassRestartsFromPersistedIntent(t *testing.T) {
	f := newCommitFixture(t, "stale-restart")
	f.srv.completeRunCommitFn = f.st.CompleteRunCommit
	var passAErr error
	store := &firstHeadHook{commitObjectStore: f.objects}
	store.hook = func() { passAErr = f.srv.commitRun(f.ctx, f.runID) }
	f.srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return store, nil }

	passBErr := f.finalize()

	if passAErr != nil {
		t.Fatalf("pass A: %v", passAErr)
	}
	if passBErr != nil {
		t.Fatalf("pass B: %v", passBErr)
	}
	f.assertRun("SUCCEEDED", "COMPLETE")
	if n := f.objects.putCount(f.manifestKey); n != 1 {
		t.Fatalf("manifest puts=%d, want 1", n)
	}
	if n := f.objects.putCount(f.stateKey); n != 1 {
		t.Fatalf("state puts=%d, want 1", n)
	}
	assertCleanCommit(t, f.st, f.runID)
}

// State naming this run with no intent persisted anywhere is still a genuine
// integrity conflict.
func TestStateWithoutAnyPersistedIntentStillConflicts(t *testing.T) {
	f := newCommitFixture(t, "orphan-state")
	f.objects.objects[f.stateKey] = mustJSONRaw(t, map[string]any{
		"last_committed_run_id": f.runID,
		"committed_at":          "2026-07-22T10:00:00Z",
		"commit_id":             "orphan",
		"manifest_key":          f.manifestKey,
	})

	err := f.finalize()

	if err == nil || !strings.Contains(err.Error(), "exists without durable intent") {
		t.Fatalf("err=%v, want durable intent conflict", err)
	}
}

// newManyTaskCommitRun creates a RUNNING run with n assigned tasks, each with
// a verified artifact present in the scripted store.
func newManyTaskCommitRun(t *testing.T, suffix string, n int) (*db.Store, *scriptedCommitStore, []*grpcpb.ReportTaskResultRequest, string) {
	t.Helper()
	ctx := context.Background()
	st := openGRPCTestStore(t)
	objects := newScriptedCommitStore()
	runID, jobID := "run-"+suffix, "job-"+suffix
	prefix := "cert/" + suffix
	srcID, tgtID := "src-"+suffix, "tgt-"+suffix
	srcSecret, err := crypto.Encrypt(testCryptoKey, []byte(`{"dsn":"sqlserver://example"}`), []byte(srcID))
	if err != nil {
		t.Fatal(err)
	}
	tgtSecret, err := crypto.Encrypt(testCryptoKey, []byte(`{"access_key_id":"a","secret_access_key":"b"}`), []byte(tgtID))
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: srcID, Name: srcID, Kind: "source", Engine: "mssql", MetadataJSON: []byte(`{}`), SecretEncBlob: srcSecret}); err != nil {
		t.Fatal(err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: tgtID, Name: tgtID, Kind: "target", Engine: "s3", MetadataJSON: mustJSONRaw(t, map[string]any{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": "bucket1", "prefix": prefix, "force_path_style": true}), SecretEncBlob: tgtSecret}); err != nil {
		t.Fatal(err)
	}
	hwmColumn := "id"
	if err := st.CreateJob(ctx, db.Job{ID: jobID, Name: jobID, SourceConnectionID: srcID, TargetConnectionID: tgtID, SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", Incremental: true, HWMColumn: &hwmColumn, OptionsJSON: mustJSONRaw(t, map[string]any{"table": "dbo.orders", "partition_strategy": "ordered_cursor", "cursor_column": "id"})}); err != nil {
		t.Fatal(err)
	}
	datasetKey := dataset.StorageKey("http://minio:9000", "bucket1", prefix)
	if err := st.CreateRun(ctx, db.Run{ID: runID, JobID: jobID, DatasetKey: datasetKey, Status: "RUNNING", CorrelationID: "corr-" + suffix, StartedAt: "2026-07-22T10:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	inserts := make([]db.TaskInsert, n)
	for i := range inserts {
		inserts[i] = db.TaskInsert{ID: fmt.Sprintf("task-%s-%d", suffix, i), RunID: runID, TaskIndex: i + 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}
	}
	if err := st.InsertTasks(ctx, inserts); err != nil {
		t.Fatal(err)
	}
	reqs := make([]*grpcpb.ReportTaskResultRequest, n)
	for i := 0; i < n; i++ {
		attemptID, token := fmt.Sprintf("attempt-%s-%d", suffix, i), fmt.Sprintf("token-%s-%d", suffix, i)
		one := func(v string) func() (string, error) { return func() (string, error) { return v, nil } }
		assigned, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-1", time.Now(), db.LeasePolicy{Duration: time.Hour, MaxAttempts: 3}, one(attemptID), one(token))
		if err != nil || !ok {
			t.Fatalf("assign %d ok=%v err=%v", i, ok, err)
		}
		key := fmt.Sprintf("%s/_runs/run-%s/part-%06d-000.parquet", prefix, runID, i+1)
		body := []byte(fmt.Sprintf("PAR1-test-%d", i))
		objects.objects[key] = body
		digest := sha256.Sum256(body)
		reqs[i] = &grpcpb.ReportTaskResultRequest{
			WorkerId: "worker-1", TaskId: assigned.ID, RunId: runID, AttemptId: assigned.AttemptID, FencingToken: assigned.FencingToken, Status: "SUCCEEDED",
			RowsRead: 10, BytesWritten: int64(len(body)), ParquetObjectKeys: []string{key}, MaxHwmValue: "42",
			Artifacts: []*grpcpb.ArtifactIntegrity{{
				ObjectKey: key, ByteSize: int64(len(body)), Sha256: hex.EncodeToString(digest[:]), RowCount: 10, SchemaFingerprint: strings.Repeat("a", 64),
				RunId: runID, TaskId: assigned.ID, AttemptId: assigned.AttemptID, AttemptNumber: int32(assigned.AttemptNumber), FileIndex: 0,
				FormatVersion: int32(artifact.FormatVersion), VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHwm: "42",
			}},
		}
	}
	return st, objects, reqs, runID
}

// Many tasks finishing at once while reconciliation scans run must commit the
// run exactly once and never fail it with an integrity conflict.
func TestManyTasksFinishingTogetherCommitOnce(t *testing.T) {
	const tasks = 8
	st, objects, reqs, runID := newManyTaskCommitRun(t, "many-tasks", tasks)
	srv := NewServer(nil, st, nil, testCryptoKey, time.Second, nil)
	srv.runIcebergRegistrationFn = nil
	srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return objects, nil }
	srv.upsertHWMFn = func(ctx context.Context, jobID, value string) error { return st.UpsertHWM(ctx, jobID, value) }

	var wg sync.WaitGroup
	for _, req := range reqs {
		wg.Add(2)
		go func() {
			defer wg.Done()
			if _, err := srv.ReportTaskResult(context.Background(), req); err != nil {
				t.Errorf("ReportTaskResult: %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := srv.ReconcileCommittingRuns(context.Background()); err != nil {
				t.Errorf("ReconcileCommittingRuns: %v", err)
			}
		}()
	}
	wg.Wait()

	waitFor(t, "run to succeed", func() bool {
		run, err := st.GetRun(context.Background(), runID)
		return err == nil && run.Status == "SUCCEEDED" && run.CommitPhase == "COMPLETE"
	})
	prefix := "cert/many-tasks"
	if n := objects.putCount(prefix + "/_commits/run-" + runID + ".json"); n != 1 {
		t.Fatalf("manifest puts=%d, want 1", n)
	}
	if n := objects.putCount(prefix + "/_state.json"); n != 1 {
		t.Fatalf("state puts=%d, want 1", n)
	}
	assertCleanCommit(t, st, runID)
}
