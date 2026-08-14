package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

func TestReconcileCommittingRunsConcurrency(t *testing.T) {
	ctx := context.Background()
	st := openGRPCTestStore(t)
	objects := newScriptedCommitStore()

	var runIDs []string
	for i := 0; i < 15; i++ {
		suffix := fmt.Sprintf("conc-%d", i)
		runID, jobID := "run-"+suffix, "job-"+suffix
		prefix := "cert/" + suffix
		parquetKey := prefix + "/_runs/run-" + runID + "/part-000001.parquet"
		datasetKey := dataset.StorageKey("http://minio:9000", "bucket1", prefix)

		srcID, tgtID := "src-"+suffix, "tgt-"+suffix
		srcSecret, _ := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"sqlserver://example"}`), []byte(srcID))
		tgtSecret, _ := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"a","secret_access_key":"b"}`), []byte(tgtID))

		_ = st.CreateConnection(ctx, db.Connection{ID: srcID, Name: srcID, Kind: "source", Engine: "mssql", MetadataJSON: []byte(`{}`), SecretEncBlob: srcSecret})
		_ = st.CreateConnection(ctx, db.Connection{ID: tgtID, Name: tgtID, Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"endpoint": "http://minio:9000", "region": "us-east-1", "bucket": "bucket1", "prefix": "` + prefix + `", "force_path_style": true}`), SecretEncBlob: tgtSecret})

		hwmColumn := "id"
		_ = st.CreateJob(ctx, db.Job{ID: jobID, Name: jobID, SourceConnectionID: srcID, TargetConnectionID: tgtID, SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", Incremental: true, HWMColumn: &hwmColumn, OptionsJSON: []byte(`{"table": "dbo.orders", "partition_strategy": "ordered_cursor", "cursor_column": "id"}`)})

		_ = st.CreateRun(ctx, db.Run{ID: runID, JobID: jobID, DatasetKey: datasetKey, Status: "RUNNING", CorrelationID: "corr-" + suffix, StartedAt: "2026-07-22T10:00:00Z"})

		taskID := "task-" + suffix
		_ = st.InsertTasks(ctx, []db.TaskInsert{{ID: taskID, RunID: runID, TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"}})

		parquetBytes := []byte("PAR1-test")
		objects.objects[parquetKey] = parquetBytes

		assigned, _, _ := st.AssignNextPendingTaskWithLease(ctx, "", "worker-"+suffix, time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC), db.LeasePolicy{Duration: time.Hour, MaxAttempts: 3}, func() (string, error) { return "attempt-" + suffix, nil }, func() (string, error) { return "token-" + suffix, nil })

		digest := sha256.Sum256(parquetBytes)
		record := artifact.Record{ObjectKey: parquetKey, ByteSize: int64(len(parquetBytes)), SHA256: hex.EncodeToString(digest[:]), RowCount: 10, SchemaFingerprint: strings.Repeat("a", 64), RunID: runID, TaskID: taskID, AttemptID: assigned.AttemptID, AttemptNumber: assigned.AttemptNumber, FileIndex: 0, FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHWM: "42"}

		_, _, _, _ = st.CompleteTaskAttemptWithArtifactsAt(ctx, "", taskID, assigned.AttemptID, assigned.FencingToken, "worker-"+suffix, "SUCCEEDED", nil, []artifact.Record{record}, 10, 0, int64(len(parquetBytes)), time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC))

		_, status, _ := st.TryFinalizeRun(ctx, runID)
		if status != "COMMITTING" {
			t.Fatalf("failed to finalize run %s", runID)
		}
		runIDs = append(runIDs, runID)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, time.Second, nil)
	srv.runIcebergRegistrationFn = nil
	srv.newCommitObjectStoreFn = func(context.Context, s3io.Config) (commitObjectStore, error) { return objects, nil }
	srv.upsertHWMFn = func(ctx context.Context, jobID, value string) error {
		return st.UpsertHWM(ctx, jobID, value)
	}

	if err := srv.ReconcileCommittingRuns(ctx); err != nil {
		t.Fatalf("ReconcileCommittingRuns failed: %v", err)
	}

	for _, runID := range runIDs {
		run, _ := st.GetRun(ctx, runID)
		if run.Status != "SUCCEEDED" || run.CommitPhase != "COMPLETE" {
			t.Fatalf("run %s is %s/%s, expected SUCCEEDED/COMPLETE", runID, run.Status, run.CommitPhase)
		}
	}
}
