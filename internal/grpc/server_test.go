package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBuildParquetObjectPayloadsIncludesOptionalFields(t *testing.T) {
	got := buildParquetObjectPayloads([]string{"a.parquet", "b.parquet"}, "cursor-42", 12, 34)
	want := []map[string]any{
		{"key": "a.parquet", "max_hwm": "cursor-42", "rows": int64(12), "bytes": int64(34)},
		{"key": "b.parquet", "max_hwm": "cursor-42", "rows": int64(12), "bytes": int64(34)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payloads = %#v, want %#v", got, want)
	}
}

func TestWorkerProtocolCompatibilityFailsClosed(t *testing.T) {
	st := openGRPCTestStore(t)
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	for _, version := range []int32{0, 4, 6} {
		_, err := srv.RequestTask(context.Background(), &grpcpb.RequestTaskRequest{WorkerId: "legacy", ProtocolVersion: version})
		if status.Code(err) != codes.FailedPrecondition ||
			!strings.Contains(err.Error(), "accepted version=5") ||
			!strings.Contains(err.Error(), "exact match required") {
			t.Fatalf("version=%d error=%v", version, err)
		}
	}
	if _, err := srv.RequestTask(context.Background(), &grpcpb.RequestTaskRequest{WorkerId: "current", ProtocolVersion: 5}); err != nil {
		t.Fatalf("current protocol rejected: %v", err)
	}
}

func TestCatalogWorkAdmissionIsBoundedAndNonblocking(t *testing.T) {
	srv := NewServer(nil, openGRPCTestStore(t), nil, crypto.Key{}, time.Second, nil)
	srv.SetCatalogWorkLimit(1)
	release, ok := srv.tryAcquireCatalogWork()
	if !ok {
		t.Fatal("first catalog work was not admitted")
	}
	if secondRelease, ok := srv.tryAcquireCatalogWork(); ok {
		secondRelease()
		t.Fatal("catalog work exceeded configured limit")
	}
	release()
	release, ok = srv.tryAcquireCatalogWork()
	if !ok {
		t.Fatal("catalog work did not become eligible after release")
	}
	release()
}

func TestBuildParquetObjectPayloadsOmitsEmptyOptionalFields(t *testing.T) {
	got := buildParquetObjectPayloads([]string{"only.parquet"}, "", 0, 0)
	want := []map[string]any{
		{"key": "only.parquet"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payloads = %#v, want %#v", got, want)
	}
}

func TestCollectParquetObjectInfosDedupesAndSkipsMalformed(t *testing.T) {
	tasks := []db.Task{
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "a.parquet", "rows": 10},
				{"key": "   ", "rows": 99},
			}),
		},
		{
			ParquetObjects: json.RawMessage(`not-json`),
		},
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "b.parquet", "bytes": 20},
				{"key": "a.parquet", "rows": 11},
			}),
		},
	}

	got := collectParquetObjectInfos(tasks)
	want := []map[string]any{
		{"key": "a.parquet", "rows": float64(10)},
		{"key": "b.parquet", "bytes": float64(20)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("infos = %#v, want %#v", got, want)
	}
}

func TestCollectParquetKeysKeepsFirstSeenOrder(t *testing.T) {
	tasks := []db.Task{
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "a.parquet"},
				{"key": "b.parquet"},
			}),
		},
		{
			ParquetObjects: json.RawMessage(`not-json`),
		},
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "  "},
				{"key": "b.parquet"},
				{"key": "c.parquet"},
			}),
		},
	}

	got := collectParquetKeys(tasks)
	want := []string{"a.parquet", "b.parquet", "c.parquet"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("keys = %#v, want %#v", got, want)
	}
}

func TestMaxPartNumberSupportsRolledTaskSuffixes(t *testing.T) {
	keys := []string{
		"exports/orders/_runs/run-1/part-000001.parquet",
		"exports/orders/_runs/run-1/part-000001-001.parquet",
		"exports/orders/_runs/run-1/part-000002-000.parquet",
		"exports/orders/_runs/run-1/part-000005-002.parquet",
		"exports/orders/_runs/run-1/not-a-part.parquet",
	}

	if got := maxPartNumber(keys); got != 5 {
		t.Fatalf("maxPartNumber()=%d want 5", got)
	}
}

func TestDeriveMaxCursorSkipsMalformedAndFindsMaximum(t *testing.T) {
	tasks := []db.Task{
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "a.parquet", "max_hwm": "5"},
				{"key": "b.parquet", "max_hwm": ""},
			}),
		},
		{
			ParquetObjects: json.RawMessage(`not-json`),
		},
		{
			ParquetObjects: mustJSONRaw(t, []map[string]any{
				{"key": "c.parquet", "max_hwm": "11"},
				{"key": "d.parquet", "max_hwm": "9"},
			}),
		},
	}

	got := deriveMaxCursor(tasks, connectors.CursorDomainInt64)
	if got != "11" {
		t.Fatalf("max cursor = %q, want %q", got, "11")
	}
}

func TestReportTaskProgressReturnsCanceledForCanceledRun(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRunAndTask(t, st, "run-progress-cancel", "job-progress-cancel", "task-progress-cancel", "PENDING")
	assignGRPCTestAttempt(t, st, "task-progress-cancel", "worker-1")

	if _, _, _, err := st.CancelRun(ctx, "run-progress-cancel", "canceled during execution"); err != nil {
		t.Fatalf("cancel run: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	_, err := srv.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{
		WorkerId:     "worker-1",
		TaskId:       "task-progress-cancel",
		RunId:        "run-progress-cancel",
		AttemptId:    "attempt-task-progress-cancel",
		FencingToken: "token-task-progress-cancel",
		RowsRead:     123,
	})
	if err == nil {
		t.Fatal("expected canceled progress error")
	}
	if got := status.Code(err); got != codes.Canceled {
		t.Fatalf("status.Code(err)=%v want %v", got, codes.Canceled)
	}

	tasks, err := st.ListTasksForRun(ctx, "run-progress-cancel")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if tasks[0].RowsRead != 0 {
		t.Fatalf("rows_read=%d want 0", tasks[0].RowsRead)
	}
}

func TestReportTaskResultCoercesLateSuccessToCanceled(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRunAndTask(t, st, "run-result-cancel", "job-result-cancel", "task-result-cancel", "PENDING")
	assignGRPCTestAttempt(t, st, "task-result-cancel", "worker-1")

	if _, _, _, err := st.CancelRun(ctx, "run-result-cancel", "canceled during execution"); err != nil {
		t.Fatalf("cancel run: %v", err)
	}

	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	_, err := srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId:     "worker-1",
		TaskId:       "task-result-cancel",
		RunId:        "run-result-cancel",
		AttemptId:    "attempt-task-result-cancel",
		FencingToken: "token-task-result-cancel",
		Status:       "SUCCEEDED",
		RowsRead:     77,
		BytesRead:    88,
		BytesWritten: 99,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late canceled result err=%v", err)
	}

	tasks, err := st.ListTasksForRun(ctx, "run-result-cancel")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if tasks[0].Status != "CANCELED" {
		t.Fatalf("task status=%q want CANCELED", tasks[0].Status)
	}

	run, err := st.GetRun(ctx, "run-result-cancel")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if run.Status != "CANCELED" {
		t.Fatalf("run status=%q want CANCELED", run.Status)
	}
}

func TestStaleAttemptRejectionEventsAreVisibleAndBounded(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRunAndTask(t, st, "run-rejections", "job-rejections", "task-rejections", "PENDING")
	t0 := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	one := func(v string) func() (string, error) { return func() (string, error) { return v, nil } }
	a, ok, err := st.AssignNextPendingTaskWithLease(ctx, "", "worker-old", t0, db.LeasePolicy{Duration: time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Second}, one("attempt-old"), one("secret-token"))
	if err != nil || !ok {
		t.Fatalf("assign ok=%v err=%v", ok, err)
	}
	if _, err := st.ExpireTaskAttempts(ctx, t0.Add(2*time.Second), db.LeasePolicy{Duration: time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: time.Second}); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	srv.nowFn = func() time.Time { return t0.Add(3 * time.Second) }
	for i := 0; i < 20; i++ {
		_, _ = srv.RenewTaskLease(ctx, &grpcpb.RenewTaskLeaseRequest{WorkerId: "worker-old", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken})
		_, _ = srv.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{WorkerId: "worker-old", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken})
		_, _ = srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{WorkerId: "worker-old", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken, Status: "SUCCEEDED"})
	}
	events, err := st.ListEventsForRun(ctx, "run-rejections", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{"STALE_RENEWAL_REJECTED", "STALE_PROGRESS_REJECTED", "STALE_RESULT_REJECTED"} {
		count := 0
		for _, event := range events {
			if strings.Contains(string(event.FieldsJSON), `"event_type":"`+eventType+`"`) {
				count++
			}
			if strings.Contains(string(event.FieldsJSON), "secret-token") {
				t.Fatal("event exposed fencing token")
			}
		}
		if count != 1 {
			t.Fatalf("%s events=%d want 1", eventType, count)
		}
	}
}

func TestArtifactFailureEventsAreSafeStructuredAndBounded(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRunAndTask(t, st, "run-artifact-event", "job-artifact-event", "task-artifact-event", "PENDING")
	a := assignGRPCTestAttempt(t, st, "task-artifact-event", "worker-1")
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	fields := `{"artifact_failure":{"classification":"REMOTE_CHECKSUM_MISMATCH","attempt_id":"` + a.AttemptID + `","attempt_number":1,"worker_id":"worker-1","file_index":2,"object_key":"safe/key.parquet","verification_method":"PORTABLE_FULL_SHA256","retryable":false,"ambiguous":false,"reconciliation_allowed":false,"fencing_token":"` + a.FencingToken + `"}}`
	for i := 0; i < 20; i++ {
		if _, err := srv.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{WorkerId: "worker-1", TaskId: a.ID, RunId: "worker-invented-run", AttemptId: a.AttemptID, FencingToken: a.FencingToken, FieldsJson: fields}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := st.ListEventsForRun(ctx, "run-artifact-event", 100)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"classification":"REMOTE_CHECKSUM_MISMATCH"`) {
			count++
			if event.Level != "ERROR" || event.Message != "artifact operation failed" || event.RunID != "run-artifact-event" {
				t.Fatalf("event=%+v", event)
			}
			if strings.Contains(string(event.FieldsJSON), a.FencingToken) {
				t.Fatal("event exposed fencing token")
			}
		}
	}
	if count != 1 {
		t.Fatalf("bounded artifact events=%d", count)
	}
	for _, classification := range []string{"LOCAL_PARQUET_INVALID", "LOCAL_SIZE_MISMATCH", "LOCAL_ROW_COUNT_MISMATCH", "LOCAL_SCHEMA_MISMATCH", "UPLOAD_FAILED", "MULTIPART_PART_FAILED", "MULTIPART_COMPLETE_AMBIGUOUS", "REMOTE_SIZE_MISMATCH", "REMOTE_METADATA_MISMATCH", "EXISTING_OBJECT_CONFLICT", "VERIFICATION_UNAVAILABLE"} {
		payload := `{"artifact_failure":{"classification":"` + classification + `","file_index":2,"object_key":"safe/key.parquet"}}`
		for i := 0; i < 3; i++ {
			if _, err := srv.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{WorkerId: "worker-1", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken, FieldsJson: payload}); err != nil {
				t.Fatal(err)
			}
		}
	}
	events, _ = st.ListEventsForRun(ctx, "run-artifact-event", 100)
	for _, classification := range []string{"LOCAL_PARQUET_INVALID", "LOCAL_SIZE_MISMATCH", "LOCAL_ROW_COUNT_MISMATCH", "LOCAL_SCHEMA_MISMATCH", "UPLOAD_FAILED", "MULTIPART_PART_FAILED", "MULTIPART_COMPLETE_AMBIGUOUS", "REMOTE_SIZE_MISMATCH", "REMOTE_METADATA_MISMATCH", "EXISTING_OBJECT_CONFLICT", "VERIFICATION_UNAVAILABLE"} {
		n := 0
		for _, event := range events {
			if strings.Contains(string(event.FieldsJSON), `"classification":"`+classification+`"`) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%s events=%d", classification, n)
		}
	}
	concurrent := `{"artifact_failure":{"classification":"REMOTE_CHECKSUM_MISMATCH","file_index":4,"object_key":"safe/concurrent.parquet"}}`
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = srv.ReportTaskProgress(ctx, &grpcpb.ReportTaskProgressRequest{WorkerId: "worker-1", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken, FieldsJson: concurrent})
		}()
	}
	wg.Wait()
	events, _ = st.ListEventsForRun(ctx, "run-artifact-event", 100)
	concurrentCount := 0
	for _, event := range events {
		if strings.Contains(string(event.FieldsJSON), `"object_key":"safe/concurrent.parquet"`) {
			concurrentCount++
		}
	}
	if concurrentCount != 1 {
		t.Fatalf("concurrent bounded events=%d", concurrentCount)
	}
}

func TestDuplicateResultEventsAreIdempotentAndConflictsBounded(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRunAndTask(t, st, "run-duplicate-events", "job-duplicate-events", "task-duplicate-events", "PENDING")
	if err := st.InsertTasks(ctx, []db.TaskInsert{{ID: "task-block-finalize", RunID: "run-duplicate-events", TaskIndex: 2, PartitionSpec: []byte(`{}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	a := assignGRPCTestAttempt(t, st, "task-duplicate-events", "worker-1")
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, nil)
	req := &grpcpb.ReportTaskResultRequest{WorkerId: "worker-1", TaskId: a.ID, AttemptId: a.AttemptID, FencingToken: a.FencingToken, Status: "SUCCEEDED", ParquetObjectKeys: []string{"attempt/object.parquet"}}
	req.BytesWritten = 1
	req.Artifacts = []*grpcpb.ArtifactIntegrity{{ObjectKey: "attempt/object.parquet", ByteSize: 1, Sha256: strings.Repeat("a", 64), RowCount: 0, SchemaFingerprint: strings.Repeat("b", 64), RunId: "run-duplicate-events", TaskId: a.ID, AttemptId: a.AttemptID, AttemptNumber: int32(a.AttemptNumber), FileIndex: 0, FormatVersion: 1, VerificationMethod: "PORTABLE_FULL_SHA256", VerificationStatus: "VERIFIED"}}
	if _, err := srv.ReportTaskResult(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.ReportTaskResult(ctx, req); err != nil {
		t.Fatal(err)
	}
	conflict := &grpcpb.ReportTaskResultRequest{
		WorkerId:          req.WorkerId,
		TaskId:            req.TaskId,
		AttemptId:         req.AttemptId,
		FencingToken:      req.FencingToken,
		Status:            req.Status,
		BytesWritten:      req.BytesWritten,
		ParquetObjectKeys: []string{"different/object.parquet"},
		Artifacts: []*grpcpb.ArtifactIntegrity{{
			ObjectKey:          "different/object.parquet",
			ByteSize:           1,
			Sha256:             strings.Repeat("c", 64),
			RowCount:           0,
			SchemaFingerprint:  strings.Repeat("b", 64),
			RunId:              req.Artifacts[0].RunId,
			TaskId:             req.TaskId,
			AttemptId:          req.AttemptId,
			AttemptNumber:      req.Artifacts[0].AttemptNumber,
			FileIndex:          0,
			FormatVersion:      1,
			VerificationMethod: "PORTABLE_FULL_SHA256",
			VerificationStatus: "VERIFIED",
		}},
	}
	for i := 0; i < 10; i++ {
		if _, err := srv.ReportTaskResult(ctx, conflict); status.Code(err) != codes.AlreadyExists {
			t.Fatalf("conflict err=%v", err)
		}
	}
	events, err := st.ListEventsForRun(ctx, "run-duplicate-events", 100)
	if err != nil {
		t.Fatal(err)
	}
	completed, conflicts := 0, 0
	for _, event := range events {
		if event.Message == "task task-duplicate-events SUCCEEDED" {
			completed++
		}
		if strings.Contains(string(event.FieldsJSON), `"event_type":"CONFLICTING_RESULT_REJECTED"`) {
			conflicts++
		}
	}
	if completed != 1 || conflicts != 1 {
		t.Fatalf("completion events=%d conflict events=%d", completed, conflicts)
	}
}

func TestReportTaskResultSchedulesIcebergRegistrationAfterCommit(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRegistrableRunAndTask(t, st, "run-ice-success", "job-ice-success", "task-ice-success")
	assignGRPCTestAttempt(t, st, "task-ice-success", "worker-1")

	committed := make(chan struct{})
	registrar := &fakeIcebergRegistrar{
		t:         t,
		committed: committed,
		reqCh:     make(chan icebergreg.RunRequest, 1),
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, registrar)
	srv.commitRunFn = func(ctx context.Context, runID string) error {
		if err := saveGRPCTestVerifiedEmptyIntent(ctx, st, runID, "exports/orders"); err != nil {
			return err
		}
		close(committed)
		return nil
	}

	resp, err := srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId:     "worker-1",
		TaskId:       "task-ice-success",
		RunId:        "run-ice-success",
		AttemptId:    "attempt-task-ice-success",
		FencingToken: "token-task-ice-success",
		Status:       "SUCCEEDED",
	})
	if err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("accepted=%v want true", resp.Accepted)
	}

	select {
	case req := <-registrar.reqCh:
		if req.RunID != "run-ice-success" {
			t.Fatalf("run_id=%q want %q", req.RunID, "run-ice-success")
		}
		if req.Registration.Table != "mssql.orders" {
			t.Fatalf("registration.table=%q want %q", req.Registration.Table, "mssql.orders")
		}
		if req.Registration.URI != "http://catalog:8181" {
			t.Fatalf("registration.uri=%q want %q", req.Registration.URI, "http://catalog:8181")
		}
		if req.SourceDSN != "sqlserver://sa:pass@example:1433?database=SalesDB" {
			t.Fatalf("source_dsn=%q", req.SourceDSN)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for iceberg registration")
	}
}

func TestRegistrationNoOpCallbackCompletesDurableLifecycle(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	const runID = "run-ice-noop"
	createGRPCTestRegistrableRunAndTask(t, st, runID, "job-ice-noop", "task-ice-noop")
	assignGRPCTestAttempt(t, st, "task-ice-noop", "worker-1")

	called := make(chan struct{})
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, noOpLifecycleRegistrar{called: called})
	srv.commitRunFn = func(ctx context.Context, gotRunID string) error {
		return saveGRPCTestVerifiedEmptyIntent(ctx, st, gotRunID, "exports/orders")
	}

	resp, err := srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId:     "worker-1",
		TaskId:       "task-ice-noop",
		RunId:        runID,
		AttemptId:    "attempt-task-ice-noop",
		FencingToken: "token-task-ice-noop",
		Status:       "SUCCEEDED",
	})
	if err != nil || !resp.Accepted {
		t.Fatalf("response=%+v err=%v", resp, err)
	}
	select {
	case <-called:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for no-op registration callbacks")
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		registration, err := st.GetRegistrationForRun(ctx, runID)
		if err == nil && registration.Status == db.RegistrationRegistered {
			receipt, parseErr := icebergreg.ParseCatalogReceipt(registration.Receipt)
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			if !receipt.NoOp || receipt.NoOpReason != "ALL_ARTIFACTS_ALREADY_APPLIED" || len(receipt.NoOpEvidenceDigest) != 64 {
				t.Fatalf("receipt=%+v", receipt)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("registration did not complete: %+v err=%v", registration, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunIcebergRegistrationPassesIceSnapshotToRegistrar(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRegistrableRunAndTaskWithSnapshot(t, st, "run-ice-cli", "job-ice-cli", "task-ice-cli", mustJSONRaw(t, map[string]any{
		"enabled":      true,
		"engine":       "ice",
		"table":        "mssql.orders",
		"uri":          "http://catalog:8181",
		"bearer_token": "token",
		"config_yaml":  "uri: http://catalog:8181\nbearerToken: token\nhttpCacheDir: data/ice/http/cache\n",
		"s3": map[string]any{
			"endpoint":          "http://minio:9000",
			"region":            "us-east-1",
			"path_style_access": true,
			"access_key_id":     "minioadmin",
			"secret_access_key": "minioadmin",
		},
	}))

	committed := make(chan struct{})
	close(committed)
	registrar := &fakeIcebergRegistrar{
		t:         t,
		committed: committed,
		reqCh:     make(chan icebergreg.RunRequest, 1),
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, registrar)

	ran, _, err := srv.runIcebergRegistration(ctx, "run-ice-cli")
	if err != nil {
		t.Fatalf("runIcebergRegistration: %v", err)
	}
	if !ran {
		t.Fatalf("ran=%v want true", ran)
	}

	select {
	case req := <-registrar.reqCh:
		if req.Registration.Engine != "ice" {
			t.Fatalf("engine=%q want ice", req.Registration.Engine)
		}
		if req.Registration.ConfigYAML == "" {
			t.Fatal("expected config_yaml in persisted ice snapshot")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for registrar request")
	}
}

func TestRunIcebergRegistrationPassesQueryModeSourceToRegistrar(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	query := "SELECT id, name FROM public.customers"
	queryHash := connectors.QueryHash(query)
	createGRPCTestQueryRegistrableRunAndTask(t, st, "run-query-ice", "job-query-ice", "task-query-ice", query, queryHash)

	committed := make(chan struct{})
	close(committed)
	registrar := &fakeIcebergRegistrar{
		t:         t,
		committed: committed,
		reqCh:     make(chan icebergreg.RunRequest, 1),
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, registrar)

	ran, _, err := srv.runIcebergRegistration(ctx, "run-query-ice")
	if err != nil {
		t.Fatalf("runIcebergRegistration: %v", err)
	}
	if !ran {
		t.Fatalf("ran=%v want true", ran)
	}

	select {
	case req := <-registrar.reqCh:
		if req.Registration.Table != "oproject.pg_sql1" {
			t.Fatalf("registration.table=%q want oproject.pg_sql1", req.Registration.Table)
		}
		if req.SourceMode != "query" {
			t.Fatalf("source_mode=%q want query", req.SourceMode)
		}
		if req.SourceTable != "" {
			t.Fatalf("source_table=%q want empty", req.SourceTable)
		}
		if req.SourceQuery != query {
			t.Fatalf("source_query=%q want %q", req.SourceQuery, query)
		}
		if req.QueryHash != queryHash {
			t.Fatalf("query_hash=%q want %q", req.QueryHash, queryHash)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for registrar request")
	}
}

func TestReportTaskResultDoesNotScheduleIcebergRegistrationWhenCommitFails(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRegistrableRunAndTask(t, st, "run-ice-fail", "job-ice-fail", "task-ice-fail")
	assignGRPCTestAttempt(t, st, "task-ice-fail", "worker-1")

	registrar := &fakeIcebergRegistrar{
		t:         t,
		committed: make(chan struct{}),
		reqCh:     make(chan icebergreg.RunRequest, 1),
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, registrar)
	srv.commitRunFn = func(context.Context, string) error {
		return status.Error(codes.Internal, "commit blew up")
	}

	resp, err := srv.ReportTaskResult(ctx, &grpcpb.ReportTaskResultRequest{
		WorkerId:     "worker-1",
		TaskId:       "task-ice-fail",
		RunId:        "run-ice-fail",
		AttemptId:    "attempt-task-ice-fail",
		FencingToken: "token-task-ice-fail",
		Status:       "SUCCEEDED",
	})
	if err != nil {
		t.Fatalf("ReportTaskResult: %v", err)
	}
	if !resp.Accepted {
		t.Fatalf("accepted=%v want true", resp.Accepted)
	}
	if resp.Message != "rpc error: code = Internal desc = commit blew up" {
		t.Fatalf("message=%q", resp.Message)
	}

	select {
	case req := <-registrar.reqCh:
		t.Fatalf("unexpected iceberg registration request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestRunIcebergRegistrationFailsClearlyWhenSnapshotMissing(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRegistrableRunAndTaskWithSnapshot(t, st, "run-ice-missing-snapshot", "job-ice-missing-snapshot", "task-ice-missing-snapshot", nil)

	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, &fakeIcebergRegistrar{
		t:         t,
		committed: make(chan struct{}),
		reqCh:     make(chan icebergreg.RunRequest, 1),
	})

	ran, _, err := srv.runIcebergRegistration(ctx, "run-ice-missing-snapshot")
	if err == nil {
		t.Fatal("expected missing snapshot error")
	}
	if !ran {
		t.Fatalf("ran=%v want true", ran)
	}
	if got := err.Error(); got != "missing persisted run registration config snapshot; resubmit the run after the master upgrade" {
		t.Fatalf("error=%q", got)
	}
}

func TestRunIcebergRegistrationSkipsExplicitDisabledSnapshot(t *testing.T) {
	st := openGRPCTestStore(t)
	ctx := context.Background()
	createGRPCTestRegistrableRunAndTaskWithSnapshot(t, st, "run-ice-explicit-disabled", "job-ice-explicit-disabled", "task-ice-explicit-disabled", json.RawMessage(`{"enabled":false}`))

	registrar := &fakeIcebergRegistrar{
		t:         t,
		committed: make(chan struct{}),
		reqCh:     make(chan icebergreg.RunRequest, 1),
	}
	srv := NewServer(nil, st, nil, crypto.Key{}, 5*time.Second, registrar)

	ran, _, err := srv.runIcebergRegistration(ctx, "run-ice-explicit-disabled")
	if err != nil {
		t.Fatalf("runIcebergRegistration: %v", err)
	}
	if ran {
		t.Fatalf("ran=%v want false", ran)
	}
	select {
	case req := <-registrar.reqCh:
		t.Fatalf("unexpected iceberg registration request: %+v", req)
	case <-time.After(200 * time.Millisecond):
	}
}

func mustJSONRaw(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal test payload: %v", err)
	}
	return b
}

func openGRPCTestStore(t *testing.T) *db.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "grpc-test.sqlite")
	st, err := db.Open(context.Background(), db.Config{Path: path}, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = st.Close()
	})
	return st
}

func createGRPCTestRunAndTask(t *testing.T, st *db.Store, runID, jobID, taskID, taskStatus string) {
	t.Helper()
	ctx := context.Background()
	if err := st.CreateJob(ctx, db.Job{
		ID:                 jobID,
		Name:               jobID,
		SourceConnectionID: "src",
		TargetConnectionID: "tgt",
		SourceSQL:          "select 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON:        []byte(`{}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, db.Run{
		ID:            runID,
		JobID:         jobID,
		Status:        "RUNNING",
		CorrelationID: "corr-" + runID,
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{
		ID:            taskID,
		RunID:         runID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        taskStatus,
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
}

func assignGRPCTestAttempt(t *testing.T, st *db.Store, taskID, workerID string) db.Task {
	t.Helper()
	one := func(v string) func() (string, error) { return func() (string, error) { return v, nil } }
	task, ok, err := st.AssignNextPendingTaskWithLease(context.Background(), "", workerID, time.Now().UTC(), db.LeasePolicy{Duration: time.Minute, MaxAttempts: 3}, one("attempt-"+taskID), one("token-"+taskID))
	if err != nil || !ok || task.ID != taskID {
		t.Fatalf("assign test attempt task=%+v ok=%v err=%v", task, ok, err)
	}
	return task
}

func saveGRPCTestVerifiedEmptyIntent(ctx context.Context, st *db.Store, runID, prefix string) error {
	run, err := st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	manifest, err := json.Marshal(map[string]any{
		"schema_version": 2,
		"run_id":         runID,
		"bucket":         "bucket1",
		"prefix":         prefix,
		"objects":        []string{},
		"artifacts":      []artifact.Record{},
	})
	if err != nil {
		return err
	}
	digest := sha256.Sum256(manifest)
	commitID := hex.EncodeToString(digest[:])
	manifestKey := prefix + "/_commits/run-" + runID + ".json"
	state, err := json.Marshal(map[string]any{
		"bucket":                "bucket1",
		"prefix":                prefix,
		"last_committed_run_id": runID,
		"commit_id":             commitID,
		"manifest_key":          manifestKey,
	})
	if err != nil {
		return err
	}
	intent, err := json.Marshal(durableCommitIntent{
		CommitID:    commitID,
		DatasetID:   run.DatasetKey,
		ManifestKey: manifestKey,
		StateKey:    prefix + "/_state.json",
		Destination: durableCommitDestination{
			Endpoint:       "http://minio:9000",
			Region:         "us-east-1",
			Bucket:         "bucket1",
			Prefix:         prefix,
			ForcePathStyle: true,
		},
		Manifest:      manifest,
		ProposedState: state,
	})
	if err != nil {
		return err
	}
	if err := st.SaveCommitIntent(ctx, runID, commitID, intent); err != nil {
		return err
	}
	return st.SetCommitPhase(ctx, runID, "VERIFIED")
}

func createGRPCTestRegistrableRunAndTask(t *testing.T, st *db.Store, runID, jobID, taskID string) {
	createGRPCTestRegistrableRunAndTaskWithSnapshot(t, st, runID, jobID, taskID, mustJSONRaw(t, map[string]any{
		"enabled":      true,
		"engine":       "rest-go",
		"table":        "mssql.orders",
		"uri":          "http://catalog:8181",
		"bearer_token": "token",
		"s3": map[string]any{
			"endpoint":          "http://minio:9000",
			"region":            "us-east-1",
			"path_style_access": true,
			"access_key_id":     "minioadmin",
			"secret_access_key": "minioadmin",
		},
	}))
}

func createGRPCTestRegistrableRunAndTaskWithSnapshot(t *testing.T, st *db.Store, runID, jobID, taskID string, registrationConfig json.RawMessage) {
	t.Helper()
	ctx := context.Background()

	srcSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"sqlserver://sa:pass@example:1433?database=SalesDB"}`), []byte("src"))
	if err != nil {
		t.Fatalf("encrypt source secret: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:            "src",
		Name:          "src",
		Kind:          "source",
		Engine:        "mssql",
		MetadataJSON:  mustJSONRaw(t, map[string]any{}),
		SecretEncBlob: srcSecret,
	}); err != nil {
		t.Fatalf("create source connection: %v", err)
	}

	tgtSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`), []byte("tgt"))
	if err != nil {
		t.Fatalf("encrypt target secret: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:     "tgt",
		Name:   "tgt",
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: mustJSONRaw(t, map[string]any{
			"endpoint":         "http://minio:9000",
			"region":           "us-east-1",
			"bucket":           "bucket1",
			"prefix":           "exports/orders",
			"force_path_style": true,
		}),
		SecretEncBlob: tgtSecret,
	}); err != nil {
		t.Fatalf("create target connection: %v", err)
	}

	if err := st.CreateJob(ctx, db.Job{
		ID:                 jobID,
		Name:               jobID,
		SourceConnectionID: "src",
		TargetConnectionID: "tgt",
		SourceSQL:          "select 1",
		TargetNamespace:    "ns",
		TargetTable:        "tbl",
		WriteMode:          "append",
		OptionsJSON: mustJSONRaw(t, map[string]any{
			"table":              "SalesDB.dbo.Orders",
			"partition_strategy": "ordered_cursor",
			"iceberg_registration": map[string]any{
				"enabled": true,
				"engine":  "rest-go",
				"table":   "mssql.orders",
			},
		}),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, db.Run{
		ID:                     runID,
		JobID:                  jobID,
		Status:                 "RUNNING",
		CorrelationID:          "corr-" + runID,
		StartedAt:              time.Now().UTC().Format(time.RFC3339Nano),
		RegistrationConfigJSON: append(json.RawMessage(nil), registrationConfig...),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{
		ID:            taskID,
		RunID:         runID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
}

func createGRPCTestQueryRegistrableRunAndTask(t *testing.T, st *db.Store, runID, jobID, taskID, query, queryHash string) {
	t.Helper()
	ctx := context.Background()

	srcSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"dsn":"postgres://app:pass@example:5432/app?sslmode=disable"}`), []byte("src-query"))
	if err != nil {
		t.Fatalf("encrypt source secret: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:            "src-query",
		Name:          "src-query",
		Kind:          "source",
		Engine:        "postgres",
		MetadataJSON:  mustJSONRaw(t, map[string]any{}),
		SecretEncBlob: srcSecret,
	}); err != nil {
		t.Fatalf("create source connection: %v", err)
	}

	tgtSecret, err := crypto.Encrypt(crypto.Key{}, []byte(`{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}`), []byte("tgt-query"))
	if err != nil {
		t.Fatalf("encrypt target secret: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{
		ID:     "tgt-query",
		Name:   "tgt-query",
		Kind:   "target",
		Engine: "s3",
		MetadataJSON: mustJSONRaw(t, map[string]any{
			"endpoint":         "http://minio:9000",
			"region":           "us-east-1",
			"bucket":           "bucket1",
			"prefix":           "oproject/pg_sql1",
			"force_path_style": true,
		}),
		SecretEncBlob: tgtSecret,
	}); err != nil {
		t.Fatalf("create target connection: %v", err)
	}

	if err := st.CreateJob(ctx, db.Job{
		ID:                 jobID,
		Name:               jobID,
		SourceConnectionID: "src-query",
		TargetConnectionID: "tgt-query",
		SourceSQL:          query,
		TargetNamespace:    "oproject",
		TargetTable:        "pg_sql1",
		WriteMode:          "append",
		Incremental:        true,
		OptionsJSON: mustJSONRaw(t, map[string]any{
			"source_mode":        "query",
			"source_name":        "query_" + queryHash,
			"query":              query,
			"query_hash":         queryHash,
			"partition_strategy": "ordered_cursor",
			"cursor_column":      "id",
			"id_column":          "id",
			"iceberg_registration": map[string]any{
				"enabled": true,
				"engine":  "rest-go",
				"table":   "oproject.pg_sql1",
			},
		}),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	if err := st.CreateRun(ctx, db.Run{
		ID:            runID,
		JobID:         jobID,
		Status:        "RUNNING",
		CorrelationID: "corr-" + runID,
		StartedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		RegistrationConfigJSON: mustJSONRaw(t, map[string]any{
			"enabled": true,
			"engine":  "rest-go",
			"table":   "oproject.pg_sql1",
			"uri":     "http://catalog:8181",
			"s3": map[string]any{
				"endpoint":          "http://minio:9000",
				"region":            "us-east-1",
				"path_style_access": true,
				"access_key_id":     "minioadmin",
				"secret_access_key": "minioadmin",
			},
		}),
	}); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []db.TaskInsert{{
		ID:            taskID,
		RunID:         runID,
		TaskIndex:     1,
		PartitionSpec: []byte(`{"type":"single"}`),
		Status:        "PENDING",
	}}); err != nil {
		t.Fatalf("insert task: %v", err)
	}
}

type fakeIcebergRegistrar struct {
	t         *testing.T
	committed <-chan struct{}
	reqCh     chan icebergreg.RunRequest
	result    icebergreg.RunResult
}

type noOpLifecycleRegistrar struct {
	called chan<- struct{}
}

func (f noOpLifecycleRegistrar) RegisterRun(_ context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	if !req.ExactArtifactSetVerified {
		return icebergreg.RunResult{}, fmt.Errorf("expected verified artifact set")
	}
	raw, err := req.CatalogReceiptFactory()
	if err != nil {
		return icebergreg.RunResult{}, err
	}
	receipt, err := icebergreg.ParseCatalogReceipt(raw)
	if err != nil {
		return icebergreg.RunResult{}, err
	}
	receipt.NoOp = true
	receipt.NoOpReason = "ALL_ARTIFACTS_ALREADY_APPLIED"
	receipt.NoOpEvidenceDigest = strings.Repeat("c", 64)
	body, err := receipt.MarshalDeterministic()
	if err != nil {
		return icebergreg.RunResult{}, err
	}
	if err := req.CatalogNoOp(string(body)); err != nil {
		return icebergreg.RunResult{}, err
	}
	if err := req.IceStateWriting(); err != nil {
		return icebergreg.RunResult{}, err
	}
	f.called <- struct{}{}
	return icebergreg.RunResult{}, nil
}

func (f *fakeIcebergRegistrar) RegisterRun(_ context.Context, req icebergreg.RunRequest) (icebergreg.RunResult, error) {
	f.t.Helper()
	select {
	case <-f.committed:
	default:
		f.t.Fatal("iceberg registration ran before commit completed")
	}
	f.reqCh <- req
	return f.result, nil
}
