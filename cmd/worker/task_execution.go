package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/workerworkspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type taskCanceledError struct {
	reason string
}

func (e *taskCanceledError) Error() string {
	if e == nil || strings.TrimSpace(e.reason) == "" {
		return "task canceled"
	}
	return e.reason
}

func asTaskCanceledError(err error) (*taskCanceledError, bool) {
	var out *taskCanceledError
	if errors.As(err, &out) {
		return out, true
	}
	return nil, false
}

func taskCanceledErrorFromRPC(err error) error {
	if status.Code(err) != codes.Canceled {
		return err
	}
	reason := strings.TrimSpace(status.Convert(err).Message())
	if reason == "" {
		reason = "task canceled"
	}
	return &taskCanceledError{reason: reason}
}

func reportProgressBestEffort(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskProgressRequest) error {
	// Progress is optional; avoid failing the task for transient master/DB contention.
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	_, err := cp.ReportTaskProgress(callCtx, req)
	cancel()
	if err != nil {
		if cancelErr, ok := asTaskCanceledError(taskCanceledErrorFromRPC(err)); ok {
			return cancelErr
		}
		log.Debug("progress report dropped", slog.String("err", err.Error()))
	}
	return nil
}

func reportArtifactFailureBestEffort(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, failure *artifact.Failure) {
	if failure == nil || t == nil {
		return
	}
	fields, _ := json.Marshal(map[string]any{"artifact_failure": map[string]any{
		"classification": failure.Classification, "attempt_id": t.AttemptId, "attempt_number": t.AttemptNumber,
		"worker_id": workerID, "file_index": failure.FileIndex, "object_key": failure.ObjectKey,
		"verification_method": failure.VerificationMethod, "retryable": failure.Retryable,
		"ambiguous": failure.Ambiguous, "reconciliation_allowed": failure.ReconciliationOK,
	}})
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err := cp.ReportTaskProgress(callCtx, &grpcpb.ReportTaskProgressRequest{WorkerId: workerID, TaskId: t.TaskId, RunId: t.RunId, AttemptId: t.AttemptId, FencingToken: t.FencingToken, FieldsJson: string(fields)})
	if err != nil {
		log.Warn("artifact failure event not persisted", slog.String("task_id", t.TaskId), slog.String("failure_class", string(failure.Classification)), slog.String("err", err.Error()))
	}
}

func reportProgressWithRetry(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, req *grpcpb.ReportTaskProgressRequest) error {
	backoff := 100 * time.Millisecond
	deadline := time.Now().Add(15 * time.Second)
	for {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err := cp.ReportTaskProgress(callCtx, req)
		cancel()
		if err == nil {
			return nil
		}
		if cancelErr, ok := asTaskCanceledError(taskCanceledErrorFromRPC(err)); ok {
			return cancelErr
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		code := status.Code(err)
		retryable := isBusyRPC(err) || code == codes.Unavailable || code == codes.DeadlineExceeded
		if !retryable {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		log.Debug("progress report retry", slog.String("err", err.Error()), slog.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
}

func checkTaskCancellation(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, rowsRead, bytesRead, bytesWritten int64) error {
	return reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
		WorkerId:          workerID,
		TaskId:            t.TaskId,
		RunId:             t.RunId,
		AttemptId:         t.AttemptId,
		FencingToken:      t.FencingToken,
		RowsRead:          rowsRead,
		BytesRead:         bytesRead,
		BytesWritten:      bytesWritten,
		UncompressedBytes: bytesWritten, // Fallback if logical bytes not explicitly passed
	})
}

type taskOwnershipLostError struct{ err error }

func (e *taskOwnershipLostError) Error() string { return "task ownership lost: " + e.err.Error() }

func executeTaskManaged(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID, workerInstanceID string, t *grpcpb.TaskAssignment, clients *clientCache, manager *workerworkspace.Manager) error {
	workspace, err := manager.Create(t.RunId, t.TaskId, t.AttemptId, t.AttemptNumber, workerID, workerInstanceID)
	if err != nil {
		return err
	}
	log.Info("workspace created", slog.String("task_id", t.TaskId), slog.String("attempt_id", t.AttemptId))
	err = executeTaskWithBody(ctx, cp, workerID, t, realLeaseClock{}, func(taskCtx context.Context) error {
		taskCtx = withWorkspaceDir(taskCtx, workspace.Path)
		creds, err := fetchTaskCredentials(taskCtx, cp, workerID, t)
		if err != nil {
			return err
		}
		return executeTaskBody(withTaskCredentials(taskCtx, creds), log, cp, workerID, t, clients)
	})
	state := "COMPLETED"
	if err != nil {
		state = "FAILED"
		var ownershipLost *taskOwnershipLostError
		if errors.As(err, &ownershipLost) {
			state = "OWNERSHIP_LOST"
		} else if _, ok := asTaskCanceledError(err); ok {
			state = "CANCELED"
		}
	}
	if cleanupErr := workspace.Cleanup(state); cleanupErr != nil {
		log.Warn("workspace cleanup deferred to scavenger", slog.String("task_id", t.TaskId), slog.String("attempt_id", t.AttemptId), slog.String("err", cleanupErr.Error()))
		if err == nil {
			return cleanupErr
		}
	}
	return err
}

func executeTaskWithBody(ctx context.Context, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, clock leaseClock, body func(context.Context) error) error {
	if t.AttemptId == "" || t.FencingToken == "" {
		return &taskOwnershipLostError{err: errors.New("missing fenced attempt credentials")}
	}
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	lost := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		maintainTaskLeaseWithClock(taskCtx, cp, workerID, t, cancel, lost, clock)
	}()
	err := body(taskCtx)
	cancel()
	<-done
	select {
	case leaseErr := <-lost:
		return &taskOwnershipLostError{err: leaseErr}
	default:
		return err
	}
}

func executeTaskBody(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, clients *clientCache) error {
	taskStart := time.Now()

	// Parse partition spec.
	var ps partitionSpec
	if strings.TrimSpace(t.PartitionSpecJson) != "" {
		if err := json.Unmarshal([]byte(t.PartitionSpecJson), &ps); err != nil {
			return fmt.Errorf("parse partition_spec_json: %w", err)
		}
	}
	ps = normalizePartitionSpec(ps)
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, 0, 0, 0); err != nil {
		return err
	}

	engine := connectors.NormalizeSourceEngine(t.SourceEngine)
	var extracted sourceExtract
	var err error
	switch {
	case connectors.SupportsDocumentReader(engine):
		extracted, err = extractDocumentTask(ctx, log, cp, workerID, t, ps, clients, engine)
	case connectors.SupportsOrderedCursor(engine):
		extracted, err = extractSQLCursorTask(ctx, log, cp, workerID, t, ps, clients, engine)
	case engine == "flightsql":
		extracted, err = extractFlightSQLTask(ctx, log, cp, workerID, t, ps, clients)
	default:
		return fmt.Errorf("unsupported source_engine %q", t.SourceEngine)
	}
	if err != nil {
		return err
	}

	if len(extracted.ParquetFiles) == 0 {
		// No rows in this partition. Emit a bench event so CLI can still print totals.
		{
			fields := map[string]any{
				"bench": map[string]any{
					"db_connect_ms":     extracted.DBConnectMS,
					"query_ms":          extracted.QueryMS,
					"s3_init_ms":        int64(0),
					"convert_ms":        extracted.ConvertMS,
					"parquet_close_ms":  extracted.ParquetCloseMS,
					"parquet_meta_ms":   int64(0),
					"minio_upload_ms":   int64(0),
					"task_total_ms":     time.Since(taskStart).Milliseconds(),
					"rows":              extracted.Rows,
					"parquet_bytes":     int64(0),
					"parquet_files":     int64(0),
					"target_file_bytes": t.TargetFileBytes,
					"cursor_domain":     extracted.CursorDomain,
					"partition_lower":   extracted.PartitionLower,
					"partition_upper":   extracted.PartitionUpper,
					"output_part":       extracted.OutputPart,
					"query_hash":        ps.QueryHash,
					"upload_skipped":    true,
				},
			}
			b, _ := json.Marshal(fields)
			if err := reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     extracted.Rows,
				FieldsJson:   string(b),
			}); err != nil {
				if cancelErr, ok := asTaskCanceledError(err); ok {
					return cancelErr
				}
				log.Warn("task benchmark progress not persisted", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
			}
		}
		if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, 0); err != nil {
			return err
		}
		return reportResultWithRetry(ctx, log, cp, &grpcpb.ReportTaskResultRequest{
			WorkerId:          workerID,
			TaskId:            t.TaskId,
			RunId:             t.RunId,
			AttemptId:         t.AttemptId,
			FencingToken:      t.FencingToken,
			Status:            "SUCCEEDED",
			RowsRead:          extracted.Rows,
			BytesRead:         0,
			BytesWritten:      0,
			UncompressedBytes: 0,
		})
	}
	defer func() {
		for _, f := range extracted.ParquetFiles {
			if strings.TrimSpace(f.Path) != "" {
				_ = os.Remove(f.Path)
			}
		}
	}()

	s3Cfg := s3io.Config{
		Endpoint:       t.S3Endpoint,
		Region:         t.S3Region,
		Bucket:         t.S3Bucket,
		ForcePathStyle: t.S3ForcePathStyle,
		Credentials:    taskCredentialsFromContext(ctx).S3,
	}
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, extracted.ParquetBytes); err != nil {
		return err
	}
	u, s3InitMS, err := clients.S3(ctx, s3Cfg)
	if err != nil {
		return fmt.Errorf("init s3: %w", err)
	}

	datasetPrefix := strings.TrimSuffix(strings.TrimSpace(t.S3Prefix), "/")
	partNo := extracted.OutputPart
	if partNo <= 0 {
		partNo = int64(t.TaskIndex)
	}

	// Upload objects under a run-scoped prefix. This avoids costly promote/copy on commit and prevents collisions.
	runPrefix := buildAttemptRunPrefix(datasetPrefix, t.RunId, t.TaskId, t.AttemptId)
	objectKeys := buildTaskParquetObjectKeys(runPrefix, partNo, len(extracted.ParquetFiles))
	if len(objectKeys) != len(extracted.ParquetFiles) {
		return fmt.Errorf("build object keys: got %d keys for %d parquet files", len(objectKeys), len(extracted.ParquetFiles))
	}
	artifactRecords := make([]*grpcpb.ArtifactIntegrity, len(extracted.ParquetFiles))
	for i, pf := range extracted.ParquetFiles {
		artifactRecords[i] = &grpcpb.ArtifactIntegrity{ObjectKey: objectKeys[i], ByteSize: pf.Bytes, Sha256: pf.SHA256, RowCount: pf.Rows, SchemaFingerprint: pf.SchemaFingerprint, RunId: t.RunId, TaskId: t.TaskId, AttemptId: t.AttemptId, AttemptNumber: t.AttemptNumber, FileIndex: int32(i), FormatVersion: artifact.FormatVersion, VerificationMethod: artifact.VerificationPortable, VerificationStatus: artifact.VerificationVerified, MaxHwm: extracted.MaxCursor}
	}

	var (
		uploadMS      int64
		uploadSkipped = true
	)

	uploadCtx, releaseUploadCapacity, err := holdUploadCapacity(ctx, cp, workerID, t)
	if err != nil {
		return err
	}
	uploadCapacityReleased := false
	defer func() {
		if !uploadCapacityReleased {
			_ = releaseUploadCapacity()
		}
	}()
	uploadStart := time.Now()
	var wg sync.WaitGroup
	errCh := make(chan error, len(extracted.ParquetFiles))
	skipCh := make(chan bool, len(extracted.ParquetFiles))

	for i, pf := range extracted.ParquetFiles {
		if err := uploadCtx.Err(); err != nil {
			guardErr := releaseUploadCapacity()
			uploadCapacityReleased = true
			if guardErr != nil {
				return guardErr
			}
			return err
		}
		wg.Add(1)
		go func(idx int, path string) {
			defer wg.Done()
			record := artifactRecords[idx]
			meta := map[string]string{
				"run_id":              t.RunId,
				"task_id":             t.TaskId,
				"attempt_id":          t.AttemptId,
				"attempt_number":      fmt.Sprintf("%d", t.AttemptNumber),
				"worker_id":           workerID,
				"part":                fmt.Sprintf("%06d", partNo),
				"file_index":          fmt.Sprintf("%03d", idx),
				"byte_size":           fmt.Sprintf("%d", record.ByteSize),
				"sha256":              record.Sha256,
				"row_count":           fmt.Sprintf("%d", record.RowCount),
				"schema_fingerprint":  record.SchemaFingerprint,
				"format_version":      fmt.Sprintf("%d", record.FormatVersion),
				"verification_method": record.VerificationMethod,
			}
			multipartObserver := func(eventCtx context.Context, event s3io.MultipartEvent) error {
				_, err := cp.ReportMultipartLifecycle(
					eventCtx,
					&grpcpb.ReportMultipartLifecycleRequest{
						WorkerId:         workerID,
						RunId:            t.RunId,
						TaskId:           t.TaskId,
						AttemptId:        t.AttemptId,
						FencingToken:     t.FencingToken,
						Event:            event.Event,
						FileIndex:        int32(event.FileIndex),
						ObjectKey:        event.ObjectKey,
						ProviderUploadId: event.ProviderUploadID,
						Sha256:           event.SHA256,
						Size:             event.Size,
						ErrorClass:       event.ErrorClass,
						ErrorMessage:     event.ErrorMessage,
					},
				)
				return taskCanceledErrorFromRPC(err)
			}
			upRes, err := u.UploadFileVerifiedTracked(uploadCtx, objectKeys[idx], path, meta, record.ByteSize, record.Sha256, idx, multipartObserver)
			if err != nil {
				if failure, ok := artifact.AsFailure(err); ok {
					failure.FileIndex = idx
				}
				errCh <- fmt.Errorf("upload parquet file %d: %w", idx, err)
				return
			}
			if upRes.VerificationMethod != "" {
				record.VerificationMethod = upRes.VerificationMethod
			}
			record.ProviderChecksumSha256 = upRes.ProviderChecksumSHA256
			skipCh <- upRes.Skipped
		}(i, pf.Path)
	}

	wg.Wait()
	close(errCh)
	close(skipCh)

	var uploadErr error
	for err := range errCh {
		if err != nil && uploadErr == nil {
			uploadErr = err
		}
	}
	for skipped := range skipCh {
		uploadSkipped = uploadSkipped && skipped
	}
	if err := releaseUploadCapacity(); err != nil {
		return err
	}
	uploadCapacityReleased = true
	if uploadErr != nil {
		return uploadErr
	}
	uploadMS = time.Since(uploadStart).Milliseconds()

	minFileBytes, maxFileBytes, avgFileBytes := parquetFileSizeStats(extracted.ParquetFiles)

	// Emit a single benchmark event for the task (no message so it does not spam SSE output).
	// The CLI aggregates these at the end of `orabbit-client run`.
	{
		fields := map[string]any{
			"bench": map[string]any{
				"db_connect_ms":     extracted.DBConnectMS,
				"query_ms":          extracted.QueryMS,
				"s3_init_ms":        s3InitMS,
				"convert_ms":        extracted.ConvertMS,
				"parquet_close_ms":  extracted.ParquetCloseMS,
				"parquet_meta_ms":   int64(0),
				"minio_upload_ms":   uploadMS,
				"task_total_ms":     time.Since(taskStart).Milliseconds(),
				"rows":              extracted.Rows,
				"parquet_bytes":     extracted.ParquetBytes,
				"parquet_files":     len(extracted.ParquetFiles),
				"target_file_bytes": t.TargetFileBytes,
				"min_file_bytes":    minFileBytes,
				"max_file_bytes":    maxFileBytes,
				"avg_file_bytes":    avgFileBytes,
				"partition_keys":    t.PartitionKeys,
				"cursor_domain":     extracted.CursorDomain,
				"partition_lower":   extracted.PartitionLower,
				"partition_upper":   extracted.PartitionUpper,
				"output_part":       partNo,
				"query_hash":        ps.QueryHash,
				"upload_skipped":    uploadSkipped,
			},
		}
		b, _ := json.Marshal(fields)
		if err := reportProgressWithRetry(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
			WorkerId:          workerID,
			TaskId:            t.TaskId,
			RunId:             t.RunId,
			AttemptId:         t.AttemptId,
			FencingToken:      t.FencingToken,
			RowsRead:          extracted.Rows,
			BytesWritten:      extracted.ParquetBytes,
			UncompressedBytes: extracted.LogicalBytes,
			FieldsJson:        string(b),
		}); err != nil {
			if cancelErr, ok := asTaskCanceledError(err); ok {
				return cancelErr
			}
			log.Warn("task benchmark progress not persisted", slog.String("task_id", t.TaskId), slog.String("err", err.Error()))
		}
	}
	if err := checkTaskCancellation(ctx, log, cp, workerID, t, extracted.Rows, 0, extracted.ParquetBytes); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Report result.
	err = reportResultWithRetry(ctx, log, cp, &grpcpb.ReportTaskResultRequest{
		WorkerId:          workerID,
		TaskId:            t.TaskId,
		RunId:             t.RunId,
		AttemptId:         t.AttemptId,
		FencingToken:      t.FencingToken,
		Status:            "SUCCEEDED",
		RowsRead:          extracted.Rows,
		BytesRead:         0,
		BytesWritten:      extracted.ParquetBytes,
		UncompressedBytes: extracted.LogicalBytes,
		ParquetObjectKeys: objectKeys,
		Artifacts:         artifactRecords,
		MaxHwmValue:       extracted.MaxCursor,
	})
	if err != nil {
		return err
	}

	log.Info("task done",
		slog.String("task_id", t.TaskId),
		slog.Int64("rows", extracted.Rows),
		slog.Int64("bytes", extracted.ParquetBytes),
		slog.Int("parquet_files", len(extracted.ParquetFiles)),
		slog.Bool("upload_skipped", uploadSkipped),
		slog.String("first_object_key", objectKeys[0]),
	)
	return nil
}

func buildAttemptRunPrefix(datasetPrefix, runID, _, _ string) string {
	return strings.TrimSuffix(datasetPrefix, "/") + "/_runs/run-" + runID
}
