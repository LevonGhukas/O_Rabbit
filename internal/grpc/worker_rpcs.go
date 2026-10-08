package grpcapi

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// buildParquetObjectPayloads constructs the task's Parquet object metadata in one pass.
// rows and bytes remain task totals copied onto each object, not per-object metrics.
func buildParquetObjectPayloads(keys []string, maxHWM string, rowsRead, bytesWritten int64) []map[string]any {
	objs := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		obj := map[string]any{"key": k}
		if maxHWM != "" {
			obj["max_hwm"] = maxHWM
		}
		if rowsRead != 0 {
			obj["rows"] = rowsRead
		}
		if bytesWritten != 0 {
			obj["bytes"] = bytesWritten
		}
		objs = append(objs, obj)
	}
	return objs
}

func (s *Server) ExpireLeases(ctx context.Context) (int, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return 0, err
	}
	return s.st.ExpireTaskAttempts(ctx, s.nowFn(), s.leasePolicy)
}

// RegisterWorker is idempotent and can be called multiple times by the same worker (e.g. on restart) or different workers (e.g. for autoscaling).
func (s *Server) RegisterWorker(ctx context.Context, req *grpcpb.RegisterWorkerRequest) (*grpcpb.RegisterWorkerResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	id := strings.TrimSpace(req.WorkerId)
	if id == "" {
		id = newID()
	}
	if err := s.st.UpdateWorkerHeartbeat(ctx, req.BootId, id, req.Addr, req.CapabilitiesJson, req.Hostname, req.Version, int(req.Pid)); err != nil {
		return nil, err
	}
	return &grpcpb.RegisterWorkerResponse{WorkerId: id, HeartbeatIntervalMs: s.hbInterval.Milliseconds()}, nil
}

// Heartbeat is best-effort and does not return an error if the worker is not found (e.g. first heartbeat before registration or after cleanup).
func (s *Server) Heartbeat(ctx context.Context, req *grpcpb.HeartbeatRequest) (*grpcpb.HeartbeatResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	// Best-effort: liveness only.
	_ = s.st.TouchWorkerHeartbeat(ctx, req.BootId, req.WorkerId)
	return &grpcpb.HeartbeatResponse{}, nil
}

// RequestTask returns at most one pending task assignment for the worker, if available.
// It is expected that workers call this in a loop (e.g. with a short sleep on empty response) to continuously process tasks.
func (s *Server) RequestTask(ctx context.Context, req *grpcpb.RequestTaskRequest) (*grpcpb.RequestTaskResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if req.ProtocolVersion != workerProtocolVersion {
		return nil, grpcstatus.Errorf(codes.FailedPrecondition, "worker protocol version %d is unsupported; accepted version=%d (exact match required)", req.ProtocolVersion, workerProtocolVersion)
	}
	// Best-effort: update heartbeat/capabilities.
	_ = s.st.UpdateWorkerHeartbeat(ctx, req.BootId, req.WorkerId, "", req.CapabilitiesJson, "", "", 0)
	if _, err := s.st.AdmitPendingRuns(ctx); err != nil {
		return nil, err
	}

	// Authenticated workers only receive tasks, and so credentials, of jobs in
	// their own pool. Unauthenticated development workers are unrestricted.
	pool := ""
	if worker, ok := authenticatedWorkerFrom(ctx); ok {
		pool = worker.Pool
	}
	t, ok, err := s.st.AssignNextPendingTaskInPool(ctx, req.BootId, req.WorkerId, pool, s.nowFn(), s.leasePolicy, s.attemptIDFn, s.fencingTokenFn)
	if err != nil {
		return nil, err
	}
	if !ok {
		return &grpcpb.RequestTaskResponse{Task: &grpcpb.TaskAssignment{}}, nil
	}

	assignment, err := s.buildAssignment(ctx, t)
	if err != nil {
		if requeueErr := s.st.AbandonTaskAttemptWithPolicy(ctx, t.ID, t.AttemptID, req.WorkerId, err.Error(), s.nowFn(), s.leasePolicy); requeueErr != nil {
			s.log.Warn("failed to requeue task after assignment build error",
				slog.String("task_id", t.ID),
				slog.String("worker_id", req.WorkerId),
				slog.String("err", requeueErr.Error()),
			)
		}
		return nil, err
	}
	return &grpcpb.RequestTaskResponse{Task: assignment}, nil
}

func (s *Server) RenewTaskLease(ctx context.Context, req *grpcpb.RenewTaskLeaseRequest) (*grpcpb.RenewTaskLeaseResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	deadline, err := s.st.RenewTaskLease(ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId, s.nowFn(), s.leasePolicy.Duration)
	if err != nil {
		if db.IsAttemptFenced(err) {
			s.recordAttemptRejection(ctx, req.TaskId, req.AttemptId, req.WorkerId, "STALE_RENEWAL_REJECTED", "OWNERSHIP_FENCED")
			return nil, grpcstatus.Error(codes.FailedPrecondition, "task ownership lost")
		}
		return nil, err
	}
	tm, err := time.Parse(time.RFC3339Nano, deadline)
	if err != nil {
		return nil, err
	}
	return &grpcpb.RenewTaskLeaseResponse{LeaseDeadlineUnixMs: tm.UnixMilli()}, nil
}

func (s *Server) AcquireUploadCapacity(ctx context.Context, req *grpcpb.AcquireUploadCapacityRequest) (*grpcpb.AcquireUploadCapacityResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	lease, acquired, err := s.st.AcquireUploadCapacity(ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId, s.nowFn(), s.uploadCapacityLeaseTTL, s.uploadCapacityLimit, nil, nil)
	if err != nil {
		if db.IsUploadCapacityFenced(err) {
			return nil, grpcstatus.Error(codes.FailedPrecondition, "task ownership lost")
		}
		return nil, err
	}
	if !acquired {
		retry := s.uploadCapacityLeaseTTL / 10
		if retry > time.Second {
			retry = time.Second
		}
		if retry < 100*time.Millisecond {
			retry = 100 * time.Millisecond
		}
		return &grpcpb.AcquireUploadCapacityResponse{RetryAfterMs: retry.Milliseconds()}, nil
	}
	deadline, err := time.Parse(time.RFC3339Nano, lease.LeaseDeadline)
	if err != nil {
		return nil, err
	}
	return &grpcpb.AcquireUploadCapacityResponse{Acquired: true, LeaseId: lease.ID, LeaseToken: lease.Token, LeaseDeadlineUnixMs: deadline.UnixMilli()}, nil
}

func (s *Server) ReleaseUploadCapacity(ctx context.Context, req *grpcpb.ReleaseUploadCapacityRequest) (*grpcpb.ReleaseUploadCapacityResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	err := s.st.ReleaseUploadCapacity(ctx, req.BootId, req.TaskId, req.AttemptId, req.WorkerId, req.LeaseId, req.LeaseToken, s.nowFn())
	if err != nil {
		if db.IsUploadCapacityFenced(err) {
			return nil, grpcstatus.Error(codes.FailedPrecondition, "upload capacity lease lost")
		}
		return nil, err
	}
	return &grpcpb.ReleaseUploadCapacityResponse{}, nil
}

// ReportTaskProgress is best-effort and does not return an error if the task is not found (e.g. late progress after task completion).
func (s *Server) ReportTaskProgress(ctx context.Context, req *grpcpb.ReportTaskProgressRequest) (*grpcpb.ReportTaskProgressResponse, error) {
	if len(req.Message) > maxWorkerProgressMessageBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "progress message too large")
	}

	if len(req.FieldsJson) > maxWorkerProgressFieldsBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "progress fields too large")
	}
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if strings.TrimSpace(req.AttemptId) == "" || strings.TrimSpace(req.FencingToken) == "" {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "fenced task protocol required")
	}
	eventRunID := strings.TrimSpace(req.RunId)
	if taskID := strings.TrimSpace(req.TaskId); taskID != "" {
		state, err := s.st.GetTaskExecutionState(ctx, taskID)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, grpcstatus.Error(codes.NotFound, "task not found")
			}
			return nil, err
		}
		eventRunID = state.RunID
		if state.RunStatus == "CANCELED" || state.TaskStatus == "CANCELED" {
			reason := "task canceled"
			switch {
			case state.RunError != nil && strings.TrimSpace(*state.RunError) != "":
				reason = strings.TrimSpace(*state.RunError)
			case state.TaskError != nil && strings.TrimSpace(*state.TaskError) != "":
				reason = strings.TrimSpace(*state.TaskError)
			}
			return nil, grpcstatus.Error(codes.Canceled, reason)
		}
	}

	if err := s.st.UpdateTaskProgressFencedAt(ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId, req.RowsRead, req.BytesRead, req.BytesWritten, s.nowFn()); err != nil {
		if db.IsAttemptFenced(err) {
			s.recordAttemptRejection(ctx, req.TaskId, req.AttemptId, req.WorkerId, "STALE_PROGRESS_REJECTED", "OWNERSHIP_FENCED")
			return nil, grpcstatus.Error(codes.FailedPrecondition, "task ownership lost")
		}
		return nil, err
	}

	message := strings.TrimSpace(req.Message)
	fields := strings.TrimSpace(req.FieldsJson)
	if message == "" && fields == "" {
		return &grpcpb.ReportTaskProgressResponse{}, nil
	}

	eventID, level, eventMessage := newID(), "INFO", message
	var envelope struct {
		ArtifactFailure struct {
			Classification        string `json:"classification"`
			AttemptID             string `json:"attempt_id"`
			AttemptNumber         int    `json:"attempt_number"`
			WorkerID              string `json:"worker_id"`
			FileIndex             int    `json:"file_index"`
			ObjectKey             string `json:"object_key"`
			VerificationMethod    string `json:"verification_method"`
			Retryable             bool   `json:"retryable"`
			Ambiguous             bool   `json:"ambiguous"`
			ReconciliationAllowed bool   `json:"reconciliation_allowed"`
		} `json:"artifact_failure"`
	}
	if json.Unmarshal([]byte(orJSON(fields)), &envelope) == nil && envelope.ArtifactFailure.Classification != "" {
		f := envelope.ArtifactFailure
		sum := sha256.Sum256([]byte(req.AttemptId + "\x00" + f.Classification + "\x00" + f.ObjectKey + "\x00" + strconv.Itoa(f.FileIndex)))
		eventID, level, eventMessage = "artifact-failure-"+hex.EncodeToString(sum[:16]), "ERROR", "artifact operation failed"
		sanitized, _ := json.Marshal(map[string]any{"artifact_failure": map[string]any{
			"classification": f.Classification, "attempt_id": req.AttemptId, "attempt_number": f.AttemptNumber,
			"worker_id": req.WorkerId, "file_index": f.FileIndex, "object_key": f.ObjectKey,
			"verification_method": f.VerificationMethod, "retryable": f.Retryable,
			"ambiguous": f.Ambiguous, "reconciliation_allowed": f.ReconciliationAllowed,
		}})
		fields = string(sanitized)
	}
	e := db.Event{
		ID:         eventID,
		RunID:      eventRunID,
		TS:         db.FormatTimestamp(time.Now()),
		Level:      level,
		Message:    eventMessage,
		FieldsJSON: []byte(orJSON(fields)),
	}
	if req.TaskId != "" {
		tid := req.TaskId
		e.TaskID = &tid
	}
	_ = s.st.InsertEvent(ctx, e)
	if s.bc != nil {
		s.bc.Publish(e)
	}
	return &grpcpb.ReportTaskProgressResponse{}, nil
}

func (s *Server) ReportMultipartLifecycle(
	ctx context.Context,
	req *grpcpb.ReportMultipartLifecycleRequest,
) (*grpcpb.ReportMultipartLifecycleResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if len(req.ObjectKey) > maxMultipartObjectKeyBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "multipart object key too large")
	}
	if len(req.ProviderUploadId) > maxMultipartUploadIDBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "multipart upload id too large")
	}
	if len(req.ErrorClass) > maxMultipartErrorClassBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "multipart error class too large")
	}
	if len(req.ErrorMessage) > maxMultipartErrorMessageBytes {
		return nil, grpcstatus.Error(codes.InvalidArgument, "multipart error message too large")
	}
	if strings.TrimSpace(req.AttemptId) == "" ||
		strings.TrimSpace(req.FencingToken) == "" {
		return nil, grpcstatus.Error(
			codes.FailedPrecondition,
			"fenced task protocol required",
		)
	}
	_, err := s.st.ApplyMultipartLifecycle(
		ctx,
		db.MultipartLifecycleUpdate{
			Event:        req.Event,
			RunID:        req.RunId,
			TaskID:       req.TaskId,
			AttemptID:    req.AttemptId,
			WorkerID:     req.WorkerId,
			FencingToken: req.FencingToken,
			FileIndex:    int(req.FileIndex),
			ObjectKey:    req.ObjectKey,
			UploadID:     req.ProviderUploadId,
			SHA256:       req.Sha256,
			Size:         req.Size,
			ErrorClass:   req.ErrorClass,
			ErrorMessage: req.ErrorMessage,
		},
		s.nowFn(),
	)
	if errors.Is(err, db.ErrMultipartFenced) {
		return nil, grpcstatus.Error(
			codes.FailedPrecondition,
			"multipart lifecycle ownership lost",
		)
	}
	if err != nil {
		return nil, err
	}
	return &grpcpb.ReportMultipartLifecycleResponse{}, nil
}

// ReportTaskResult updates the task status and emits a task event.
// If the task is marked as completed, it also tries to finalize the run (which may trigger a commit if all tasks are completed).
func (s *Server) ReportTaskResult(ctx context.Context, req *grpcpb.ReportTaskResultRequest) (*grpcpb.ReportTaskResultResponse, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	ctx, cancelLeadership := s.leadershipContext(ctx)
	defer cancelLeadership()
	if strings.TrimSpace(req.AttemptId) == "" || strings.TrimSpace(req.FencingToken) == "" {
		return nil, grpcstatus.Error(codes.FailedPrecondition, "fenced task protocol required")
	}
	status := strings.ToUpper(strings.TrimSpace(req.Status))
	var errMsg *string
	if strings.TrimSpace(req.ErrorMessage) != "" {
		tmp := req.ErrorMessage
		errMsg = &tmp
	}

	runID, err := s.st.GetTaskRunID(ctx, req.TaskId)
	if err != nil {
		return nil, err
	}
	if rid := strings.TrimSpace(req.RunId); rid != "" && rid != runID {
		s.log.Warn("worker reported mismatched run_id; using task-owned run_id",
			slog.String("task_id", req.TaskId),
			slog.String("reported_run_id", rid),
			slog.String("task_run_id", runID),
		)
	}

	records := make([]artifact.Record, len(req.Artifacts))
	for i, a := range req.Artifacts {
		if a == nil {
			return nil, grpcstatus.Error(codes.InvalidArgument, "nil artifact integrity record")
		}
		records[i] = artifact.Record{ObjectKey: a.ObjectKey, ByteSize: a.ByteSize, SHA256: a.Sha256, RowCount: a.RowCount, SchemaFingerprint: a.SchemaFingerprint, RunID: a.RunId, TaskID: a.TaskId, AttemptID: a.AttemptId, AttemptNumber: int(a.AttemptNumber), FileIndex: int(a.FileIndex), FormatVersion: int(a.FormatVersion), VerificationMethod: a.VerificationMethod, VerificationStatus: a.VerificationStatus, VerifiedAt: a.VerifiedAt, MaxHWM: a.MaxHwm, ProviderChecksumSHA256: a.ProviderChecksumSha256}
		if records[i].RunID != runID {
			return nil, grpcstatus.Error(codes.InvalidArgument, "artifact run identity mismatch")
		}
	}
	if len(req.ParquetObjectKeys) != len(records) {
		return nil, grpcstatus.Error(codes.InvalidArgument, "artifact integrity records must cover every object key")
	}
	for i, key := range req.ParquetObjectKeys {
		if i >= len(records) || records[i].ObjectKey != key {
			return nil, grpcstatus.Error(codes.InvalidArgument, "artifact object keys do not match result ordering")
		}
	}

	var accepted bool
	var msg, finalStatus string
	if len(records) > 0 {
		accepted, msg, finalStatus, err = s.st.CompleteTaskAttemptWithArtifactsAndFailureClassAt(
			ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId,
			status, errMsg, req.FailureClass, records, req.RowsRead, req.BytesRead, req.BytesWritten, s.nowFn())
	} else {
		b, errJSON := json.Marshal(req.ParquetObjectKeys)
		if errJSON != nil {
			return nil, errJSON
		}
		accepted, msg, finalStatus, err = s.st.CompleteTaskAttemptWithFailureClassAt(
			ctx, req.BootId, req.TaskId, req.AttemptId, req.FencingToken, req.WorkerId,
			status, errMsg, req.FailureClass, b, req.RowsRead, req.BytesRead, req.BytesWritten, s.nowFn())
	}

	if err != nil {
		if db.IsAttemptFenced(err) {
			s.recordAttemptRejection(ctx, req.TaskId, req.AttemptId, req.WorkerId, "STALE_RESULT_REJECTED", "OWNERSHIP_FENCED")
			return nil, grpcstatus.Error(codes.FailedPrecondition, "task ownership lost")
		}
		if strings.Contains(err.Error(), "result conflict") {
			s.recordAttemptRejection(ctx, req.TaskId, req.AttemptId, req.WorkerId, "CONFLICTING_RESULT_REJECTED", "RESULT_CONFLICT")
			return nil, grpcstatus.Error(codes.AlreadyExists, err.Error())
		}
		return nil, err
	}

	if msg != "already accepted" {
		telemetry.ObserveTaskResult(finalStatus, req.RowsRead, req.BytesRead, req.BytesWritten)
		// Emit exactly one logical completion event for an accepted attempt.
		tid := req.TaskId
		e := db.Event{ID: newID(), RunID: runID, TaskID: &tid, TS: db.FormatTimestamp(time.Now()), Level: "INFO", Message: fmt.Sprintf("task %s %s", req.TaskId, finalStatus), FieldsJSON: []byte(`{}`)}
		_ = s.st.InsertEvent(ctx, e)
		if s.bc != nil {
			s.bc.Publish(e)
		}
	}

	// Try to finalize the run (commit only when all tasks succeeded).
	changed, newStatus, ferr := s.st.TryFinalizeRun(ctx, runID)
	if ferr != nil {
		return nil, ferr
	}
	if changed {
		// Publishing a run (verifying artifacts, writing the manifest and
		// dataset state) can take many minutes, so it never runs inside this
		// worker's RPC. The run is now COMMITTING; a claimed committer on the
		// master's own context publishes it and emits "run committed".
		re := db.Event{ID: newID(), RunID: runID, TS: db.FormatTimestamp(time.Now()), Level: "INFO", Message: fmt.Sprintf("run %s", newStatus), FieldsJSON: []byte(`{}`)}
		_ = s.st.InsertEvent(ctx, re)
		if s.bc != nil {
			s.bc.Publish(re)
		}
		if newStatus == "COMMITTING" {
			s.launchCommit(runID)
		}
	}

	return &grpcpb.ReportTaskResultResponse{Accepted: accepted, Message: msg}, nil
}

func (s *Server) recordAttemptRejection(ctx context.Context, taskID, attemptID, workerID, eventType, classification string) {
	if err := s.st.InsertAttemptRejectionEvent(ctx, taskID, attemptID, workerID, eventType, classification, s.nowFn()); err != nil && err != sql.ErrNoRows {
		s.log.Warn("failed to persist bounded attempt rejection event", slog.String("task_id", taskID), slog.String("attempt_id", attemptID), slog.String("event_type", eventType), slog.String("err", err.Error()))
	}
}

// buildAssignment constructs a TaskAssignment message for the given task, including decrypted connection details.
func (s *Server) buildAssignment(ctx context.Context, t db.Task) (*grpcpb.TaskAssignment, error) {
	run, err := s.st.GetRun(ctx, t.RunID)
	if err != nil {
		return nil, err
	}
	// The run's configuration snapshot, not the live job, decides where and
	// how it writes. Credentials are fetched separately by the leaseholder
	// with GetTaskCredentials.
	r, err := s.resolveRun(ctx, run)
	if err != nil {
		return nil, err
	}
	leaseDeadline, err := time.Parse(time.RFC3339Nano, t.LeaseDeadline)
	if err != nil {
		return nil, fmt.Errorf("task %s lease deadline: %w", t.ID, err)
	}
	return &grpcpb.TaskAssignment{
		TaskId:              t.ID,
		RunId:               run.ID,
		JobId:               r.config.Job.ID,
		TaskIndex:           int32(t.TaskIndex),
		CorrelationId:       run.CorrelationID,
		AttemptId:           t.AttemptID,
		FencingToken:        t.FencingToken,
		AttemptNumber:       int32(t.AttemptNumber),
		LeaseDeadlineUnixMs: leaseDeadline.UnixMilli(),
		PartitionSpecJson:   string(t.PartitionSpec),
		SourceEngine:        r.config.SourceEngine,
		SourceSql:           r.config.Job.SourceSQL,
		S3Endpoint:          r.target.Endpoint,
		S3Region:            r.target.Region,
		S3Bucket:            r.target.Bucket,
		S3Prefix:            r.prefix,
		S3ForcePathStyle:    r.target.ForcePathStyle,
		TargetFileBytes:     r.opts.TargetFileBytes,
		PartitionKeys:       r.opts.PartitionKeys,
	}, nil
}
