// internal/grpc/server.go
// this file implements the gRPC control plane server for worker registration, task assignment, progress reporting, and task result reporting.

package grpcapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	httpapi "github.com/LevonGhukas/O_Rabbit/internal/http"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
)

// Config holds the gRPC server configuration.
type Config struct {
	Addr              string
	Insecure          bool
	TLSCertFile       string
	TLSKeyFile        string
	WorkerAuthToken   string
	HeartbeatInterval time.Duration
}

type icebergRegistrationRunner interface {
	RegisterRun(context.Context, icebergreg.RunRequest) (icebergreg.RunResult, error)
}

// commitObjectStore is the narrow object-storage surface required by durable
// run publication. Keeping it small also makes ambiguous responses and
// read-back failures deterministic to exercise in tests.
type commitObjectStore interface {
	Head(context.Context, string) error
	GetObjectBytes(context.Context, string) ([]byte, bool, error)
	PutObjectBytes(context.Context, string, []byte, string, map[string]string) error
	OpenObject(context.Context, string) (io.ReadCloser, bool, error)
}

type s3CommitObjectStore struct{ uploader *s3io.Uploader }

type durableCommitIntent struct {
	CommitID      string                   `json:"commit_id"`
	DatasetID     string                   `json:"dataset_id,omitempty"`
	ManifestKey   string                   `json:"manifest_key"`
	StateKey      string                   `json:"state_key"`
	Destination   durableCommitDestination `json:"destination,omitempty"`
	Manifest      json.RawMessage          `json:"manifest"`
	PreviousState json.RawMessage          `json:"previous_state"`
	ProposedState json.RawMessage          `json:"proposed_state"`
	IcebergSchema json.RawMessage          `json:"iceberg_schema,omitempty"`
}

type durableCommitDestination struct {
	Endpoint       string `json:"endpoint"`
	Region         string `json:"region,omitempty"`
	Bucket         string `json:"bucket"`
	Prefix         string `json:"prefix"`
	ForcePathStyle bool   `json:"force_path_style"`
}

func (s s3CommitObjectStore) Head(ctx context.Context, key string) error {
	_, err := s.uploader.Head(ctx, key)
	return err
}

func (s s3CommitObjectStore) GetObjectBytes(ctx context.Context, key string) ([]byte, bool, error) {
	return s.uploader.GetObjectBytes(ctx, key)
}

func (s s3CommitObjectStore) PutObjectBytes(ctx context.Context, key string, b []byte, contentType string, meta map[string]string) error {
	return s.uploader.PutObjectBytes(ctx, key, b, contentType, meta)
}

func (s s3CommitObjectStore) OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error) {
	return s.uploader.OpenObject(ctx, key)
}

// Server implements the gRPC control plane server.
type Server struct {
	grpcpb.UnimplementedControlPlaneServer

	log *slog.Logger
	st  *db.Store
	bc  *httpapi.Broadcaster
	k   crypto.Key

	hbInterval time.Duration

	icebergRegistrar           icebergRegistrationRunner
	commitRunFn                func(context.Context, string) error
	completeRunCommitFn        func(context.Context, string) error
	runIcebergRegistrationFn   func(context.Context, string) (bool, icebergreg.RunResult, error)
	newCommitObjectStoreFn     func(context.Context, s3io.Config) (commitObjectStore, error)
	upsertHWMFn                func(context.Context, string, string) error
	leasePolicy                db.LeasePolicy
	nowFn                      func() time.Time
	attemptIDFn                func() (string, error)
	fencingTokenFn             func() (string, error)
	leadership                 interface{ Assert(context.Context) error }
	multipartCleanupGrace      time.Duration
	multipartCleanupRetry      time.Duration
	multipartCleanupMax        int
	newMultipartCleanerFn      func(context.Context, s3io.Config) (multipartCleaner, error)
	canceledObjectRetry        time.Duration
	canceledObjectMax          int
	canceledObjectDryRun       bool
	newCanceledObjectCleanerFn func(context.Context, s3io.Config) (canceledObjectCleaner, error)
	catalogWorkSlots           chan struct{}
	uploadCapacityLimit        int
	uploadCapacityLeaseTTL     time.Duration
}

type multipartCleaner interface {
	VerifyTrackedFinalObject(context.Context, string, int64, string, map[string]string) (bool, error)
	ListManagedMultipartUploads(context.Context, string, int) ([]s3io.MultipartUploadInfo, error)
	AbortTrackedMultipart(context.Context, string, string) error
	MultipartUploadExists(context.Context, string, string, string) (bool, error)
}

type canceledObjectCleaner interface {
	ObserveExactObject(context.Context, string, int64, string, map[string]string) (s3io.ExactObjectObservation, error)
	DeleteExactObject(context.Context, string, string) error
}

const workerProtocolVersion = 5

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

// NewServer creates a new gRPC server instance.
func NewServer(log *slog.Logger, st *db.Store, bc *httpapi.Broadcaster, k crypto.Key, hbInterval time.Duration, registrar icebergRegistrationRunner) *Server {
	if log == nil {
		log = slog.Default()
	}
	if hbInterval <= 0 {
		hbInterval = 5 * time.Second
	}
	s := &Server{
		log:              log,
		st:               st,
		bc:               bc,
		k:                k,
		hbInterval:       hbInterval,
		icebergRegistrar: registrar,
	}
	s.commitRunFn = s.commitRun
	s.completeRunCommitFn = st.CompleteRunCommit
	s.runIcebergRegistrationFn = s.runIcebergRegistration
	s.newCommitObjectStoreFn = func(ctx context.Context, cfg s3io.Config) (commitObjectStore, error) {
		u, err := s3io.New(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return s3CommitObjectStore{uploader: u}, nil
	}
	s.upsertHWMFn = st.UpsertHWM
	s.leasePolicy = db.LeasePolicy{Duration: 30 * time.Second, MaxAttempts: 3, BackoffBase: time.Second, BackoffMax: 30 * time.Second}
	s.nowFn = time.Now
	s.multipartCleanupGrace, s.multipartCleanupRetry, s.multipartCleanupMax = 15*time.Minute, time.Minute, 5
	s.newMultipartCleanerFn = func(ctx context.Context, cfg s3io.Config) (multipartCleaner, error) {
		return s3io.New(ctx, cfg)
	}
	s.canceledObjectRetry, s.canceledObjectMax, s.canceledObjectDryRun = 5*time.Minute, 5, true
	s.catalogWorkSlots = make(chan struct{}, 2)
	s.uploadCapacityLimit, s.uploadCapacityLeaseTTL = 8, 2*time.Minute
	s.newCanceledObjectCleanerFn = func(ctx context.Context, cfg s3io.Config) (canceledObjectCleaner, error) {
		return s3io.New(ctx, cfg)
	}
	return s
}

func (s *Server) ExpireLeases(ctx context.Context) (int, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return 0, err
	}
	return s.st.ExpireTaskAttempts(ctx, s.nowFn(), s.leasePolicy)
}

func (s *Server) SetLeasePolicy(policy db.LeasePolicy) { s.leasePolicy = policy }
func (s *Server) SetUploadCapacityPolicy(limit int, ttl time.Duration) {
	if limit > 0 {
		s.uploadCapacityLimit = limit
	}
	if ttl > 0 {
		s.uploadCapacityLeaseTTL = ttl
	}
}
func (s *Server) SetLeadershipGuard(guard interface{ Assert(context.Context) error }) {
	s.leadership = guard
}
func (s *Server) SetMultipartCleanupPolicy(grace, retry time.Duration, max int) {
	if grace > 0 {
		s.multipartCleanupGrace = grace
	}
	if retry > 0 {
		s.multipartCleanupRetry = retry
	}
	if max > 0 {
		s.multipartCleanupMax = max
	}
}
func (s *Server) SetCanceledObjectCleanupPolicy(retry time.Duration, max int, dryRun bool) {
	if retry > 0 {
		s.canceledObjectRetry = retry
	}
	if max > 0 {
		s.canceledObjectMax = max
	}
	s.canceledObjectDryRun = dryRun
}
func (s *Server) requireLeadership(ctx context.Context) error {
	if s.leadership == nil {
		return nil
	}
	if err := s.leadership.Assert(ctx); err != nil {
		return grpcstatus.Error(codes.Unavailable, "master is not the active leader")
	}
	return nil
}

func (s *Server) leadershipContext(ctx context.Context) (context.Context, context.CancelFunc) {
	leader, ok := s.leadership.(interface{ WorkContext() context.Context })
	if !ok || leader.WorkContext() == nil {
		return context.WithCancel(ctx)
	}
	combined, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(leader.WorkContext(), cancel)
	return combined, func() {
		stop()
		cancel()
	}
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

	t, ok, err := s.st.AssignNextPendingTaskWithLease(ctx, req.BootId, req.WorkerId, s.nowFn(), s.leasePolicy, s.attemptIDFn, s.fencingTokenFn)
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
	if err := s.requireLeadership(ctx); err != nil {
		return nil, err
	}
	if req.Message == "MULTIPART_LIFECYCLE" {
		var lifecycle struct {
			Event        string `json:"event"`
			FileIndex    int    `json:"file_index"`
			ObjectKey    string `json:"object_key"`
			UploadID     string `json:"provider_upload_id"`
			SHA256       string `json:"sha256"`
			Size         int64  `json:"size"`
			ErrorClass   string `json:"error_class"`
			ErrorMessage string `json:"error_message"`
		}
		if err := json.Unmarshal([]byte(req.FieldsJson), &lifecycle); err != nil {
			return nil, grpcstatus.Error(codes.InvalidArgument, "invalid multipart lifecycle payload")
		}
		_, err := s.st.ApplyMultipartLifecycle(ctx, db.MultipartLifecycleUpdate{Event: lifecycle.Event, RunID: req.RunId, TaskID: req.TaskId, AttemptID: req.AttemptId, WorkerID: req.WorkerId, FencingToken: req.FencingToken, FileIndex: lifecycle.FileIndex, ObjectKey: lifecycle.ObjectKey, UploadID: lifecycle.UploadID, SHA256: lifecycle.SHA256, Size: lifecycle.Size, ErrorClass: lifecycle.ErrorClass, ErrorMessage: lifecycle.ErrorMessage}, s.nowFn())
		if errors.Is(err, db.ErrMultipartFenced) {
			return nil, grpcstatus.Error(codes.FailedPrecondition, "multipart lifecycle ownership lost")
		}
		if err != nil {
			return nil, err
		}
		return &grpcpb.ReportTaskProgressResponse{}, nil
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
		TS:         time.Now().UTC().Format(time.RFC3339Nano),
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
		records[i] = artifact.Record{ObjectKey: a.ObjectKey, ByteSize: a.ByteSize, SHA256: a.Sha256, RowCount: a.RowCount, SchemaFingerprint: a.SchemaFingerprint, RunID: a.RunId, TaskID: a.TaskId, AttemptID: a.AttemptId, AttemptNumber: int(a.AttemptNumber), FileIndex: int(a.FileIndex), FormatVersion: int(a.FormatVersion), VerificationMethod: a.VerificationMethod, VerificationStatus: a.VerificationStatus, VerifiedAt: a.VerifiedAt, MaxHWM: a.MaxHwm}
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
		// Emit exactly one logical completion event for an accepted attempt.
		tid := req.TaskId
		e := db.Event{ID: newID(), RunID: runID, TaskID: &tid, TS: time.Now().UTC().Format(time.RFC3339Nano), Level: "INFO", Message: fmt.Sprintf("task %s %s", req.TaskId, finalStatus), FieldsJSON: []byte(`{}`)}
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
		launchIcebergRegistration := false
		emitRunStatusEvent := true
		// NOTE: when a run reaches the SUCCEEDED state, it is not fully finished until we commit it:
		// promote staged objects into their final keys and write <prefix>/_state.json.
		//
		// We publish "run SUCCEEDED" only after commitRun succeeds so clients (and the CLI) can safely
		// proceed immediately to downstream steps (like Iceberg insert) without racing _state.json.
		if newStatus == "COMMITTING" {
			commitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
			defer cancel()
			if err := s.finalizeRunCommit(commitCtx, runID); err != nil {
				msg := err.Error()

				fields, _ := json.Marshal(map[string]any{"error": msg})
				fe := db.Event{ID: newID(), RunID: runID, TS: time.Now().UTC().Format(time.RFC3339Nano), Level: "ERROR", Message: "commit failed", FieldsJSON: fields}
				_ = s.st.InsertEvent(ctx, fe)
				if s.bc != nil {
					s.bc.Publish(fe)
				}

				return &grpcpb.ReportTaskResultResponse{Accepted: accepted, Message: msg}, nil
			}
			newStatus = "SUCCEEDED"
			launchIcebergRegistration = true
			// CompleteRunCommit atomically records the sole final completion event.
			emitRunStatusEvent = false
		}

		if emitRunStatusEvent {
			re := db.Event{ID: newID(), RunID: runID, TS: time.Now().UTC().Format(time.RFC3339Nano), Level: "INFO", Message: fmt.Sprintf("run %s", newStatus), FieldsJSON: []byte(`{}`)}
			_ = s.st.InsertEvent(ctx, re)
			if s.bc != nil {
				s.bc.Publish(re)
			}
		}
		if launchIcebergRegistration {
			s.launchIcebergRegistration(runID)
		}
	}

	return &grpcpb.ReportTaskResultResponse{Accepted: accepted, Message: msg}, nil
}

func (s *Server) recordAttemptRejection(ctx context.Context, taskID, attemptID, workerID, eventType, classification string) {
	if err := s.st.InsertAttemptRejectionEvent(ctx, taskID, attemptID, workerID, eventType, classification, s.nowFn()); err != nil && err != sql.ErrNoRows {
		s.log.Warn("failed to persist bounded attempt rejection event", slog.String("task_id", taskID), slog.String("attempt_id", attemptID), slog.String("event_type", eventType), slog.String("err", err.Error()))
	}
}

type catalogInspector interface {
	InspectCatalog(context.Context, icebergreg.InspectionRequest) (icebergreg.CatalogObservation, error)
}

func (s *Server) ProcessReconciliationOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	release, admitted := s.tryAcquireCatalogWork()
	if !admitted {
		return false, nil
	}
	defer release()
	const reconciliationLease = 30 * time.Second
	r, a, ok, err := s.st.ClaimReconciliation(ctx, s.nowFn(), reconciliationLease)
	if err != nil || !ok {
		return ok, err
	}
	s.log.Info(
		"catalog reconciliation claimed",
		slog.String("run_id", r.RunID),
		slog.String("registration_id", r.ID),
		slog.Int("reconciliation_attempt", a.AttemptNumber),
		slog.String("commit_id", r.CommitID),
		slog.String("catalog_status", r.Status),
		slog.String("error_class", r.LastErrorClass),
		slog.String("executor", "master-owned-reconciliation-loop"),
	)
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		ticker := time.NewTicker(reconciliationLease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-attemptCtx.Done():
				return
			case <-ticker.C:
				if err := s.st.RenewReconciliationLease(attemptCtx, r.ID, a.ID, a.FencingToken, s.nowFn(), reconciliationLease); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	inspector, ok := s.icebergRegistrar.(catalogInspector)
	if !ok {
		return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, icebergreg.OutcomeInsufficientEvidence, "", "", "", "", "", 0, 0, s.nowFn(), 2)
	}
	run, err := s.st.GetRun(ctx, r.RunID)
	if err != nil {
		return true, err
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return true, err
	}
	reg, err := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
	if err != nil {
		return true, err
	}
	tgt, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return true, err
	}
	src, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return true, err
	}
	var meta map[string]any
	_ = json.Unmarshal(tgt.MetadataJSON, &meta)
	bucket, _ := meta["bucket"].(string)
	opts, _ := jobopts.Parse(job.OptionsJSON)
	prefix := datasetPrefixForJob(job, src.Engine, opts, meta)
	artifacts, err := s.st.ListArtifactsForRun(ctx, r.RunID)
	if err != nil {
		return true, err
	}
	expected := make([]icebergreg.ExpectedFile, 0, len(artifacts))
	for _, x := range artifacts {
		expected = append(expected, icebergreg.ExpectedFile{Path: "s3://" + bucket + "/" + x.ObjectKey, Size: x.ByteSize, Records: x.RowCount, SHA256: x.SHA256, SchemaFingerprint: x.SchemaFingerprint})
	}
	table := reg.Table
	if table == "" {
		table = icebergreg.DefaultTable(src.Engine, strings.TrimSpace(opts.Table))
	}
	endpoint, _ := meta["endpoint"].(string)
	region, _ := meta["region"].(string)
	obs, err := inspector.InspectCatalog(attemptCtx, icebergreg.InspectionRequest{Registration: reg, Table: table, DatasetBucket: bucket, DatasetPrefix: prefix, DatasetS3: s3io.Config{Endpoint: endpoint, Region: region, Bucket: bucket, ForcePathStyle: true}})
	if err != nil {
		if obs.TableExists && obs.MetadataStart != "" {
			op := icebergreg.OperationIdentity{RegistrationID: r.ID, RunID: r.RunID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, ManifestKey: r.ManifestKey}
			decision, decisionErr := icebergreg.DecideReconciliation(op, expected, obs)
			if decisionErr == nil {
				return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, obs.MetadataStart, obs.MetadataEnd, decision.SnapshotID, "", decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), 2)
			}
		}
		return true, s.st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, "CATALOG_OBSERVATION_UNAVAILABLE", err.Error(), s.nowFn(), time.Second, 5)
	}
	op := icebergreg.OperationIdentity{RegistrationID: r.ID, RunID: r.RunID, CommitID: r.CommitID, ArtifactSetDigest: r.ArtifactSetDigest, ManifestKey: r.ManifestKey}
	decision, err := icebergreg.DecideReconciliation(op, expected, obs)
	if err != nil {
		return true, s.st.RetryReconciliationObservation(ctx, r.ID, a.ID, a.FencingToken, "INSUFFICIENT_HISTORY", err.Error(), s.nowFn(), time.Second, 5)
	}
	receipt := ""
	if decision.Outcome == icebergreg.OutcomeExactlyCommitted {
		receipt, err = db.ReconciliationReceipt(r, decision, s.nowFn())
		if err != nil {
			return true, err
		}
	}
	s.log.Info(
		"catalog reconciliation decision",
		slog.String("run_id", r.RunID),
		slog.String("registration_id", r.ID),
		slog.Int("reconciliation_attempt", a.AttemptNumber),
		slog.String("commit_id", r.CommitID),
		slog.String("catalog_status", r.Status),
		slog.String("decision", decision.Outcome),
		slog.Bool("operator_action_required", decision.OperatorActionRequired),
	)
	return true, s.st.ApplyReconciliationDecision(ctx, r.ID, a.ID, a.FencingToken, decision.Outcome, decision.EvidenceDigest, obs.MetadataStart, obs.MetadataEnd, decision.SnapshotID, receipt, decision.MatchedFiles, decision.ExpectedFiles, s.nowFn(), 2)
}

func (s *Server) ProcessMultipartCleanupOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	if _, err := s.st.ExpireMultipartCleanupClaims(ctx, s.nowFn()); err != nil {
		return false, err
	}
	record, ok, err := s.st.ClaimMultipartCleanup(ctx, s.nowFn(), s.multipartCleanupGrace, 30*time.Second)
	if err != nil || !ok {
		return ok, err
	}
	finish := func(outcome, class string, err error) (bool, error) {
		message := ""
		if err != nil {
			message = class
		}
		return true, s.st.FinishMultipartCleanup(ctx, record.ID, record.CleanupToken, outcome, class, message, s.nowFn(), s.multipartCleanupRetry, s.multipartCleanupMax)
	}
	run, err := s.st.GetRun(ctx, record.RunID)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	target, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	var metadata, secret map[string]any
	_ = json.Unmarshal(target.MetadataJSON, &metadata)
	plain, err := crypto.Decrypt(s.k, target.SecretEncBlob, []byte(target.ID))
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	_ = json.Unmarshal(plain, &secret)
	cfg := s3io.Config{Endpoint: stringMapValue(metadata, "endpoint"), Region: stringMapValue(metadata, "region"), Bucket: stringMapValue(metadata, "bucket"), ForcePathStyle: true, AccessKeyID: stringMapValue(secret, "access_key_id"), SecretAccessKey: stringMapValue(secret, "secret_access_key"), SessionToken: stringMapValue(secret, "session_token")}
	uploader, err := s.newMultipartCleanerFn(ctx, cfg)
	if err != nil {
		return finish("RETRY", "MULTIPART_TRACKING_FAILED", err)
	}
	expectedMeta := map[string]string{"run_id": record.RunID, "task_id": record.TaskID, "attempt_id": record.AttemptID, "file_index": fmt.Sprintf("%03d", record.FileIndex), "sha256": record.ObjectSHA256, "byte_size": fmt.Sprint(record.ObjectSize)}
	exists, verifyErr := uploader.VerifyTrackedFinalObject(ctx, record.ObjectKey, record.ObjectSize, record.ObjectSHA256, expectedMeta)
	if exists {
		if verifyErr != nil {
			return finish("REVIEW", "MULTIPART_FINAL_OBJECT_CONFLICT", verifyErr)
		}
		return finish("COMPLETED", "", nil)
	}
	if verifyErr != nil {
		return finish("RETRY", "MULTIPART_ABORT_AMBIGUOUS", verifyErr)
	}
	uploadID := record.ProviderUploadID
	if uploadID == "" {
		items, listErr := uploader.ListManagedMultipartUploads(ctx, record.ManagedPrefix, 100)
		if listErr != nil {
			return finish("RETRY", "MULTIPART_UNKNOWN_OWNERSHIP", listErr)
		}
		var matches []s3io.MultipartUploadInfo
		for _, item := range items {
			if item.Key == record.ObjectKey {
				matches = append(matches, item)
			}
		}
		switch len(matches) {
		case 0:
			return finish("ABORTED", "", nil)
		case 1:
			uploadID = matches[0].UploadID
			if err := s.st.AdoptMultipartUploadForCleanup(ctx, record.ID, record.CleanupToken, uploadID, s.nowFn()); err != nil {
				return true, err
			}
		default:
			return finish("REVIEW", "MULTIPART_DISCOVERY_CONFLICT", errors.New("multiple managed uploads match"))
		}
	}
	abortErr := uploader.AbortTrackedMultipart(ctx, record.ObjectKey, uploadID)
	if abortErr == nil {
		return finish("ABORTED", "", nil)
	}
	stillExists, inspectErr := uploader.MultipartUploadExists(ctx, record.ManagedPrefix, record.ObjectKey, uploadID)
	if inspectErr != nil {
		return finish("RETRY", "MULTIPART_ABORT_AMBIGUOUS", inspectErr)
	}
	if !stillExists {
		return finish("ABORTED", "", nil)
	}
	return finish("RETRY", "MULTIPART_ABORT_FAILED", abortErr)
}

func (s *Server) ProcessCanceledObjectCleanupOnce(ctx context.Context) (bool, error) {
	if err := s.requireLeadership(ctx); err != nil {
		return false, err
	}
	if _, err := s.st.ExpireCanceledObjectCleanupAttempts(ctx, s.nowFn()); err != nil {
		return false, err
	}
	candidate, attempt, ok, err := s.st.ClaimCanceledObjectCleanup(ctx, s.nowFn(), 30*time.Second)
	if err != nil || !ok {
		return ok, err
	}
	finish := func(outcome, class string, dryRun bool) (bool, error) {
		return true, s.st.FinishCanceledObjectCleanup(ctx, candidate.ID, attempt.ID, attempt.FencingToken, outcome, class, s.nowFn(), s.canceledObjectRetry, s.canceledObjectMax, dryRun)
	}
	run, err := s.st.GetRun(ctx, candidate.RunID)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	target, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	var metadata, secret map[string]any
	_ = json.Unmarshal(target.MetadataJSON, &metadata)
	plain, err := crypto.Decrypt(s.k, target.SecretEncBlob, []byte(target.ID))
	if err != nil {
		return finish("FAILED", "CLEANUP_REFERENCE_AMBIGUOUS", false)
	}
	_ = json.Unmarshal(plain, &secret)
	cfg := s3io.Config{Endpoint: stringMapValue(metadata, "endpoint"), Region: stringMapValue(metadata, "region"), Bucket: stringMapValue(metadata, "bucket"), ForcePathStyle: true, AccessKeyID: stringMapValue(secret, "access_key_id"), SecretAccessKey: stringMapValue(secret, "secret_access_key"), SessionToken: stringMapValue(secret, "session_token")}
	cleaner, err := s.newCanceledObjectCleanerFn(ctx, cfg)
	if err != nil {
		return finish("FAILED", "CLEANUP_OBJECT_VERIFICATION_FAILED", false)
	}
	expectedMeta := map[string]string{"run_id": candidate.RunID, "task_id": candidate.TaskID, "attempt_id": candidate.AttemptID, "sha256": candidate.ExpectedSHA256, "byte_size": fmt.Sprint(candidate.ExpectedSize)}
	observation, observeErr := cleaner.ObserveExactObject(ctx, candidate.ObjectKey, candidate.ExpectedSize, candidate.ExpectedSHA256, expectedMeta)
	if observeErr != nil {
		if observation.Exists {
			return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
		}
		return finish("FAILED", "CLEANUP_OBJECT_VERIFICATION_FAILED", false)
	}
	if !observation.Exists {
		return finish("MISSING", "", false)
	}
	if !observation.Matches {
		return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
	}
	if err := s.st.AuthorizeCanceledObjectDelete(ctx, candidate.ID, attempt.ID, attempt.FencingToken, observation.Identity, observation.VersionID, s.nowFn()); err != nil {
		return true, err
	}
	if s.canceledObjectDryRun {
		return finish("DRY_RUN", "", true)
	}
	deleteErr := cleaner.DeleteExactObject(ctx, candidate.ObjectKey, observation.VersionID)
	after, inspectErr := cleaner.ObserveExactObject(ctx, candidate.ObjectKey, candidate.ExpectedSize, candidate.ExpectedSHA256, expectedMeta)
	if inspectErr == nil && !after.Exists {
		return finish("DELETED", "", false)
	}
	if inspectErr == nil && after.Exists && !after.Matches {
		return finish("CONFLICT", "CLEANUP_OBJECT_IDENTITY_CONFLICT", false)
	}
	if inspectErr != nil {
		return finish("AMBIGUOUS", "CLEANUP_DELETE_AMBIGUOUS", false)
	}
	if deleteErr != nil || after.Exists {
		return finish("FAILED", "CLEANUP_DELETE_FAILED", false)
	}
	return finish("FAILED", "CLEANUP_DELETE_FAILED", false)
}

func stringMapValue(values map[string]any, key string) string {
	value, _ := values[key].(string)
	return value
}

// buildAssignment constructs a TaskAssignment message for the given task, including decrypted connection details.
func (s *Server) buildAssignment(ctx context.Context, t db.Task) (*grpcpb.TaskAssignment, error) {
	run, err := s.st.GetRun(ctx, t.RunID)
	if err != nil {
		return nil, err
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return nil, err
	}
	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return nil, err
	}
	tgtConn, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return nil, err
	}

	srcSecret, err := crypto.Decrypt(s.k, srcConn.SecretEncBlob, []byte(srcConn.ID))
	if err != nil {
		return nil, err
	}
	tgtSecret, err := crypto.Decrypt(s.k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		return nil, err
	}

	// Secrets are JSON; decode minimal expected fields.
	var src map[string]any
	_ = json.Unmarshal(srcSecret, &src)
	var tgt map[string]any
	_ = json.Unmarshal(tgtSecret, &tgt)

	sourceDSN, _ := src["dsn"].(string)
	accessKey, _ := tgt["access_key_id"].(string)
	secretKey, _ := tgt["secret_access_key"].(string)
	sessionToken, _ := tgt["session_token"].(string)

	// Target metadata for S3.
	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)
	endpoint, _ := tgtMeta["endpoint"].(string)
	region, _ := tgtMeta["region"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	forcePathStyle := true
	if v, ok := tgtMeta["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}
	if strings.TrimSpace(bucket) == "" {
		return nil, fmt.Errorf("target connection metadata missing bucket")
	}

	opts, _ := jobopts.Parse(job.OptionsJSON)
	outPrefix := datasetPrefixForJob(job, srcConn.Engine, opts, tgtMeta)

	return &grpcpb.TaskAssignment{
		TaskId:              t.ID,
		RunId:               run.ID,
		JobId:               job.ID,
		TaskIndex:           int32(t.TaskIndex),
		CorrelationId:       run.CorrelationID,
		AttemptId:           t.AttemptID,
		FencingToken:        t.FencingToken,
		AttemptNumber:       int32(t.AttemptNumber),
		LeaseDeadlineUnixMs: func() int64 { tm, _ := time.Parse(time.RFC3339Nano, t.LeaseDeadline); return tm.UnixMilli() }(),
		PartitionSpecJson:   string(t.PartitionSpec),
		SourceEngine:        srcConn.Engine,
		SourceDsn:           sourceDSN,
		SourceSql:           job.SourceSQL,
		S3Endpoint:          endpoint,
		S3Region:            region,
		S3Bucket:            bucket,
		S3Prefix:            outPrefix,
		S3ForcePathStyle:    forcePathStyle,
		S3AccessKeyId:       accessKey,
		S3SecretAccessKey:   secretKey,
		S3SessionToken:      sessionToken,
		TargetFileBytes:     opts.TargetFileBytes,
		PartitionKeys:       opts.PartitionKeys,
	}, nil
}

// commitRun finalizes a successful run by writing commit metadata and updating dataset state.
// It is called by the master when a run is finalized (all tasks completed successfully).
func (s *Server) commitRun(ctx context.Context, runID string) error {
	startedAt := time.Now()

	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		return err
	}
	if run.Status == "SUCCEEDED" {
		return nil
	}
	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		return err
	}
	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return err
	}
	tasks, err := s.st.ListTasksForRun(ctx, runID)
	if err != nil {
		return err
	}
	if len(tasks) == 0 {
		return fmt.Errorf("commit verification: run %s has no durable tasks", runID)
	}
	for _, task := range tasks {
		if task.Status != "SUCCEEDED" {
			return fmt.Errorf("commit verification: task %s is %s", task.ID, task.Status)
		}
		if task.BytesWritten > 0 && len(collectParquetKeys([]db.Task{task})) == 0 {
			return fmt.Errorf("commit verification: task %s has bytes but no durable object key", task.ID)
		}
	}
	acceptedArtifacts, err := s.st.ListArtifactsForRun(ctx, runID)
	if err != nil {
		return err
	}
	artifactsByTask := make(map[string][]artifact.Record)
	for _, record := range acceptedArtifacts {
		if err := record.Validate(); err != nil {
			return fmt.Errorf("commit artifact %s: %w", record.ObjectKey, err)
		}
		artifactsByTask[record.TaskID] = append(artifactsByTask[record.TaskID], record)
	}
	strictArtifacts := false
	for _, task := range tasks {
		if task.AttemptCount > 0 {
			strictArtifacts = true
		}
		records := artifactsByTask[task.ID]
		if task.AttemptCount > 0 && task.BytesWritten > 0 && len(records) == 0 {
			return fmt.Errorf("commit integrity: task %s lacks durable verified artifacts", task.ID)
		}
		var recordRows, recordBytes int64
		for _, record := range records {
			recordRows += record.RowCount
			recordBytes += record.ByteSize
		}
		if len(records) > 0 && (recordRows != task.RowsRead || recordBytes != task.BytesWritten) {
			return fmt.Errorf("commit integrity: task %s artifact aggregates mismatch", task.ID)
		}
	}
	committedKeys := collectParquetKeys(tasks)
	if strictArtifacts {
		committedKeys = committedKeys[:0]
		for _, record := range acceptedArtifacts {
			committedKeys = append(committedKeys, record.ObjectKey)
		}
	}
	sort.Strings(committedKeys)

	// Load target connection for S3 writes.
	tgtConn, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return err
	}
	tgtSecret, err := crypto.Decrypt(s.k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		return err
	}
	var tgt map[string]any
	_ = json.Unmarshal(tgtSecret, &tgt)
	accessKey, _ := tgt["access_key_id"].(string)
	secretKey, _ := tgt["secret_access_key"].(string)
	sessionToken, _ := tgt["session_token"].(string)

	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)
	endpoint, _ := tgtMeta["endpoint"].(string)
	region, _ := tgtMeta["region"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	forcePathStyle := true
	if v, ok := tgtMeta["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}
	if strings.TrimSpace(bucket) == "" {
		return fmt.Errorf("target connection metadata missing bucket")
	}
	opts, _ := jobopts.Parse(job.OptionsJSON)
	if opts.NormalizedSourceMode() == "query" && strings.TrimSpace(opts.QueryHash) == "" {
		sourceQuery := strings.TrimSpace(opts.Query)
		if sourceQuery == "" {
			sourceQuery = strings.TrimSpace(job.SourceSQL)
		}
		if normalized, err := connectors.NormalizeReadOnlySQLQuery(sourceQuery); err == nil {
			opts.Query = normalized
			opts.QueryHash = connectors.QueryHash(normalized)
			if strings.TrimSpace(opts.SourceName) == "" {
				opts.SourceName = "query_" + opts.QueryHash
			}
		}
	}
	basePrefix := datasetPrefixForJob(job, srcConn.Engine, opts, tgtMeta)
	currentDestination := durableCommitDestination{
		Endpoint:       endpoint,
		Region:         region,
		Bucket:         bucket,
		Prefix:         basePrefix,
		ForcePathStyle: forcePathStyle,
	}
	var intent durableCommitIntent
	var cb, b []byte
	var commitID, commitKey, stateKey string
	hasPersistedIntent := len(run.CommitIntentJSON) > 0
	if hasPersistedIntent {
		intent, currentDestination, err = validatePersistedCommitIntent(run, run.CommitIntentJSON, currentDestination, committedKeys, acceptedArtifacts)
		if err != nil {
			return err
		}
		endpoint = currentDestination.Endpoint
		region = currentDestination.Region
		bucket = currentDestination.Bucket
		forcePathStyle = currentDestination.ForcePathStyle
		basePrefix = currentDestination.Prefix
		commitID, commitKey, stateKey = intent.CommitID, intent.ManifestKey, intent.StateKey
		cb, b = intent.Manifest, intent.ProposedState
	} else {
		currentDestination.Endpoint = normalizeEndpoint(currentDestination.Endpoint)
		currentDestination.Bucket = strings.TrimSpace(currentDestination.Bucket)
		currentDestination.Prefix = normalizePrefix(currentDestination.Prefix)
		commitKey = currentDestination.Prefix + "/_commits/run-" + runID + ".json"
		stateKey = currentDestination.Prefix + "/_state.json"
	}

	u, err := s.newCommitObjectStoreFn(ctx, s3io.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
	})
	if err != nil {
		return err
	}

	// Collect uploaded Parquet object keys for this run.
	// Keys are run-scoped (uploaded under <prefix>/_runs/run-<RUN_ID>/...), so commit is a metadata write (no promote/copy).
	for _, key := range committedKeys {
		if err := u.Head(ctx, key); err != nil {
			return fmt.Errorf("commit verification: required object %s: %w", key, err)
		}
	}
	for _, record := range acceptedArtifacts {
		body, found, err := u.OpenObject(ctx, record.ObjectKey)
		if err != nil {
			return fmt.Errorf("commit artifact verification %s: %w", record.ObjectKey, err)
		}
		if !found {
			return fmt.Errorf("commit artifact verification: missing object %s", record.ObjectKey)
		}
		digest, size, hashErr := artifact.StreamSHA256(ctx, body)
		_ = body.Close()
		if hashErr != nil {
			return fmt.Errorf("commit artifact verification %s: %w", record.ObjectKey, hashErr)
		}
		if size != record.ByteSize {
			return fmt.Errorf("commit artifact size mismatch: %s", record.ObjectKey)
		}
		if digest != record.SHA256 {
			return fmt.Errorf("commit artifact sha256 mismatch: %s", record.ObjectKey)
		}
	}

	existingStateBytes, existingStateFound, err := u.GetObjectBytes(ctx, stateKey)
	if err != nil {
		return fmt.Errorf("read dataset state: %w", err)
	}
	existingMaxPart, existingHWM := parseExistingState(existingStateBytes)
	var existingState struct {
		RunID       string `json:"last_committed_run_id"`
		CommitID    string `json:"commit_id"`
		ManifestKey string `json:"manifest_key"`
		CommittedAt string `json:"committed_at"`
	}
	if existingStateFound {
		if err := json.Unmarshal(existingStateBytes, &existingState); err != nil || strings.TrimSpace(existingState.RunID) == "" || strings.TrimSpace(existingState.CommittedAt) == "" {
			return fmt.Errorf("checkpoint integrity conflict: existing dataset state is malformed")
		}
	}

	cursorDomain := resolveCursorDomain(opts, tasks)
	maxHWM := existingHWM
	if runMax := deriveMaxCursor(tasks, cursorDomain); strings.TrimSpace(runMax) != "" {
		maxHWM = runMax
	}
	maxPart := maxInt(maxPartNumber(committedKeys), existingMaxPart)

	// The timestamp and object order are stable so retries reconstruct identical intent.
	committedAt := run.StartedAt
	if existingStateFound && !hasPersistedIntent {
		currentTime, currentErr := time.Parse(time.RFC3339Nano, existingState.CommittedAt)
		runTime, runErr := time.Parse(time.RFC3339Nano, committedAt)
		if currentErr != nil || runErr != nil {
			return fmt.Errorf("checkpoint integrity conflict: invalid commit timestamp")
		}
		if existingState.RunID != runID && !currentTime.Before(runTime) {
			return fmt.Errorf("checkpoint fencing conflict: dataset state belongs to run %s", existingState.RunID)
		}
		if existingState.RunID == runID {
			return fmt.Errorf("checkpoint integrity conflict: state for run %s exists without durable intent", runID)
		}
	}

	// Write a per-run commit manifest (source of truth for committed objects).
	// Once present, the authenticated persisted intent above supplies every
	// destination key and byte sequence; mutable job/connection state is not
	// allowed to reconstruct it.
	if !hasPersistedIntent {
		commitObj := map[string]any{
			"schema_version": 1,
			"job_id":         job.ID,
			"job_name":       job.Name,
			"source_mode":    opts.NormalizedSourceMode(),
			"query_hash":     strings.TrimSpace(opts.QueryHash),
			"bucket":         bucket,
			"prefix":         basePrefix,
			"run_id":         runID,
			"committed_at":   committedAt,
			"max_hwm_value":  maxHWM,
			"max_part":       maxPart,
			"objects":        committedKeys,
			"objects_v2":     collectParquetObjectInfos(tasks),
		}
		if strictArtifacts {
			commitObj["schema_version"] = 2
			commitObj["artifacts"] = acceptedArtifacts
		}
		cb, err = json.Marshal(commitObj)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(cb)
		commitID = hex.EncodeToString(digest[:])
		stateObj := map[string]any{
			"job_id":                job.ID,
			"job_name":              job.Name,
			"source_mode":           opts.NormalizedSourceMode(),
			"query_hash":            strings.TrimSpace(opts.QueryHash),
			"bucket":                bucket,
			"prefix":                basePrefix,
			"last_committed_run_id": runID,
			"committed_at":          committedAt,
			"max_hwm_value":         maxHWM,
			"max_part":              maxPart,
			"next_part":             maxPart + 1,
			"last_run_objects":      committedKeys,
			"commit_id":             commitID,
			"manifest_key":          commitKey,
		}
		b, err = json.Marshal(stateObj)
		if err != nil {
			return err
		}
		intent = durableCommitIntent{
			CommitID:      commitID,
			DatasetID:     run.DatasetKey,
			ManifestKey:   commitKey,
			StateKey:      stateKey,
			Destination:   currentDestination,
			Manifest:      cb,
			ProposedState: b,
		}
		if strictArtifacts && len(acceptedArtifacts) == 0 {
			registration, configErr := icebergreg.ParseRunConfig(run.RegistrationConfigJSON)
			if configErr != nil {
				return &classifiedCommitError{class: commitFailureValidation, component: "registration_config", err: fmt.Errorf("empty dataset schema snapshot: %w", configErr)}
			}
			if registration.Enabled {
				sourceSecret, decryptErr := crypto.Decrypt(s.k, srcConn.SecretEncBlob, []byte(srcConn.ID))
				if decryptErr != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema is unavailable: %w", decryptErr)}
				}
				var sourceCredentials map[string]any
				if json.Unmarshal(sourceSecret, &sourceCredentials) != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema credentials are malformed")}
				}
				sourceDSN, _ := sourceCredentials["dsn"].(string)
				sourceQuery := strings.TrimSpace(opts.Query)
				if sourceQuery == "" {
					sourceQuery = strings.TrimSpace(job.SourceSQL)
				}
				intent.IcebergSchema, err = icebergreg.InferDurableIcebergSchema(ctx, srcConn.Engine, sourceDSN, opts.NormalizedSourceMode(), strings.TrimSpace(opts.Table), sourceQuery, opts.RecordPath, opts.FileFormat)
				if err != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema is unavailable: %w", err)}
				}
			}
		}
		if existingStateFound {
			intent.PreviousState = append([]byte(nil), existingStateBytes...)
		}
		intentJSON, err := json.Marshal(intent)
		if err != nil {
			return err
		}
		if err := s.st.SaveCommitIntent(ctx, runID, commitID, intentJSON); err != nil {
			return err
		}
	}
	if len(b) == 0 {
		return fmt.Errorf("commit integrity conflict: durable intent missing proposed state")
	}
	if got, ok, err := u.GetObjectBytes(ctx, commitKey); err != nil {
		return fmt.Errorf("verify commit manifest: %w", err)
	} else if ok && string(got) != string(cb) {
		return fmt.Errorf("commit integrity conflict: manifest %s already has different content", commitKey)
	} else if !ok {
		if err := u.PutObjectBytes(ctx, commitKey, cb, "application/json", map[string]string{"job_id": job.ID, "run_id": runID, "commit_id": commitID}); err != nil {
			// Verify ambiguity for diagnostics, but never declare this attempt
			// successful solely by interpreting a failed response. The next
			// attempt reuses matching durable content.
			got, found, readErr := u.GetObjectBytes(ctx, commitKey)
			if readErr == nil && found && string(got) != string(cb) {
				return fmt.Errorf("write commit manifest: response error followed by conflicting durable content: %w", err)
			}
			return fmt.Errorf("write commit manifest: ambiguous response (durable_match=%t): %w", readErr == nil && found && string(got) == string(cb), err)
		}
	}
	if got, ok, err := u.GetObjectBytes(ctx, commitKey); err != nil || !ok || string(got) != string(cb) {
		return fmt.Errorf("verify commit manifest: durable content mismatch")
	}
	_ = s.st.SetCommitPhase(ctx, runID, "MANIFEST_VERIFIED")

	if existingStateFound {
		if !bytes.Equal(existingStateBytes, b) && !bytes.Equal(existingStateBytes, intent.PreviousState) {
			return fmt.Errorf("checkpoint fencing conflict: dataset state changed after commit intent")
		}
	} else if len(intent.PreviousState) > 0 && string(intent.PreviousState) != "null" {
		return fmt.Errorf("checkpoint fencing conflict: expected previous dataset state is missing")
	}
	if !existingStateFound || string(existingStateBytes) != string(b) {
		if err := u.PutObjectBytes(ctx, stateKey, b, "application/json", map[string]string{"job_id": job.ID, "run_id": runID, "commit_id": commitID}); err != nil {
			got, found, readErr := u.GetObjectBytes(ctx, stateKey)
			if readErr == nil && found && string(got) != string(b) {
				return fmt.Errorf("write dataset state: response error followed by conflicting durable content: %w", err)
			}
			return fmt.Errorf("write dataset state: ambiguous response (durable_match=%t): %w", readErr == nil && found && string(got) == string(b), err)
		}
	}
	if got, ok, err := u.GetObjectBytes(ctx, stateKey); err != nil || !ok || string(got) != string(b) {
		return fmt.Errorf("verify dataset state: durable content mismatch")
	}
	_ = s.st.SetCommitPhase(ctx, runID, "STATE_VERIFIED")

	fields, _ := json.Marshal(map[string]any{
		"parquet_object_keys": committedKeys,
		"max_hwm_value":       maxHWM,
		"commit_ms":           time.Since(startedAt).Milliseconds(),
		"commit_id":           commitID,
		"manifest_key":        commitKey,
		"dataset_key":         run.DatasetKey,
		"finalization_phase":  "VERIFIED",
	})
	if job.HWMColumn != nil && maxHWM != "" {
		if err := s.upsertHWMFn(ctx, job.ID, maxHWM); err != nil {
			return fmt.Errorf("repair SQLite HWM mirror: %w", err)
		}
	}
	if err := s.st.SetCommitPhase(ctx, runID, "VERIFIED"); err != nil {
		return err
	}
	e := db.Event{ID: "commit-storage-" + commitID, RunID: runID, TS: time.Now().UTC().Format(time.RFC3339Nano), Level: "INFO", Message: "storage publication verified", FieldsJSON: fields}
	_ = s.st.InsertEventOnce(ctx, e)
	return nil
}

// collectParquetObjectInfos extracts Parquet object info maps from completed tasks.
// Each element is expected to contain at least {"key": "..."}. Additional fields like
// "rows" and "bytes" may be present.
func collectParquetObjectInfos(tasks []db.Task) []map[string]any {
	seen := make(map[string]struct{})
	out := make([]map[string]any, 0)
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		k, _ := o["key"].(string)
		if strings.TrimSpace(k) == "" {
			k, _ = o["object_key"].(string)
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		out = append(out, o)
	})
	return out
}

// collectParquetKeys extracts the unique Parquet object keys from the completed tasks of the run.
func collectParquetKeys(tasks []db.Task) []string {
	seen := make(map[string]struct{})
	var out []string
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		k, _ := o["key"].(string)
		if strings.TrimSpace(k) == "" {
			k, _ = o["object_key"].(string)
		}
		k = strings.TrimSpace(k)
		if k == "" {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		out = append(out, k)
	})
	return out
}

// forEachTaskParquetObject decodes each task's Parquet object metadata and invokes fn for each object.
// Malformed task metadata is skipped best-effort.
func forEachTaskParquetObject(tasks []db.Task, fn func(map[string]any)) {
	for _, t := range tasks {
		if len(t.ParquetObjects) == 0 {
			continue
		}
		var arr []map[string]any
		if err := json.Unmarshal(t.ParquetObjects, &arr); err != nil {
			continue
		}
		for _, o := range arr {
			fn(o)
		}
	}
}

// maxPartNumber scans the list of S3 object keys to find the maximum logical part number based on
// the naming convention "part-xxxxxx.parquet" or "part-xxxxxx-yyy.parquet".
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func parseExistingState(b []byte) (maxPart int, maxHWM string) {
	if len(b) == 0 {
		return 0, ""
	}
	var s struct {
		MaxHWMValue string `json:"max_hwm_value"`
		MaxPart     int    `json:"max_part"`
		NextPart    int    `json:"next_part"`
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return 0, ""
	}
	mp := s.MaxPart
	if s.NextPart > 0 {
		mp = maxInt(mp, s.NextPart-1)
	}
	return mp, strings.TrimSpace(s.MaxHWMValue)
}

func maxPartNumber(keys []string) int {
	max := 0
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		base := k
		if i := strings.LastIndex(base, "/"); i >= 0 {
			base = base[i+1:]
		}
		if !strings.HasPrefix(base, "part-") || !strings.HasSuffix(base, ".parquet") {
			continue
		}
		ns := strings.TrimSuffix(strings.TrimPrefix(base, "part-"), ".parquet")
		if dash := strings.IndexByte(ns, '-'); dash >= 0 {
			suffix := ns[dash+1:]
			if suffix == "" {
				continue
			}
			valid := true
			for _, r := range suffix {
				if r < '0' || r > '9' {
					valid = false
					break
				}
			}
			if !valid {
				continue
			}
			ns = ns[:dash]
			if ns == "" {
				continue
			}
		}
		iv, err := strconv.Atoi(ns)
		if err != nil {
			continue
		}
		if iv > max {
			max = iv
		}
	}
	return max
}

func resolveCursorDomain(opts jobopts.Options, tasks []db.Task) connectors.CursorDomain {
	if d := connectors.NormalizeCursorDomain(opts.CursorDomain); d != connectors.CursorDomainUnknown {
		return d
	}
	for _, t := range tasks {
		var part struct {
			Type         string `json:"type"`
			CursorDomain string `json:"cursor_domain"`
		}
		if err := json.Unmarshal(t.PartitionSpec, &part); err != nil {
			continue
		}
		if d := connectors.NormalizeCursorDomain(part.CursorDomain); d != connectors.CursorDomainUnknown {
			return d
		}
		switch part.Type {
		case "sql_int_range", "mssql_int_range":
			return connectors.CursorDomainInt64
		}
	}
	return connectors.CursorDomainUnknown
}

// deriveMaxCursor scans the task metadata for the maximum high-water mark (HWM) value observed across all tasks of the run.
func deriveMaxCursor(tasks []db.Task, domain connectors.CursorDomain) string {
	max := ""
	forEachTaskParquetObject(tasks, func(o map[string]any) {
		mv, _ := o["max_hwm"].(string)
		if mv == "" {
			return
		}
		if max == "" || connectors.CompareCursorValues(domain, max, mv) < 0 {
			max = mv
		}
	})
	return strings.TrimSpace(max)
}

// orJSON returns the input string if it is non-empty and non-whitespace; otherwise, it returns an empty JSON object "{}".
func orJSON(s string) string {
	if strings.TrimSpace(s) == "" {
		return "{}"
	}
	return s
}

// newID generates a new random ID as a hex string. It is used for worker IDs, event IDs, etc.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ListenAndServe starts the gRPC server and listens for incoming connections.
func ListenAndServe(ctx context.Context, cfg Config, srv *Server) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}

	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		c, err := credentials.NewServerTLSFromFile(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			return err
		}
		creds = c
	}

	g := grpc.NewServer(
		grpc.Creds(creds),
		grpc.UnaryInterceptor(workerAuthUnaryServerInterceptor(cfg.WorkerAuthToken)),
	)
	grpcpb.RegisterControlPlaneServer(g, srv)
	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	errCh := make(chan error, 1)
	go func() { errCh <- g.Serve(lis) }()

	select {
	case <-ctx.Done():
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		g.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// finalKeyForRun computes the final S3 object key for a staged object by removing the run staging prefix.

// datasetPrefixForJob derives the dataset prefix for the job based on the source engine, target metadata, and job options.
func datasetPrefixForJob(job db.Job, srcEngine string, opts jobopts.Options, tgtMeta map[string]any) string {
	targetPrefix := ""
	if tgtMeta != nil {
		if p, ok := tgtMeta["prefix"].(string); ok {
			targetPrefix = p
		}
	}

	sourceName := strings.TrimSpace(opts.SourceName)
	if sourceName == "" {
		if opts.NormalizedSourceMode() == "query" {
			if hash := strings.TrimSpace(opts.QueryHash); hash != "" {
				sourceName = "query_" + hash
			} else {
				sourceName = "query"
			}
		} else {
			sourceName = strings.TrimSpace(opts.Table)
		}
	}
	if sourceName == "" {
		sourceName = strings.TrimSpace(job.TargetTable)
	}
	return dataset.Prefix(targetPrefix, srcEngine, sourceName)
}

// tableToPath converts a database table identifier into a stable S3 path segment.

// defaultFullRunRetainCount is the default number of successful full-refresh runs
// to keep in S3. Override via ORABBIT_FULL_RUN_RETAIN_COUNT env var.
const defaultFullRunRetainCount = 1

// purgeStaleFullRunsForRun deletes the S3 objects for obsolete full-refresh run
// directories after a new full-refresh run has been successfully committed and
// published to the Iceberg catalog.
//
// Safety guarantees:
//   - Only called after ICEBERG_REGISTRATION_SUCCEEDED (new dataset fully published).
//   - Only operates on non-incremental (full-refresh) jobs.
//   - Keeps the latest retainCount runs intact.
//   - Failures are best-effort: logged but never propagated to the caller.
//   - Idempotent: re-running on already-purged directories is a no-op.
func (s *Server) purgeStaleFullRunsForRun(ctx context.Context, runID string) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return
	}

	run, err := s.st.GetRun(ctx, runID)
	if err != nil {
		s.log.Warn("full-run purge: failed to load run",
			slog.String("run_id", runID),
			slog.String("err", err.Error()),
		)
		return
	}

	job, err := s.st.GetJob(ctx, run.JobID)
	if err != nil {
		s.log.Warn("full-run purge: failed to load job",
			slog.String("run_id", runID),
			slog.String("job_id", run.JobID),
			slog.String("err", err.Error()),
		)
		return
	}

	// Only apply retention to full (non-incremental) jobs.
	if job.Incremental {
		return
	}

	// Determine how many successful full runs to retain.
	retainCount := defaultFullRunRetainCount
	if raw := strings.TrimSpace(os.Getenv("ORABBIT_FULL_RUN_RETAIN_COUNT")); raw != "" {
		if n, err2 := strconv.Atoi(raw); err2 == nil && n >= 1 {
			retainCount = n
		}
	}

	// Collect all succeeded runs for this job, ordered oldest-first.
	allRuns, err := s.st.ListSucceededRunsForJob(ctx, job.ID)
	if err != nil {
		s.log.Warn("full-run purge: failed to list succeeded runs",
			slog.String("run_id", runID),
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}

	if len(allRuns) <= retainCount {
		// Nothing to purge yet.
		return
	}

	// to_purge = all except the latest retainCount runs.
	toPurge := allRuns[:len(allRuns)-retainCount]

	// Resolve S3 target connection details (needed to build the S3 client).
	tgtConn, err := s.st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		s.log.Warn("full-run purge: failed to load target connection",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}
	tgtSecret, err := crypto.Decrypt(s.k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		s.log.Warn("full-run purge: failed to decrypt target credentials",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}
	var tgt map[string]any
	_ = json.Unmarshal(tgtSecret, &tgt)
	accessKey, _ := tgt["access_key_id"].(string)
	secretKey, _ := tgt["secret_access_key"].(string)
	sessionToken, _ := tgt["session_token"].(string)

	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)
	endpoint, _ := tgtMeta["endpoint"].(string)
	region, _ := tgtMeta["region"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	forcePathStyle := true
	if v, ok := tgtMeta["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}
	if strings.TrimSpace(bucket) == "" {
		s.log.Warn("full-run purge: target connection has no bucket", slog.String("job_id", job.ID))
		return
	}

	u, err := s3io.New(ctx, s3io.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		ForcePathStyle:  forcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
	})
	if err != nil {
		s.log.Warn("full-run purge: failed to create S3 client",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}

	// Derive the dataset base prefix for this job (same logic used by commitRun).
	srcConn, err := s.st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		s.log.Warn("full-run purge: failed to load source connection",
			slog.String("job_id", job.ID),
			slog.String("err", err.Error()),
		)
		return
	}
	opts, _ := jobopts.Parse(job.OptionsJSON)
	basePrefix := strings.TrimSuffix(strings.TrimSpace(datasetPrefixForJob(job, srcConn.Engine, opts, tgtMeta)), "/")
	if basePrefix == "" {
		s.log.Warn("full-run purge: could not resolve dataset prefix", slog.String("job_id", job.ID))
		return
	}

	totalPurged := 0
	for _, staleRun := range toPurge {
		// Guard: never delete the current (just-published) run.
		if staleRun.ID == runID {
			continue
		}

		runPrefix := basePrefix + "/_runs/run-" + staleRun.ID + "/"
		keys, listErr := u.ListKeys(ctx, runPrefix)
		if listErr != nil {
			s.log.Warn("full-run purge: list objects failed",
				slog.String("run_id", staleRun.ID),
				slog.String("prefix", runPrefix),
				slog.String("err", listErr.Error()),
			)
			continue
		}
		if len(keys) == 0 {
			// Already purged or never uploaded.
			continue
		}

		deleted, delErr := u.DeleteObjects(ctx, keys)
		if delErr != nil {
			s.log.Warn("full-run purge: delete objects failed",
				slog.String("run_id", staleRun.ID),
				slog.String("prefix", runPrefix),
				slog.String("err", delErr.Error()),
			)
			continue
		}

		totalPurged += deleted
		s.log.Info("full-run purge: deleted stale run objects",
			slog.String("current_run_id", runID),
			slog.String("purged_run_id", staleRun.ID),
			slog.String("prefix", runPrefix),
			slog.Int("objects_deleted", deleted),
		)
	}

	if totalPurged > 0 {
		s.log.Info("full-run purge: complete",
			slog.String("run_id", runID),
			slog.String("job_id", job.ID),
			slog.Int("runs_purged", len(toPurge)),
			slog.Int("total_objects_deleted", totalPurged),
		)
	}
}
