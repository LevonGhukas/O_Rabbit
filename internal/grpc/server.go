// internal/grpc/server.go
// The gRPC control plane server: configuration, policies, leadership and
// serving. Worker RPCs are in worker_rpcs.go, run commits in commit_run.go,
// and background reconciliation/cleanup loops in cleanup_loops.go.

package grpcapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	httpapi "github.com/LevonGhukas/O_Rabbit/internal/http"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/telemetry"
	"github.com/LevonGhukas/O_Rabbit/internal/workeridentity"
	"github.com/aws/aws-sdk-go-v2/aws"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
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

// Server implements the gRPC control plane server.
type Server struct {
	commitCatalog CommitCatalogPolicy
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
	workerCA                   *workeridentity.CA
	workerCertTTL              time.Duration
	assumeRoleFn               func(context.Context, stsRequest) (aws.Credentials, error)
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

// WorkerProtocolVersion is the worker/master protocol revision. Version 6
// moved task secrets from TaskAssignment to GetTaskCredentials; version 7
// moved multipart lifecycle reports to ReportMultipartLifecycle.
const WorkerProtocolVersion = 7

const workerProtocolVersion = WorkerProtocolVersion

// ListenAndServe starts the gRPC server and listens for incoming connections.
const (
	// maxRecvMsgBytes bounds each worker request. Results and progress
	// payloads are far smaller; the limit caps worker-supplied data.
	maxRecvMsgBytes = 4 << 20
	maxSendMsgBytes = 16 << 20

	// gracefulStopTimeout bounds shutdown: a long commit running inside
	// ReportTaskResult must not keep a stopping master alive indefinitely.
	// Its run stays COMMITTING and is resumed by the next leader.
	gracefulStopTimeout = 30 * time.Second
)

const (
	maxWorkerProgressMessageBytes = 4 << 10
	maxWorkerProgressFieldsBytes  = 64 << 10
	maxMultipartErrorMessageBytes = 4 << 10
	maxMultipartObjectKeyBytes    = 4 << 10
	maxMultipartUploadIDBytes     = 2 << 10
	maxMultipartErrorClassBytes   = 256
)

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
	s.workerCertTTL = 24 * time.Hour
	s.commitCatalog = DefaultCommitCatalogPolicy()
	s.assumeRoleFn = assumeRoleWithSTS
	return s
}

func (s *Server) SetLeasePolicy(policy db.LeasePolicy) { s.leasePolicy = policy }

// CommitCatalogPolicy holds the timeouts, leases and retry budgets of run
// publication and catalog registration/reconciliation.
type CommitCatalogPolicy struct {
	// CommitTimeout bounds one publication attempt of a run.
	CommitTimeout time.Duration
	// CommitMaxAttempts bounds retries of a failing commit.
	CommitMaxAttempts int
	// RegistrationTimeout bounds one catalog registration.
	RegistrationTimeout time.Duration
	Registration        db.RegistrationPolicy
	// ReconciliationLease and ReconciliationMaxAttempts govern catalog
	// observation after an ambiguous registration.
	ReconciliationLease       time.Duration
	ReconciliationMaxAttempts int
}

// DefaultCommitCatalogPolicy returns the built-in commit and catalog policy.
func DefaultCommitCatalogPolicy() CommitCatalogPolicy {
	return CommitCatalogPolicy{
		CommitTimeout:             30 * time.Minute,
		CommitMaxAttempts:         5,
		RegistrationTimeout:       30 * time.Minute,
		Registration:              db.RegistrationPolicy{LeaseDuration: 30 * time.Second, MaxAttempts: 5, BackoffBase: time.Second, BackoffMax: time.Minute},
		ReconciliationLease:       30 * time.Second,
		ReconciliationMaxAttempts: 5,
	}
}

// SetCommitCatalogPolicy replaces the commit and catalog policy. Zero fields
// keep their defaults.
func (s *Server) SetCommitCatalogPolicy(p CommitCatalogPolicy) {
	d := DefaultCommitCatalogPolicy()
	if p.CommitTimeout <= 0 {
		p.CommitTimeout = d.CommitTimeout
	}
	if p.CommitMaxAttempts <= 0 {
		p.CommitMaxAttempts = d.CommitMaxAttempts
	}
	if p.RegistrationTimeout <= 0 {
		p.RegistrationTimeout = d.RegistrationTimeout
	}
	if p.Registration.LeaseDuration <= 0 {
		p.Registration.LeaseDuration = d.Registration.LeaseDuration
	}
	if p.Registration.MaxAttempts <= 0 {
		p.Registration.MaxAttempts = d.Registration.MaxAttempts
	}
	if p.Registration.BackoffBase <= 0 {
		p.Registration.BackoffBase = d.Registration.BackoffBase
	}
	if p.Registration.BackoffMax <= 0 {
		p.Registration.BackoffMax = d.Registration.BackoffMax
	}
	if p.ReconciliationLease <= 0 {
		p.ReconciliationLease = d.ReconciliationLease
	}
	if p.ReconciliationMaxAttempts <= 0 {
		p.ReconciliationMaxAttempts = d.ReconciliationMaxAttempts
	}
	s.commitCatalog = p
}

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
				var warnings []typesystem.TypeWarning
				intent.IcebergSchema, warnings, err = icebergreg.InferDurableIcebergSchemaWithWarnings(ctx, srcConn.Engine, sourceDSN, opts.NormalizedSourceMode(), strings.TrimSpace(opts.Table), sourceQuery, opts.RecordPath, opts.FileFormat, effectiveColumnTypes(tasks, opts.ColumnTypes), opts.SelectColumns)
				if err != nil {
					return &classifiedCommitError{class: commitFailureValidation, component: "source_schema", err: fmt.Errorf("empty dataset source schema is unavailable: %w", err)}
				}
				if len(warnings) > 0 {
					if err := s.st.SetRunTypeWarnings(ctx, runID, warnings); err != nil {
						return fmt.Errorf("persist document type warnings: %w", err)
					}
					for _, warning := range warnings {
						s.log.Warn("type mapping fallback", "run_id", runID, "source_engine", srcConn.Engine, "column", warning.Column, "logical_type", warning.LogicalType, "storage_type", warning.StorageType, "mapping_class", warning.Class, "reason", warning.Reason)
					}
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
// effectiveColumnTypes returns the column types the planner verified and
// handed to workers (see planner.resolveEffectiveColumnTypes). Registration
// must use the same types so the Iceberg schema matches the written Parquet.
func effectiveColumnTypes(tasks []db.Task, fallback map[string]string) map[string]string {
	for _, t := range tasks {
		var spec struct {
			ColumnTypes map[string]string `json:"column_types"`
		}
		if json.Unmarshal(t.PartitionSpec, &spec) == nil && spec.ColumnTypes != nil {
			return spec.ColumnTypes
		}
	}
	return fallback
}

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

// serverOptions are the control-plane server options besides credentials.
func serverOptions(cfg Config, srv *Server) []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			telemetry.UnaryServerInterceptor(),
			recoveryUnaryServerInterceptor(srv.log),
			workerAuthUnaryServerInterceptor(cfg.WorkerAuthToken),
			srv.workerIdentityUnaryInterceptor(!cfg.Insecure),
		),
		grpc.ChainStreamInterceptor(recoveryStreamServerInterceptor(srv.log)),
		grpc.MaxRecvMsgSize(maxRecvMsgBytes),
		grpc.MaxSendMsgSize(maxSendMsgBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			// Ping connections idle this long and drop them if the ping is
			// not acknowledged, so vanished workers are detected promptly.
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
			// Bounded connection age makes workers re-handshake periodically,
			// so a renewed identity certificate replaces the one a connection
			// was opened with. The grace period covers long result RPCs.
			MaxConnectionAge:      time.Hour,
			MaxConnectionAgeGrace: 35 * time.Minute,
		}),
		// Accept the worker's keepalive pings (WorkerKeepaliveParams); pings
		// more frequent than MinTime are answered with GOAWAY.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             WorkerKeepaliveParams.Time / 2,
			PermitWithoutStream: false,
		}),
	}
}

// WorkerKeepaliveParams are the client keepalive settings workers use when
// dialing the master; they must stay within the server enforcement policy.
var WorkerKeepaliveParams = keepalive.ClientParameters{
	Time:                30 * time.Second,
	Timeout:             10 * time.Second,
	PermitWithoutStream: false,
}

func stopGracefully(g *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		g.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		g.Stop()
		<-done
	}
}

func ListenAndServe(ctx context.Context, cfg Config, srv *Server) error {
	lis, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return err
	}
	return Serve(ctx, lis, cfg, srv)
}

// Serve runs the control-plane gRPC server on lis until ctx is canceled.
func Serve(ctx context.Context, lis net.Listener, cfg Config, srv *Server) error {

	var creds credentials.TransportCredentials
	if cfg.Insecure {
		creds = insecure.NewCredentials()
	} else {
		if srv.workerCA == nil {
			_ = lis.Close()
			return errors.New("gRPC TLS requires the worker identity CA; call SetWorkerIdentity")
		}
		tlsCfg, err := ServerTLSConfig(cfg.TLSCertFile, cfg.TLSKeyFile, srv.workerCA.Pool())
		if err != nil {
			_ = lis.Close()
			return err
		}
		creds = credentials.NewTLS(tlsCfg)
	}

	g := grpc.NewServer(append([]grpc.ServerOption{grpc.Creds(creds)}, serverOptions(cfg, srv)...)...)
	grpcpb.RegisterControlPlaneServer(g, srv)
	healthSrv := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g, healthSrv)
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	errCh := make(chan error, 1)
	go func() { errCh <- g.Serve(lis) }()

	select {
	case <-ctx.Done():
		healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		stopGracefully(g, gracefulStopTimeout)
		return nil
	case err := <-errCh:
		return err
	}
}
