package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"

	_ "modernc.org/sqlite"
)

type Store struct {
	db                      *sql.DB
	log                     *slog.Logger
	canceledObjectRetention time.Duration
	maxActiveRuns           int
}

type Config struct {
	Path string
}

func Open(ctx context.Context, cfg Config, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}

	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, err
	}

	// SQLite is single-writer; keep one connection to avoid SQLITE_BUSY under concurrent gRPC.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// WAL + sane defaults.
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=15000;",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, log: log, canceledObjectRetention: 24 * time.Hour}, nil
}

func (s *Store) SetCanceledObjectRetention(retention time.Duration) {
	if retention > 0 {
		s.canceledObjectRetention = retention
	}
}

func (s *Store) Close() error { return s.db.Close() }

// Ready reports whether the control-plane store is currently queryable.
// It uses a short, caller-bounded probe suitable for HTTP readiness checks.
func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store is not initialized")
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}

	if err := s.db.PingContext(ctx); err != nil {
		return err
	}

	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT 1;`).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("unexpected readiness probe result: %d", n)
	}
	return nil
}

func nowUTC() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "SQLITE_LOCKED") || strings.Contains(msg, "database is locked")
}

func withBusyRetry(ctx context.Context, fn func() error) error {
	backoff := 25 * time.Millisecond
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return nil
		}
		last = err
		if !isSQLiteBusy(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	return last
}

var ErrActiveDatasetRun = errors.New("an active run already exists for this dataset")

func wrapRunRegistrationConfigColumnErr(err error) error {
	if err == nil {
		return nil
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "no such column: registration_config_json") ||
		strings.Contains(msg, "has no column named registration_config_json") {
		return fmt.Errorf("database missing runs.registration_config_json; apply the latest master DB migration: %w", err)
	}
	return err
}

func normalizeRunFailureReason(reason string) string {
	if strings.TrimSpace(reason) == "" {
		return "superseded"
	}
	return reason
}

func (s *Store) failRunIDs(ctx context.Context, runIDs []string, reason string) error {
	if len(runIDs) == 0 {
		return nil
	}
	now := nowUTC()
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		for _, rid := range runIDs {
			_, err = tx.ExecContext(ctx, `UPDATE tasks SET status='FAILED', error_message=?, finished_at=? WHERE run_id=? AND status IN ('PENDING','RUNNING');`, reason, now, rid)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE runs SET status='FAILED', finished_at=?, error_summary=? WHERE id=? AND status IN ('RUNNING','PLANNING');`, now, reason, rid)
			if err != nil {
				return err
			}
		}
		return tx.Commit()
	})
}

func (s *Store) FailAllRunningRuns(ctx context.Context, reason string) (int, error) {
	reason = normalizeRunFailureReason(reason)
	var runIDs []string
	err := withBusyRetry(ctx, func() error {
		ids := make([]string, 0)
		rows, err := s.db.QueryContext(ctx, `SELECT id FROM runs WHERE status IN ('RUNNING','PLANNING');`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		runIDs = ids
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(runIDs) == 0 {
		return 0, nil
	}
	if err := s.failRunIDs(ctx, runIDs, reason); err != nil {
		return 0, err
	}
	return len(runIDs), nil
}

func (s *Store) FailRunningRunsForJob(ctx context.Context, jobID string, reason string) (int, error) {
	if strings.TrimSpace(jobID) == "" {
		return 0, nil
	}
	reason = normalizeRunFailureReason(reason)
	var runIDs []string
	err := withBusyRetry(ctx, func() error {
		ids := make([]string, 0)
		rows, err := s.db.QueryContext(ctx, `SELECT id FROM runs WHERE status IN ('RUNNING','PLANNING') AND job_id=?;`, jobID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		runIDs = ids
		return nil
	})
	if err != nil {
		return 0, err
	}
	if len(runIDs) == 0 {
		return 0, nil
	}
	if err := s.failRunIDs(ctx, runIDs, reason); err != nil {
		return 0, err
	}
	return len(runIDs), nil
}

// --- Models

type Connection struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Kind          string          `json:"kind"`
	Engine        string          `json:"engine"`
	MetadataJSON  json.RawMessage `json:"metadata_json"`
	SecretEncBlob []byte          `json:"-"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
}
type WorkerInstance struct {
	BootID        string `json:"boot_id"`
	WorkerID      string `json:"worker_id"`
	Hostname      string `json:"hostname"`
	PID           int    `json:"pid"`
	Version       string `json:"version"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	LastHeartbeat string `json:"last_heartbeat"`
}

type Worker struct {
	ID            string           `json:"id"`
	Addr          string           `json:"addr"`
	Status        string           `json:"status"`
	LastHeartbeat string           `json:"last_heartbeat"`
	Capabilities  json.RawMessage  `json:"capabilities_json"`
	Instances     []WorkerInstance `json:"instances"`
}

type Job struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	SourceConnectionID string          `json:"source_connection_id"`
	TargetConnectionID string          `json:"target_connection_id"`
	SourceSQL          string          `json:"source_sql"`
	TargetNamespace    string          `json:"target_namespace"`
	TargetTable        string          `json:"target_table"`
	WriteMode          string          `json:"write_mode"`
	Incremental        bool            `json:"incremental"`
	HWMColumn          *string         `json:"hwm_column"`
	OptionsJSON        json.RawMessage `json:"options_json"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
}

type Run struct {
	ID                             string                    `json:"id"`
	JobID                          string                    `json:"job_id"`
	DatasetKey                     string                    `json:"dataset_key,omitempty"`
	Status                         string                    `json:"status"`
	CorrelationID                  string                    `json:"correlation_id"`
	StartedAt                      string                    `json:"started_at"`
	FinishedAt                     *string                   `json:"finished_at"`
	ErrorSummary                   *string                   `json:"error_summary"`
	FailureClass                   string                    `json:"failure_class,omitempty"`
	TypeWarnings                   []typesystem.TypeWarning  `json:"type_warnings"`
	RegistrationConfigJSON         json.RawMessage           `json:"-"`
	CommitID                       string                    `json:"commit_id,omitempty"`
	CommitIntentJSON               json.RawMessage           `json:"-"`
	CommitPhase                    string                    `json:"commit_phase,omitempty"`
	CommitReconciliationStatus     string                    `json:"commit_reconciliation_status,omitempty"`
	CommitReconciliationAttempt    int                       `json:"commit_reconciliation_attempt,omitempty"`
	CommitReconciliationNextRetry  *string                   `json:"commit_reconciliation_next_retry_at,omitempty"`
	OperatorActionRequired         bool                      `json:"operator_action_required"`
	DataStatus                     string                    `json:"data_status,omitempty"`
	CatalogStatus                  string                    `json:"catalog_status,omitempty"`
	Readiness                      string                    `json:"readiness,omitempty"`
	RegistrationID                 string                    `json:"registration_id,omitempty"`
	RegistrationAttempt            int                       `json:"registration_attempt"`
	RegistrationLastErrorClass     string                    `json:"registration_last_error_class,omitempty"`
	RegistrationErrorClass         string                    `json:"registration_error_class,omitempty"`
	RegistrationNextRetryAt        *string                   `json:"registration_next_retry_at,omitempty"`
	RegistrationBlockedBy          string                    `json:"registration_blocked_by,omitempty"`
	RegisteredSnapshotOrMetadataID string                    `json:"registered_snapshot_or_metadata_id,omitempty"`
	CatalogReceipt                 string                    `json:"catalog_receipt,omitempty"`
	Reconciliation                 ReconciliationProjection  `json:"reconciliation,omitempty"`
	MultipartUploads               []MultipartLifecycle      `json:"multipart_uploads,omitempty"`
	CanceledObjectCleanup          []CanceledObjectCandidate `json:"canceled_object_cleanup,omitempty"`
}

type Task struct {
	ID                 string          `json:"id"`
	RunID              string          `json:"run_id"`
	TaskIndex          int             `json:"task_index"`
	PartitionSpec      json.RawMessage `json:"partition_spec_json"`
	WorkerID           *string         `json:"worker_id"`
	Status             string          `json:"status"`
	RowsRead           int64           `json:"rows_read"`
	BytesRead          int64           `json:"bytes_read"`
	BytesWritten       int64           `json:"bytes_written"`
	ParquetObjects     json.RawMessage `json:"parquet_objects_json"`
	StartedAt          *string         `json:"started_at"`
	FinishedAt         *string         `json:"finished_at"`
	ErrorMessage       *string         `json:"error_message"`
	CurrentAttemptID   *string         `json:"current_attempt_id,omitempty"`
	AttemptCount       int             `json:"attempt_count"`
	NextEligibleAt     *string         `json:"next_eligible_at,omitempty"`
	AttemptID          string          `json:"-"`
	AttemptNumber      int             `json:"attempt_number,omitempty"`
	FencingToken       string          `json:"-"`
	LeaseDeadline      string          `json:"lease_deadline,omitempty"`
	LastRenewedAt      string          `json:"last_renewed_at,omitempty"`
	AttemptStatus      string          `json:"attempt_status,omitempty"`
	FailureClass       string          `json:"failure_class,omitempty"`
	ArtifactCount      int             `json:"artifact_count"`
	ArtifactBytes      int64           `json:"artifact_bytes"`
	ArtifactRows       int64           `json:"artifact_rows"`
	VerificationStatus string          `json:"artifact_verification_status,omitempty"`
	VerificationMethod string          `json:"artifact_verification_method,omitempty"`
	ArtifactVerifiedAt string          `json:"artifact_verified_at,omitempty"`
}

type TaskExecutionState struct {
	TaskID     string  `json:"task_id"`
	RunID      string  `json:"run_id"`
	TaskStatus string  `json:"task_status"`
	RunStatus  string  `json:"run_status"`
	TaskError  *string `json:"task_error,omitempty"`
	RunError   *string `json:"run_error,omitempty"`
}

type Event struct {
	ID         string          `json:"id"`
	RunID      string          `json:"run_id"`
	TaskID     *string         `json:"task_id"`
	TS         string          `json:"ts"`
	Level      string          `json:"level"`
	Message    string          `json:"message"`
	FieldsJSON json.RawMessage `json:"fields_json"`
}

// --- Connections

func (s *Store) CreateConnection(ctx context.Context, c Connection) error {
	c = prepareConnectionForCreate(c)
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createConnectionTx(ctx, tx, c)
	})
}

func (s *Store) GetConnection(ctx context.Context, id string) (Connection, error) {
	var c Connection
	var meta string
	row := s.db.QueryRowContext(ctx, `SELECT id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at FROM connections WHERE id=?;`, id)
	if err := row.Scan(&c.ID, &c.Name, &c.Kind, &c.Engine, &meta, &c.SecretEncBlob, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return Connection{}, err
	}
	c.MetadataJSON = []byte(meta)
	return c, nil
}

func (s *Store) ListConnections(ctx context.Context) ([]Connection, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at FROM connections ORDER BY created_at DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Connection
	for rows.Next() {
		var c Connection
		var meta string
		if err := rows.Scan(&c.ID, &c.Name, &c.Kind, &c.Engine, &meta, &c.SecretEncBlob, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.MetadataJSON = []byte(meta)
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) UpdateConnection(ctx context.Context, c Connection) error {
	if len(c.MetadataJSON) == 0 {
		c.MetadataJSON = []byte(`{}`)
	}
	c.UpdatedAt = nowUTC()
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateConnectionTx(ctx, tx, c)
	})
}

func (s *Store) DeleteConnection(ctx context.Context, id string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return deleteConnectionTx(ctx, tx, id)
	})
}

func (s *Store) CountJobsUsingConnection(ctx context.Context, connectionID string) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE source_connection_id=? OR target_connection_id=?;`, connectionID, connectionID)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// --- Jobs

func (s *Store) CreateJob(ctx context.Context, j Job) error {
	j = prepareJobForCreate(j)
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createJobTx(ctx, tx, j)
	})
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	var j Job
	var options string
	var inc int
	row := s.db.QueryRowContext(ctx, `SELECT id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at FROM jobs WHERE id=?;`, id)
	if err := row.Scan(&j.ID, &j.Name, &j.SourceConnectionID, &j.TargetConnectionID, &j.SourceSQL, &j.TargetNamespace, &j.TargetTable, &j.WriteMode, &inc, &j.HWMColumn, &options, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return Job{}, err
	}
	j.Incremental = inc != 0
	j.OptionsJSON = []byte(options)
	return j, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at FROM jobs ORDER BY created_at DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var options string
		var inc int
		if err := rows.Scan(&j.ID, &j.Name, &j.SourceConnectionID, &j.TargetConnectionID, &j.SourceSQL, &j.TargetNamespace, &j.TargetTable, &j.WriteMode, &inc, &j.HWMColumn, &options, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, err
		}
		j.Incremental = inc != 0
		j.OptionsJSON = []byte(options)
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) UpdateJob(ctx context.Context, j Job) error {
	if len(j.OptionsJSON) == 0 {
		j.OptionsJSON = []byte(`{}`)
	}
	j.UpdatedAt = nowUTC()
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateJobTx(ctx, tx, j)
	})
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return deleteJobTx(ctx, tx, id)
	})
}

func (s *Store) CountRunsForJob(ctx context.Context, jobID string) (total int, active int, err error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			SUM(CASE WHEN status IN ('PLANNING','RUNNING','COMMITTING') THEN 1 ELSE 0 END) AS active
		FROM runs
		WHERE job_id=?;`, jobID)
	var activeN sql.NullInt64
	if err := row.Scan(&total, &activeN); err != nil {
		return 0, 0, err
	}
	if activeN.Valid {
		active = int(activeN.Int64)
	}
	return total, active, nil
}

func (s *Store) FindActiveRunByDatasetKey(ctx context.Context, datasetKey string) (Run, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class
		FROM runs
		WHERE dataset_key=? AND status IN ('PLANNING','RUNNING','COMMITTING')
		ORDER BY started_at ASC
		LIMIT 1;`, datasetKey)
	var r Run
	if err := row.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, false, nil
		}
		return Run{}, false, err
	}
	return r, true, nil
}

// --- Runs / Tasks

func (s *Store) CreateRun(ctx context.Context, r Run) error {
	var err error
	registrationConfig := string(r.RegistrationConfigJSON)
	warnings, err := json.Marshal(r.TypeWarnings)
	if err != nil {
		return err
	}
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO runs(id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, registration_config_json, type_warnings_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			r.ID, r.JobID, r.DatasetKey, r.Status, r.CorrelationID, r.StartedAt, r.FinishedAt, r.ErrorSummary, r.FailureClass, registrationConfig, string(warnings))
		if err != nil {
			msg := err.Error()
			if strings.Contains(msg, "idx_runs_dataset_active") || strings.Contains(msg, "runs.dataset_key") {
				return fmt.Errorf("%w (dataset_key=%s)", ErrActiveDatasetRun, r.DatasetKey)
			}
		}
		return wrapRunRegistrationConfigColumnErr(err)
	})
	return err
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, registration_config_json, type_warnings_json, commit_id, commit_intent_json, commit_phase FROM runs ORDER BY started_at DESC;`)
	if err != nil {
		return nil, wrapRunRegistrationConfigColumnErr(err)
	}
	var out []Run
	for rows.Next() {
		var r Run
		var registrationConfig, warningJSON, commitIntent string
		if err := rows.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &registrationConfig, &warningJSON, &r.CommitID, &commitIntent, &r.CommitPhase); err != nil {
			return nil, err
		}
		if strings.TrimSpace(registrationConfig) != "" {
			r.RegistrationConfigJSON = []byte(registrationConfig)
		}
		if strings.TrimSpace(commitIntent) != "" {
			r.CommitIntentJSON = []byte(commitIntent)
		}
		if err := decodeRunWarnings(warningJSON, &r); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		s.attachCommitReconciliationProjection(ctx, &out[i])
		s.attachRegistrationProjection(ctx, &out[i])
	}
	return out, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	var r Run
	var registrationConfig, warningJSON, commitIntent string
	row := s.db.QueryRowContext(ctx, `SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, registration_config_json, type_warnings_json, commit_id, commit_intent_json, commit_phase FROM runs WHERE id=?;`, id)
	if err := row.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &registrationConfig, &warningJSON, &r.CommitID, &commitIntent, &r.CommitPhase); err != nil {
		return Run{}, wrapRunRegistrationConfigColumnErr(err)
	}
	if err := decodeRunWarnings(warningJSON, &r); err != nil {
		return Run{}, err
	}
	if strings.TrimSpace(registrationConfig) != "" {
		r.RegistrationConfigJSON = []byte(registrationConfig)
	}
	if strings.TrimSpace(commitIntent) != "" {
		r.CommitIntentJSON = []byte(commitIntent)
	}
	s.attachCommitReconciliationProjection(ctx, &r)
	s.attachRegistrationProjection(ctx, &r)
	return r, nil
}

// ListSucceededRunsForJob returns succeeded runs oldest-first for retention.
func (s *Store) ListSucceededRunsForJob(ctx context.Context, jobID string) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, registration_config_json, type_warnings_json FROM runs WHERE job_id = ? AND status = 'SUCCEEDED' ORDER BY started_at ASC;`, jobID)
	if err != nil {
		return nil, wrapRunRegistrationConfigColumnErr(err)
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var registrationConfig, warningJSON string
		if err := rows.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &registrationConfig, &warningJSON); err != nil {
			return nil, err
		}
		if err := decodeRunWarnings(warningJSON, &r); err != nil {
			return nil, err
		}
		if strings.TrimSpace(registrationConfig) != "" {
			r.RegistrationConfigJSON = []byte(registrationConfig)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func decodeRunWarnings(raw string, r *Run) error {
	r.TypeWarnings = []typesystem.TypeWarning{}
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(raw), &r.TypeWarnings); err != nil {
		return fmt.Errorf("decode run type warnings: %w", err)
	}
	if r.TypeWarnings == nil {
		r.TypeWarnings = []typesystem.TypeWarning{}
	}
	return nil
}

func (s *Store) attachRegistrationProjection(ctx context.Context, r *Run) {
	if uploads, err := s.ListMultipartUploadsForRun(ctx, r.ID); err == nil {
		r.MultipartUploads = uploads
	}
	if candidates, err := s.ListCanceledObjectCandidates(ctx, r.ID); err == nil {
		r.CanceledObjectCleanup = candidates
	}
	r.DataStatus = r.Status
	reg, err := s.GetRegistrationForRun(ctx, r.ID)
	if err == nil {
		r.CatalogStatus = reg.Status
		r.RegistrationID = reg.ID
		r.RegistrationAttempt = reg.AttemptCount
		r.RegistrationLastErrorClass = reg.LastErrorClass
		r.RegistrationErrorClass = reg.LastErrorClass
		r.RegistrationNextRetryAt = reg.NextEligibleAt
		r.RegisteredSnapshotOrMetadataID = reg.Receipt
		r.CatalogReceipt = reg.Receipt
		if projection, err := s.GetReconciliationProjection(ctx, reg.ID); err == nil {
			r.Reconciliation = projection
		}
		_ = s.db.QueryRowContext(ctx, `SELECT id FROM iceberg_registrations WHERE dataset_id=? AND target_key=? AND dataset_sequence<? AND status<>'REGISTERED' ORDER BY dataset_sequence LIMIT 1`, reg.DatasetID, reg.TargetKey, reg.DatasetSequence).Scan(&r.RegistrationBlockedBy)
	}
	r.Readiness = RegistrationReadiness(r.Status, r.CatalogStatus)
	if r.Status == "COMMITTING" {
		switch r.CommitReconciliationStatus {
		case CommitReconciliationRetryRequired:
			r.Readiness = "COMMIT_RETRYING"
		case CommitReconciliationActionRequired:
			r.Readiness = "OPERATOR_ACTION_REQUIRED"
		default:
			r.Readiness = "COMMIT_RECONCILING"
		}
	}
}

func (s *Store) UpdateRunStatus(ctx context.Context, runID, status string, finished bool, errSummary *string) error {
	if status == "SUCCEEDED" {
		return fmt.Errorf("SUCCEEDED requires verified commit completion")
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateRunStatusTx(ctx, tx, runID, status, finished, errSummary)
	})
}

func (s *Store) CancelRun(ctx context.Context, runID, reason string) (bool, string, int, error) {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "canceled by client"
	}

	var (
		changed            bool
		status             string
		pendingTasksKilled int
	)
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		row := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?;`, runID)
		var cur string
		if err := row.Scan(&cur); err != nil {
			return err
		}
		switch cur {
		case "SUCCEEDED", "FAILED", "CANCELED", "COMMITTING":
			changed = false
			status = cur
			return tx.Commit()
		}

		finished := nowUTC()
		attemptRows, err := tx.QueryContext(ctx, `SELECT a.id,a.task_id,a.attempt_number,a.worker_id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.run_id=? AND a.status='ACTIVE'`, runID)
		if err != nil {
			return err
		}
		type canceledAttempt struct {
			id, taskID, workerID string
			number               int
		}
		var canceledAttempts []canceledAttempt
		for attemptRows.Next() {
			var a canceledAttempt
			if err := attemptRows.Scan(&a.id, &a.taskID, &a.number, &a.workerID); err != nil {
				attemptRows.Close()
				return err
			}
			canceledAttempts = append(canceledAttempts, a)
		}
		if err := attemptRows.Close(); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELED', finished_at=?, error_message=? WHERE run_id=? AND status='PENDING';`, finished, reason, runID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			pendingTasksKilled = int(n)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE task_attempts SET status='CANCELED', finished_at=?, failure_class='CANCELED', failure_message=?, updated_at=? WHERE status='ACTIVE' AND task_id IN (SELECT id FROM tasks WHERE run_id=?);`, finished, reason, finished, runID); err != nil {
			return err
		}
		for _, a := range canceledAttempts {
			if err := insertAttemptEventTx(ctx, tx, runID, a.taskID, a.id, a.number, a.workerID, "ATTEMPT_CANCELED", "RUN_CANCELED", finished, map[string]any{"reason": reason}); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELED', finished_at=?, error_message=?, current_attempt_id=NULL WHERE run_id=? AND status='RUNNING';`, finished, reason, runID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='CANCELED', finished_at=?, error_summary=? WHERE id=?;`, finished, reason, runID); err != nil {
			return err
		}
		if err := s.createCanceledObjectCandidatesTx(ctx, tx, runID, finished); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		changed = true
		status = "CANCELED"
		return nil
	})
	if err != nil {
		return false, "", 0, err
	}
	return changed, status, pendingTasksKilled, nil
}

func (s *Store) ListTasksForRun(ctx context.Context, runID string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT t.id,t.run_id,t.task_index,t.partition_spec_json,t.worker_id,t.status,t.rows_read,t.bytes_read,t.bytes_written,t.parquet_objects_json,t.started_at,t.finished_at,t.error_message,t.current_attempt_id,t.attempt_count,t.next_eligible_at,COALESCE(a.attempt_number,0),COALESCE(a.lease_deadline,''),COALESCE(a.last_renewed_at,''),COALESCE(a.status,''),COALESCE(a.failure_class,''),COALESCE(ar.artifact_count,0),COALESCE(ar.artifact_bytes,0),COALESCE(ar.artifact_rows,0),COALESCE(ar.verification_status,''),COALESCE(ar.verification_method,''),COALESCE(ar.verified_at,'') FROM tasks t LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.attempt_count LEFT JOIN (SELECT task_id,COUNT(*) artifact_count,SUM(byte_size) artifact_bytes,SUM(row_count) artifact_rows,MIN(verification_status) verification_status,MIN(verification_method) verification_method,MAX(verified_at) verified_at FROM task_artifacts GROUP BY task_id) ar ON ar.task_id=t.id WHERE t.run_id=? ORDER BY t.task_index ASC;`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Task
	for rows.Next() {
		var t Task
		var part, objs string
		if err := rows.Scan(&t.ID, &t.RunID, &t.TaskIndex, &part, &t.WorkerID, &t.Status, &t.RowsRead, &t.BytesRead, &t.BytesWritten, &objs, &t.StartedAt, &t.FinishedAt, &t.ErrorMessage, &t.CurrentAttemptID, &t.AttemptCount, &t.NextEligibleAt, &t.AttemptNumber, &t.LeaseDeadline, &t.LastRenewedAt, &t.AttemptStatus, &t.FailureClass, &t.ArtifactCount, &t.ArtifactBytes, &t.ArtifactRows, &t.VerificationStatus, &t.VerificationMethod, &t.ArtifactVerifiedAt); err != nil {
			return nil, err
		}
		t.PartitionSpec = []byte(part)
		t.ParquetObjects = []byte(objs)
		out = append(out, t)
	}
	return out, rows.Err()
}

type TaskInsert struct {
	ID             string
	RunID          string
	TaskIndex      int
	PartitionSpec  json.RawMessage
	ParquetObjects json.RawMessage
	Status         string
}

func (s *Store) InsertTasks(ctx context.Context, tasks []TaskInsert) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return insertTasksTx(ctx, tx, tasks)
	})
}

// AssignNextPendingTask atomically assigns the next pending task to a worker.
func (s *Store) AssignNextPendingTask(ctx context.Context, workerID string) (Task, bool, error) {
	// busy-retry wrapper for AssignNextPendingTask
	var (
		out Task
		ok  bool
	)
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		row := tx.QueryRowContext(ctx, `
				WITH running AS (
					SELECT run_id, COUNT(*) AS cnt
					FROM tasks
					WHERE status='RUNNING'
					GROUP BY run_id
				)
				SELECT t.id, t.run_id, t.task_index, t.partition_spec_json, t.status
				FROM tasks t
				JOIN runs r ON r.id = t.run_id
				JOIN jobs j ON j.id = r.job_id
				LEFT JOIN running rn ON rn.run_id = r.id
				WHERE t.status='PENDING'
				  AND r.status='RUNNING'
				  AND (
					COALESCE(CAST(json_extract(j.options_json, '$.max_in_flight_tasks') AS INTEGER), 0) <= 0
					OR COALESCE(rn.cnt, 0) < COALESCE(CAST(json_extract(j.options_json, '$.max_in_flight_tasks') AS INTEGER), 0)
				  )
				ORDER BY COALESCE(rn.cnt, 0) ASC, r.started_at ASC, t.run_id ASC, t.task_index ASC
				LIMIT 1;`)

		var (
			t      Task
			part   string
			status string
		)
		if err := row.Scan(&t.ID, &t.RunID, &t.TaskIndex, &part, &status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				ok = false
				return tx.Commit()
			}
			return err
		}
		started := nowUTC()
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='RUNNING', worker_id=?, started_at=? WHERE id=? AND status='PENDING';`, workerID, started, t.ID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			// Lost race; treat as no work.
			ok = false
			return tx.Commit()
		}

		t.PartitionSpec = []byte(part)
		t.Status = "RUNNING"
		t.WorkerID = &workerID
		t.StartedAt = &started

		if err := tx.Commit(); err != nil {
			return err
		}
		out = t
		ok = true
		return nil
	})
	if err != nil {
		return Task{}, false, err
	}
	return out, ok, nil
}

func (s *Store) UpdateWorkerHeartbeat(ctx context.Context, bootID, workerID, addr, capabilities, hostname, version string, pid int) error {
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		now := nowUTC()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO workers(id, addr, status, last_heartbeat, capabilities_json)
			VALUES (?, ?, 'ACTIVE', ?, ?)
			ON CONFLICT(id) DO UPDATE SET
				addr=COALESCE(NULLIF(excluded.addr, ''), workers.addr),
				status='ACTIVE',
				last_heartbeat=excluded.last_heartbeat,
				capabilities_json=COALESCE(NULLIF(excluded.capabilities_json, ''), workers.capabilities_json);`,
			workerID, addr, now, capabilities,
		)
		if err != nil {
			return err
		}

		if bootID != "" {
			_, err = tx.ExecContext(ctx, `
				INSERT INTO worker_instances(boot_id, worker_id, hostname, pid, version, status, started_at, last_heartbeat)
				VALUES (?, ?, ?, ?, ?, 'ACTIVE', ?, ?)
				ON CONFLICT(boot_id) DO UPDATE SET
					status='ACTIVE',
					last_heartbeat=excluded.last_heartbeat;`,
				bootID, workerID, hostname, pid, version, now, now,
			)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	})
	return err
}

func (s *Store) loadWorkerInstances(ctx context.Context, workers []Worker) error {
	if len(workers) == 0 {
		return nil
	}
	// Fetch all instances and group by worker
	rows, err := s.db.QueryContext(ctx, `SELECT boot_id, worker_id, hostname, pid, version, status, started_at, last_heartbeat FROM worker_instances ORDER BY last_heartbeat DESC`)
	if err != nil {
		return err
	}
	defer rows.Close()

	byWorker := make(map[string][]WorkerInstance)
	for rows.Next() {
		var inst WorkerInstance
		if err := rows.Scan(&inst.BootID, &inst.WorkerID, &inst.Hostname, &inst.PID, &inst.Version, &inst.Status, &inst.StartedAt, &inst.LastHeartbeat); err != nil {
			return err
		}
		byWorker[inst.WorkerID] = append(byWorker[inst.WorkerID], inst)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range workers {
		workers[i].Instances = byWorker[workers[i].ID]
	}
	return nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, addr, status, last_heartbeat, capabilities_json FROM workers ORDER BY last_heartbeat DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var capStr string
		if err := rows.Scan(&w.ID, &w.Addr, &w.Status, &w.LastHeartbeat, &capStr); err != nil {
			return nil, err
		}
		w.Capabilities = json.RawMessage(capStr)
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.loadWorkerInstances(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) ListWorkersActive(ctx context.Context, activeSince string) ([]Worker, error) {
	if strings.TrimSpace(activeSince) == "" {
		return s.ListWorkers(ctx)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, addr, status, last_heartbeat, capabilities_json FROM workers WHERE last_heartbeat >= ? ORDER BY last_heartbeat DESC;`, activeSince)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var capStr string
		if err := rows.Scan(&w.ID, &w.Addr, &w.Status, &w.LastHeartbeat, &capStr); err != nil {
			return nil, err
		}
		w.Capabilities = json.RawMessage(capStr)
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.loadWorkerInstances(ctx, out); err != nil {
		return nil, err
	}
	return out, nil
}

// TouchWorkerHeartbeat updates only liveness fields, preserving addr/capabilities.
func (s *Store) TouchWorkerHeartbeat(ctx context.Context, bootID, workerID string) error {
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		now := nowUTC()

		_, err = tx.ExecContext(ctx, `
			INSERT INTO workers(id, addr, status, last_heartbeat, capabilities_json)
			VALUES (?, '', 'ACTIVE', ?, '{}')
			ON CONFLICT(id) DO UPDATE SET
				status='ACTIVE',
				last_heartbeat=excluded.last_heartbeat;`, workerID, now)
		if err != nil {
			return err
		}

		if bootID != "" {
			_, err = tx.ExecContext(ctx, `
				UPDATE worker_instances SET status='ACTIVE', last_heartbeat=? WHERE boot_id=?;`,
				now, bootID)
			if err != nil {
				return err
			}
		}

		return tx.Commit()
	})
	return err
}

func (s *Store) UpdateTaskProgress(ctx context.Context, taskID string, rowsRead, bytesRead, bytesWritten int64) error {
	var err error
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `UPDATE tasks SET rows_read=?, bytes_read=?, bytes_written=? WHERE id=?;`, rowsRead, bytesRead, bytesWritten, taskID)
		return err
	})
	return err
}

func (s *Store) GetTaskExecutionState(ctx context.Context, taskID string) (TaskExecutionState, error) {
	var out TaskExecutionState
	row := s.db.QueryRowContext(ctx, `
		SELECT
			t.id,
			t.run_id,
			t.status,
			r.status,
			t.error_message,
			r.error_summary
		FROM tasks t
		JOIN runs r ON r.id = t.run_id
		WHERE t.id=?;`, taskID)
	if err := row.Scan(&out.TaskID, &out.RunID, &out.TaskStatus, &out.RunStatus, &out.TaskError, &out.RunError); err != nil {
		return TaskExecutionState{}, err
	}
	return out, nil
}

func (s *Store) GetTaskRunID(ctx context.Context, taskID string) (string, error) {
	row := s.db.QueryRowContext(ctx, `SELECT run_id FROM tasks WHERE id=?;`, taskID)
	var runID string
	if err := row.Scan(&runID); err != nil {
		return "", err
	}
	return runID, nil
}

func (s *Store) RequeueTaskAssignment(ctx context.Context, taskID, workerID string) error {
	return withBusyRetry(ctx, func() error {
		if strings.TrimSpace(workerID) == "" {
			_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status='PENDING', worker_id=NULL, started_at=NULL WHERE id=? AND status='RUNNING';`, taskID)
			return err
		}
		_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status='PENDING', worker_id=NULL, started_at=NULL WHERE id=? AND status='RUNNING' AND worker_id=?;`, taskID, workerID)
		return err
	})
}

func (s *Store) CompleteTask(ctx context.Context, taskID string, status string, errMsg *string, parquetObjectsJSON json.RawMessage, rowsRead, bytesRead, bytesWritten int64) (bool, string, string, error) {
	if status != "SUCCEEDED" && status != "FAILED" && status != "CANCELED" {
		return false, "", "", fmt.Errorf("invalid task status %q", status)
	}
	if len(parquetObjectsJSON) == 0 {
		parquetObjectsJSON = []byte(`[]`)
	}

	// busy-retry wrapper for CompleteTask
	var (
		accepted    bool
		msg         string
		finalStatus string
	)
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		row := tx.QueryRowContext(ctx, `
			SELECT
				t.status,
				r.status,
				t.error_message,
				r.error_summary
			FROM tasks t
			JOIN runs r ON r.id = t.run_id
			WHERE t.id=?;`, taskID)
		var (
			curStatus string
			runStatus string
			taskErr   *string
			runErr    *string
		)
		if err := row.Scan(&curStatus, &runStatus, &taskErr, &runErr); err != nil {
			return err
		}

		switch curStatus {
		case "SUCCEEDED":
			accepted = true
			msg = "already succeeded"
			finalStatus = "SUCCEEDED"
			return tx.Commit()
		case "FAILED":
			accepted = true
			msg = "already failed"
			finalStatus = "FAILED"
			return tx.Commit()
		case "CANCELED":
			accepted = true
			msg = "already canceled"
			finalStatus = "CANCELED"
			return tx.Commit()
		case "RUNNING", "PENDING":
			// ok
		default:
			return fmt.Errorf("unexpected current task status %q", curStatus)
		}

		finalStatus = status
		effectiveErr := errMsg
		if runStatus == "CANCELED" {
			finalStatus = "CANCELED"
			if effectiveErr == nil || strings.TrimSpace(*effectiveErr) == "" {
				switch {
				case runErr != nil && strings.TrimSpace(*runErr) != "":
					reason := strings.TrimSpace(*runErr)
					effectiveErr = &reason
				case taskErr != nil && strings.TrimSpace(*taskErr) != "":
					reason := strings.TrimSpace(*taskErr)
					effectiveErr = &reason
				default:
					reason := "canceled by client"
					effectiveErr = &reason
				}
			}
		}
		if finalStatus == "CANCELED" && runStatus != "CANCELED" {
			return fmt.Errorf("cannot mark task canceled while run status is %q", runStatus)
		}

		finished := nowUTC()
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET status=?, error_message=?, parquet_objects_json=?, rows_read=?, bytes_read=?, bytes_written=?, finished_at=? WHERE id=?;`,
			finalStatus, effectiveErr, string(parquetObjectsJSON), rowsRead, bytesRead, bytesWritten, finished, taskID)
		if err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		accepted = true
		if finalStatus != status {
			msg = "run canceled"
		} else {
			msg = "accepted"
		}
		return nil
	})
	if err != nil {
		return false, "", "", err
	}
	return accepted, msg, finalStatus, nil
}

// TryFinalizeRun moves a run to COMMITTING when all tasks are successful. Durable
// object publication must complete before CompleteRunCommit can mark it SUCCEEDED.
//
// Returns (changed=true, newStatus="SUCCEEDED"|"FAILED") when it performed an update.
func (s *Store) TryFinalizeRun(ctx context.Context, runID string) (bool, string, error) {
	// busy-retry wrapper for TryFinalizeRun
	var (
		changed bool
		status  string
	)
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()

		row := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?;`, runID)
		var cur string
		if err := row.Scan(&cur); err != nil {
			return err
		}
		if cur == "SUCCEEDED" || cur == "FAILED" || cur == "CANCELED" || cur == "COMMITTING" {
			changed = false
			status = cur
			return tx.Commit()
		}

		row = tx.QueryRowContext(ctx, `
			SELECT
				COUNT(*),
				SUM(CASE WHEN status='SUCCEEDED' THEN 1 ELSE 0 END),
				SUM(CASE WHEN status IN ('FAILED','QUARANTINED') THEN 1 ELSE 0 END)
			FROM tasks
			WHERE run_id=?;`, runID)
		var total, succ, fail int
		if err := row.Scan(&total, &succ, &fail); err != nil {
			return err
		}
		if total == 0 {
			return fmt.Errorf("run %s has no tasks", runID)
		}

		if fail > 0 {
			summary := fmt.Sprintf("%d task(s) failed", fail)
			fin := nowUTC()
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='FAILED', finished_at=?, error_summary=? WHERE id=?;`, fin, summary, runID); err != nil {
				return err
			}
			_, _ = tx.ExecContext(ctx, `UPDATE tasks SET status='CANCELED', finished_at=?, error_message='canceled' WHERE run_id=? AND status='PENDING';`, fin, runID)
			if err := tx.Commit(); err != nil {
				return err
			}
			changed = true
			status = "FAILED"
			return nil
		}
		if succ == total {
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='COMMITTING', finished_at=NULL, error_summary=NULL, failure_class='', commit_phase='PREPARING', commit_reconciliation_status='PENDING', commit_reconciliation_attempt_count=0, commit_reconciliation_next_eligible_at=NULL, operator_action_required=0 WHERE id=? AND status='RUNNING';`, runID); err != nil {
				return err
			}
			if err := tx.Commit(); err != nil {
				return err
			}
			changed = true
			status = "COMMITTING"
			return nil
		}

		changed = false
		status = cur
		return tx.Commit()
	})
	if err != nil {
		return false, "", err
	}
	return changed, status, nil
}

// SaveCommitIntent records the immutable, deterministic description of a run's
// publication. A retry may supply the same identity, but never replace it.
func (s *Store) SaveCommitIntent(ctx context.Context, runID, commitID string, intent []byte) error {
	return withBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE runs SET commit_id=?, commit_intent_json=?, commit_phase='INTENT' WHERE id=? AND status='COMMITTING' AND commit_id='';`, commitID, string(intent), runID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 1 {
			return nil
		}
		var gotID, gotIntent, status string
		if err := s.db.QueryRowContext(ctx, `SELECT commit_id, commit_intent_json, status FROM runs WHERE id=?;`, runID).Scan(&gotID, &gotIntent, &status); err != nil {
			return err
		}
		if status != "COMMITTING" {
			return fmt.Errorf("run %s is %s, not COMMITTING", runID, status)
		}
		if gotID != commitID || gotIntent != string(intent) {
			return fmt.Errorf("commit integrity conflict for run %s", runID)
		}
		return nil
	})
}

func (s *Store) SetCommitPhase(ctx context.Context, runID, phase string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runs SET commit_phase=? WHERE id=? AND status='COMMITTING';`, phase, runID)
	return err
}

func (s *Store) CompleteRunCommit(ctx context.Context, runID string) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var datasetID, commitID, configJSON, intentJSON string
		if err := tx.QueryRowContext(ctx, `SELECT dataset_key,commit_id,registration_config_json,commit_intent_json FROM runs WHERE id=? AND status='COMMITTING' AND commit_id<>'' AND commit_phase='VERIFIED'`, runID).Scan(&datasetID, &commitID, &configJSON, &intentJSON); err != nil {
			if err == sql.ErrNoRows {
				return fmt.Errorf("run %s commit completion precondition failed", runID)
			}
			return err
		}
		now := nowUTC()
		if err := ensureRegistrationTx(ctx, tx, runID, datasetID, commitID, configJSON, intentJSON, now); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE runs SET status='SUCCEEDED', finished_at=?, error_summary=NULL, failure_class='', commit_phase='COMPLETE', commit_reconciliation_status='COMPLETE', commit_reconciliation_next_eligible_at=NULL, operator_action_required=0 WHERE id=? AND status='COMMITTING' AND commit_id=? AND commit_phase='VERIFIED';`, now, runID, commitID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("run %s commit completion precondition failed", runID)
		}
		fields, err := json.Marshal(map[string]any{"commit_id": commitID, "finalization_phase": "COMPLETE"})
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'INFO','run committed',?)`,
			"commit-"+commitID, runID, now, string(fields)); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) ListCommittingRunIDs(ctx context.Context) ([]string, error) {
	return s.ListCommittingRunIDsAt(ctx, time.Now().UTC())
}

func (s *Store) ListCommittingRunIDsAt(ctx context.Context, now time.Time) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM runs WHERE status='COMMITTING' AND commit_reconciliation_status IN ('','PENDING','RETRY_REQUIRED') AND (commit_reconciliation_next_eligible_at IS NULL OR commit_reconciliation_next_eligible_at<=?) ORDER BY started_at;`, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// --- Events

func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	if len(e.FieldsJSON) == 0 {
		e.FieldsJSON = []byte(`{}`)
	}
	var err error
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO events(id, run_id, task_id, ts, level, message, fields_json) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			e.ID, e.RunID, e.TaskID, e.TS, e.Level, e.Message, string(e.FieldsJSON))
		return err
	})
	return err
}

// InsertEventOnce records an idempotent lifecycle observation. Callers must use
// a stable event ID that identifies the logical transition or observation.
func (s *Store) InsertEventOnce(ctx context.Context, e Event) error {
	if len(e.FieldsJSON) == 0 {
		e.FieldsJSON = []byte(`{}`)
	}
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO events(id, run_id, task_id, ts, level, message, fields_json) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			e.ID, e.RunID, e.TaskID, e.TS, e.Level, e.Message, string(e.FieldsJSON))
		return err
	})
}

func (s *Store) ListEventsForRun(ctx context.Context, runID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id, run_id, task_id, ts, level, message, fields_json FROM events WHERE run_id=? ORDER BY ts ASC LIMIT ?;`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var fields string
		if err := rows.Scan(&e.ID, &e.RunID, &e.TaskID, &e.TS, &e.Level, &e.Message, &fields); err != nil {
			return nil, err
		}
		e.FieldsJSON = []byte(fields)
		out = append(out, e)
	}
	return out, rows.Err()
}

// MaxPartIndexForJob scans completed task outputs for a job and returns the max part number
// found in object keys like ".../part-000123-000.parquet". It also accepts
// the legacy base-file form so existing completed runs remain visible.
//
// This is used to keep part numbering monotonically increasing when writing into a shared prefix.
func (s *Store) MaxPartIndexForJob(ctx context.Context, jobID string) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.parquet_objects_json
		FROM tasks t
		JOIN runs r ON r.id = t.run_id
		WHERE r.job_id=? AND t.status='SUCCEEDED';`, jobID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	re := regexp.MustCompile(`(?:^|/)part-(\d+)(?:-\d+)?\.parquet$`)
	max := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		var arr []map[string]any
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			continue
		}
		for _, o := range arr {
			k, _ := o["key"].(string)
			if k == "" {
				continue
			}
			m := re.FindStringSubmatch(k)
			if len(m) != 2 {
				continue
			}
			v, err := strconv.Atoi(m[1])
			if err != nil {
				continue
			}
			if v > max {
				max = v
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return max, nil
}

// --- HWM

func (s *Store) GetHWM(ctx context.Context, jobID string) (string, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT hwm_value FROM hwm WHERE job_id=?;`, jobID)
	var v string
	if err := row.Scan(&v); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return v, true, nil
}

func (s *Store) UpsertHWM(ctx context.Context, jobID, value string) error {
	var err error
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO hwm(job_id, hwm_value, updated_at) VALUES (?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET hwm_value=excluded.hwm_value, updated_at=excluded.updated_at;`, jobID, value, nowUTC())
		return err
	})
	return err
}
