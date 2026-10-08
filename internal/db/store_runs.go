package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

const encryptedRegistrationConfigPrefix = "enc:v1:"

func runRegistrationConfigAAD(runID string) []byte {
	return []byte("run-registration-config:" + runID)
}

func (s *Store) encryptRunRegistrationConfig(runID string, plaintext []byte) (string, error) {
	return encryptStoredJSON(s.masterKey, plaintext, runRegistrationConfigAAD(runID))
}

func (s *Store) decryptRunRegistrationConfig(runID, stored string) ([]byte, error) {
	plaintext, err := decryptStoredJSON(s.masterKey, stored, runRegistrationConfigAAD(runID))
	if err != nil {
		return nil, fmt.Errorf("run %s registration config: %w", runID, err)
	}
	return plaintext, nil
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

// FailAbandonedPlanningRuns fails runs left in PLANNING without any tasks.
// Planning creates the run and inserts its tasks in one request, so such a
// run was interrupted (for example by a master crash) and would otherwise hold
// its dataset forever. Call it only during startup recovery, before the HTTP
// API accepts new runs; a PLANNING run with tasks is a queued run and is kept.
func (s *Store) FailAbandonedPlanningRuns(ctx context.Context, now time.Time) ([]string, error) {
	var failed []string
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		failed = failed[:0]
		rows, err := tx.QueryContext(ctx, `SELECT id FROM runs r WHERE r.status='PLANNING' AND NOT EXISTS (SELECT 1 FROM tasks t WHERE t.run_id=r.id)`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			failed = append(failed, id)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		const reason = "run planning was interrupted before tasks were created"
		ts := now.UTC().Format(TimestampLayout)
		for _, id := range failed {
			// The master stopped mid-planning: whether planning would have
			// succeeded is unknown, so the failure is classified as ambiguous.
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='FAILED', finished_at=?, error_summary=?, failure_class=?, failure_phase=? WHERE id=? AND status='PLANNING'`, ts, reason, string(failure.FailureUnknownAmbiguous), RunFailurePhasePlanning, id); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'ERROR',?,?)`,
				"planning-abandoned-"+id, id, ts, "run FAILED", `{"event_type":"RUN_PLANNING_ABANDONED"}`); err != nil {
				return err
			}
		}
		return nil
	})
	return failed, err
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
	FailurePhase                   string                    `json:"failure_phase,omitempty"`
	TypeWarnings                   []typesystem.TypeWarning  `json:"type_warnings"`
	RegistrationConfigJSON         json.RawMessage           `json:"-"`
	ConfigSnapshotJSON             json.RawMessage           `json:"-"`
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

func (s *Store) FindActiveRunByDatasetKey(ctx context.Context, datasetKey string) (Run, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, failure_phase
		FROM runs
		WHERE dataset_key=? AND status IN ('PLANNING','RUNNING','COMMITTING')
		ORDER BY started_at ASC
		LIMIT 1;`, datasetKey)
	var r Run
	if err := row.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &r.FailurePhase); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, false, nil
		}
		return Run{}, false, err
	}
	return r, true, nil
}

func (s *Store) CreateRun(ctx context.Context, r Run) error {
	var err error
	registrationConfig, err := s.encryptRunRegistrationConfig(
		r.ID,
		r.RegistrationConfigJSON,
	)
	if err != nil {
		return err
	}
	warnings, err := json.Marshal(r.TypeWarnings)
	if err != nil {
		return err
	}
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO runs(id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, failure_phase, registration_config_json, type_warnings_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
			r.ID, r.JobID, r.DatasetKey, r.Status, r.CorrelationID, normalizeTimestamp(r.StartedAt), r.FinishedAt, r.ErrorSummary, r.FailureClass, r.FailurePhase, registrationConfig, string(warnings))
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

func (s *Store) SetRunTypeWarnings(ctx context.Context, runID string, warnings []typesystem.TypeWarning) error {
	raw, err := json.Marshal(warnings)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE runs SET type_warnings_json=? WHERE id=?`, string(raw), runID)
	return err
}

func (s *Store) ListRuns(ctx context.Context) ([]Run, error) {
	out, _, err := s.ListRunsPage(ctx, 0, "")
	return out, err
}

// ListRunsPage lists runs newest first, paged like ListConnectionsPage.
func (s *Store) ListRunsPage(ctx context.Context, limit int, cursor string) ([]Run, string, error) {
	query, args, err := keysetPage(`SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, failure_phase, registration_config_json, type_warnings_json, commit_id, commit_intent_json, commit_phase FROM runs`, "started_at", limit, cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", wrapRunRegistrationConfigColumnErr(err)
	}
	var out []Run
	for rows.Next() {
		var r Run
		var registrationConfig, warningJSON, commitIntent string
		if err := rows.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &r.FailurePhase, &registrationConfig, &warningJSON, &r.CommitID, &commitIntent, &r.CommitPhase); err != nil {
			return nil, "", err
		}
		if strings.TrimSpace(registrationConfig) != "" {
			decrypted, err := s.decryptRunRegistrationConfig(r.ID, registrationConfig)
			if err != nil {
				return nil, "", err
			}
			r.RegistrationConfigJSON = decrypted
		}
		if strings.TrimSpace(commitIntent) != "" {
			r.CommitIntentJSON = []byte(commitIntent)
		}
		if err := decodeRunWarnings(warningJSON, &r); err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, "", err
	}
	if err := rows.Close(); err != nil {
		return nil, "", err
	}
	next := ""
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = formatEventCursor(last.StartedAt, last.ID)
	}
	for i := range out {
		s.attachCommitReconciliationProjection(ctx, &out[i])
		s.attachRegistrationProjection(ctx, &out[i])
	}
	return out, next, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (Run, error) {
	var r Run
	var registrationConfig, warningJSON, commitIntent, configSnapshot string
	row := s.db.QueryRowContext(ctx, `SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, failure_phase, registration_config_json, type_warnings_json, commit_id, commit_intent_json, commit_phase, config_snapshot_json FROM runs WHERE id=?;`, id)
	if err := row.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &r.FailurePhase, &registrationConfig, &warningJSON, &r.CommitID, &commitIntent, &r.CommitPhase, &configSnapshot); err != nil {
		return Run{}, wrapRunRegistrationConfigColumnErr(err)
	}
	if strings.TrimSpace(configSnapshot) != "" {
		r.ConfigSnapshotJSON = []byte(configSnapshot)
	}
	if err := decodeRunWarnings(warningJSON, &r); err != nil {
		return Run{}, err
	}
	if strings.TrimSpace(registrationConfig) != "" {
		decrypted, err := s.decryptRunRegistrationConfig(r.ID, registrationConfig)
		if err != nil {
			return Run{}, err
		}
		r.RegistrationConfigJSON = decrypted
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
	rows, err := s.db.QueryContext(ctx, `SELECT id, job_id, dataset_key, status, correlation_id, started_at, finished_at, error_summary, failure_class, failure_phase, registration_config_json, type_warnings_json FROM runs WHERE job_id = ? AND status = 'SUCCEEDED' ORDER BY started_at ASC;`, jobID)
	if err != nil {
		return nil, wrapRunRegistrationConfigColumnErr(err)
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var registrationConfig, warningJSON string
		if err := rows.Scan(&r.ID, &r.JobID, &r.DatasetKey, &r.Status, &r.CorrelationID, &r.StartedAt, &r.FinishedAt, &r.ErrorSummary, &r.FailureClass, &r.FailurePhase, &registrationConfig, &warningJSON); err != nil {
			return nil, err
		}
		if err := decodeRunWarnings(warningJSON, &r); err != nil {
			return nil, err
		}
		if strings.TrimSpace(registrationConfig) != "" {
			decrypted, err := s.decryptRunRegistrationConfig(r.ID, registrationConfig)
			if err != nil {
				return nil, err
			}
			r.RegistrationConfigJSON = decrypted
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
		_ = s.rdb.QueryRowContext(ctx, `SELECT id FROM iceberg_registrations WHERE dataset_id=? AND target_key=? AND dataset_sequence<? AND status<>'REGISTERED' ORDER BY dataset_sequence LIMIT 1`, reg.DatasetID, reg.TargetKey, reg.DatasetSequence).Scan(&r.RegistrationBlockedBy)
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
			summary, class, err := taskFailureSummary(ctx, tx, runID, fail, total)
			if err != nil {
				return err
			}
			fin := nowUTC()
			if _, err := tx.ExecContext(ctx, `UPDATE runs SET status='FAILED', finished_at=?, error_summary=?, failure_class=?, failure_phase=? WHERE id=?;`, fin, summary, class, RunFailurePhaseExtract, runID); err != nil {
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

		configBytes, err := s.decryptRunRegistrationConfig(runID, configJSON)
		if err != nil {
			return err
		}

		if err := ensureRegistrationTx(
			ctx,
			tx,
			runID,
			datasetID,
			commitID,
			string(configBytes),
			intentJSON,
			now,
		); err != nil {
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
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM runs WHERE status='COMMITTING' AND commit_reconciliation_status IN ('','PENDING','RETRY_REQUIRED') AND (commit_reconciliation_next_eligible_at IS NULL OR commit_reconciliation_next_eligible_at<=?) ORDER BY started_at;`, now.UTC().Format(TimestampLayout))
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
