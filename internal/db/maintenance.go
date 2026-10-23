package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Maintenance operation kinds and statuses. An operation is PENDING until the
// leader claims it, RUNNING while it holds a lease, and SUBMITTED when a
// compaction has handed its work to a run; the run's commit finishes it.
const (
	MaintenanceOrphanCleanup = "ORPHAN_CLEANUP"
	MaintenanceCompaction    = "COMPACTION"

	MaintenancePending   = "PENDING"
	MaintenanceRunning   = "RUNNING"
	MaintenanceSubmitted = "SUBMITTED"
	MaintenanceSucceeded = "SUCCEEDED"
	MaintenanceFailed    = "FAILED"
	MaintenanceAborted   = "ABORTED"
)

var (
	// ErrMaintenanceBusy means the job already has an active operation.
	ErrMaintenanceBusy = errors.New("job already has an active maintenance operation")
	// ErrMaintenanceFenced means the caller no longer holds the operation's
	// lease; it must stop without changing anything else.
	ErrMaintenanceFenced = errors.New("maintenance operation lease lost")
)

// MaintenanceOperation is one explicit orphan-cleanup or compaction request
// and its outcome.
type MaintenanceOperation struct {
	ID               string          `json:"id"`
	Kind             string          `json:"kind"`
	JobID            string          `json:"job_id"`
	DryRun           bool            `json:"dry_run"`
	Params           json.RawMessage `json:"params"`
	Status           string          `json:"status"`
	SourceRunID      string          `json:"source_run_id,omitempty"`
	TableIdentifier  string          `json:"table_identifier,omitempty"`
	TableUUID        string          `json:"table_uuid,omitempty"`
	TableLocation    string          `json:"table_location,omitempty"`
	SnapshotID       string          `json:"snapshot_id,omitempty"`
	MetadataLocation string          `json:"metadata_location,omitempty"`
	RunID            string          `json:"run_id,omitempty"`
	Report           json.RawMessage `json:"report"`
	ErrorClass       string          `json:"error_class,omitempty"`
	ErrorMessage     string          `json:"error_message,omitempty"`
	AttemptCount     int             `json:"attempt_count"`
	CreatedAt        string          `json:"created_at"`
	UpdatedAt        string          `json:"updated_at"`
	StartedAt        string          `json:"started_at,omitempty"`
	FinishedAt       string          `json:"finished_at,omitempty"`

	// LeaseToken is the fencing token of the current claim; never serialized.
	LeaseToken string `json:"-"`
}

// MaintenanceCandidate is one object an operation examined: an orphan it
// would delete (or deleted), an object it kept and why, or a compaction input.
type MaintenanceCandidate struct {
	ObjectKey    string          `json:"object_key"`
	SizeBytes    int64           `json:"size_bytes"`
	LastModified string          `json:"last_modified,omitempty"`
	AgeSeconds   int64           `json:"age_seconds"`
	Decision     string          `json:"decision"`
	Reason       string          `json:"reason"`
	Evidence     json.RawMessage `json:"evidence"`
	Status       string          `json:"status"`
	UpdatedAt    string          `json:"updated_at"`
}

const maintenanceSelect = `SELECT id,kind,job_id,dry_run,params_json,status,source_run_id,table_identifier,table_uuid,table_location,snapshot_id,metadata_location,run_id,report_json,error_class,error_message,attempt_count,lease_token,created_at,updated_at,COALESCE(started_at,''),COALESCE(finished_at,'') FROM maintenance_operations`

func scanMaintenance(row rowScanner) (MaintenanceOperation, error) {
	var op MaintenanceOperation
	var dryRun int
	var params, report string
	err := row.Scan(&op.ID, &op.Kind, &op.JobID, &dryRun, &params, &op.Status, &op.SourceRunID, &op.TableIdentifier, &op.TableUUID, &op.TableLocation, &op.SnapshotID, &op.MetadataLocation, &op.RunID, &report, &op.ErrorClass, &op.ErrorMessage, &op.AttemptCount, &op.LeaseToken, &op.CreatedAt, &op.UpdatedAt, &op.StartedAt, &op.FinishedAt)
	op.DryRun = dryRun != 0
	op.Params, op.Report = json.RawMessage(params), json.RawMessage(report)
	return op, err
}

// CreateMaintenanceOperation stores a PENDING operation. It returns
// ErrMaintenanceBusy when the job already has an active one, so cleanup and
// compaction of one table never overlap.
func (s *Store) CreateMaintenanceOperation(ctx context.Context, op MaintenanceOperation, now time.Time) error {
	if len(op.Params) == 0 {
		op.Params = json.RawMessage(`{}`)
	}
	ns := FormatTimestamp(now)
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `INSERT INTO maintenance_operations(id,kind,job_id,dry_run,params_json,status,created_at,updated_at) VALUES(?,?,?,?,?,'PENDING',?,?)`,
			op.ID, op.Kind, op.JobID, boolInt(op.DryRun), string(op.Params), ns, ns)
		if err != nil && strings.Contains(err.Error(), "idx_maintenance_one_active_per_job") {
			return ErrMaintenanceBusy
		}
		if err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed: maintenance_operations.job_id") {
			return ErrMaintenanceBusy
		}
		return err
	})
}

// GetMaintenanceOperation returns one operation or sql.ErrNoRows.
func (s *Store) GetMaintenanceOperation(ctx context.Context, id string) (MaintenanceOperation, error) {
	return scanMaintenance(s.db.QueryRowContext(ctx, maintenanceSelect+` WHERE id=?`, id))
}

// GetMaintenanceOperationForRun returns the compaction operation that
// created runID, if any.
func (s *Store) GetMaintenanceOperationForRun(ctx context.Context, runID string) (MaintenanceOperation, bool, error) {
	if strings.TrimSpace(runID) == "" {
		return MaintenanceOperation{}, false, nil
	}
	op, err := scanMaintenance(s.db.QueryRowContext(ctx, maintenanceSelect+` WHERE run_id=? AND kind='COMPACTION'`, runID))
	if errors.Is(err, sql.ErrNoRows) {
		return MaintenanceOperation{}, false, nil
	}
	return op, err == nil, err
}

// ListMaintenanceOperations lists a job's operations, newest first; an empty
// jobID lists every job's.
func (s *Store) ListMaintenanceOperations(ctx context.Context, jobID string, limit int) ([]MaintenanceOperation, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query, args := maintenanceSelect+` ORDER BY created_at DESC,id LIMIT ?`, []any{limit}
	if strings.TrimSpace(jobID) != "" {
		query, args = maintenanceSelect+` WHERE job_id=? ORDER BY created_at DESC,id LIMIT ?`, []any{jobID, limit}
	}
	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MaintenanceOperation
	for rows.Next() {
		op, err := scanMaintenance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, op)
	}
	return out, rows.Err()
}

// ListMaintenanceCandidates returns an operation's report rows in key order.
func (s *Store) ListMaintenanceCandidates(ctx context.Context, operationID string) ([]MaintenanceCandidate, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT object_key,size_bytes,last_modified,age_seconds,decision,reason,evidence_json,status,updated_at FROM maintenance_candidates WHERE operation_id=? ORDER BY object_key`, operationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MaintenanceCandidate
	for rows.Next() {
		var c MaintenanceCandidate
		var evidence string
		if err := rows.Scan(&c.ObjectKey, &c.SizeBytes, &c.LastModified, &c.AgeSeconds, &c.Decision, &c.Reason, &evidence, &c.Status, &c.UpdatedAt); err != nil {
			return nil, err
		}
		c.Evidence = json.RawMessage(evidence)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ClaimMaintenanceOperation leases the oldest PENDING operation, or a RUNNING
// one whose lease expired (its holder crashed or lost leadership). Each claim
// gets a fresh fencing token; every later write must present it.
func (s *Store) ClaimMaintenanceOperation(ctx context.Context, now time.Time, lease time.Duration) (MaintenanceOperation, bool, error) {
	var op MaintenanceOperation
	ok := false
	err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		ns := FormatTimestamp(now)
		claimed, err := scanMaintenance(tx.QueryRowContext(ctx, maintenanceSelect+` WHERE status='PENDING' OR (status='RUNNING' AND lease_deadline<=?) ORDER BY created_at,id LIMIT 1`, ns))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		token, err := randomMaintenanceToken()
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE maintenance_operations SET status='RUNNING',lease_token=?,lease_deadline=?,attempt_count=attempt_count+1,
			leader_epoch=(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'),
			started_at=COALESCE(started_at,?),updated_at=? WHERE id=? AND lease_token=?`,
			token, FormatTimestamp(now.Add(lease)), ns, ns, claimed.ID, claimed.LeaseToken)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return nil
		}
		claimed.Status, claimed.LeaseToken, claimed.AttemptCount = MaintenanceRunning, token, claimed.AttemptCount+1
		op, ok = claimed, true
		return nil
	})
	return op, ok, err
}

// RenewMaintenanceLease extends a held lease. It fails with
// ErrMaintenanceFenced when the lease expired or another claim replaced it,
// which is checked before every destructive step.
func (s *Store) RenewMaintenanceLease(ctx context.Context, id, token string, now time.Time, lease time.Duration) error {
	return withBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE maintenance_operations SET lease_deadline=?,updated_at=? WHERE id=? AND status='RUNNING' AND lease_token=? AND lease_deadline>?`,
			FormatTimestamp(now.Add(lease)), FormatTimestamp(now), id, token, FormatTimestamp(now))
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrMaintenanceFenced
		}
		return nil
	})
}

// MaintenanceTable is the catalog evidence an operation recorded.
type MaintenanceTable struct {
	SourceRunID, Identifier, UUID, Location, SnapshotID, MetadataLocation string
}

// RecordMaintenancePlan replaces the operation's report rows and stores the
// table evidence they were computed against, under the caller's lease.
func (s *Store) RecordMaintenancePlan(ctx context.Context, id, token string, table MaintenanceTable, candidates []MaintenanceCandidate, report any, now time.Time) error {
	reportJSON, err := json.Marshal(report)
	if err != nil {
		return err
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		ns := FormatTimestamp(now)
		res, err := tx.ExecContext(ctx, `UPDATE maintenance_operations SET source_run_id=?,table_identifier=?,table_uuid=?,table_location=?,snapshot_id=?,metadata_location=?,report_json=?,updated_at=? WHERE id=? AND status='RUNNING' AND lease_token=? AND lease_deadline>?`,
			table.SourceRunID, table.Identifier, table.UUID, table.Location, table.SnapshotID, table.MetadataLocation, string(reportJSON), ns, id, token, ns)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrMaintenanceFenced
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM maintenance_candidates WHERE operation_id=?`, id); err != nil {
			return err
		}
		stmt, err := tx.PrepareContext(ctx, `INSERT INTO maintenance_candidates(operation_id,object_key,size_bytes,last_modified,age_seconds,decision,reason,evidence_json,status,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, c := range candidates {
			evidence := string(c.Evidence)
			if evidence == "" {
				evidence = "{}"
			}
			if _, err := stmt.ExecContext(ctx, id, c.ObjectKey, c.SizeBytes, c.LastModified, c.AgeSeconds, c.Decision, c.Reason, evidence, c.Status, ns); err != nil {
				return fmt.Errorf("record maintenance candidate %s: %w", c.ObjectKey, err)
			}
		}
		return nil
	})
}

// SetMaintenanceCandidateStatus records what happened to one candidate,
// under the caller's lease.
func (s *Store) SetMaintenanceCandidateStatus(ctx context.Context, id, token, key, status string, now time.Time) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		ns := FormatTimestamp(now)
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM maintenance_operations WHERE id=? AND status='RUNNING' AND lease_token=? AND lease_deadline>?`, id, token, ns).Scan(new(int)); err != nil {
			return ErrMaintenanceFenced
		}
		_, err := tx.ExecContext(ctx, `UPDATE maintenance_candidates SET status=?,updated_at=? WHERE operation_id=? AND object_key=?`, status, ns, id, key)
		return err
	})
}

// SetMaintenanceRunID links a compaction operation to the run it created,
// under the caller's lease.
func (s *Store) SetMaintenanceRunID(ctx context.Context, id, token, runID string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		ns := FormatTimestamp(now)
		res, err := s.db.ExecContext(ctx, `UPDATE maintenance_operations SET run_id=?,updated_at=? WHERE id=? AND status='RUNNING' AND lease_token=? AND lease_deadline>?`, runID, ns, id, token, ns)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrMaintenanceFenced
		}
		return nil
	})
}

// FinishMaintenanceOperation ends a RUNNING operation held with token. A
// compaction that started a run ends as SUBMITTED with that run's ID.
func (s *Store) FinishMaintenanceOperation(ctx context.Context, id, token, status, runID string, report any, errClass, errMessage string, now time.Time) error {
	var reportJSON any
	if report != nil {
		b, err := json.Marshal(report)
		if err != nil {
			return err
		}
		reportJSON = string(b)
	}
	return withBusyRetry(ctx, func() error {
		ns := FormatTimestamp(now)
		var finished any = ns
		if status == MaintenanceSubmitted {
			finished = nil
		}
		res, err := s.db.ExecContext(ctx, `UPDATE maintenance_operations SET status=?,run_id=CASE WHEN ?<>'' THEN ? ELSE run_id END,report_json=COALESCE(?,report_json),error_class=?,error_message=?,lease_token='',lease_deadline=NULL,finished_at=?,updated_at=? WHERE id=? AND status='RUNNING' AND lease_token=?`,
			status, runID, runID, reportJSON, errClass, errMessage, finished, ns, id, token)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrMaintenanceFenced
		}
		return nil
	})
}

// CompleteCompactionOperation records the outcome of a SUBMITTED compaction
// when its run commits or fails. Candidate rows follow the outcome.
func (s *Store) CompleteCompactionOperation(ctx context.Context, runID, status, errClass, errMessage string, now time.Time) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		ns := FormatTimestamp(now)
		var id string
		if err := tx.QueryRowContext(ctx, `SELECT id FROM maintenance_operations WHERE run_id=? AND kind='COMPACTION' AND status='SUBMITTED'`, runID).Scan(&id); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE maintenance_operations SET status=?,error_class=?,error_message=?,finished_at=?,updated_at=? WHERE id=?`, status, errClass, errMessage, ns, ns, id); err != nil {
			return err
		}
		if status == MaintenanceSucceeded {
			_, err := tx.ExecContext(ctx, `UPDATE maintenance_candidates SET status='COMPACTED',updated_at=? WHERE operation_id=? AND decision='COMPACT'`, ns, id)
			return err
		}
		return nil
	})
}

// FailSubmittedCompactions ends SUBMITTED compactions whose run ended without
// committing (task failure or cancellation).
func (s *Store) FailSubmittedCompactions(ctx context.Context, now time.Time) (int64, error) {
	var n int64
	err := withBusyRetry(ctx, func() error {
		ns := FormatTimestamp(now)
		res, err := s.db.ExecContext(ctx, `UPDATE maintenance_operations SET status=CASE WHEN (SELECT status FROM runs WHERE id=run_id)='CANCELED' THEN 'ABORTED' ELSE 'FAILED' END,
			error_class=CASE WHEN error_class='' THEN 'COMPACTION_RUN_'||(SELECT status FROM runs WHERE id=run_id) ELSE error_class END,
			error_message=CASE WHEN error_message='' THEN COALESCE((SELECT error_summary FROM runs WHERE id=run_id),'') ELSE error_message END,
			finished_at=?,updated_at=?
			WHERE kind='COMPACTION' AND status='SUBMITTED' AND (SELECT status FROM runs WHERE id=run_id) IN ('FAILED','CANCELED')`, ns, ns)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	return n, err
}

// ArtifactFingerprints maps the given object keys to the schema fingerprint
// of the verified artifact that produced them. Keys O_Rabbit did not write
// are absent.
func (s *Store) ArtifactFingerprints(ctx context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	const batch = 500
	for start := 0; start < len(keys); start += batch {
		end := min(start+batch, len(keys))
		args := make([]any, 0, end-start)
		for _, k := range keys[start:end] {
			args = append(args, k)
		}
		rows, err := s.rdb.QueryContext(ctx, `SELECT object_key,schema_fingerprint FROM task_artifacts WHERE object_key IN (?`+strings.Repeat(",?", len(args)-1)+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var key, fp string
			if err := rows.Scan(&key, &fp); err != nil {
				rows.Close()
				return nil, err
			}
			out[key] = fp
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// LatestRegisteredRun returns the job's newest run whose Iceberg registration
// is REGISTERED; its persisted registration config identifies the table.
func (s *Store) LatestRegisteredRun(ctx context.Context, jobID string) (Run, bool, error) {
	var runID string
	err := s.db.QueryRowContext(ctx, `SELECT r.id FROM runs r JOIN iceberg_registrations g ON g.run_id=r.id
		WHERE r.job_id=? AND r.status='SUCCEEDED' AND g.status='REGISTERED' ORDER BY r.started_at DESC,r.id DESC LIMIT 1`, jobID).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, false, nil
	}
	if err != nil {
		return Run{}, false, err
	}
	run, err := s.GetRun(ctx, runID)
	return run, err == nil, err
}

// DatasetRegistrationsInFlight counts the dataset's registrations that have
// not reached a terminal state; their files may not be in the catalog yet.
func (s *Store) DatasetRegistrationsInFlight(ctx context.Context, datasetKey string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_registrations WHERE dataset_id=? AND status IN ('PENDING','REGISTERING','RETRY_REQUIRED','RECONCILING','BLOCKED')`, datasetKey).Scan(&n)
	return n, err
}

// RunRegistrationStatus returns the registration status of runID, or "" when
// the run has no registration.
func (s *Store) RunRegistrationStatus(ctx context.Context, runID string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM iceberg_registrations WHERE run_id=? ORDER BY id LIMIT 1`, runID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return status, err
}

func randomMaintenanceToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
