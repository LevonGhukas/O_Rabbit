package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type DiagnosisAttempt struct {
	ID            string `json:"attempt_id"`
	Number        int    `json:"attempt_number"`
	Status        string `json:"status"`
	WorkerID      string `json:"worker_id,omitempty"`
	LeaseDeadline string `json:"lease_deadline,omitempty"`
	LeaseExpired  bool   `json:"lease_expired"`
	FailureClass  string `json:"failure_class,omitempty"`
}

type DiagnosisTask struct {
	ID             string             `json:"task_id"`
	Index          int                `json:"task_index"`
	Status         string             `json:"status"`
	WorkerID       string             `json:"worker_id,omitempty"`
	AttemptCount   int                `json:"attempt_count"`
	MaxAttempts    int                `json:"max_attempts"`
	NextEligibleAt string             `json:"next_eligible_at,omitempty"`
	Blocking       bool               `json:"blocking"`
	RecentAttempts []DiagnosisAttempt `json:"recent_attempts,omitempty"`
}

type DiagnosisRegistration struct {
	ID              string                   `json:"registration_id"`
	Status          string                   `json:"status"`
	AttemptCount    int                      `json:"attempt_count"`
	CurrentPhase    string                   `json:"current_phase,omitempty"`
	LeaseDeadline   string                   `json:"lease_deadline,omitempty"`
	LeaseExpired    bool                     `json:"lease_expired"`
	ReceiptRecorded bool                     `json:"receipt_recorded"`
	LastErrorClass  string                   `json:"last_error_class,omitempty"`
	NextEligibleAt  string                   `json:"next_eligible_at,omitempty"`
	BlockedBy       string                   `json:"blocked_by,omitempty"`
	Reconciliation  ReconciliationProjection `json:"reconciliation"`
}

type RunDiagnosis struct {
	RunID               string                 `json:"run_id"`
	Status              string                 `json:"status"`
	CommitPhase         string                 `json:"commit_phase,omitempty"`
	CommitAgeSeconds    float64                `json:"commit_age_seconds,omitempty"`
	LastClassifiedError string                 `json:"last_classified_error,omitempty"`
	TaskCounts          map[string]int         `json:"task_counts"`
	BlockingTasks       []DiagnosisTask        `json:"blocking_tasks,omitempty"`
	Registration        *DiagnosisRegistration `json:"registration,omitempty"`
	OperatorReview      bool                   `json:"operator_review_required"`
	SuggestedNextAction string                 `json:"suggested_next_action"`
	GeneratedAt         string                 `json:"generated_at"`
}

func (s *Store) DiagnoseRun(ctx context.Context, runID string, now time.Time, maxAttempts int) (RunDiagnosis, error) {
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	run, err := s.GetRun(ctx, runID)
	if err != nil {
		return RunDiagnosis{}, err
	}
	out := RunDiagnosis{
		RunID:       run.ID,
		Status:      run.Status,
		CommitPhase: run.CommitPhase,
		TaskCounts:  map[string]int{},
		GeneratedAt: now.UTC().Format(time.RFC3339Nano),
	}
	if run.Status == "COMMITTING" {
		if started, parseErr := time.Parse(time.RFC3339Nano, run.StartedAt); parseErr == nil && now.After(started) {
			out.CommitAgeSeconds = now.Sub(started).Seconds()
		}
	}

	tasks, err := s.ListTasksForRun(ctx, runID)
	if err != nil {
		return RunDiagnosis{}, err
	}
	for _, task := range tasks {
		out.TaskCounts[task.Status]++
		blocking := task.Status == "RUNNING" || task.Status == "FAILED" || task.Status == "QUARANTINED" ||
			(task.Status == "PENDING" && task.AttemptCount >= maxAttempts)
		if !blocking {
			continue
		}
		item := DiagnosisTask{ID: task.ID, Index: task.TaskIndex, Status: task.Status, AttemptCount: task.AttemptCount, MaxAttempts: maxAttempts, Blocking: true}
		if task.WorkerID != nil {
			item.WorkerID = *task.WorkerID
		}
		if task.NextEligibleAt != nil {
			item.NextEligibleAt = *task.NextEligibleAt
		}
		attempts, listErr := s.ListTaskAttempts(ctx, task.ID)
		if listErr != nil {
			return RunDiagnosis{}, listErr
		}
		if len(attempts) > 3 {
			attempts = attempts[len(attempts)-3:]
		}
		for i := len(attempts) - 1; i >= 0; i-- {
			attempt := attempts[i]
			item.RecentAttempts = append(item.RecentAttempts, DiagnosisAttempt{
				ID:            attempt.ID,
				Number:        attempt.AttemptNumber,
				Status:        attempt.Status,
				WorkerID:      attempt.WorkerID,
				LeaseDeadline: attempt.LeaseDeadline,
				LeaseExpired:  deadlineExpired(attempt.LeaseDeadline, now),
				FailureClass:  attempt.FailureClass,
			})
			if out.LastClassifiedError == "" && attempt.FailureClass != "" {
				out.LastClassifiedError = attempt.FailureClass
			}
		}
		out.BlockingTasks = append(out.BlockingTasks, item)
	}

	if run.RegistrationID != "" {
		reg, regErr := s.GetRegistrationForRun(ctx, runID)
		if regErr != nil {
			return RunDiagnosis{}, regErr
		}
		diag := &DiagnosisRegistration{
			ID:              reg.ID,
			Status:          reg.Status,
			AttemptCount:    reg.AttemptCount,
			ReceiptRecorded: strings.TrimSpace(reg.Receipt) != "",
			LastErrorClass:  reg.LastErrorClass,
			BlockedBy:       run.RegistrationBlockedBy,
			Reconciliation:  run.Reconciliation,
		}
		if reg.NextEligibleAt != nil {
			diag.NextEligibleAt = *reg.NextEligibleAt
		}
		_ = s.db.QueryRowContext(ctx, `SELECT phase,lease_deadline FROM iceberg_registration_attempts WHERE registration_id=? ORDER BY attempt_number DESC LIMIT 1`, reg.ID).Scan(&diag.CurrentPhase, &diag.LeaseDeadline)
		diag.LeaseExpired = deadlineExpired(diag.LeaseDeadline, now)
		out.Registration = diag
		if out.LastClassifiedError == "" {
			if run.Reconciliation.ErrorClass != "" {
				out.LastClassifiedError = run.Reconciliation.ErrorClass
			} else {
				out.LastClassifiedError = reg.LastErrorClass
			}
		}
	}

	out.OperatorReview, out.SuggestedNextAction = diagnoseNextAction(run, out)
	return out, nil
}

func deadlineExpired(raw string, now time.Time) bool {
	deadline, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	return err == nil && !deadline.After(now)
}

func diagnoseNextAction(run Run, diagnosis RunDiagnosis) (bool, string) {
	if run.Status == "COMMITTING" {
		return false, "request commit reconciliation; if it remains stale, verify durable leadership"
	}
	for _, task := range diagnosis.BlockingTasks {
		if task.Status == "QUARANTINED" || task.Status == "FAILED" || (task.Status == "PENDING" && task.AttemptCount >= task.MaxAttempts) {
			return true, "inspect the terminal task attempt and acknowledge quarantine after resolving the underlying cause"
		}
	}
	if diagnosis.Registration != nil {
		reg := diagnosis.Registration
		if reg.Reconciliation.Outcome == "INSUFFICIENT_EVIDENCE" || reg.Reconciliation.OperatorActionRequired {
			return true, "do not replay the catalog mutation; inspect reconciliation evidence and retry observation only when safe"
		}
		switch reg.Status {
		case RegistrationRetryRequired:
			if reg.ReceiptRecorded {
				return true, "repair local registration state; catalog replay is forbidden because a receipt is durable"
			}
			return false, "request registration retry"
		case RegistrationReconciling:
			return true, "request reconciliation retry only after the catalog observation failure is understood"
		case RegistrationBlocked:
			return true, "resolve the earlier registration identified by blocked_by"
		case RegistrationQuarantined, RegistrationFailed:
			return true, "inspect registration evidence and acknowledge operator review"
		}
	}
	if run.Status == "SUCCEEDED" {
		return false, "no recovery action required"
	}
	return false, "continue monitoring lifecycle events"
}

type LifecycleMetrics struct {
	RunsByStatus                    map[string]int
	RunsByCommitPhase               map[string]int
	OldestCommittingAgeSeconds      float64
	TasksByStatus                   map[string]int
	LeasedTasks                     int
	ExpiredActiveLeases             int
	RegistrationsByStatus           map[string]int
	RegistrationAttemptsByPhase     map[string]int
	ReconciliationsByStatus         map[string]int
	ReconciliationsByClassification map[string]int
	RegistrationBlocked             int
	RegistrationRetryRequired       int
	LeadershipActive                int
	LeadershipEpoch                 int64
	LeadershipRenewalFailures       int
}

func (s *Store) LifecycleMetrics(ctx context.Context, now time.Time) (LifecycleMetrics, error) {
	out := LifecycleMetrics{
		RunsByStatus:                    map[string]int{},
		RunsByCommitPhase:               map[string]int{},
		TasksByStatus:                   map[string]int{},
		RegistrationsByStatus:           map[string]int{},
		RegistrationAttemptsByPhase:     map[string]int{},
		ReconciliationsByStatus:         map[string]int{},
		ReconciliationsByClassification: map[string]int{},
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT status,COUNT(*) FROM runs GROUP BY status`, out.RunsByStatus); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT CASE WHEN trim(commit_phase)='' THEN 'NONE' ELSE commit_phase END,COUNT(*) FROM runs GROUP BY CASE WHEN trim(commit_phase)='' THEN 'NONE' ELSE commit_phase END`, out.RunsByCommitPhase); err != nil {
		return LifecycleMetrics{}, err
	}
	var oldest sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT MIN(started_at) FROM runs WHERE status='COMMITTING'`).Scan(&oldest); err != nil {
		return LifecycleMetrics{}, err
	}
	if oldest.Valid {
		if started, err := time.Parse(time.RFC3339Nano, oldest.String); err == nil && now.After(started) {
			out.OldestCommittingAgeSeconds = now.Sub(started).Seconds()
		}
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT status,COUNT(*) FROM tasks GROUP BY status`, out.TasksByStatus); err != nil {
		return LifecycleMetrics{}, err
	}
	nowS := now.UTC().Format(time.RFC3339Nano)
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_attempts WHERE status='ACTIVE'`).Scan(&out.LeasedTasks); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_attempts WHERE status='ACTIVE' AND lease_deadline<=?`, nowS).Scan(&out.ExpiredActiveLeases); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT status,COUNT(*) FROM iceberg_registrations GROUP BY status`, out.RegistrationsByStatus); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT phase,COUNT(*) FROM iceberg_registration_attempts WHERE status='ACTIVE' GROUP BY phase`, out.RegistrationAttemptsByPhase); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT CASE WHEN trim(reconciliation_status)='' THEN 'NONE' ELSE reconciliation_status END,COUNT(*) FROM iceberg_registrations GROUP BY CASE WHEN trim(reconciliation_status)='' THEN 'NONE' ELSE reconciliation_status END`, out.ReconciliationsByStatus); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := scanMetricCounts(ctx, s.db, `SELECT CASE WHEN trim(reconciliation_error_class)='' THEN 'NONE' ELSE reconciliation_error_class END,COUNT(*) FROM iceberg_registrations GROUP BY CASE WHEN trim(reconciliation_error_class)='' THEN 'NONE' ELSE reconciliation_error_class END`, out.ReconciliationsByClassification); err != nil {
		return LifecycleMetrics{}, err
	}
	out.RegistrationBlocked = out.RegistrationsByStatus[RegistrationBlocked]
	out.RegistrationRetryRequired = out.RegistrationsByStatus[RegistrationRetryRequired]
	if err := s.db.QueryRowContext(ctx, `SELECT CASE WHEN status='ACTIVE' AND lease_deadline_ms>? THEN 1 ELSE 0 END,epoch FROM master_leadership WHERE leadership_name='master'`, now.UnixMilli()).Scan(&out.LeadershipActive, &out.LeadershipEpoch); err != nil {
		return LifecycleMetrics{}, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM master_leadership_history WHERE event_type='MASTER_LEADERSHIP_LOST'`).Scan(&out.LeadershipRenewalFailures); err != nil {
		return LifecycleMetrics{}, err
	}
	return out, nil
}

func scanMetricCounts(ctx context.Context, db *sql.DB, query string, out map[string]int) error {
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var label string
		var count int
		if err := rows.Scan(&label, &count); err != nil {
			return err
		}
		out[label] = count
	}
	return rows.Err()
}

var ErrRecoveryRefused = errors.New("recovery action refused")

type RecoveryResult struct {
	Action  string `json:"action"`
	Changed bool   `json:"changed"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (s *Store) RequestLifecycleRecovery(ctx context.Context, runID, action, reason string, audit AuditRecord) (RecoveryResult, error) {
	action = strings.ToLower(strings.TrimSpace(action))
	reason = strings.TrimSpace(reason)
	if reason == "" {
		return RecoveryResult{}, fmt.Errorf("%w: reason is required", ErrRecoveryRefused)
	}
	result := RecoveryResult{Action: action}
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		var runStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&runStatus); err != nil {
			return err
		}
		_ = runStatus
		var registrationID string
		switch action {
		case "registration_retry":
			var status, receipt string
			var current sql.NullString
			if err := tx.QueryRowContext(ctx, `SELECT id,status,current_attempt_id,registered_snapshot_or_metadata_id FROM iceberg_registrations WHERE run_id=? ORDER BY id LIMIT 1`, runID).Scan(&registrationID, &status, &current, &receipt); err != nil {
				return err
			}
			if strings.TrimSpace(receipt) != "" {
				return fmt.Errorf("%w: catalog replay forbidden after durable receipt", ErrRecoveryRefused)
			}
			if status != RegistrationRetryRequired || current.Valid {
				return fmt.Errorf("%w: registration is %s, not idle RETRY_REQUIRED", ErrRecoveryRefused, status)
			}
			res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET next_eligible_at=NULL,updated_at=? WHERE id=? AND status='RETRY_REQUIRED' AND current_attempt_id IS NULL AND registered_snapshot_or_metadata_id='' AND next_eligible_at IS NOT NULL`, nowUTC(), registrationID)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			result.Changed, result.Status, result.Message = n == 1, status, "registration retry is eligible"
		case "reconciliation_retry":
			var status, reconciliationStatus string
			var current sql.NullString
			if err := tx.QueryRowContext(ctx, `SELECT id,status,reconciliation_status,current_reconciliation_attempt_id FROM iceberg_registrations WHERE run_id=? ORDER BY id LIMIT 1`, runID).Scan(&registrationID, &status, &reconciliationStatus, &current); err != nil {
				return err
			}
			if status != RegistrationReconciling || reconciliationStatus != "RETRY_REQUIRED" || current.Valid {
				return fmt.Errorf("%w: reconciliation is %s/%s, not idle RECONCILING/RETRY_REQUIRED", ErrRecoveryRefused, status, reconciliationStatus)
			}
			res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET reconciliation_next_eligible_at=NULL,updated_at=? WHERE id=? AND status='RECONCILING' AND reconciliation_status='RETRY_REQUIRED' AND current_reconciliation_attempt_id IS NULL AND reconciliation_next_eligible_at IS NOT NULL`, nowUTC(), registrationID)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			result.Changed, result.Status, result.Message = n == 1, reconciliationStatus, "reconciliation retry is eligible"
		case "acknowledge_quarantine":
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM tasks WHERE run_id=? AND status='QUARANTINED'`, runID).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf("%w: run has no quarantined tasks", ErrRecoveryRefused)
			}
			result.Status, result.Message = "QUARANTINED", "operator review acknowledged; task state was not changed"
		default:
			return fmt.Errorf("%w: unsupported action %q", ErrRecoveryRefused, action)
		}
		meta, err := json.Marshal(map[string]any{"action": action, "reason": reason, "changed": result.Changed, "status": result.Status})
		if err != nil {
			return err
		}
		audit.Action = "run.recovery." + action
		audit.ResourceType = "run"
		audit.ResourceID = runID
		audit.MetadataJSON = meta
		if err := insertAuditRecordTx(ctx, tx, audit); err != nil {
			return err
		}
		fields, _ := json.Marshal(map[string]any{"event_type": "OPERATOR_RECOVERY_REQUESTED", "actor_type": audit.ActorType, "actor_id": audit.ActorID, "action": action, "reason": reason, "changed": result.Changed, "registration_id": registrationID})
		eventID := recoveryEventID(runID, action)
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'WARN','operator recovery requested',?)`, eventID, runID, nowUTC(), string(fields))
		return err
	})
	return result, err
}

func (s *Store) RecordRecoveryRequest(ctx context.Context, runID, action, reason, status string, audit AuditRecord) error {
	action = strings.ToLower(strings.TrimSpace(action))
	reason = strings.TrimSpace(reason)
	if action == "" || reason == "" {
		return fmt.Errorf("%w: action and reason are required", ErrRecoveryRefused)
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		var durableStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE id=?`, runID).Scan(&durableStatus); err != nil {
			return err
		}
		meta, err := json.Marshal(map[string]any{"action": action, "reason": reason, "status": status, "durable_run_status": durableStatus})
		if err != nil {
			return err
		}
		audit.Action = "run.recovery." + action
		audit.ResourceType = "run"
		audit.ResourceID = runID
		audit.MetadataJSON = meta
		if err := insertAuditRecordTx(ctx, tx, audit); err != nil {
			return err
		}
		fields, _ := json.Marshal(map[string]any{"event_type": "OPERATOR_RECOVERY_REQUESTED", "actor_type": audit.ActorType, "actor_id": audit.ActorID, "action": action, "reason": reason, "status": status})
		_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'WARN','operator recovery requested',?)`, recoveryEventID(runID, action), runID, nowUTC(), string(fields))
		return err
	})
}

func recoveryEventID(runID, action string) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + action))
	return "recovery-" + hex.EncodeToString(sum[:16])
}
