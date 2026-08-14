package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

const (
	CommitReconciliationPending        = "PENDING"
	CommitReconciliationRetryRequired  = "RETRY_REQUIRED"
	CommitReconciliationTerminal       = "TERMINAL"
	CommitReconciliationActionRequired = "ACTION_REQUIRED"
	CommitReconciliationComplete       = "COMPLETE"
)

type CommitReconciliationPolicy struct {
	MaxAttempts             int
	BackoffBase, BackoffMax time.Duration
}

func (s *Store) attachCommitReconciliationProjection(ctx context.Context, run *Run) {
	if run == nil {
		return
	}
	var next sql.NullString
	var operator int
	if err := s.db.QueryRowContext(ctx, `SELECT commit_reconciliation_status,commit_reconciliation_attempt_count,commit_reconciliation_next_eligible_at,operator_action_required FROM runs WHERE id=?`, run.ID).Scan(
		&run.CommitReconciliationStatus,
		&run.CommitReconciliationAttempt,
		&next,
		&operator,
	); err != nil {
		return
	}
	if next.Valid {
		run.CommitReconciliationNextRetry = &next.String
	}
	run.OperatorActionRequired = operator != 0
}

// RecordCommitReconciliationFailure durably classifies storage-commit
// reconciliation. Only transient failures remain eligible for the live scan.
func (s *Store) RecordCommitReconciliationFailure(ctx context.Context, runID, class, message string, retryable, operatorAction bool, now time.Time, policy CommitReconciliationPolicy) error {
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 5
	}
	if policy.BackoffBase <= 0 {
		policy.BackoffBase = time.Second
	}
	if policy.BackoffMax <= 0 {
		policy.BackoffMax = time.Minute
	}
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var status string
		var attempt int
		if err := tx.QueryRowContext(ctx, `SELECT status,commit_reconciliation_attempt_count FROM runs WHERE id=?`, runID).Scan(&status, &attempt); err != nil {
			return err
		}
		if status != "COMMITTING" {
			return nil
		}
		attempt++
		reconciliationStatus := CommitReconciliationTerminal
		runStatus := "FAILED"
		var next any
		if operatorAction {
			reconciliationStatus = CommitReconciliationActionRequired
		} else if retryable && attempt < policy.MaxAttempts {
			reconciliationStatus = CommitReconciliationRetryRequired
			runStatus = "COMMITTING"
			backoff := policy.BackoffBase
			for i := 1; i < attempt; i++ {
				backoff *= 2
				if backoff >= policy.BackoffMax {
					backoff = policy.BackoffMax
					break
				}
			}
			next = now.Add(backoff).UTC().Format(time.RFC3339Nano)
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		finished := any(nil)
		commitPhase := "RETRY_REQUIRED"
		if runStatus == "FAILED" {
			finished = ns
			commitPhase = "FAILED"
		}
		res, err := tx.ExecContext(ctx, `UPDATE runs SET status=?,finished_at=?,error_summary=?,failure_class=?,commit_reconciliation_status=?,commit_reconciliation_attempt_count=?,commit_reconciliation_next_eligible_at=?,operator_action_required=?,commit_phase=? WHERE id=? AND status='COMMITTING'`, runStatus, finished, message, class, reconciliationStatus, attempt, next, operatorAction, commitPhase, runID)
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			if err != nil {
				return err
			}
			return fmt.Errorf("run %s commit reconciliation update was fenced", runID)
		}
		fields := fmt.Sprintf(`{"event_type":"COMMIT_RECONCILIATION_FAILED","classification":%q,"attempt":%d,"retryable":%t,"operator_action_required":%t}`, class, attempt, reconciliationStatus == CommitReconciliationRetryRequired, operatorAction)
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'ERROR',?,?)`, fmt.Sprintf("commit-reconciliation-%s-%d", runID, attempt), runID, ns, message, fields); err != nil {
			return err
		}
		return tx.Commit()
	})
}
