package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrCommitClaimLost is returned when a committer no longer holds the run's
// commit claim, for example because its lease expired.
var ErrCommitClaimLost = errors.New("commit claim lost")

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

// ClaimCommittingRun gives the caller the exclusive right to publish a
// COMMITTING run until the claim expires. It succeeds only if the run is
// eligible for a commit attempt and no other live claim exists.
func (s *Store) ClaimCommittingRun(ctx context.Context, runID, token string, now time.Time, lease time.Duration) (bool, error) {
	nowS := now.UTC().Format(time.RFC3339Nano)
	var claimed bool
	err := withBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE runs SET commit_claim_token=?,commit_claim_expires_at=?
			WHERE id=? AND status='COMMITTING'
			AND commit_reconciliation_status IN ('','PENDING','RETRY_REQUIRED')
			AND (commit_reconciliation_next_eligible_at IS NULL OR julianday(commit_reconciliation_next_eligible_at)<=julianday(?))
			AND (commit_claim_token='' OR commit_claim_expires_at IS NULL OR julianday(commit_claim_expires_at)<=julianday(?))`,
			token, now.Add(lease).UTC().Format(time.RFC3339Nano), runID, nowS, nowS)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		claimed = n == 1
		return err
	})
	return claimed, err
}

// RenewCommitClaim extends a held claim; it returns ErrCommitClaimLost if the
// claim expired or the run is no longer committing.
func (s *Store) RenewCommitClaim(ctx context.Context, runID, token string, now time.Time, lease time.Duration) error {
	return withBusyRetry(ctx, func() error {
		res, err := s.db.ExecContext(ctx, `UPDATE runs SET commit_claim_expires_at=? WHERE id=? AND status='COMMITTING' AND commit_claim_token=? AND julianday(commit_claim_expires_at)>julianday(?)`,
			now.Add(lease).UTC().Format(time.RFC3339Nano), runID, token, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		if n, err := res.RowsAffected(); err != nil || n != 1 {
			if err != nil {
				return err
			}
			return ErrCommitClaimLost
		}
		return nil
	})
}

// ReleaseCommitClaim drops a claim held with token, whatever the run status.
func (s *Store) ReleaseCommitClaim(ctx context.Context, runID, token string) error {
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `UPDATE runs SET commit_claim_token='',commit_claim_expires_at=NULL WHERE id=? AND commit_claim_token=?`, runID, token)
		return err
	})
}
