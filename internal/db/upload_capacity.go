package db

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

var ErrUploadCapacityFenced = errors.New("upload capacity request is no longer owned by the task attempt")

type UploadCapacityLease struct {
	ID            string
	TaskID        string
	AttemptID     string
	WorkerID      string
	Token         string
	LeaseDeadline string
}

// AcquireUploadCapacity atomically validates task ownership, reclaims expired
// grants, and acquires or renews one global upload slot for the attempt.
func (s *Store) AcquireUploadCapacity(ctx context.Context, bootID, taskID, attemptID, fencingToken, workerID string, now time.Time, ttl time.Duration, limit int, idFn, tokenFn func() (string, error)) (UploadCapacityLease, bool, error) {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	if limit <= 0 {
		limit = 1
	}
	if idFn == nil {
		idFn = func() (string, error) { return secureAttemptValue("upload-lease-") }
	}
	if tokenFn == nil {
		tokenFn = func() (string, error) { return secureAttemptValue("") }
	}
	now = now.UTC()
	nowS := now.Format(time.RFC3339Nano)
	deadline := now.Add(ttl).Format(time.RFC3339Nano)
	var out UploadCapacityLease
	acquired := false
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if _, err := tx.ExecContext(ctx, `UPDATE upload_capacity_leases SET status='EXPIRED',updated_at=? WHERE status='ACTIVE' AND julianday(lease_deadline)<=julianday(?)`, nowS, nowS); err != nil {
			return err
		}
		var owned int
		err = tx.QueryRowContext(ctx, `
			SELECT 1
			FROM task_attempts a JOIN tasks t ON t.id=a.task_id
			WHERE a.id=? AND a.task_id=? AND a.worker_id=? AND a.worker_boot_id=? AND a.fencing_token=?
			  AND a.status='ACTIVE' AND t.status='RUNNING' AND t.current_attempt_id=a.id
			  AND julianday(a.lease_deadline)>julianday(?)`,
			attemptID, taskID, workerID, bootID, fencingToken, nowS).Scan(&owned)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrUploadCapacityFenced
		}
		if err != nil {
			return err
		}

		var status string
		err = tx.QueryRowContext(ctx, `SELECT id,task_id,attempt_id,worker_id,lease_token,status,lease_deadline FROM upload_capacity_leases WHERE attempt_id=?`, attemptID).
			Scan(&out.ID, &out.TaskID, &out.AttemptID, &out.WorkerID, &out.Token, &status, &out.LeaseDeadline)
		switch {
		case err == nil && status == "ACTIVE" && out.TaskID == taskID && out.WorkerID == workerID:
			if _, err := tx.ExecContext(ctx, `UPDATE upload_capacity_leases SET lease_deadline=?,updated_at=? WHERE id=? AND status='ACTIVE'`, deadline, nowS, out.ID); err != nil {
				return err
			}
			out.LeaseDeadline = deadline
			acquired = true
			return tx.Commit()
		case err != nil && !errors.Is(err, sql.ErrNoRows):
			return err
		}

		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM upload_capacity_leases WHERE status='ACTIVE' AND julianday(lease_deadline)>julianday(?)`, nowS).Scan(&active); err != nil {
			return err
		}
		if active >= limit {
			return tx.Commit()
		}
		leaseID, err := idFn()
		if err != nil {
			return err
		}
		token, err := tokenFn()
		if err != nil {
			return err
		}
		if out.AttemptID == "" {
			_, err = tx.ExecContext(ctx, `INSERT INTO upload_capacity_leases(id,task_id,attempt_id,worker_id,lease_token,status,lease_deadline,created_at,updated_at) VALUES(?,?,?,?,?,'ACTIVE',?,?,?)`, leaseID, taskID, attemptID, workerID, token, deadline, nowS, nowS)
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE upload_capacity_leases SET id=?,task_id=?,worker_id=?,lease_token=?,status='ACTIVE',lease_deadline=?,updated_at=?,released_at=NULL WHERE attempt_id=?`, leaseID, taskID, workerID, token, deadline, nowS, attemptID)
		}
		if err != nil {
			return err
		}
		out = UploadCapacityLease{ID: leaseID, TaskID: taskID, AttemptID: attemptID, WorkerID: workerID, Token: token, LeaseDeadline: deadline}
		acquired = true
		return tx.Commit()
	})
	return out, acquired, err
}

// ReleaseUploadCapacity is idempotent for a matching lease credential.
func (s *Store) ReleaseUploadCapacity(ctx context.Context, bootID, taskID, attemptID, workerID, leaseID, leaseToken string, now time.Time) error {
	nowS := now.UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE upload_capacity_leases
		SET status=CASE WHEN status='ACTIVE' THEN 'RELEASED' ELSE status END,
		    updated_at=?,
		    released_at=CASE WHEN status='ACTIVE' THEN ? ELSE released_at END
		WHERE id=? AND task_id=? AND attempt_id=? AND worker_id=? AND lease_token=?`,
		nowS, nowS, strings.TrimSpace(leaseID), taskID, attemptID, workerID, leaseToken)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrUploadCapacityFenced
	}
	return nil
}

func IsUploadCapacityFenced(err error) bool {
	return errors.Is(err, ErrUploadCapacityFenced)
}
