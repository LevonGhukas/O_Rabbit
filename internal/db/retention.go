package db

import (
	"context"
	"time"
)

// HistoryPruneResult counts the rows removed by one PruneHistory pass.
type HistoryPruneResult struct {
	Events               int64
	TaskAttempts         int64
	RegistrationAttempts int64
	LeadershipHistory    int64
}

func (r HistoryPruneResult) Total() int64 {
	return r.Events + r.TaskAttempts + r.RegistrationAttempts + r.LeadershipHistory
}

// historyPruneBatch bounds each delete statement so one pass never holds the
// single writer connection for long.
const historyPruneBatch = 500

// PruneHistory deletes operational history older than cutoff. It only touches
// runs that finished before cutoff, and never removes run, task, job, HWM,
// audit or registration rows, or anything still needed for object cleanup:
//
//   - events of SUCCEEDED, FAILED and CANCELED runs;
//   - task attempts (and, by cascade, their artifacts) of FAILED and CANCELED
//     runs with no unfinished canceled-object candidate or multipart upload;
//   - finished catalog registration/reconciliation attempts, keeping the
//     latest attempt of each registration;
//   - master leadership history.
//
// Artifacts of SUCCEEDED runs are kept: they describe published data.
func (s *Store) PruneHistory(ctx context.Context, cutoff time.Time) (HistoryPruneResult, error) {
	var res HistoryPruneResult
	c := cutoff.UTC().Format(TimestampLayout)
	steps := []struct {
		n    *int64
		stmt string
		args []any
	}{
		{&res.Events, `
			DELETE FROM events WHERE rowid IN (
				SELECT e.rowid FROM events e JOIN runs r ON r.id=e.run_id
				WHERE r.status IN ('SUCCEEDED','FAILED','CANCELED')
				  AND r.finished_at IS NOT NULL AND r.finished_at<?
				LIMIT ?)`, []any{c, historyPruneBatch}},
		{&res.TaskAttempts, `
			DELETE FROM task_attempts WHERE rowid IN (
				SELECT a.rowid FROM task_attempts a
				JOIN tasks t ON t.id=a.task_id
				JOIN runs r ON r.id=t.run_id
				WHERE r.status IN ('FAILED','CANCELED')
				  AND r.finished_at IS NOT NULL AND r.finished_at<?
				  AND a.status <> 'ACTIVE'
				  AND NOT EXISTS(SELECT 1 FROM canceled_object_candidates c WHERE c.run_id=r.id AND c.status <> 'DELETED')
				  AND NOT EXISTS(SELECT 1 FROM multipart_uploads m WHERE m.run_id=r.id AND m.status NOT IN ('COMPLETED','ABORTED'))
				LIMIT ?)`, []any{c, historyPruneBatch}},
		{&res.RegistrationAttempts, `
			DELETE FROM iceberg_registration_attempts WHERE rowid IN (
				SELECT a.rowid FROM iceberg_registration_attempts a
				WHERE a.status <> 'ACTIVE'
				  AND a.finished_at IS NOT NULL AND a.finished_at<?
				  AND a.attempt_number < (SELECT MAX(b.attempt_number) FROM iceberg_registration_attempts b WHERE b.registration_id=a.registration_id)
				LIMIT ?)`, []any{c, historyPruneBatch}},
		{&res.RegistrationAttempts, `
			DELETE FROM iceberg_reconciliation_attempts WHERE rowid IN (
				SELECT a.rowid FROM iceberg_reconciliation_attempts a
				WHERE a.status <> 'ACTIVE'
				  AND a.finished_at IS NOT NULL AND a.finished_at<?
				  AND a.attempt_number < (SELECT MAX(b.attempt_number) FROM iceberg_reconciliation_attempts b WHERE b.registration_id=a.registration_id)
				LIMIT ?)`, []any{c, historyPruneBatch}},
		{&res.LeadershipHistory, `
			DELETE FROM master_leadership_history WHERE rowid IN (
				SELECT rowid FROM master_leadership_history WHERE occurred_at_ms < ? LIMIT ?)`,
			[]any{cutoff.UnixMilli(), historyPruneBatch}},
	}
	for _, step := range steps {
		for {
			var n int64
			err := withBusyRetry(ctx, func() error {
				r, err := s.db.ExecContext(ctx, step.stmt, step.args...)
				if err != nil {
					return err
				}
				n, err = r.RowsAffected()
				return err
			})
			if err != nil {
				return res, err
			}
			*step.n += n
			if n < historyPruneBatch {
				break
			}
			if err := ctx.Err(); err != nil {
				return res, err
			}
		}
	}
	return res, nil
}
