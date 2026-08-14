package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// SetMaxActiveRuns configures the durable system-wide cap for runs in
// execution-owned states. A non-positive value leaves the cap disabled.
// Masters configure this once before accepting run submissions.
func (s *Store) SetMaxActiveRuns(limit int) {
	s.maxActiveRuns = limit
}

func (s *Store) admitRunTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	if strings.TrimSpace(runID) == "" {
		return false, fmt.Errorf("missing run id")
	}
	if s.maxActiveRuns > 0 {
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE status IN ('RUNNING','COMMITTING')`).Scan(&active); err != nil {
			return false, err
		}
		if active >= s.maxActiveRuns {
			return false, nil
		}
	}
	res, err := tx.ExecContext(ctx, `UPDATE runs SET status='RUNNING',finished_at=NULL,error_summary=NULL WHERE id=? AND status='PLANNING'`, runID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// StartRunWithTasks persists planned work and atomically admits the run when
// global capacity is available. When capacity is exhausted, the run remains
// PLANNING and every task remains PENDING without consuming an attempt.
func (s *Store) StartRunWithTasks(ctx context.Context, run Run, tasks []TaskInsert) (bool, error) {
	if strings.TrimSpace(run.ID) == "" {
		return false, fmt.Errorf("missing run id")
	}
	if len(tasks) == 0 {
		return false, fmt.Errorf("missing tasks")
	}
	admitted := false
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		if err := insertTasksTx(ctx, tx, tasks); err != nil {
			return err
		}
		var err error
		admitted, err = s.admitRunTx(ctx, tx, run.ID)
		return err
	})
	return admitted, err
}

// AdmitPendingRuns fills available global run capacity in durable creation
// order. It is safe to call concurrently; SQLite serializes the count and
// transitions in this transaction.
func (s *Store) AdmitPendingRuns(ctx context.Context) (int, error) {
	if s.maxActiveRuns <= 0 {
		return 0, nil
	}
	admitted := 0
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		var active int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE status IN ('RUNNING','COMMITTING')`).Scan(&active); err != nil {
			return err
		}
		available := s.maxActiveRuns - active
		if available <= 0 {
			return nil
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT r.id
			FROM runs r
			WHERE r.status='PLANNING'
			  AND EXISTS(SELECT 1 FROM tasks t WHERE t.run_id=r.id AND t.status='PENDING')
			ORDER BY r.started_at,r.id
			LIMIT ?`, available)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, id := range ids {
			res, err := tx.ExecContext(ctx, `UPDATE runs SET status='RUNNING',finished_at=NULL,error_summary=NULL WHERE id=? AND status='PLANNING'`, id)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			admitted += int(n)
			if n == 1 {
				if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, "run-admitted-"+id, id, nowUTC(), "INFO", "run admitted by global active-run admission", `{"admission":"MAX_ACTIVE_RUNS","status":"RUNNING"}`); err != nil {
					return err
				}
			}
		}
		return nil
	})
	return admitted, err
}
