package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

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

func (s *Store) ListTasksForRun(ctx context.Context, runID string) ([]Task, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT t.id,t.run_id,t.task_index,t.partition_spec_json,t.worker_id,t.status,t.rows_read,t.bytes_read,t.bytes_written,t.parquet_objects_json,t.started_at,t.finished_at,t.error_message,t.current_attempt_id,t.attempt_count,t.next_eligible_at,COALESCE(a.attempt_number,0),COALESCE(a.lease_deadline,''),COALESCE(a.last_renewed_at,''),COALESCE(a.status,''),COALESCE(a.failure_class,''),COALESCE(ar.artifact_count,0),COALESCE(ar.artifact_bytes,0),COALESCE(ar.artifact_rows,0),COALESCE(ar.verification_status,''),COALESCE(ar.verification_method,''),COALESCE(ar.verified_at,'') FROM tasks t LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.attempt_count LEFT JOIN (SELECT task_id,COUNT(*) artifact_count,SUM(byte_size) artifact_bytes,SUM(row_count) artifact_rows,MIN(verification_status) verification_status,MIN(verification_method) verification_method,MAX(verified_at) verified_at FROM task_artifacts GROUP BY task_id) ar ON ar.task_id=t.id WHERE t.run_id=? ORDER BY t.task_index ASC;`, runID)
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
