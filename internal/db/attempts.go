package db

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
)

const (
	AttemptActive     = "ACTIVE"
	AttemptSucceeded  = "SUCCEEDED"
	AttemptFailed     = "FAILED"
	AttemptExpired    = "EXPIRED"
	AttemptSuperseded = "SUPERSEDED"
	AttemptCanceled   = "CANCELED"
)

var ErrAttemptFenced = errors.New("task attempt is no longer current")

type Attempt struct {
	ID, TaskID, WorkerID, FencingToken, Status       string
	AttemptNumber                                    int
	AssignedAt, LeaseDeadline, LastRenewedAt         string
	StartedAt, FinishedAt, FailureMessage            *string
	FailureClass, ResultDigest, CreatedAt, UpdatedAt string
}

type LeasePolicy struct {
	Duration       time.Duration
	MaxAttempts    int
	MaxActiveTasks int
	BackoffBase    time.Duration
	BackoffMax     time.Duration
}

func attemptEventID(eventType, attemptID, classification string) string {
	sum := sha256.Sum256([]byte(eventType + "\x00" + attemptID + "\x00" + classification))
	return "attempt-event-" + hex.EncodeToString(sum[:16])
}

func insertAttemptEventTx(ctx context.Context, tx *sql.Tx, runID, taskID, attemptID string, attemptNumber int, workerID, eventType, classification, ts string, extra map[string]any) error {
	fields := map[string]any{
		"event_type": eventType, "attempt_id": attemptID, "attempt_number": attemptNumber,
		"worker_id": workerID, "classification": classification,
	}
	for k, v := range extra {
		fields[k] = v
	}
	b, err := json.Marshal(fields)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,task_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?,?)`, attemptEventID(eventType, attemptID, classification), runID, taskID, ts, "INFO", "task attempt "+strings.ToLower(strings.ReplaceAll(eventType, "_", " ")), string(b))
	return err
}

func secureAttemptValue(prefix string) (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b), nil
}

func (p LeasePolicy) normalized() LeasePolicy {
	if p.Duration <= 0 {
		p.Duration = 30 * time.Second
	}
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 3
	}
	if p.BackoffBase <= 0 {
		p.BackoffBase = time.Second
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = 30 * time.Second
	}
	return p
}

func (s *Store) AssignNextPendingTaskWithLease(ctx context.Context, bootID, workerID string, now time.Time, policy LeasePolicy, idFn, tokenFn func() (string, error)) (Task, bool, error) {
	policy = policy.normalized()
	if idFn == nil {
		idFn = func() (string, error) { return secureAttemptValue("attempt-") }
	}
	if tokenFn == nil {
		tokenFn = func() (string, error) { return secureAttemptValue("") }
	}
	attemptID, err := idFn()
	if err != nil {
		return Task{}, false, err
	}
	token, err := tokenFn()
	if err != nil {
		return Task{}, false, err
	}
	now = now.UTC()
	nowS := now.Format(time.RFC3339Nano)
	deadline := now.Add(policy.Duration).Format(time.RFC3339Nano)
	var out Task
	ok := false
	err = withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		row := tx.QueryRowContext(ctx, `
				WITH running AS (SELECT run_id, COUNT(*) cnt FROM tasks WHERE status='RUNNING' GROUP BY run_id)
				SELECT t.id,t.run_id,t.task_index,t.partition_spec_json,t.attempt_count,
					   COALESCE((SELECT a.id FROM task_attempts a WHERE a.task_id=t.id ORDER BY a.attempt_number DESC LIMIT 1),''),
					   COALESCE((SELECT a.worker_id FROM task_attempts a WHERE a.task_id=t.id ORDER BY a.attempt_number DESC LIMIT 1),'')
				FROM tasks t JOIN runs r ON r.id=t.run_id JOIN jobs j ON j.id=r.job_id LEFT JOIN running rn ON rn.run_id=r.id
				WHERE t.status='PENDING' AND r.status='RUNNING' AND (t.next_eligible_at IS NULL OR julianday(t.next_eligible_at)<=julianday(?))
				AND t.attempt_count<? AND (COALESCE(CAST(json_extract(j.options_json,'$.max_in_flight_tasks') AS INTEGER),0)<=0 OR COALESCE(rn.cnt,0)<COALESCE(CAST(json_extract(j.options_json,'$.max_in_flight_tasks') AS INTEGER),0))
				AND (?<=0 OR (SELECT COUNT(*) FROM tasks WHERE status='RUNNING')<?)
				ORDER BY COALESCE(rn.cnt,0),r.started_at,t.run_id,t.task_index LIMIT 1`, nowS, policy.MaxAttempts, policy.MaxActiveTasks, policy.MaxActiveTasks)
		var part string
		var count int
		var previousAttemptID, previousWorkerID string
		if err := row.Scan(&out.ID, &out.RunID, &out.TaskIndex, &part, &count, &previousAttemptID, &previousWorkerID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				ok = false
				return tx.Commit()
			}
			return err
		}
		num := count + 1
		res, err := tx.ExecContext(ctx, `UPDATE tasks SET status='RUNNING',worker_id=?,started_at=COALESCE(started_at,?),current_attempt_id=?,attempt_count=?,next_eligible_at=NULL WHERE id=? AND status='PENDING' AND current_attempt_id IS NULL`, workerID, nowS, attemptID, num, out.ID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			ok = false
			return tx.Commit()
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,worker_boot_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,started_at,created_at,updated_at,assigned_by_leader_epoch) VALUES(?,?,?,?,?,?,'ACTIVE',?,?,?,?,?,?,(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'))`, attemptID, out.ID, num, workerID, bootID, token, nowS, deadline, nowS, nowS, nowS, nowS)
		if err != nil {
			return err
		}
		if err := insertAttemptEventTx(ctx, tx, out.RunID, out.ID, attemptID, num, workerID, "ATTEMPT_ASSIGNED", "ASSIGNED", nowS, map[string]any{"lease_deadline": deadline}); err != nil {
			return err
		}
		if previousAttemptID != "" {
			if err := insertAttemptEventTx(ctx, tx, out.RunID, out.ID, previousAttemptID, num-1, previousWorkerID, "ATTEMPT_SUPERSEDED", "REASSIGNED", nowS, map[string]any{"replacement_attempt_id": attemptID}); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		out.PartitionSpec = []byte(part)
		out.Status = "RUNNING"
		out.WorkerID = &workerID
		out.CurrentAttemptID = &attemptID
		out.AttemptID = attemptID
		out.AttemptNumber = num
		out.FencingToken = token
		out.LeaseDeadline = deadline
		out.AttemptCount = num
		ok = true
		return nil
	})
	return out, ok, err
}

func (s *Store) RenewTaskLease(ctx context.Context, bootID, taskID, attemptID, token, workerID string, now time.Time, duration time.Duration) (string, error) {
	if duration <= 0 {
		duration = 30 * time.Second
	}
	now = now.UTC()
	deadline := now.Add(duration).Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `UPDATE task_attempts SET lease_deadline=?,last_renewed_at=?,updated_at=? WHERE id=? AND task_id=? AND fencing_token=? AND worker_id=? AND worker_boot_id=? AND status='ACTIVE' AND julianday(lease_deadline)>julianday(?) AND EXISTS(SELECT 1 FROM tasks WHERE id=? AND status='RUNNING' AND current_attempt_id=?)`, deadline, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), attemptID, taskID, token, workerID, bootID, now.Format(time.RFC3339Nano), taskID, attemptID)
	if err != nil {
		return "", err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return "", ErrAttemptFenced
	}
	return deadline, nil
}

func retryBackoff(p LeasePolicy, attempt int) time.Duration {
	p = p.normalized()
	d := p.BackoffBase
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= p.BackoffMax {
			return p.BackoffMax
		}
	}
	return d
}

func (s *Store) ExpireTaskAttempts(ctx context.Context, now time.Time, policy LeasePolicy) (int, error) {
	policy = policy.normalized()
	now = now.UTC()
	nowS := now.Format(time.RFC3339Nano)
	expired := 0
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT a.id,a.task_id,a.attempt_number,a.worker_id,a.lease_deadline,t.run_id FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN runs r ON r.id=t.run_id WHERE a.status='ACTIVE' AND julianday(a.lease_deadline)<=julianday(?) AND t.status='RUNNING' AND t.current_attempt_id=a.id AND r.status='RUNNING'`, nowS)
		if err != nil {
			return err
		}
		type item struct {
			id, task, worker, deadline, run string
			num                             int
		}
		var items []item
		for rows.Next() {
			var x item
			if err := rows.Scan(&x.id, &x.task, &x.num, &x.worker, &x.deadline, &x.run); err != nil {
				rows.Close()
				return err
			}
			items = append(items, x)
		}
		rows.Close()
		for _, x := range items {
			// Candidate scan is a snapshot. Recheck deadline while updating: a
			// current worker may have renewed after selection but before cleanup.
			res, err := tx.ExecContext(ctx, `UPDATE task_attempts SET status='EXPIRED',finished_at=?,failure_class='LEASE_EXPIRED',failure_message='lease expired',updated_at=? WHERE id=? AND status='ACTIVE' AND julianday(lease_deadline)<=julianday(?)`, nowS, nowS, x.id, nowS)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				continue
			}
			if err := insertAttemptEventTx(ctx, tx, x.run, x.task, x.id, x.num, x.worker, "ATTEMPT_EXPIRED", "LEASE_EXPIRED", nowS, map[string]any{"lease_deadline": x.deadline}); err != nil {
				return err
			}
			if x.num >= policy.MaxAttempts {
				_, err = tx.ExecContext(ctx, `UPDATE tasks SET status='QUARANTINED',worker_id=NULL,current_attempt_id=NULL,finished_at=?,error_message='retry limit exhausted',next_eligible_at=NULL WHERE id=? AND current_attempt_id=?`, nowS, x.task, x.id)
				if err == nil {
					_, err = tx.ExecContext(ctx, `UPDATE runs SET status='FAILED',finished_at=?,error_summary='task retry limit exhausted' WHERE id=? AND status='RUNNING'`, nowS, x.run)
				}
				if err == nil {
					err = insertAttemptEventTx(ctx, tx, x.run, x.task, x.id, x.num, x.worker, "TASK_QUARANTINED", "RETRY_LIMIT_EXHAUSTED", nowS, map[string]any{"max_attempts": policy.MaxAttempts})
				}
			} else {
				next := now.Add(retryBackoff(policy, x.num)).Format(time.RFC3339Nano)
				_, err = tx.ExecContext(ctx, `UPDATE tasks SET status='PENDING',worker_id=NULL,current_attempt_id=NULL,next_eligible_at=?,error_message='lease expired' WHERE id=? AND current_attempt_id=?`, next, x.task, x.id)
				if err == nil {
					err = insertAttemptEventTx(ctx, tx, x.run, x.task, x.id, x.num, x.worker, "TASK_RETRY_SCHEDULED", "LEASE_EXPIRED", nowS, map[string]any{"next_eligible_at": next})
				}
			}
			if err != nil {
				return err
			}
			expired++
		}
		return tx.Commit()
	})
	return expired, err
}

// InsertAttemptRejectionEvent records at most one durable event per attempt and
// rejection class. Repeated stale traffic therefore cannot flood the event log.
func (s *Store) InsertAttemptRejectionEvent(ctx context.Context, taskID, attemptID, workerID, eventType, classification string, now time.Time) error {
	var runID, storedWorker string
	var num int
	err := s.db.QueryRowContext(ctx, `SELECT t.run_id,a.attempt_number,a.worker_id FROM tasks t JOIN task_attempts a ON a.task_id=t.id WHERE t.id=? AND a.id=?`, taskID, attemptID).Scan(&runID, &num, &storedWorker)
	if err != nil {
		return err
	}
	if workerID == "" {
		workerID = storedWorker
	}
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := insertAttemptEventTx(ctx, tx, runID, taskID, attemptID, num, workerID, eventType, classification, now.UTC().Format(time.RFC3339Nano), nil); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func resultDigest(status string, objects []byte, rows, bytesRead, bytesWritten int64, message string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00%d\x00%d\x00%d\x00%s", status, objects, rows, bytesRead, bytesWritten, message)
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Store) CompleteTaskAttempt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, objects []byte, rows, bytesRead, bytesWritten int64) (bool, string, string, error) {
	return s.CompleteTaskAttemptAt(ctx, bootID, taskID, attemptID, token, workerID, status, errMsg, objects, rows, bytesRead, bytesWritten, time.Now())
}

func (s *Store) CompleteTaskAttemptAt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, objects []byte, rows, bytesRead, bytesWritten int64, masterNow time.Time) (bool, string, string, error) {
	return s.completeTaskAttemptAt(ctx, bootID, taskID, attemptID, token, workerID, status, errMsg, objects, rows, bytesRead, bytesWritten, masterNow, nil, false, "")
}

func (s *Store) CompleteTaskAttemptWithFailureClassAt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, failureClass string, objects []byte, rows, bytesRead, bytesWritten int64, masterNow time.Time) (bool, string, string, error) {
	return s.completeTaskAttemptAt(ctx, bootID, taskID, attemptID, token, workerID, status, errMsg, objects, rows, bytesRead, bytesWritten, masterNow, nil, false, failureClass)
}

func (s *Store) CompleteTaskAttemptWithArtifactsAt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, records []artifact.Record, rows, bytesRead, bytesWritten int64, masterNow time.Time) (bool, string, string, error) {
	return s.CompleteTaskAttemptWithArtifactsAndFailureClassAt(ctx, bootID, taskID, attemptID, token, workerID, status, errMsg, "", records, rows, bytesRead, bytesWritten, masterNow)
}

func (s *Store) CompleteTaskAttemptWithArtifactsAndFailureClassAt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, failureClass string, records []artifact.Record, rows, bytesRead, bytesWritten int64, masterNow time.Time) (bool, string, string, error) {
	sort.Slice(records, func(i, j int) bool {
		if records[i].FileIndex != records[j].FileIndex {
			return records[i].FileIndex < records[j].FileIndex
		}
		return records[i].ObjectKey < records[j].ObjectKey
	})
	for i, record := range records {
		if err := record.Validate(); err != nil {
			return false, "", "", fmt.Errorf("artifact %d: %w", i, err)
		}
		if record.TaskID != taskID || record.AttemptID != attemptID || record.FileIndex != i {
			return false, "", "", fmt.Errorf("artifact %d identity mismatch", i)
		}
	}
	objects, err := json.Marshal(records)
	if err != nil {
		return false, "", "", err
	}
	return s.completeTaskAttemptAt(ctx, bootID, taskID, attemptID, token, workerID, status, errMsg, objects, rows, bytesRead, bytesWritten, masterNow, records, true, failureClass)
}

func (s *Store) completeTaskAttemptAt(ctx context.Context, bootID, taskID, attemptID, token, workerID, status string, errMsg *string, objects []byte, rows, bytesRead, bytesWritten int64, masterNow time.Time, records []artifact.Record, strict bool, failureClassOverride string) (bool, string, string, error) {
	digest := resultDigest(status, objects, rows, bytesRead, bytesWritten, func() string {
		if errMsg != nil {
			return *errMsg
		}
		return ""
	}())
	now := nowUTC()
	var accepted bool
	var msg, final string
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var aStatus, aWorker, aBoot, aToken, aDigest, leaseDeadline, tStatus, current, runStatus, durableRunID string
		var durableAttemptNumber int
		err = tx.QueryRowContext(ctx, `SELECT a.status,a.worker_id,a.worker_boot_id,a.fencing_token,a.result_digest,a.lease_deadline,t.status,COALESCE(t.current_attempt_id,''),r.status,r.id,a.attempt_number FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN runs r ON r.id=t.run_id WHERE a.id=? AND a.task_id=?`, attemptID, taskID).Scan(&aStatus, &aWorker, &aBoot, &aToken, &aDigest, &leaseDeadline, &tStatus, &current, &runStatus, &durableRunID, &durableAttemptNumber)
		if err != nil {
			return err
		}
		if aWorker != workerID || aBoot != bootID || aToken != token {
			return ErrAttemptFenced
		}
		if aStatus == AttemptCanceled || aStatus == AttemptExpired || aStatus == AttemptSuperseded {
			return ErrAttemptFenced
		}
		if aStatus == AttemptSucceeded || aStatus == AttemptFailed {
			if aDigest == digest {
				accepted = true
				msg = "already accepted"
				final = tStatus
				return tx.Commit()
			}
			return fmt.Errorf("attempt result conflict")
		}
		if current != attemptID {
			return ErrAttemptFenced
		}
		deadline, parseErr := time.Parse(time.RFC3339Nano, leaseDeadline)
		if parseErr != nil || !deadline.After(masterNow.UTC()) {
			return ErrAttemptFenced
		}
		if aStatus != AttemptActive || tStatus != "RUNNING" || runStatus != "RUNNING" {
			return ErrAttemptFenced
		}
		if strict && status == "SUCCEEDED" && bytesWritten > 0 && len(records) == 0 {
			return fmt.Errorf("successful task with bytes requires verified artifacts")
		}
		if strict && status == "SUCCEEDED" {
			for _, record := range records {
				if record.RunID != durableRunID || record.AttemptNumber != durableAttemptNumber {
					return fmt.Errorf("artifact durable identity mismatch")
				}
				id := attemptEventID("ARTIFACT", attemptID, fmt.Sprintf("%06d", record.FileIndex))
				res, err := tx.ExecContext(ctx, `INSERT INTO task_artifacts(id,task_id,attempt_id,file_index,object_key,byte_size,sha256,row_count,schema_fingerprint,run_id,attempt_number,format_version,verification_method,verification_status,verified_at,max_hwm,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, id, taskID, attemptID, record.FileIndex, record.ObjectKey, record.ByteSize, record.SHA256, record.RowCount, record.SchemaFingerprint, record.RunID, record.AttemptNumber, record.FormatVersion, record.VerificationMethod, record.VerificationStatus, masterNow.UTC().Format(time.RFC3339Nano), record.MaxHWM, now)
				if err != nil {
					return fmt.Errorf("persist artifact %d: %w", record.FileIndex, err)
				}
				if n, _ := res.RowsAffected(); n != 1 {
					return fmt.Errorf("persist artifact %d failed", record.FileIndex)
				}
			}
		}
		if status != "SUCCEEDED" && status != "FAILED" && status != "CANCELED" {
			return fmt.Errorf("invalid task status %q", status)
		}
		attemptStatus := status
		failureClass := ""
		if status == "FAILED" {
			failureClass = failureClassOverride
			if failureClass == "" {
				failureClass = "PERMANENT_TASK_FAILURE"
				if errMsg != nil {
					if classified := artifact.ClassificationFromMessage(*errMsg); classified != "" {
						failureClass = string(classified)
					}
				}
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE task_attempts SET status=?,finished_at=?,failure_class=?,failure_message=?,result_digest=?,updated_at=? WHERE id=? AND status='ACTIVE'`, attemptStatus, now, failureClass, errMsg, digest, now, attemptID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAttemptFenced
		}
		res, err = tx.ExecContext(ctx, `UPDATE tasks SET status=?,error_message=?,parquet_objects_json=?,rows_read=?,bytes_read=?,bytes_written=?,finished_at=?,current_attempt_id=NULL WHERE id=? AND status='RUNNING' AND current_attempt_id=?`, status, errMsg, string(objects), rows, bytesRead, bytesWritten, now, taskID, attemptID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAttemptFenced
		}
		accepted = true
		msg = "accepted"
		final = status
		return tx.Commit()
	})
	return accepted, msg, final, err
}

// AbandonTaskAttempt releases an assignment that could not be materialized for
// the worker. It preserves the attempt record and makes the logical task
// eligible for a fresh fenced assignment.
func (s *Store) AbandonTaskAttempt(ctx context.Context, taskID, attemptID, workerID, reason string) error {
	return s.AbandonTaskAttemptWithPolicy(ctx, taskID, attemptID, workerID, reason, time.Now(), LeasePolicy{})
}

// AbandonTaskAttemptWithPolicy applies the same retry/backoff/quarantine policy
// as lease expiration when assignment materialization fails.
func (s *Store) AbandonTaskAttemptWithPolicy(ctx context.Context, taskID, attemptID, workerID, reason string, now time.Time, policy LeasePolicy) error {
	policy = policy.normalized()
	now = now.UTC()
	nowS := now.Format(time.RFC3339Nano)
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var runID string
		var attemptNumber int
		if err := tx.QueryRowContext(ctx, `SELECT t.run_id,a.attempt_number FROM tasks t JOIN task_attempts a ON a.task_id=t.id WHERE t.id=? AND a.id=? AND a.worker_id=? AND a.status='ACTIVE' AND t.status='RUNNING' AND t.current_attempt_id=a.id`, taskID, attemptID, workerID).Scan(&runID, &attemptNumber); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrAttemptFenced
			}
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE task_attempts SET status='FAILED',finished_at=?,failure_class='ASSIGNMENT_BUILD_FAILURE',failure_message=?,updated_at=? WHERE id=? AND task_id=? AND worker_id=? AND status='ACTIVE'`, nowS, reason, nowS, attemptID, taskID, workerID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAttemptFenced
		}
		if err := insertAttemptEventTx(ctx, tx, runID, taskID, attemptID, attemptNumber, workerID, "ATTEMPT_ABANDONED", "ASSIGNMENT_BUILD_FAILURE", nowS, nil); err != nil {
			return err
		}
		if attemptNumber >= policy.MaxAttempts {
			res, err = tx.ExecContext(ctx, `UPDATE tasks SET status='QUARANTINED',worker_id=NULL,current_attempt_id=NULL,finished_at=?,next_eligible_at=NULL,error_message=? WHERE id=? AND status='RUNNING' AND current_attempt_id=?`, nowS, reason, taskID, attemptID)
			if err == nil {
				_, err = tx.ExecContext(ctx, `UPDATE runs SET status='FAILED',finished_at=?,error_summary='task retry limit exhausted' WHERE id=? AND status='RUNNING'`, nowS, runID)
			}
			if err == nil {
				err = insertAttemptEventTx(ctx, tx, runID, taskID, attemptID, attemptNumber, workerID, "TASK_QUARANTINED", "RETRY_LIMIT_EXHAUSTED", nowS, map[string]any{"max_attempts": policy.MaxAttempts})
			}
		} else {
			next := now.Add(retryBackoff(policy, attemptNumber)).Format(time.RFC3339Nano)
			res, err = tx.ExecContext(ctx, `UPDATE tasks SET status='PENDING',worker_id=NULL,current_attempt_id=NULL,next_eligible_at=?,error_message=? WHERE id=? AND status='RUNNING' AND current_attempt_id=?`, next, reason, taskID, attemptID)
			if err == nil {
				err = insertAttemptEventTx(ctx, tx, runID, taskID, attemptID, attemptNumber, workerID, "TASK_RETRY_SCHEDULED", "ASSIGNMENT_BUILD_FAILURE", nowS, map[string]any{"next_eligible_at": next})
			}
		}
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrAttemptFenced
		}
		return tx.Commit()
	})
}

func (s *Store) UpdateTaskProgressFenced(ctx context.Context, bootID, taskID, attemptID, token, workerID string, rows, bytesRead, bytesWritten int64) error {
	return s.UpdateTaskProgressFencedAt(ctx, bootID, taskID, attemptID, token, workerID, rows, bytesRead, bytesWritten, time.Now())
}

func (s *Store) UpdateTaskProgressFencedAt(ctx context.Context, bootID, taskID, attemptID, token, workerID string, rows, bytesRead, bytesWritten int64, masterNow time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE tasks SET rows_read=?,bytes_read=?,bytes_written=? WHERE id=? AND status='RUNNING' AND current_attempt_id=? AND EXISTS(SELECT 1 FROM task_attempts WHERE id=? AND task_id=? AND worker_id=? AND worker_boot_id=? AND fencing_token=? AND status='ACTIVE' AND julianday(lease_deadline)>julianday(?))`, rows, bytesRead, bytesWritten, taskID, attemptID, attemptID, taskID, workerID, bootID, token, masterNow.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrAttemptFenced
	}
	return nil
}

func (s *Store) ListTaskAttempts(ctx context.Context, taskID string) ([]Attempt, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,started_at,finished_at,failure_class,failure_message,result_digest,created_at,updated_at FROM task_attempts WHERE task_id=? ORDER BY attempt_number`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.ID, &a.TaskID, &a.AttemptNumber, &a.WorkerID, &a.FencingToken, &a.Status, &a.AssignedAt, &a.LeaseDeadline, &a.LastRenewedAt, &a.StartedAt, &a.FinishedAt, &a.FailureClass, &a.FailureMessage, &a.ResultDigest, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) ListArtifactsForRun(ctx context.Context, runID string) ([]artifact.Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT object_key,byte_size,sha256,row_count,schema_fingerprint,run_id,task_id,attempt_id,attempt_number,file_index,format_version,verification_method,verification_status,verified_at,max_hwm FROM task_artifacts WHERE run_id=? ORDER BY task_id,file_index,object_key`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []artifact.Record
	for rows.Next() {
		var r artifact.Record
		if err := rows.Scan(&r.ObjectKey, &r.ByteSize, &r.SHA256, &r.RowCount, &r.SchemaFingerprint, &r.RunID, &r.TaskID, &r.AttemptID, &r.AttemptNumber, &r.FileIndex, &r.FormatVersion, &r.VerificationMethod, &r.VerificationStatus, &r.VerifiedAt, &r.MaxHWM); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) CancelActiveAttempts(ctx context.Context, runID, reason string) error {
	now := nowUTC()
	_, err := s.db.ExecContext(ctx, `UPDATE task_attempts SET status='CANCELED',finished_at=?,failure_class='CANCELED',failure_message=?,updated_at=? WHERE status='ACTIVE' AND task_id IN(SELECT id FROM tasks WHERE run_id=?)`, now, reason, now, runID)
	return err
}

func IsAttemptFenced(err error) bool {
	return errors.Is(err, ErrAttemptFenced) || strings.Contains(fmt.Sprint(err), "no longer current")
}
