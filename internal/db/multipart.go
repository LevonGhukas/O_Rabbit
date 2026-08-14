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
	"path"
	"strings"
	"time"
)

var ErrMultipartFenced = errors.New("multipart lifecycle update fenced")

type MultipartLifecycle struct {
	ID                  string  `json:"id"`
	RunID               string  `json:"run_id"`
	TaskID              string  `json:"task_id"`
	AttemptID           string  `json:"attempt_id"`
	AttemptNumber       int     `json:"attempt_number"`
	FileIndex           int     `json:"file_index"`
	ObjectKey           string  `json:"object_key"`
	ManagedPrefix       string  `json:"-"`
	ProviderUploadID    string  `json:"-"`
	ProviderUploadHash  string  `json:"provider_upload_id,omitempty"`
	Status              string  `json:"multipart_status"`
	WorkerID            string  `json:"worker_id"`
	ObjectSHA256        string  `json:"object_sha256,omitempty"`
	ObjectSize          int64   `json:"object_size"`
	CreatedAt           string  `json:"created_at"`
	LastActivityAt      string  `json:"last_activity_at"`
	NextCleanupAt       *string `json:"next_cleanup_at,omitempty"`
	CleanupAttemptCount int     `json:"cleanup_attempt_count"`
	LastErrorClass      string  `json:"last_error_class,omitempty"`
	OperatorAction      bool    `json:"operator_action_required"`
	CleanupToken        string  `json:"-"`
	CleanupLease        string  `json:"-"`
}

type MultipartLifecycleUpdate struct {
	Event, RunID, TaskID, AttemptID, WorkerID, FencingToken string
	FileIndex                                               int
	ObjectKey, UploadID, SHA256                             string
	Size                                                    int64
	ErrorClass, ErrorMessage                                string
}

func MultipartRecordID(attemptID string, fileIndex int, objectKey string) string {
	sum := sha256.Sum256([]byte(attemptID + "\x00" + fmt.Sprint(fileIndex) + "\x00" + objectKey))
	return "multipart-" + hex.EncodeToString(sum[:16])
}

func managedObjectPrefix(key string) string {
	clean := path.Clean("/" + strings.TrimSpace(key))
	parts := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	if len(parts) < 3 {
		return ""
	}
	return strings.Join(parts[:len(parts)-1], "/") + "/"
}

func (s *Store) ApplyMultipartLifecycle(ctx context.Context, u MultipartLifecycleUpdate, now time.Time) (MultipartLifecycle, error) {
	var out MultipartLifecycle
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var runID, workerID, token, status, deadline string
		var attemptNumber int
		err = tx.QueryRowContext(ctx, `SELECT t.run_id,a.worker_id,a.fencing_token,a.status,a.lease_deadline,a.attempt_number FROM tasks t JOIN task_attempts a ON a.task_id=t.id WHERE t.id=? AND a.id=?`, u.TaskID, u.AttemptID).Scan(&runID, &workerID, &token, &status, &deadline, &attemptNumber)
		if err != nil || runID != u.RunID || workerID != u.WorkerID || token != u.FencingToken || status != "ACTIVE" {
			return ErrMultipartFenced
		}
		lease, err := time.Parse(time.RFC3339Nano, deadline)
		if err != nil || !lease.After(now) {
			return ErrMultipartFenced
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		id := MultipartRecordID(u.AttemptID, u.FileIndex, u.ObjectKey)
		switch u.Event {
		case "PREPARED":
			prefix := managedObjectPrefix(u.ObjectKey)
			if prefix == "" || strings.TrimSpace(u.SHA256) == "" || u.Size < 0 {
				return errors.New("invalid managed multipart intent")
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO multipart_uploads(id,run_id,task_id,attempt_id,attempt_number,file_index,object_key,managed_prefix,status,worker_id,leader_epoch,created_at,last_activity_at,object_sha256,object_size,updated_at) VALUES(?,?,?,?,?,?,?,?, 'PREPARED',?,(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'),?,?,?,?,?) ON CONFLICT(attempt_id,file_index) DO NOTHING`,
				id, u.RunID, u.TaskID, u.AttemptID, attemptNumber, u.FileIndex, u.ObjectKey, prefix, u.WorkerID, ns, ns, u.SHA256, u.Size, ns)
			if err != nil {
				return err
			}
		case "CREATED":
			res, err := tx.ExecContext(ctx, `UPDATE multipart_uploads SET provider_upload_id=?,status='ACTIVE',provider_created_at=COALESCE(provider_created_at,?),last_activity_at=?,updated_at=? WHERE id=? AND attempt_id=? AND object_key=? AND status IN ('PREPARED','ACTIVE') AND (provider_upload_id IS NULL OR provider_upload_id=?)`, u.UploadID, ns, ns, ns, id, u.AttemptID, u.ObjectKey, u.UploadID)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return ErrMultipartFenced
			}
		case "COMPLETING":
			err = updateMultipartOwned(tx, id, u.AttemptID, `status='COMPLETING',completion_started_at=?,last_activity_at=?,updated_at=?`, ns, ns, ns)
		case "COMPLETION_AMBIGUOUS":
			err = updateMultipartOwned(tx, id, u.AttemptID, `status='COMPLETION_AMBIGUOUS',last_error_class='MULTIPART_COMPLETION_AMBIGUOUS',last_error_message=?,last_activity_at=?,updated_at=?`, safeError(u.ErrorMessage), ns, ns)
		case "COMPLETED":
			err = updateMultipartOwned(tx, id, u.AttemptID, `status='COMPLETED',completed_at=?,last_activity_at=?,updated_at=?`, ns, ns, ns)
		case "ABORT_PENDING":
			err = updateMultipartOwned(tx, id, u.AttemptID, `status='ABORT_PENDING',abort_requested_at=?,last_activity_at=?,updated_at=?`, ns, ns, ns)
		default:
			return fmt.Errorf("unknown multipart lifecycle event %q", u.Event)
		}
		if err != nil {
			return err
		}
		eventID := "multipart-event-" + id + "-" + strings.ToLower(u.Event)
		fields, _ := json.Marshal(map[string]any{"multipart_id": id, "event_type": "MULTIPART_" + u.Event, "task_id": u.TaskID, "attempt_id": u.AttemptID, "object_key": u.ObjectKey})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, eventID, u.RunID, ns, "INFO", "multipart lifecycle "+strings.ToLower(u.Event), string(fields))
		if err := scanMultipart(tx.QueryRowContext(ctx, multipartSelect+` WHERE id=?`, id), &out); err != nil {
			return err
		}
		if out.RunID != u.RunID || out.TaskID != u.TaskID || out.AttemptID != u.AttemptID || out.FileIndex != u.FileIndex || out.ObjectKey != u.ObjectKey || out.WorkerID != u.WorkerID || out.ObjectSHA256 != u.SHA256 || out.ObjectSize != u.Size {
			return errors.New("conflicting multipart intent")
		}
		return tx.Commit()
	})
	return out, err
}

func updateMultipartOwned(tx *sql.Tx, id, attemptID, set string, args ...any) error {
	query := `UPDATE multipart_uploads SET ` + set + ` WHERE id=? AND attempt_id=? AND status NOT IN ('COMPLETED','ABORTED','UNKNOWN_REVIEW')`
	args = append(args, id, attemptID)
	res, err := tx.Exec(query, args...)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrMultipartFenced
	}
	return nil
}

const multipartSelect = `SELECT id,run_id,task_id,attempt_id,attempt_number,file_index,object_key,managed_prefix,COALESCE(provider_upload_id,''),status,worker_id,object_sha256,object_size,created_at,last_activity_at,next_cleanup_at,cleanup_attempt_count,last_error_class,cleanup_token,cleanup_lease_deadline FROM multipart_uploads`

func scanMultipart(row rowScanner, out *MultipartLifecycle) error {
	var next, cleanupToken, cleanupLease sql.NullString
	if err := row.Scan(&out.ID, &out.RunID, &out.TaskID, &out.AttemptID, &out.AttemptNumber, &out.FileIndex, &out.ObjectKey, &out.ManagedPrefix, &out.ProviderUploadID, &out.Status, &out.WorkerID, &out.ObjectSHA256, &out.ObjectSize, &out.CreatedAt, &out.LastActivityAt, &next, &out.CleanupAttemptCount, &out.LastErrorClass, &cleanupToken, &cleanupLease); err != nil {
		return err
	}
	if next.Valid {
		out.NextCleanupAt = &next.String
	}
	out.CleanupToken, out.CleanupLease = cleanupToken.String, cleanupLease.String
	if out.ProviderUploadID != "" {
		sum := sha256.Sum256([]byte(out.ProviderUploadID))
		out.ProviderUploadHash = hex.EncodeToString(sum[:8])
	}
	out.OperatorAction = out.Status == "UNKNOWN_REVIEW" || out.CleanupAttemptCount >= 5
	return nil
}

func randomCleanupToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *Store) ClaimMultipartCleanup(ctx context.Context, now time.Time, grace, lease time.Duration) (MultipartLifecycle, bool, error) {
	var out MultipartLifecycle
	ok := false
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		cutoff := now.Add(-grace).UTC().Format(time.RFC3339Nano)
		row := tx.QueryRowContext(ctx, multipartSelect+` m WHERE m.status IN ('PREPARED','ACTIVE','COMPLETING','COMPLETION_AMBIGUOUS','ABORT_PENDING','ABORT_FAILED') AND m.last_activity_at<=? AND (m.next_cleanup_at IS NULL OR m.next_cleanup_at<=?) AND NOT EXISTS(SELECT 1 FROM task_artifacts ar WHERE ar.object_key=m.object_key AND ar.verification_status='VERIFIED') AND NOT EXISTS(SELECT 1 FROM task_attempts a WHERE a.id=m.attempt_id AND a.status='ACTIVE' AND a.lease_deadline>?) ORDER BY m.last_activity_at,m.id LIMIT 1`, cutoff, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano))
		if err := scanMultipart(row, &out); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return tx.Commit()
			}
			return err
		}
		var attemptStatus, attemptLease string
		if err := tx.QueryRowContext(ctx, `SELECT status,lease_deadline FROM task_attempts WHERE id=?`, out.AttemptID).Scan(&attemptStatus, &attemptLease); err != nil {
			return err
		}
		if attemptStatus == "ACTIVE" {
			parsed, err := time.Parse(time.RFC3339Nano, attemptLease)
			if err != nil || parsed.After(now) {
				return tx.Commit()
			}
		}
		token, err := randomCleanupToken()
		if err != nil {
			return err
		}
		deadline := now.Add(lease).UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE multipart_uploads SET status='ABORTING',cleanup_token=?,cleanup_lease_deadline=?,cleanup_attempt_count=cleanup_attempt_count+1,leader_epoch=(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'),updated_at=? WHERE id=? AND status=?`, token, deadline, now.UTC().Format(time.RFC3339Nano), out.ID, out.Status)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return tx.Commit()
		}
		out.Status, out.CleanupToken, out.CleanupLease = "ABORTING", token, deadline
		out.CleanupAttemptCount++
		fields, _ := json.Marshal(map[string]any{"multipart_id": out.ID, "event_type": "MULTIPART_ABORT_SCHEDULED", "task_id": out.TaskID, "attempt_id": out.AttemptID, "object_key": out.ObjectKey, "cleanup_attempt": out.CleanupAttemptCount})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, fmt.Sprintf("multipart-event-%s-abort-scheduled-%d", out.ID, out.CleanupAttemptCount), out.RunID, now.UTC().Format(time.RFC3339Nano), "WARN", "multipart abort scheduled", string(fields))
		ok = true
		return tx.Commit()
	})
	return out, ok, err
}

func (s *Store) ExpireMultipartCleanupClaims(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE multipart_uploads SET status='ABORT_FAILED',cleanup_token=NULL,cleanup_lease_deadline=NULL,next_cleanup_at=?,last_error_class='MULTIPART_ABORT_AMBIGUOUS',updated_at=? WHERE status='ABORTING' AND cleanup_lease_deadline<=?`, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) FinishMultipartCleanup(ctx context.Context, id, token, outcome, class, message string, now time.Time, retry time.Duration, max int) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var attempts int
		if err := tx.QueryRowContext(ctx, `SELECT cleanup_attempt_count FROM multipart_uploads WHERE id=? AND status='ABORTING' AND cleanup_token=?`, id, token).Scan(&attempts); err != nil {
			return ErrMultipartFenced
		}
		status, event := "ABORTED", "MULTIPART_ABORTED"
		var next any
		if outcome == "COMPLETED" {
			status, event = "COMPLETED", "MULTIPART_COMPLETED"
		} else if outcome == "REVIEW" {
			status, event = "UNKNOWN_REVIEW", "MULTIPART_OPERATOR_REVIEW_REQUIRED"
		} else if outcome != "ABORTED" {
			status, event = "ABORT_FAILED", "MULTIPART_ABORT_FAILED"
			next = now.Add(retry).UTC().Format(time.RFC3339Nano)
			if attempts >= max {
				status, event, next = "UNKNOWN_REVIEW", "MULTIPART_CLEANUP_EXHAUSTED", nil
			}
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE multipart_uploads SET status=?,cleanup_token=NULL,cleanup_lease_deadline=NULL,next_cleanup_at=?,last_error_class=?,last_error_message=?,aborted_at=CASE WHEN ?='ABORTED' THEN ? ELSE aborted_at END,completed_at=CASE WHEN ?='COMPLETED' THEN ? ELSE completed_at END,updated_at=? WHERE id=? AND status='ABORTING' AND cleanup_token=?`, status, next, class, safeError(message), status, ns, status, ns, ns, id, token)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrMultipartFenced
		}
		fields, _ := json.Marshal(map[string]any{"multipart_id": id, "event_type": event, "classification": class})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,ts,level,message,fields_json) VALUES(?,?,?,?,?)`, "multipart-event-"+id+"-"+strings.ToLower(event), ns, "WARN", strings.ToLower(strings.ReplaceAll(event, "_", " ")), string(fields))
		return tx.Commit()
	})
}

func (s *Store) AdoptMultipartUploadForCleanup(ctx context.Context, id, token, uploadID string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE multipart_uploads SET provider_upload_id=?,updated_at=? WHERE id=? AND status='ABORTING' AND cleanup_token=? AND provider_upload_id IS NULL`, uploadID, now.UTC().Format(time.RFC3339Nano), id, token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrMultipartFenced
	}
	return nil
}

func safeError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

func (s *Store) ListMultipartUploadsForRun(ctx context.Context, runID string) ([]MultipartLifecycle, error) {
	rows, err := s.db.QueryContext(ctx, multipartSelect+` WHERE run_id=? ORDER BY task_id,file_index`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []MultipartLifecycle
	for rows.Next() {
		var item MultipartLifecycle
		if err := scanMultipart(rows, &item); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
