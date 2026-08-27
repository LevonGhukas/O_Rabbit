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
	"strings"
	"time"
)

var ErrCanceledObjectFenced = errors.New("canceled-object cleanup fenced")

type CleanupErrorClass string

const (
	CleanupReferenceFound           CleanupErrorClass = "CLEANUP_REFERENCE_FOUND"
	CleanupReferenceAmbiguous       CleanupErrorClass = "CLEANUP_REFERENCE_AMBIGUOUS"
	CleanupCatalogUnavailable       CleanupErrorClass = "CLEANUP_CATALOG_UNAVAILABLE"
	CleanupObjectIdentityConflict   CleanupErrorClass = "CLEANUP_OBJECT_IDENTITY_CONFLICT"
	CleanupObjectVerificationFailed CleanupErrorClass = "CLEANUP_OBJECT_VERIFICATION_FAILED"
	CleanupDeleteFailed             CleanupErrorClass = "CLEANUP_DELETE_FAILED"
	CleanupDeleteAmbiguous          CleanupErrorClass = "CLEANUP_DELETE_AMBIGUOUS"
	CleanupDeleteExhausted          CleanupErrorClass = "CLEANUP_DELETE_EXHAUSTED"
	CleanupCanceled                 CleanupErrorClass = "CLEANUP_CANCELED"
	CleanupUnsupportedLegacyObject  CleanupErrorClass = "CLEANUP_UNSUPPORTED_LEGACY_OBJECT"
)

type CanceledObjectCandidate struct {
	ID                     string `json:"candidate_id"`
	RunID                  string `json:"run_id"`
	TaskID                 string `json:"task_id"`
	AttemptID              string `json:"attempt_id"`
	ArtifactID             string `json:"artifact_id,omitempty"`
	DatasetID              string `json:"dataset_id,omitempty"`
	ObjectKey              string `json:"object_key"`
	ExpectedSHA256         string `json:"-"`
	ObjectVersion          string `json:"object_version,omitempty"`
	ExpectedSize           int64  `json:"expected_size"`
	Status                 string `json:"status"`
	EligibilityReason      string `json:"eligibility_reason"`
	ReferenceDecision      string `json:"reference_decision,omitempty"`
	EvidenceDigest         string `json:"reference_evidence_digest,omitempty"`
	DiscoveredAt           string `json:"discovered_at"`
	QuarantineUntil        string `json:"quarantine_until"`
	LastVerifiedAt         string `json:"last_verified_at,omitempty"`
	LastErrorClass         string `json:"last_error_class,omitempty"`
	DryRunResult           string `json:"dry_run_result,omitempty"`
	DeleteAttemptCount     int    `json:"delete_attempt_count"`
	OperatorActionRequired bool   `json:"operator_action_required"`
	CurrentAttemptID       string `json:"-"`
}

type CanceledObjectCleanupAttempt struct {
	ID, CandidateID, FencingToken, LeaseDeadline string
	AttemptNumber                                int
}

type ReferenceEvidence struct {
	CandidateID    string            `json:"candidate_id"`
	RunStatus      string            `json:"run_status"`
	TaskStatus     string            `json:"task_status"`
	AttemptStatus  string            `json:"attempt_status"`
	CommitPhase    string            `json:"commit_phase"`
	Checks         map[string]bool   `json:"checks"`
	Decision       string            `json:"decision"`
	CatalogStatus  string            `json:"catalog_status,omitempty"`
	Details        map[string]string `json:"details,omitempty"`
	ObservedAt     string            `json:"observed_at"`
	EvidenceDigest string            `json:"evidence_digest"`
}

const canceledObjectTimestampLayout = "2006-01-02T15:04:05.000000000Z07:00"

// canceledObjectTimestamp has fixed-width fractional seconds so SQLite TEXT
// comparisons preserve the same ordering as the represented instants.
func canceledObjectTimestamp(t time.Time) string {
	return t.UTC().Format(canceledObjectTimestampLayout)
}

func canceledCandidateID(attemptID, key, digest string) string {
	sum := sha256.Sum256([]byte(attemptID + "\x00" + key + "\x00" + digest))
	return "canceled-object-" + hex.EncodeToString(sum[:16])
}

func (s *Store) createCanceledObjectCandidatesTx(ctx context.Context, tx *sql.Tx, runID, now string) error {
	retention := s.canceledObjectRetention
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	discovered, err := time.Parse(time.RFC3339Nano, now)
	if err != nil {
		discovered = time.Now().UTC()
	}
	quarantine := canceledObjectTimestamp(discovered.Add(retention))
	rows, err := tx.QueryContext(ctx, `SELECT m.run_id,m.task_id,m.attempt_id,m.object_key,m.object_size,m.object_sha256,r.dataset_key FROM multipart_uploads m JOIN runs r ON r.id=m.run_id JOIN task_attempts a ON a.id=m.attempt_id WHERE m.run_id=? AND m.status='COMPLETED' AND a.status='CANCELED' AND NOT EXISTS(SELECT 1 FROM task_artifacts ar WHERE ar.object_key=m.object_key)`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var c CanceledObjectCandidate
		if err := rows.Scan(&c.RunID, &c.TaskID, &c.AttemptID, &c.ObjectKey, &c.ExpectedSize, &c.ExpectedSHA256, &c.DatasetID); err != nil {
			return err
		}
		c.ID = canceledCandidateID(c.AttemptID, c.ObjectKey, c.ExpectedSHA256)
		res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO canceled_object_candidates(id,run_id,task_id,attempt_id,dataset_id,object_key,expected_size,expected_sha256,status,eligibility_reason,discovered_at,quarantine_until,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'QUARANTINED','CANCELED_ATTEMPT_COMPLETED_UPLOAD',?,?,?,?)`, c.ID, c.RunID, c.TaskID, c.AttemptID, c.DatasetID, c.ObjectKey, c.ExpectedSize, c.ExpectedSHA256, now, quarantine, now, now)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 1 {
			for _, event := range []string{"CANCELED_OBJECT_DISCOVERED", "CANCELED_OBJECT_QUARANTINED"} {
				fields, _ := json.Marshal(map[string]any{"event_type": event, "candidate_id": c.ID, "task_id": c.TaskID, "attempt_id": c.AttemptID, "object_key": c.ObjectKey, "quarantine_until": quarantine})
				_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, "canceled-object-event-"+c.ID+"-"+strings.ToLower(event), runID, now, "WARN", strings.ToLower(strings.ReplaceAll(event, "_", " ")), string(fields))
			}
		}
	}
	return rows.Err()
}

const canceledObjectSelect = `SELECT id,run_id,task_id,attempt_id,artifact_id,dataset_id,object_key,expected_size,expected_sha256,object_version,status,eligibility_reason,reference_decision,reference_evidence_digest,discovered_at,quarantine_until,COALESCE(last_verified_at,''),delete_attempt_count,operator_action_required,COALESCE(current_attempt_id,''),last_error_class,dry_run_result FROM canceled_object_candidates`

func scanCanceledObject(row rowScanner, c *CanceledObjectCandidate) error {
	var operator int
	if err := row.Scan(&c.ID, &c.RunID, &c.TaskID, &c.AttemptID, &c.ArtifactID, &c.DatasetID, &c.ObjectKey, &c.ExpectedSize, &c.ExpectedSHA256, &c.ObjectVersion, &c.Status, &c.EligibilityReason, &c.ReferenceDecision, &c.EvidenceDigest, &c.DiscoveredAt, &c.QuarantineUntil, &c.LastVerifiedAt, &c.DeleteAttemptCount, &operator, &c.CurrentAttemptID, &c.LastErrorClass, &c.DryRunResult); err != nil {
		return err
	}
	c.OperatorActionRequired = operator != 0
	return nil
}

func (s *Store) GetCanceledObjectCandidate(ctx context.Context, id string) (CanceledObjectCandidate, error) {
	var c CanceledObjectCandidate
	err := scanCanceledObject(s.db.QueryRowContext(ctx, canceledObjectSelect+` WHERE id=?`, id), &c)
	return c, err
}

func (s *Store) ListCanceledObjectCandidates(ctx context.Context, runID string) ([]CanceledObjectCandidate, error) {
	rows, err := s.db.QueryContext(ctx, canceledObjectSelect+` WHERE run_id=? ORDER BY discovered_at,id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CanceledObjectCandidate
	for rows.Next() {
		var c CanceledObjectCandidate
		if err := scanCanceledObject(rows, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *Store) CancelCanceledObjectCleanup(ctx context.Context, candidateID string, now time.Time) error {
	ns := now.UTC().Format(time.RFC3339Nano)
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		var current string
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(current_attempt_id,'') FROM canceled_object_candidates WHERE id=? AND status<>'DELETED'`, candidateID).Scan(&current); err != nil {
			return err
		}
		if current != "" {
			_, _ = tx.ExecContext(ctx, `UPDATE canceled_object_cleanup_attempts SET status='CANCELED',finished_at=?,error_class='CLEANUP_CANCELED',updated_at=? WHERE id=? AND status='ACTIVE'`, ns, ns, current)
		}
		_, err := tx.ExecContext(ctx, `UPDATE canceled_object_candidates SET status='CANCELED_CLEANUP',current_attempt_id=NULL,last_error_class='CLEANUP_CANCELED',updated_at=? WHERE id=?`, ns, candidateID)
		return err
	})
}

func referenceEvidenceTx(ctx context.Context, tx *sql.Tx, c CanceledObjectCandidate, now time.Time) (ReferenceEvidence, error) {
	e := ReferenceEvidence{CandidateID: c.ID, Checks: map[string]bool{}, Details: map[string]string{}, ObservedAt: now.UTC().Format(time.RFC3339Nano)}
	var commitID, commitIntent, registrationConfig string
	if err := tx.QueryRowContext(ctx, `SELECT status,commit_phase,commit_id,commit_intent_json,registration_config_json FROM runs WHERE id=?`, c.RunID).Scan(&e.RunStatus, &e.CommitPhase, &commitID, &commitIntent, &registrationConfig); err != nil {
		return e, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT status FROM tasks WHERE id=?`, c.TaskID).Scan(&e.TaskStatus); err != nil {
		return e, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT status FROM task_attempts WHERE id=?`, c.AttemptID).Scan(&e.AttemptStatus); err != nil {
		return e, err
	}
	var artifactRefs, activeMultipart, registrations int
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM task_artifacts WHERE object_key=?`, c.ObjectKey).Scan(&artifactRefs)
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM multipart_uploads WHERE object_key=? AND status NOT IN ('COMPLETED','ABORTED')`, c.ObjectKey).Scan(&activeMultipart)
	_ = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM iceberg_registrations WHERE run_id=?`, c.RunID).Scan(&registrations)
	e.Checks["artifact_reference"] = artifactRefs > 0
	e.Checks["active_multipart"] = activeMultipart > 0
	e.Checks["commit_identity"] = commitID != "" || e.CommitPhase != "" || strings.Contains(commitIntent, c.ObjectKey)
	e.Checks["registration_evidence"] = registrations > 0
	e.Checks["registration_configured"] = strings.TrimSpace(registrationConfig) != ""
	switch {
	case e.RunStatus != "CANCELED" || e.TaskStatus != "CANCELED" || e.AttemptStatus != "CANCELED":
		e.Decision = "ACTIVE"
	case e.Checks["artifact_reference"] || e.Checks["commit_identity"]:
		e.Decision = "REFERENCED"
	case e.Checks["active_multipart"]:
		e.Decision = "ACTIVE"
	case e.Checks["registration_evidence"] || e.Checks["registration_configured"]:
		e.Decision = "AMBIGUOUS"
	default:
		e.Decision = "UNREFERENCED"
	}
	canonical, _ := json.Marshal(struct {
		CandidateID, RunStatus, TaskStatus, AttemptStatus, CommitPhase, Decision string
		Checks                                                                   map[string]bool
	}{c.ID, e.RunStatus, e.TaskStatus, e.AttemptStatus, e.CommitPhase, e.Decision, e.Checks})
	sum := sha256.Sum256(canonical)
	e.EvidenceDigest = hex.EncodeToString(sum[:])
	return e, nil
}

func (s *Store) ClaimCanceledObjectCleanup(ctx context.Context, now time.Time, lease time.Duration) (CanceledObjectCandidate, CanceledObjectCleanupAttempt, bool, error) {
	var c CanceledObjectCandidate
	var a CanceledObjectCleanupAttempt
	ok := false
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		row := tx.QueryRowContext(ctx, canceledObjectSelect+` WHERE status IN ('QUARANTINED','DELETE_FAILED','DELETE_AMBIGUOUS') AND quarantine_until<=? AND current_attempt_id IS NULL ORDER BY quarantine_until,id LIMIT 1`, canceledObjectTimestamp(now))
		if err := scanCanceledObject(row, &c); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return tx.Commit()
			}
			return err
		}
		evidence, err := referenceEvidenceTx(ctx, tx, c, now)
		if err != nil {
			return err
		}
		if evidence.Decision != "UNREFERENCED" {
			status := "BLOCKED_AMBIGUOUS"
			event := "CANCELED_OBJECT_AMBIGUOUS"
			if evidence.Decision == "REFERENCED" || evidence.Decision == "ACTIVE" {
				status = "BLOCKED_REFERENCED"
				event = "CANCELED_OBJECT_REFERENCED"
			}
			_, err = tx.ExecContext(ctx, `UPDATE canceled_object_candidates SET status=?,reference_decision=?,reference_evidence_digest=?,operator_action_required=?,updated_at=? WHERE id=?`, status, evidence.Decision, evidence.EvidenceDigest, boolInt(status == "BLOCKED_AMBIGUOUS"), now.UTC().Format(time.RFC3339Nano), c.ID)
			if err != nil {
				return err
			}
			fields, _ := json.Marshal(map[string]any{"event_type": event, "candidate_id": c.ID, "reference_decision": evidence.Decision, "evidence_digest": evidence.EvidenceDigest})
			_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, "canceled-object-event-"+c.ID+"-"+strings.ToLower(event), c.RunID, now.UTC().Format(time.RFC3339Nano), "WARN", strings.ToLower(strings.ReplaceAll(event, "_", " ")), string(fields))
			return tx.Commit()
		}
		token, err := randomCanceledObjectToken()
		if err != nil {
			return err
		}
		a.AttemptNumber = c.DeleteAttemptCount + 1
		a.ID = fmt.Sprintf("cleanup-%s-%d", c.ID, a.AttemptNumber)
		a.CandidateID, a.FencingToken = c.ID, token
		a.LeaseDeadline = now.Add(lease).UTC().Format(time.RFC3339Nano)
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE canceled_object_candidates SET status='DELETE_PENDING',reference_decision='UNREFERENCED',reference_evidence_digest=?,delete_attempt_count=?,current_attempt_id=?,updated_at=? WHERE id=? AND current_attempt_id IS NULL`, evidence.EvidenceDigest, a.AttemptNumber, a.ID, ns, c.ID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return tx.Commit()
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO canceled_object_cleanup_attempts(id,candidate_id,attempt_number,leader_epoch,lease_deadline,status,fencing_token,evidence_digest,started_at,created_at,updated_at) VALUES(?,?,?,(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'),?,'ACTIVE',?,?,?, ?,?)`, a.ID, c.ID, a.AttemptNumber, a.LeaseDeadline, token, evidence.EvidenceDigest, ns, ns, ns)
		if err != nil {
			return err
		}
		fields, _ := json.Marshal(map[string]any{"event_type": "CANCELED_OBJECT_DELETE_SCHEDULED", "candidate_id": c.ID, "attempt_id": a.ID, "evidence_digest": evidence.EvidenceDigest})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, "canceled-object-event-"+c.ID+"-delete-scheduled-"+fmt.Sprint(a.AttemptNumber), c.RunID, ns, "WARN", "canceled object delete scheduled", string(fields))
		c.Status, c.ReferenceDecision, c.EvidenceDigest, c.CurrentAttemptID = "DELETE_PENDING", "UNREFERENCED", evidence.EvidenceDigest, a.ID
		ok = true
		return tx.Commit()
	})
	return c, a, ok, err
}

func (s *Store) AuthorizeCanceledObjectDelete(ctx context.Context, candidateID, attemptID, token, observation, version string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var c CanceledObjectCandidate
		if err := scanCanceledObject(tx.QueryRowContext(ctx, canceledObjectSelect+` WHERE id=? AND status='DELETE_PENDING' AND current_attempt_id=?`, candidateID, attemptID), &c); err != nil {
			return ErrCanceledObjectFenced
		}
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM canceled_object_cleanup_attempts WHERE id=? AND fencing_token=? AND status='ACTIVE'`, attemptID, token).Scan(new(int)); err != nil {
			return ErrCanceledObjectFenced
		}
		evidence, err := referenceEvidenceTx(ctx, tx, c, now)
		if err != nil || evidence.Decision != "UNREFERENCED" || evidence.EvidenceDigest != c.EvidenceDigest {
			return ErrCanceledObjectFenced
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE canceled_object_candidates SET status='DELETING',object_version=?,last_verified_at=?,delete_requested_at=?,updated_at=? WHERE id=? AND current_attempt_id=?`, version, ns, ns, ns, candidateID, attemptID)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE canceled_object_cleanup_attempts SET object_observation_identity=?,evidence_digest=?,updated_at=? WHERE id=? AND fencing_token=?`, observation, evidence.EvidenceDigest, ns, attemptID, token)
		if err != nil {
			return err
		}
		fields, _ := json.Marshal(map[string]any{"event_type": "CANCELED_OBJECT_DELETE_STARTED", "candidate_id": candidateID, "attempt_id": attemptID, "evidence_digest": evidence.EvidenceDigest, "object_observation_identity": observation})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) SELECT ?,run_id,?,'WARN','canceled object delete started',? FROM canceled_object_candidates WHERE id=?`, "canceled-object-event-"+candidateID+"-delete-started-"+attemptID, ns, string(fields), candidateID)
		return tx.Commit()
	})
}

func (s *Store) FinishCanceledObjectCleanup(ctx context.Context, candidateID, attemptID, token, outcome, class string, now time.Time, retry time.Duration, max int, dryRun bool) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT attempt_number FROM canceled_object_cleanup_attempts WHERE id=? AND candidate_id=? AND fencing_token=? AND status='ACTIVE'`, attemptID, candidateID, token).Scan(&count); err != nil {
			return ErrCanceledObjectFenced
		}
		status, attemptStatus, event := "DELETED", "SUCCEEDED", "CANCELED_OBJECT_DELETED"
		operator, dryResult := 0, ""
		var next any
		switch {
		case dryRun:
			status, attemptStatus, event, dryResult = "QUARANTINED", "SUCCEEDED", "CANCELED_OBJECT_DELETE_SCHEDULED", "WOULD_DELETE"
			next = canceledObjectTimestamp(now.Add(retry))
		case outcome == "MISSING":
			status = "DELETED"
		case outcome == "AMBIGUOUS":
			status, attemptStatus, event = "DELETE_AMBIGUOUS", "AMBIGUOUS", "CANCELED_OBJECT_DELETE_AMBIGUOUS"
			next = canceledObjectTimestamp(now.Add(retry))
		case outcome == "CONFLICT":
			status, attemptStatus, event, operator = "OPERATOR_REVIEW", "FAILED", "CANCELED_OBJECT_IDENTITY_CONFLICT", 1
		case outcome != "DELETED":
			status, attemptStatus, event = "DELETE_FAILED", "FAILED", "CANCELED_OBJECT_DELETE_FAILED"
			next = canceledObjectTimestamp(now.Add(retry))
			if count >= max {
				status, event, operator, next = "OPERATOR_REVIEW", "CANCELED_OBJECT_CLEANUP_EXHAUSTED", 1, nil
			}
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE canceled_object_cleanup_attempts SET status=?,finished_at=?,next_eligible_at=?,error_class=?,updated_at=? WHERE id=? AND fencing_token=?`, attemptStatus, ns, next, class, ns, attemptID, token)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE canceled_object_candidates SET status=?,current_attempt_id=NULL,last_error_class=?,last_error_message=NULL,operator_action_required=?,dry_run_result=?,quarantine_until=CASE WHEN ? IS NOT NULL THEN ? ELSE quarantine_until END,deleted_at=CASE WHEN ?='DELETED' THEN ? ELSE deleted_at END,updated_at=? WHERE id=? AND current_attempt_id=?`, status, class, operator, dryResult, next, next, status, ns, ns, candidateID, attemptID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrCanceledObjectFenced
		}
		fields, _ := json.Marshal(map[string]any{"event_type": event, "candidate_id": candidateID, "classification": class, "dry_run": dryRun})
		_, _ = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,ts,level,message,fields_json) VALUES(?,?,?,?,?)`, "canceled-object-event-"+candidateID+"-"+strings.ToLower(event)+"-"+fmt.Sprint(count), ns, "WARN", strings.ToLower(strings.ReplaceAll(event, "_", " ")), string(fields))
		return tx.Commit()
	})
}

func (s *Store) ExpireCanceledObjectCleanupAttempts(ctx context.Context, now time.Time) (int64, error) {
	var expired int64
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		expired, err = expireCanceledObjectCleanupAttemptsTx(ctx, tx, now.UTC().Format(time.RFC3339Nano), nil)
		if err != nil {
			return err
		}
		return tx.Commit()
	})
	return expired, err
}

func expireCanceledObjectCleanupAttemptsTx(ctx context.Context, tx *sql.Tx, now string, afterStage func(string) error) (int64, error) {
	res, err := tx.ExecContext(ctx, `
		UPDATE canceled_object_cleanup_attempts
		SET status='EXPIRED',
		    finished_at=?,
		    error_class='CLEANUP_DELETE_AMBIGUOUS',
		    updated_at=?
		WHERE status='ACTIVE'
		  AND lease_deadline<=?`,
		now, now, now,
	)
	if err != nil {
		return 0, err
	}
	expired, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if afterStage != nil {
		if err := afterStage("attempts"); err != nil {
			return 0, err
		}
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE canceled_object_candidates
		SET status='DELETE_AMBIGUOUS',
		    current_attempt_id=NULL,
		    last_error_class='CLEANUP_DELETE_AMBIGUOUS',
		    updated_at=?
		WHERE EXISTS (
			SELECT 1
			FROM canceled_object_cleanup_attempts a
			WHERE a.id=canceled_object_candidates.current_attempt_id
			  AND a.candidate_id=canceled_object_candidates.id
			  AND a.status='EXPIRED'
		)`,
		now,
	)
	if err != nil {
		return 0, err
	}
	if afterStage != nil {
		if err := afterStage("candidates"); err != nil {
			return 0, err
		}
	}
	return expired, nil
}

func randomCanceledObjectToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
