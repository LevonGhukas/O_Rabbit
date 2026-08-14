package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type ReconciliationAttempt struct {
	ID, RegistrationID, FencingToken, LeaseDeadline string
	AttemptNumber                                   int
}
type ReconciliationProjection struct {
	Status                   string  `json:"reconciliation_status,omitempty"`
	Attempt                  int     `json:"reconciliation_attempt,omitempty"`
	Outcome                  string  `json:"reconciliation_outcome,omitempty"`
	ErrorClass               string  `json:"reconciliation_error_class,omitempty"`
	NextRetryAt              *string `json:"reconciliation_next_retry_at,omitempty"`
	ObservedSnapshotID       string  `json:"observed_snapshot_id,omitempty"`
	ObservedMetadataIdentity string  `json:"observed_metadata_identity,omitempty"`
	MatchedFiles             int     `json:"matched_file_count,omitempty"`
	ExpectedFiles            int     `json:"expected_file_count,omitempty"`
	EvidenceDigest           string  `json:"evidence_digest,omitempty"`
	OperatorActionRequired   bool    `json:"operator_action_required"`
}

func (s *Store) ClaimReconciliation(ctx context.Context, now time.Time, lease time.Duration) (Registration, ReconciliationAttempt, bool, error) {
	var reg Registration
	var a ReconciliationAttempt
	ok := false
	err := withBusyRetry(ctx, func() error {
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		row := tx.QueryRowContext(ctx, `SELECT id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,catalog_namespace,table_identifier,status,attempt_count,current_attempt_id,next_eligible_at,last_error_class,last_error_message,registered_snapshot_or_metadata_id,created_at,updated_at,registered_at,retry_override_config_json FROM iceberg_registrations WHERE status='RECONCILING' AND reconciliation_status IN ('PENDING','RETRY_REQUIRED') AND (reconciliation_next_eligible_at IS NULL OR reconciliation_next_eligible_at<=?) ORDER BY dataset_sequence,id LIMIT 1`, now.UTC().Format(time.RFC3339Nano))
		r, e := scanRegistration(row)
		if e == sql.ErrNoRows {
			return tx.Commit()
		}
		if e != nil {
			return e
		}
		var n int
		if e = tx.QueryRowContext(ctx, `SELECT reconciliation_attempt_count+1 FROM iceberg_registrations WHERE id=?`, r.ID).Scan(&n); e != nil {
			return e
		}
		token, e := randomRegistrationToken()
		if e != nil {
			return e
		}
		aid := stableRegistrationID("reconcile", r.ID, fmt.Sprint(n), token)
		ns := now.UTC().Format(time.RFC3339Nano)
		dl := now.Add(lease).UTC().Format(time.RFC3339Nano)
		res, e := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET reconciliation_status='INSPECTING',reconciliation_attempt_count=?,current_reconciliation_attempt_id=?,reconciliation_next_eligible_at=NULL,updated_at=? WHERE id=? AND status='RECONCILING' AND reconciliation_status IN ('PENDING','RETRY_REQUIRED')`, n, aid, ns, r.ID)
		if e != nil {
			return e
		}
		affected, _ := res.RowsAffected()
		if affected != 1 {
			return tx.Commit()
		}
		_, e = tx.ExecContext(ctx, `INSERT INTO iceberg_reconciliation_attempts(id,registration_id,attempt_number,status,fencing_token,lease_deadline,started_at,created_at,updated_at,leader_epoch) VALUES(?,?,?,'ACTIVE',?,?,?,?,?,(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'))`, aid, r.ID, n, token, dl, ns, ns, ns)
		if e != nil {
			return e
		}
		_ = insertRegistrationEventTx(ctx, tx, "RECONCILIATION_ATTEMPT_ASSIGNED", r.ID, aid, "INFO", "reconciliation attempt assigned", ns, map[string]any{"lease_deadline": dl})
		if e = tx.Commit(); e != nil {
			return e
		}
		reg = r
		a = ReconciliationAttempt{ID: aid, RegistrationID: r.ID, AttemptNumber: n, FencingToken: token, LeaseDeadline: dl}
		ok = true
		return nil
	})
	return reg, a, ok, err
}

func (s *Store) ApplyReconciliationDecision(ctx context.Context, registrationID, attemptID, token, outcome, evidenceDigest, startID, endID, snapshotID, receipt string, matched, expected int, now time.Time, maxAmbiguities int) error {
	return withBusyRetry(ctx, func() error {
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		ns := now.UTC().Format(time.RFC3339Nano)
		var current string
		var cycles int
		if e = tx.QueryRowContext(ctx, `SELECT current_reconciliation_attempt_id,ambiguity_retry_count FROM iceberg_registrations WHERE id=? AND status='RECONCILING' AND reconciliation_status='INSPECTING'`, registrationID).Scan(&current, &cycles); e != nil || current != attemptID {
			return ErrRegistrationFenced
		}
		var regStatus, recStatus, event string
		operator := 0
		var next any
		switch outcome {
		case "EXACTLY_COMMITTED":
			regStatus, recStatus, event = "RETRY_REQUIRED", "SUCCEEDED", "RECONCILIATION_EXACT_COMMIT_FOUND"
			next = ns
		case "DEFINITELY_NOT_COMMITTED", "TABLE_NOT_FOUND":
			cycles++
			if cycles > maxAmbiguities {
				regStatus, recStatus, event = "QUARANTINED", "FAILED", "RECONCILIATION_RETRY_EXHAUSTED"
				operator = 1
			} else {
				regStatus, recStatus, event = "RETRY_REQUIRED", "SUCCEEDED", "RECONCILIATION_ABSENCE_PROVEN"
				next = ns
			}
		case "PARTIALLY_COMMITTED":
			regStatus, recStatus, event = "QUARANTINED", "FAILED", "RECONCILIATION_PARTIAL_COMMIT"
			operator = 1
		case "CONFLICTING_COMMIT":
			regStatus, recStatus, event = "QUARANTINED", "FAILED", "RECONCILIATION_CONFLICT"
			operator = 1
		default:
			regStatus, recStatus, event = "RECONCILING", "FAILED", "RECONCILIATION_INSUFFICIENT_EVIDENCE"
			operator = 1
		}
		_, e = tx.ExecContext(ctx, `UPDATE iceberg_reconciliation_attempts SET status=?,observation_start_identity=?,observation_end_identity=?,outcome=?,evidence_digest=?,finished_at=?,updated_at=? WHERE id=? AND fencing_token=? AND status='ACTIVE'`, recStatus, startID, endID, outcome, evidenceDigest, ns, ns, attemptID, token)
		if e != nil {
			return e
		}
		res, e := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status=?,reconciliation_status=?,current_reconciliation_attempt_id=NULL,reconciliation_outcome=?,observed_snapshot_id=?,observed_metadata_identity=?,matched_file_count=?,expected_file_count=?,reconciliation_evidence_digest=?,registered_snapshot_or_metadata_id=CASE WHEN ?<>'' THEN ? ELSE registered_snapshot_or_metadata_id END,next_eligible_at=?,ambiguity_retry_count=?,updated_at=? WHERE id=? AND current_reconciliation_attempt_id=?`, regStatus, recStatus, outcome, snapshotID, endID, matched, expected, evidenceDigest, receipt, receipt, next, cycles, ns, registrationID, attemptID)
		if e != nil {
			return e
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		fields := map[string]any{"outcome": outcome, "evidence_digest": evidenceDigest, "operator_action_required": operator}
		_ = insertRegistrationEventTx(ctx, tx, event, registrationID, "", "INFO", "catalog reconciliation decision persisted", ns, fields)
		return tx.Commit()
	})
}

func (s *Store) RetryReconciliationObservation(ctx context.Context, registrationID, attemptID, token, class, message string, now time.Time, backoff time.Duration, max int) error {
	return withBusyRetry(ctx, func() error {
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		var n int
		if e = tx.QueryRowContext(ctx, `SELECT attempt_number FROM iceberg_reconciliation_attempts WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE'`, attemptID, registrationID, token).Scan(&n); e != nil {
			return ErrRegistrationFenced
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		status := "RETRY_REQUIRED"
		event := "RECONCILIATION_OBSERVATION_RETRY"
		next := now.Add(backoff).UTC().Format(time.RFC3339Nano)
		if n >= max {
			status = "FAILED"
			event = "RECONCILIATION_RETRY_EXHAUSTED"
		}
		_, e = tx.ExecContext(ctx, `UPDATE iceberg_reconciliation_attempts SET status=?,error_class=?,finished_at=?,next_eligible_at=?,updated_at=? WHERE id=?`, status, class, ns, next, ns, attemptID)
		if e != nil {
			return e
		}
		_, e = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET reconciliation_status=?,current_reconciliation_attempt_id=NULL,reconciliation_error_class=?,reconciliation_next_eligible_at=?,updated_at=? WHERE id=? AND current_reconciliation_attempt_id=?`, status, class, next, ns, registrationID, attemptID)
		if e != nil {
			return e
		}
		_ = insertRegistrationEventTx(ctx, tx, event, registrationID, "", "WARN", message, ns, map[string]any{"classification": class, "next_retry_at": next})
		return tx.Commit()
	})
}

func (s *Store) RenewReconciliationLease(ctx context.Context, registrationID, attemptID, token string, now time.Time, lease time.Duration) error {
	deadline := now.Add(lease).UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_reconciliation_attempts SET lease_deadline=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND EXISTS(SELECT 1 FROM iceberg_registrations WHERE id=? AND status='RECONCILING' AND reconciliation_status='INSPECTING' AND current_reconciliation_attempt_id=?)`,
		deadline, now.UTC().Format(time.RFC3339Nano), attemptID, registrationID, token, registrationID, attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrRegistrationFenced
	}
	return nil
}

func (s *Store) ExpireReconciliationAttempts(ctx context.Context, now time.Time, backoff time.Duration, max int) (int, error) {
	count := 0
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT a.id,a.registration_id,a.attempt_number FROM iceberg_reconciliation_attempts a JOIN iceberg_registrations r ON r.id=a.registration_id WHERE a.status='ACTIVE' AND a.lease_deadline<=? AND r.status='RECONCILING' AND r.reconciliation_status='INSPECTING' AND r.current_reconciliation_attempt_id=a.id`, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		type expired struct {
			id, registrationID string
			number             int
		}
		var attempts []expired
		for rows.Next() {
			var a expired
			if err := rows.Scan(&a.id, &a.registrationID, &a.number); err != nil {
				rows.Close()
				return err
			}
			attempts = append(attempts, a)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		for _, a := range attempts {
			status, event := "RETRY_REQUIRED", "RECONCILIATION_OBSERVATION_RETRY"
			var next any = now.Add(backoff).UTC().Format(time.RFC3339Nano)
			if a.number >= max {
				status, event, next = "FAILED", "RECONCILIATION_RETRY_EXHAUSTED", nil
			}
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_reconciliation_attempts SET status='EXPIRED',error_class='CATALOG_OBSERVATION_UNAVAILABLE',finished_at=?,updated_at=? WHERE id=? AND status='ACTIVE'`, ns, ns, a.id); err != nil {
				return err
			}
			res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET reconciliation_status=?,current_reconciliation_attempt_id=NULL,reconciliation_error_class='CATALOG_OBSERVATION_UNAVAILABLE',reconciliation_next_eligible_at=?,updated_at=? WHERE id=? AND current_reconciliation_attempt_id=?`, status, next, ns, a.registrationID, a.id)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			if n != 1 {
				continue
			}
			count++
			if err := insertRegistrationEventTx(ctx, tx, event, a.registrationID, "", "WARN", "reconciliation observation lease expired", ns, map[string]any{"classification": "CATALOG_OBSERVATION_UNAVAILABLE", "next_retry_at": next}); err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return count, err
}

func (s *Store) CancelReconciliation(ctx context.Context, registrationID string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var attempt sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT current_reconciliation_attempt_id FROM iceberg_registrations WHERE id=? AND status='RECONCILING' AND reconciliation_status IN ('PENDING','RETRY_REQUIRED','INSPECTING')`, registrationID).Scan(&attempt); err != nil {
			return err
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		if attempt.Valid {
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_reconciliation_attempts SET status='CANCELED',error_class='RECONCILIATION_CANCELED',finished_at=?,updated_at=? WHERE id=? AND status='ACTIVE'`, ns, ns, attempt.String); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET reconciliation_status='CANCELED',current_reconciliation_attempt_id=NULL,reconciliation_error_class='RECONCILIATION_CANCELED',reconciliation_next_eligible_at=NULL,reconciliation_outcome='INSUFFICIENT_EVIDENCE',updated_at=? WHERE id=? AND status='RECONCILING'`, ns, registrationID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		if err := insertRegistrationEventTx(ctx, tx, "RECONCILIATION_CANCELED", registrationID, "", "WARN", "reconciliation inspection canceled; ambiguous registration remains blocked", ns, map[string]any{"classification": "RECONCILIATION_CANCELED"}); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) GetReconciliationProjection(ctx context.Context, id string) (ReconciliationProjection, error) {
	var p ReconciliationProjection
	var next sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT reconciliation_status,reconciliation_attempt_count,reconciliation_outcome,reconciliation_error_class,reconciliation_next_eligible_at,observed_snapshot_id,observed_metadata_identity,matched_file_count,expected_file_count,reconciliation_evidence_digest FROM iceberg_registrations WHERE id=?`, id).Scan(&p.Status, &p.Attempt, &p.Outcome, &p.ErrorClass, &next, &p.ObservedSnapshotID, &p.ObservedMetadataIdentity, &p.MatchedFiles, &p.ExpectedFiles, &p.EvidenceDigest)
	if next.Valid {
		p.NextRetryAt = &next.String
	}
	p.OperatorActionRequired = p.Outcome == "PARTIALLY_COMMITTED" || p.Outcome == "CONFLICTING_COMMIT" || p.Outcome == "INSUFFICIENT_EVIDENCE"
	return p, err
}

func ReconciliationReceipt(reg Registration, decision any, now time.Time) (string, error) {
	body, err := json.Marshal(map[string]any{"version": 1, "resolution": "RECONCILED_COMMITTED", "registration_id": reg.ID, "run_id": reg.RunID, "dataset_id": reg.DatasetID, "commit_id": reg.CommitID, "artifact_set_digest": reg.ArtifactSetDigest, "backend": reg.BackendType, "namespace": reg.CatalogNamespace, "table": reg.TableIdentifier, "reconciled_at": now.UTC().Format(time.RFC3339Nano), "evidence": decision})
	return string(body), err
}
