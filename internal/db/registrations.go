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

const (
	RegistrationPending       = "PENDING"
	RegistrationRegistering   = "REGISTERING"
	RegistrationRetryRequired = "RETRY_REQUIRED"
	RegistrationReconciling   = "RECONCILING"
	RegistrationRegistered    = "REGISTERED"
	RegistrationFailed        = "FAILED"
	RegistrationQuarantined   = "QUARANTINED"
	RegistrationCanceled      = "CANCELED"
	RegistrationBlocked       = "BLOCKED"
)

type Registration struct {
	ID                      string          `json:"registration_id"`
	RunID                   string          `json:"run_id"`
	DatasetID               string          `json:"dataset_id"`
	DatasetSequence         int64           `json:"dataset_sequence"`
	TargetKey               string          `json:"-"`
	CommitID                string          `json:"commit_id"`
	ManifestKey             string          `json:"manifest_key"`
	ArtifactSetDigest       string          `json:"artifact_set_digest"`
	BackendType             string          `json:"backend_type"`
	CatalogNamespace        string          `json:"catalog_namespace"`
	TableIdentifier         string          `json:"table_identifier"`
	Status                  string          `json:"catalog_status"`
	AttemptCount            int             `json:"registration_attempt"`
	CurrentAttemptID        *string         `json:"-"`
	NextEligibleAt          *string         `json:"registration_next_retry_at,omitempty"`
	LastErrorClass          string          `json:"registration_last_error_class,omitempty"`
	LastErrorMessage        *string         `json:"registration_last_error_message,omitempty"`
	Receipt                 string          `json:"registered_snapshot_or_metadata_id,omitempty"`
	CreatedAt               string          `json:"created_at"`
	UpdatedAt               string          `json:"updated_at"`
	RegisteredAt            *string         `json:"registered_at,omitempty"`
	BlockedBy               string          `json:"registration_blocked_by,omitempty"`
	RetryOverrideConfigJSON json.RawMessage `json:"-"`
}

type RegistrationAttempt struct {
	ID             string
	RegistrationID string
	AttemptNumber  int
	Status         string
	FencingToken   string
	LeaseDeadline  string
	LastRenewedAt  string
	Phase          string
	StartedAt      string
	FinishedAt     *string
	FailureClass   string
	FailureMessage *string
	NextEligibleAt *string
	CatalogReceipt string
}

type RegistrationPolicy struct {
	LeaseDuration           time.Duration
	MaxAttempts             int
	BackoffBase, BackoffMax time.Duration
}

var ErrRegistrationFenced = errors.New("registration attempt fenced")
var ErrRegistrationCancelTooLate = errors.New("registration cancellation is too late")

func stableRegistrationID(parts ...string) string {
	h := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return "reg-" + hex.EncodeToString(h[:16])
}
func randomRegistrationToken() (string, error) {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func insertRegistrationEventTx(ctx context.Context, tx *sql.Tx, eventType, registrationID, attemptID, level, message, now string, extra map[string]any) error {
	var runID, datasetID, commitID, backend, table string
	var attemptNumber int
	if err := tx.QueryRowContext(ctx, `SELECT run_id,dataset_id,commit_id,backend_type,table_identifier FROM iceberg_registrations WHERE id=?`, registrationID).Scan(&runID, &datasetID, &commitID, &backend, &table); err != nil {
		return err
	}
	if attemptID != "" {
		if err := tx.QueryRowContext(ctx, `SELECT attempt_number FROM iceberg_registration_attempts WHERE id=?`, attemptID).Scan(&attemptNumber); err != nil {
			_ = tx.QueryRowContext(ctx, `SELECT attempt_number FROM iceberg_reconciliation_attempts WHERE id=?`, attemptID).Scan(&attemptNumber)
		}
	}
	fields := map[string]any{"event_type": eventType, "registration_id": registrationID, "dataset_id": datasetID, "commit_id": commitID, "backend": backend, "table_identifier": table}
	if attemptID != "" {
		fields["attempt_id"], fields["attempt_number"] = attemptID, attemptNumber
	}
	for k, v := range extra {
		fields[k] = v
	}
	body, _ := json.Marshal(fields)
	id := "registration-event-" + stableRegistrationID(eventType, registrationID, attemptID, fmt.Sprint(extra["classification"]), fmt.Sprint(extra["next_retry_at"]))
	_, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, id, runID, now, level, message, string(body))
	return err
}

type registrationConfigIdentity struct {
	Enabled bool   `json:"enabled"`
	Engine  string `json:"engine"`
	Table   string `json:"table"`
	URI     string `json:"uri"`
}
type registrationCommitIntent struct {
	ManifestKey string          `json:"manifest_key"`
	Manifest    json.RawMessage `json:"manifest"`
}
type registrationManifest struct {
	SchemaVersion int               `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Artifacts     []json.RawMessage `json:"artifacts"`
}

// ensureRegistrationTx is part of the same SQLite transaction that exposes a
// verified commit as SUCCEEDED. It deliberately stores only a target identity;
// credentials remain in the existing persisted run snapshot.
func ensureRegistrationTx(ctx context.Context, tx *sql.Tx, runID, datasetID, commitID, configJSON, intentJSON, now string) error {
	var cfg registrationConfigIdentity
	if strings.TrimSpace(configJSON) == "" {
		return nil
	}
	if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil || !cfg.Enabled {
		return nil
	}
	cfg.Engine = strings.ToLower(strings.TrimSpace(cfg.Engine))
	if cfg.Engine == "" {
		cfg.Engine = "rest-go"
	}
	cfg.Table, cfg.URI = strings.TrimSpace(cfg.Table), strings.TrimSpace(cfg.URI)
	var intent registrationCommitIntent
	if err := json.Unmarshal([]byte(intentJSON), &intent); err != nil || strings.TrimSpace(intent.ManifestKey) == "" {
		return fmt.Errorf("registration requires durable commit intent")
	}
	var manifest registrationManifest
	if err := json.Unmarshal(intent.Manifest, &manifest); err != nil || manifest.SchemaVersion != 2 || manifest.RunID != runID {
		return fmt.Errorf("registration requires exact schema-v2 verified manifest")
	}
	manifestDigest := sha256.Sum256(intent.Manifest)
	if hex.EncodeToString(manifestDigest[:]) != commitID {
		return fmt.Errorf("registration commit id does not authenticate manifest")
	}
	// Hash the canonical bytes embedded in the immutable commit intent. The
	// manifest commit_id already authenticates these bytes.
	h := sha256.New()
	for _, a := range manifest.Artifacts {
		h.Write(a)
		h.Write([]byte{0})
	}
	artifactDigest := hex.EncodeToString(h.Sum(nil))
	targetHash := sha256.Sum256([]byte(cfg.Engine + "\x00" + cfg.URI + "\x00" + cfg.Table))
	targetKey := hex.EncodeToString(targetHash[:])
	id := stableRegistrationID(runID, targetKey)
	var seq int64
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(dataset_sequence),0)+1 FROM iceberg_registrations WHERE dataset_id=? AND target_key=?`, datasetID, targetKey).Scan(&seq); err != nil {
		return err
	}
	ns := ""
	if i := strings.LastIndex(cfg.Table, "."); i > 0 {
		ns = cfg.Table[:i]
	}
	res, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO iceberg_registrations(id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,catalog_namespace,table_identifier,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,'PENDING',?,?)`, id, runID, datasetID, seq, targetKey, commitID, intent.ManifestKey, artifactDigest, cfg.Engine, ns, cfg.Table, now, now)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil
	}
	fields, _ := json.Marshal(map[string]any{"event_type": "REGISTRATION_QUEUED", "registration_id": id, "commit_id": commitID, "manifest_key": intent.ManifestKey})
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'INFO','iceberg registration queued',?)`, "registration-queued-"+id, runID, now, string(fields))
	return err
}

func (s *Store) GetRegistrationForRun(ctx context.Context, runID string) (Registration, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,catalog_namespace,table_identifier,status,attempt_count,current_attempt_id,next_eligible_at,last_error_class,last_error_message,registered_snapshot_or_metadata_id,created_at,updated_at,registered_at,retry_override_config_json FROM iceberg_registrations WHERE run_id=? ORDER BY id LIMIT 1`, runID)
	return scanRegistration(row)
}

type rowScanner interface{ Scan(...any) error }

func scanRegistration(row rowScanner) (Registration, error) {
	var r Registration
	var retryOverride string
	err := row.Scan(&r.ID, &r.RunID, &r.DatasetID, &r.DatasetSequence, &r.TargetKey, &r.CommitID, &r.ManifestKey, &r.ArtifactSetDigest, &r.BackendType, &r.CatalogNamespace, &r.TableIdentifier, &r.Status, &r.AttemptCount, &r.CurrentAttemptID, &r.NextEligibleAt, &r.LastErrorClass, &r.LastErrorMessage, &r.Receipt, &r.CreatedAt, &r.UpdatedAt, &r.RegisteredAt, &retryOverride)
	if strings.TrimSpace(retryOverride) != "" {
		r.RetryOverrideConfigJSON = json.RawMessage(retryOverride)
	}
	return r, err
}

// RequeueRegistrationManual moves a terminal, safe registration back to the durable worker queue.
// attempt_count remains monotonic, preserving the attempt-number uniqueness constraint.
func (s *Store) RequeueRegistrationManual(ctx context.Context, runID string, override json.RawMessage, now time.Time) (Registration, bool, error) {
	var out Registration
	queued := false
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var status, phase string
		if err := tx.QueryRowContext(ctx, `SELECT status,commit_phase FROM runs WHERE id=?`, runID).Scan(&status, &phase); err != nil {
			return err
		}
		if status != "SUCCEEDED" || phase != "COMPLETE" {
			return fmt.Errorf("registration retry requires a succeeded run with a complete commit")
		}
		row := tx.QueryRowContext(ctx, `SELECT id,run_id,dataset_id,dataset_sequence,target_key,commit_id,manifest_key,artifact_set_digest,backend_type,catalog_namespace,table_identifier,status,attempt_count,current_attempt_id,next_eligible_at,last_error_class,last_error_message,registered_snapshot_or_metadata_id,created_at,updated_at,registered_at,retry_override_config_json FROM iceberg_registrations WHERE run_id=? ORDER BY id LIMIT 1`, runID)
		reg, err := scanRegistration(row)
		if err != nil {
			return err
		}
		switch reg.Status {
		case RegistrationPending, RegistrationRetryRequired:
			// The endpoint is idempotent for queued work, but a no-override retry
			// must still clear a prior queued override before it is claimed.
			if _, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET retry_override_config_json=?,updated_at=? WHERE id=? AND status IN ('PENDING','RETRY_REQUIRED') AND current_attempt_id IS NULL`, string(override), now.UTC().Format(time.RFC3339Nano), reg.ID); err != nil {
				return err
			}
			reg.RetryOverrideConfigJSON = append(json.RawMessage(nil), override...)
			out = reg
			queued = true
			return tx.Commit()
		case RegistrationFailed, RegistrationCanceled:
			// safe terminal states
		default:
			return fmt.Errorf("registration retry is not allowed while status is %s", reg.Status)
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='PENDING',current_attempt_id=NULL,next_eligible_at=NULL,last_error_class='',last_error_message=NULL,retry_override_config_json=?,manual_retry_budget=manual_retry_budget+1,updated_at=? WHERE id=? AND status IN ('FAILED','CANCELED')`, string(override), ns, reg.ID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return fmt.Errorf("registration retry was concurrently changed")
		}
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_MANUAL_RETRY_QUEUED", reg.ID, "", "INFO", "manual registration retry queued", ns, nil); err != nil {
			return err
		}
		reg.Status, reg.CurrentAttemptID, reg.NextEligibleAt, reg.LastErrorClass, reg.LastErrorMessage, reg.RetryOverrideConfigJSON = RegistrationPending, nil, nil, "", nil, append(json.RawMessage(nil), override...)
		out, queued = reg, true
		return tx.Commit()
	})
	return out, queued, err
}

func (s *Store) ClaimRegistration(ctx context.Context, now time.Time, policy RegistrationPolicy) (Registration, RegistrationAttempt, bool, error) {
	if policy.LeaseDuration <= 0 {
		policy.LeaseDuration = 30 * time.Second
	}
	if policy.MaxAttempts <= 0 {
		policy.MaxAttempts = 3
	}
	var out Registration
	var attempt RegistrationAttempt
	claimed := false
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		defer tx.Rollback()
		row := tx.QueryRowContext(ctx, `SELECT r.id,r.run_id,r.dataset_id,r.dataset_sequence,r.target_key,r.commit_id,r.manifest_key,r.artifact_set_digest,r.backend_type,r.catalog_namespace,r.table_identifier,r.status,r.attempt_count,r.current_attempt_id,r.next_eligible_at,r.last_error_class,r.last_error_message,r.registered_snapshot_or_metadata_id,r.created_at,r.updated_at,r.registered_at,r.retry_override_config_json FROM iceberg_registrations r WHERE r.status IN ('PENDING','RETRY_REQUIRED') AND (r.next_eligible_at IS NULL OR r.next_eligible_at<=?) AND NOT EXISTS (SELECT 1 FROM iceberg_registrations p WHERE p.dataset_id=r.dataset_id AND p.target_key=r.target_key AND p.dataset_sequence<r.dataset_sequence AND p.status<>'REGISTERED') ORDER BY r.dataset_sequence,r.id LIMIT 1`, now.UTC().Format(time.RFC3339Nano))
		r, err := scanRegistration(row)
		if err == sql.ErrNoRows {
			blocked, qerr := tx.QueryContext(ctx, `SELECT r.id,p.id FROM iceberg_registrations r JOIN iceberg_registrations p ON p.dataset_id=r.dataset_id AND p.target_key=r.target_key AND p.dataset_sequence=(SELECT MIN(x.dataset_sequence) FROM iceberg_registrations x WHERE x.dataset_id=r.dataset_id AND x.target_key=r.target_key AND x.dataset_sequence<r.dataset_sequence AND x.status<>'REGISTERED') WHERE r.status IN ('PENDING','RETRY_REQUIRED')`)
			if qerr != nil {
				return qerr
			}
			for blocked.Next() {
				var rid, blocker string
				if err := blocked.Scan(&rid, &blocker); err != nil {
					blocked.Close()
					return err
				}
				if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_BLOCKED", rid, "", "WARN", "registration blocked by earlier unresolved work", now.UTC().Format(time.RFC3339Nano), map[string]any{"blocking_registration_id": blocker}); err != nil {
					blocked.Close()
					return err
				}
			}
			blocked.Close()
			return tx.Commit()
		}
		if err != nil {
			return err
		}
		var manualRetryBudget int
		if err := tx.QueryRowContext(ctx, `SELECT manual_retry_budget FROM iceberg_registrations WHERE id=?`, r.ID).Scan(&manualRetryBudget); err != nil {
			return err
		}
		if r.AttemptCount >= policy.MaxAttempts+manualRetryBudget {
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='FAILED',last_error_class='RETRY_LIMIT_EXHAUSTED',updated_at=? WHERE id=?`, now.UTC().Format(time.RFC3339Nano), r.ID)
			if err != nil {
				return err
			}
			return tx.Commit()
		}
		token, err := randomRegistrationToken()
		if err != nil {
			return err
		}
		num := r.AttemptCount + 1
		aid := stableRegistrationID(r.ID, fmt.Sprint(num), token)
		ns := now.UTC().Format(time.RFC3339Nano)
		dl := now.Add(policy.LeaseDuration).UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='REGISTERING',attempt_count=?,current_attempt_id=?,next_eligible_at=NULL,updated_at=? WHERE id=? AND status IN ('PENDING','RETRY_REQUIRED') AND current_attempt_id IS NULL`, num, aid, ns, r.ID)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return tx.Commit()
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO iceberg_registration_attempts(id,registration_id,attempt_number,status,fencing_token,lease_deadline,last_renewed_at,phase,started_at,created_at,updated_at,leader_epoch) VALUES(?,?,?,'ACTIVE',?,?,?,'PREPARED',?,?,?,(SELECT epoch FROM master_leadership WHERE leadership_name='master' AND status='ACTIVE'))`, aid, r.ID, num, token, dl, ns, ns, ns, ns)
		if err != nil {
			return err
		}
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_ATTEMPT_ASSIGNED", r.ID, aid, "INFO", "registration attempt assigned", ns, map[string]any{"lease_deadline": dl}); err != nil {
			return err
		}
		if err = tx.Commit(); err != nil {
			return err
		}
		r.Status = RegistrationRegistering
		r.AttemptCount = num
		r.CurrentAttemptID = &aid
		out = r
		attempt = RegistrationAttempt{ID: aid, RegistrationID: r.ID, AttemptNumber: num, Status: "ACTIVE", FencingToken: token, LeaseDeadline: dl, LastRenewedAt: ns, Phase: "PREPARED", StartedAt: ns}
		claimed = true
		return nil
	})
	return out, attempt, claimed, err
}

func (s *Store) AdvanceRegistrationPhase(ctx context.Context, registrationID, attemptID, token, from, to string, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET phase=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND phase=? AND EXISTS(SELECT 1 FROM iceberg_registrations WHERE id=? AND current_attempt_id=? AND status='REGISTERING')`, to, now.UTC().Format(time.RFC3339Nano), attemptID, registrationID, token, from, registrationID, attemptID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrRegistrationFenced
	}
	return nil
}

func (s *Store) PersistCatalogReceipt(ctx context.Context, registrationID, attemptID, token, receipt string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET phase='CATALOG_COMMITTED',catalog_receipt=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND phase='EXTERNAL_COMMIT_STARTED'`, receipt, ns, attemptID, registrationID, token)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		res, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET registered_snapshot_or_metadata_id=?,updated_at=? WHERE id=? AND current_attempt_id=? AND status='REGISTERING'`, receipt, ns, registrationID, attemptID)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_CATALOG_COMMITTED", registrationID, attemptID, "INFO", "registration catalog commit recorded", ns, map[string]any{"catalog_receipt": receipt}); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) PersistCatalogNoOpReceipt(ctx context.Context, registrationID, attemptID, token, receipt string, now time.Time) error {
	var proof struct {
		NoOp           bool   `json:"no_op"`
		Reason         string `json:"no_op_reason"`
		EvidenceDigest string `json:"no_op_evidence_digest"`
	}
	if err := json.Unmarshal([]byte(receipt), &proof); err != nil || !proof.NoOp ||
		strings.TrimSpace(proof.Reason) == "" || len(proof.EvidenceDigest) != sha256.Size*2 {
		return fmt.Errorf("invalid catalog no-op receipt")
	}
	if _, err := hex.DecodeString(proof.EvidenceDigest); err != nil {
		return fmt.Errorf("invalid catalog no-op evidence digest")
	}
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET phase='CATALOG_COMMITTED',catalog_receipt=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND phase='PREPARED'`, receipt, ns, attemptID, registrationID, token)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		res, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET registered_snapshot_or_metadata_id=?,updated_at=? WHERE id=? AND current_attempt_id=? AND status='REGISTERING' AND registered_snapshot_or_metadata_id=''`, receipt, ns, registrationID, attemptID)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_NOOP_VERIFIED", registrationID, attemptID, "INFO", "registration no-op verified", ns, map[string]any{"catalog_receipt": receipt}); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) CompleteRegistration(ctx context.Context, registrationID, attemptID, token string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		ns := now.UTC().Format(time.RFC3339Nano)
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET status='SUCCEEDED',phase='VERIFIED',finished_at=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND phase='ICE_STATE_WRITING'`, ns, ns, attemptID, registrationID, token)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		res, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='REGISTERED',current_attempt_id=NULL,registered_at=?,updated_at=?,last_error_class='',last_error_message=NULL WHERE id=? AND current_attempt_id=? AND status='REGISTERING'`, ns, ns, registrationID, attemptID)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		if n != 1 {
			return ErrRegistrationFenced
		}
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_COMPLETED", registrationID, attemptID, "INFO", "iceberg registration SUCCEEDED", ns, nil); err != nil {
			return err
		}
		return tx.Commit()
	})
}

// FailRegistrationAttempt is conservative: once the external boundary was
// crossed, every non-definitive error is durable reconciliation work.
func (s *Store) FailRegistrationAttempt(ctx context.Context, registrationID, attemptID, token, class, message string, retryable, definiteRejection bool, now time.Time, policy RegistrationPolicy) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var phase string
		var n int
		if err = tx.QueryRowContext(ctx, `SELECT phase,attempt_number FROM iceberg_registration_attempts WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE'`, attemptID, registrationID, token).Scan(&phase, &n); err != nil {
			return ErrRegistrationFenced
		}
		status, astatus := RegistrationFailed, "FAILED"
		var next any = nil
		if phase == "CATALOG_COMMITTED" {
			class = "ICE_STATE_VERIFY_FAILED"
			if n < policy.MaxAttempts {
				status, astatus = RegistrationRetryRequired, "RETRY_REQUIRED"
				b := policy.BackoffBase
				if b <= 0 {
					b = time.Second
				}
				next = now.Add(b).UTC().Format(time.RFC3339Nano)
			} else {
				class = "RETRY_LIMIT_EXHAUSTED"
			}
		} else if phase != "PREPARED" && !definiteRejection {
			status, astatus, class = RegistrationReconciling, "RECONCILING", "EXTERNAL_COMMIT_AMBIGUOUS"
		} else if retryable && n < policy.MaxAttempts {
			status, astatus = RegistrationRetryRequired, "RETRY_REQUIRED"
			b := policy.BackoffBase
			if b <= 0 {
				b = time.Second
			}
			for i := 1; i < n; i++ {
				b *= 2
				if policy.BackoffMax > 0 && b >= policy.BackoffMax {
					b = policy.BackoffMax
					break
				}
			}
			next = now.Add(b).UTC().Format(time.RFC3339Nano)
		} else if retryable {
			class = "RETRY_LIMIT_EXHAUSTED"
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		_, err = tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET status=?,finished_at=?,failure_class=?,failure_message=?,next_eligible_at=?,updated_at=? WHERE id=?`, astatus, ns, class, message, next, ns, attemptID)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status=?,current_attempt_id=NULL,next_eligible_at=?,last_error_class=?,last_error_message=?,reconciliation_status=CASE WHEN ?='RECONCILING' THEN 'PENDING' ELSE reconciliation_status END,updated_at=? WHERE id=? AND current_attempt_id=?`, status, next, class, message, status, ns, registrationID, attemptID)
		if err != nil {
			return err
		}
		affected, _ := res.RowsAffected()
		if affected != 1 {
			return ErrRegistrationFenced
		}
		eventType, eventMessage, level := "REGISTRATION_DEFINITELY_REJECTED", "registration definitely rejected", "ERROR"
		if status == RegistrationRetryRequired {
			eventType, eventMessage, level = "REGISTRATION_RETRY_SCHEDULED", "registration retry scheduled", "WARN"
		}
		if status == RegistrationReconciling {
			eventType, eventMessage = "REGISTRATION_RECONCILIATION_REQUIRED", "registration requires reconciliation"
		}
		if class == "ICE_STATE_VERIFY_FAILED" {
			eventType, eventMessage = "REGISTRATION_ICE_STATE_REPAIR_REQUIRED", "registration ice state repair required"
		}
		if class == "RETRY_LIMIT_EXHAUSTED" {
			eventType, eventMessage = "REGISTRATION_RETRY_EXHAUSTED", "registration retry exhausted"
		}
		if err := insertRegistrationEventTx(ctx, tx, eventType, registrationID, attemptID, level, eventMessage, ns, map[string]any{"classification": class, "next_retry_at": next}); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func (s *Store) RenewRegistrationLease(ctx context.Context, registrationID, attemptID, token string, now time.Time, d time.Duration) error {
	res, err := s.db.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET lease_deadline=?,last_renewed_at=?,updated_at=? WHERE id=? AND registration_id=? AND fencing_token=? AND status='ACTIVE' AND lease_deadline>?`, now.Add(d).UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), attemptID, registrationID, token, now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n != 1 {
		return ErrRegistrationFenced
	}
	return nil
}

func (s *Store) ExpireRegistrationAttempts(ctx context.Context, now time.Time, policy RegistrationPolicy) (int, error) {
	count := 0
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT a.id,a.registration_id,a.phase,r.attempt_count FROM iceberg_registration_attempts a JOIN iceberg_registrations r ON r.id=a.registration_id AND r.current_attempt_id=a.id WHERE a.status='ACTIVE' AND a.lease_deadline<=?`, now.UTC().Format(time.RFC3339Nano))
		if err != nil {
			return err
		}
		type x struct {
			id, rid, phase string
			n              int
		}
		var xs []x
		for rows.Next() {
			var v x
			if err := rows.Scan(&v.id, &v.rid, &v.phase, &v.n); err != nil {
				rows.Close()
				return err
			}
			xs = append(xs, v)
		}
		rows.Close()
		for _, v := range xs {
			status, astatus, class := "RETRY_REQUIRED", "EXPIRED", "LEASE_EXPIRED_BEFORE_EXTERNAL_COMMIT"
			var next any = nil
			if v.phase == "CATALOG_COMMITTED" || v.phase == "ICE_STATE_WRITING" {
				status, astatus, class = "RETRY_REQUIRED", "EXPIRED", "ICE_STATE_VERIFY_FAILED"
				b := policy.BackoffBase
				if b <= 0 {
					b = time.Second
				}
				next = now.Add(b).UTC().Format(time.RFC3339Nano)
			} else if v.phase != "PREPARED" {
				status, astatus, class = "RECONCILING", "RECONCILING", "EXTERNAL_COMMIT_AMBIGUOUS"
			} else if v.n >= policy.MaxAttempts {
				status, class = "FAILED", "RETRY_LIMIT_EXHAUSTED"
			} else {
				b := policy.BackoffBase
				if b <= 0 {
					b = time.Second
				}
				for i := 1; i < v.n; i++ {
					b *= 2
					if policy.BackoffMax > 0 && b >= policy.BackoffMax {
						b = policy.BackoffMax
						break
					}
				}
				next = now.Add(b).UTC().Format(time.RFC3339Nano)
			}
			ns := now.UTC().Format(time.RFC3339Nano)
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET status=?,finished_at=?,failure_class=?,updated_at=? WHERE id=? AND status='ACTIVE'`, astatus, ns, class, ns, v.id)
			if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status=?,current_attempt_id=NULL,next_eligible_at=?,last_error_class=?,last_error_message='registration lease expired',reconciliation_status=CASE WHEN ?='RECONCILING' THEN 'PENDING' ELSE reconciliation_status END,updated_at=? WHERE id=? AND current_attempt_id=?`, status, next, class, status, ns, v.rid, v.id)
			if err != nil {
				return err
			}
			eventType := "REGISTRATION_ATTEMPT_EXPIRED"
			if status == RegistrationReconciling {
				eventType = "REGISTRATION_RECONCILIATION_REQUIRED"
			}
			if err := insertRegistrationEventTx(ctx, tx, eventType, v.rid, v.id, "WARN", "registration attempt lease expired", ns, map[string]any{"classification": class, "next_retry_at": next}); err != nil {
				return err
			}
			count++
		}
		return tx.Commit()
	})
	return count, err
}

func RegistrationReadiness(dataStatus, catalogStatus string) string {
	switch dataStatus {
	case "FAILED":
		return "FAILED"
	case "CANCELED":
		return "CANCELED"
	case "COMMITTING":
		return "COMMIT_RECONCILING"
	case "SUCCEEDED":
		// Continue into the catalog projection below.
	default:
		return "IN_PROGRESS"
	}
	switch catalogStatus {
	case "":
		return "DATA_COMMITTED"
	case RegistrationPending:
		return "CATALOG_PENDING"
	case RegistrationRegistering:
		return "CATALOG_REGISTERING"
	case RegistrationRetryRequired:
		return "CATALOG_RETRYING"
	case RegistrationReconciling:
		return "CATALOG_RECONCILING"
	case RegistrationRegistered:
		return "READY"
	case RegistrationFailed, RegistrationQuarantined, RegistrationCanceled:
		return "CATALOG_FAILED"
	default:
		return "CATALOG_BLOCKED"
	}
}

type RegistrationCancelResult struct {
	Status    string
	Changed   bool
	AttemptID string
}

func (s *Store) CancelRegistration(ctx context.Context, registrationID string, now time.Time) (RegistrationCancelResult, error) {
	var out RegistrationCancelResult
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var status string
		var aid sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT status,current_attempt_id FROM iceberg_registrations WHERE id=?`, registrationID).Scan(&status, &aid); err != nil {
			return err
		}
		out.Status = status
		if status == RegistrationRegistered || status == RegistrationFailed || status == RegistrationQuarantined || status == RegistrationCanceled {
			return ErrRegistrationCancelTooLate
		}
		ns := now.UTC().Format(time.RFC3339Nano)
		switch {
		case status == RegistrationPending || status == RegistrationRetryRequired:
			res, err := tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='CANCELED',next_eligible_at=NULL,last_error_class='REGISTRATION_CANCELED',last_error_message='canceled by operator',updated_at=? WHERE id=? AND status=?`, ns, registrationID, status)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return ErrRegistrationFenced
			}
		case status == RegistrationRegistering && aid.Valid:
			var phase string
			if err := tx.QueryRowContext(ctx, `SELECT phase FROM iceberg_registration_attempts WHERE id=? AND status='ACTIVE'`, aid.String).Scan(&phase); err != nil {
				return ErrRegistrationFenced
			}
			if phase != "PREPARED" {
				if phase == "EXTERNAL_COMMIT_STARTED" {
					_, _ = tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET status='RECONCILING',failure_class='EXTERNAL_COMMIT_AMBIGUOUS',finished_at=?,updated_at=? WHERE id=?`, ns, ns, aid.String)
					_, _ = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='RECONCILING',current_attempt_id=NULL,last_error_class='EXTERNAL_COMMIT_AMBIGUOUS',last_error_message='cancellation requested after external commit boundary',reconciliation_status='PENDING',updated_at=? WHERE id=?`, ns, registrationID)
					out.Status = RegistrationReconciling
					if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_RECONCILIATION_REQUIRED", registrationID, aid.String, "WARN", "registration cancellation requires reconciliation", ns, map[string]any{"classification": "EXTERNAL_COMMIT_AMBIGUOUS"}); err != nil {
						return err
					}
					return tx.Commit()
				}
				return ErrRegistrationCancelTooLate
			}
			res, err := tx.ExecContext(ctx, `UPDATE iceberg_registration_attempts SET status='CANCELED',failure_class='REGISTRATION_CANCELED',failure_message='canceled by operator',finished_at=?,updated_at=? WHERE id=? AND status='ACTIVE'`, ns, ns, aid.String)
			if err != nil {
				return err
			}
			if n, _ := res.RowsAffected(); n != 1 {
				return ErrRegistrationFenced
			}
			if _, err = tx.ExecContext(ctx, `UPDATE iceberg_registrations SET status='CANCELED',current_attempt_id=NULL,next_eligible_at=NULL,last_error_class='REGISTRATION_CANCELED',last_error_message='canceled by operator',updated_at=? WHERE id=? AND current_attempt_id=?`, ns, registrationID, aid.String); err != nil {
				return err
			}
			out.AttemptID = aid.String
		default:
			return ErrRegistrationCancelTooLate
		}
		out.Status, out.Changed = RegistrationCanceled, true
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_CANCELED", registrationID, out.AttemptID, "INFO", "registration canceled", ns, map[string]any{"classification": "REGISTRATION_CANCELED"}); err != nil {
			return err
		}
		return tx.Commit()
	})
	return out, err
}

func (s *Store) RecordRegistrationStaleResult(ctx context.Context, registrationID, attemptID, classification string, now time.Time) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err := insertRegistrationEventTx(ctx, tx, "REGISTRATION_STALE_RESULT_REJECTED", registrationID, attemptID, "WARN", "stale registration result rejected", now.UTC().Format(time.RFC3339Nano), map[string]any{"classification": classification}); err != nil {
			return err
		}
		return tx.Commit()
	})
}

type HistoricalClassification struct {
	RunID          string
	Classification string
	RegistrationID string
}

func (s *Store) ReconcileHistoricalRegistrations(ctx context.Context, now time.Time) ([]HistoricalClassification, error) {
	var out []HistoricalClassification
	err := withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		rows, err := tx.QueryContext(ctx, `SELECT id,dataset_key,commit_id,registration_config_json,commit_intent_json,commit_phase FROM runs WHERE status='SUCCEEDED' ORDER BY rowid`)
		if err != nil {
			return err
		}
		type item struct{ id, dataset, commit, cfg, intent, phase string }
		var items []item
		for rows.Next() {
			var v item
			if err := rows.Scan(&v.id, &v.dataset, &v.commit, &v.cfg, &v.intent, &v.phase); err != nil {
				rows.Close()
				return err
			}
			items = append(items, v)
		}
		rows.Close()
		ns := now.UTC().Format(time.RFC3339Nano)
		for _, v := range items {
			c := HistoricalClassification{RunID: v.id}
			var existingID, status, receipt string
			err := tx.QueryRowContext(ctx, `SELECT id,status,registered_snapshot_or_metadata_id FROM iceberg_registrations WHERE run_id=? LIMIT 1`, v.id).Scan(&existingID, &status, &receipt)
			if err == nil {
				c.RegistrationID = existingID
				if status == RegistrationRegistered && receipt != "" {
					c.Classification = "ALREADY_REGISTERED_VERIFIED"
				} else if status == RegistrationReconciling {
					c.Classification = "REQUIRES_RECONCILIATION"
				} else if status == RegistrationPending || status == RegistrationRetryRequired || status == RegistrationRegistering {
					c.Classification = "SAFE_TO_ENQUEUE"
				} else {
					continue
				}
			} else if err != sql.ErrNoRows {
				return err
			} else {
				var cfg registrationConfigIdentity
				if strings.TrimSpace(v.cfg) == "" {
					c.Classification = "NOT_CONFIGURED"
				} else if json.Unmarshal([]byte(v.cfg), &cfg) != nil || !cfg.Enabled {
					c.Classification = "CONFIGURATION_UNAVAILABLE"
				} else {
					var intent registrationCommitIntent
					var manifest registrationManifest
					if v.phase != "COMPLETE" || len(v.commit) != 64 || json.Unmarshal([]byte(v.intent), &intent) != nil || json.Unmarshal(intent.Manifest, &manifest) != nil || manifest.SchemaVersion != 2 || manifest.RunID != v.id || len(manifest.Artifacts) == 0 {
						c.Classification = "UNSUPPORTED_LEGACY_COMMIT"
					} else if err := ensureRegistrationTx(ctx, tx, v.id, v.dataset, v.commit, v.cfg, v.intent, ns); err != nil {
						c.Classification = "CONFIGURATION_UNAVAILABLE"
					} else {
						c.Classification = "SAFE_TO_ENQUEUE"
						_ = tx.QueryRowContext(ctx, `SELECT id FROM iceberg_registrations WHERE run_id=?`, v.id).Scan(&c.RegistrationID)
					}
				}
			}
			out = append(out, c)
			fields, _ := json.Marshal(map[string]any{"event_type": "REGISTRATION_HISTORICAL_CLASSIFIED", "classification": c.Classification, "registration_id": c.RegistrationID})
			_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,'INFO','historical registration classified',?)`, "registration-history-"+stableRegistrationID(v.id, c.Classification), v.id, ns, string(fields))
			if err != nil {
				return err
			}
		}
		return tx.Commit()
	})
	return out, err
}
