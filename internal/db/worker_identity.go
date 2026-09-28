package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	WorkerIdentityActive  = "ACTIVE"
	WorkerIdentityRevoked = "REVOKED"
)

// ErrEnrollmentTokenInvalid is returned for unknown, expired, or exhausted
// enrollment tokens. Callers must not reveal which condition applied.
var ErrEnrollmentTokenInvalid = errors.New("enrollment token is invalid or expired")

// WorkerIdentity is a master-issued worker identity. ID is the immutable UUID
// embedded in the worker's certificate; Pool bounds which tasks it may run.
type WorkerIdentity struct {
	ID                string  `json:"id"`
	Name              string  `json:"name"`
	Hostname          string  `json:"hostname"`
	Pool              string  `json:"pool"`
	Status            string  `json:"status"`
	CertSerial        string  `json:"cert_serial"`
	CertNotAfter      string  `json:"cert_not_after"`
	EnrollmentTokenID string  `json:"enrollment_token_id"`
	EnrolledAt        string  `json:"enrolled_at"`
	UpdatedAt         string  `json:"updated_at"`
	RevokedAt         *string `json:"revoked_at,omitempty"`
}

type EnrollmentToken struct {
	ID        string `json:"id"`
	Pool      string `json:"pool"`
	MaxUses   int    `json:"max_uses"`
	UseCount  int    `json:"use_count"`
	ExpiresAt string `json:"expires_at"`
	CreatedAt string `json:"created_at"`
}

// LoadOrCreateWorkerCA returns the stored worker CA, inserting the supplied
// one first if none exists. Concurrent callers converge on a single CA.
func (s *Store) LoadOrCreateWorkerCA(ctx context.Context, certPEM string, keyEnc []byte) (string, []byte, error) {
	var storedCert string
	var storedKey []byte
	err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if certPEM != "" {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO worker_ca(id,cert_pem,key_enc,created_at) VALUES(1,?,?,?)`, certPEM, keyEnc, nowUTC()); err != nil {
				return err
			}
		}
		return tx.QueryRowContext(ctx, `SELECT cert_pem,key_enc FROM worker_ca WHERE id=1`).Scan(&storedCert, &storedKey)
	})
	return storedCert, storedKey, err
}

// CreateEnrollmentTokenAudited stores the digest of a new enrollment token.
func (s *Store) CreateEnrollmentTokenAudited(ctx context.Context, id, tokenSHA256, pool string, maxUses int, expiresAt time.Time, audit AuditRecord) (EnrollmentToken, error) {
	out := EnrollmentToken{ID: id, Pool: pool, MaxUses: maxUses, ExpiresAt: expiresAt.UTC().Format(time.RFC3339Nano), CreatedAt: nowUTC()}
	err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_enrollment_tokens(id,token_sha256,pool,max_uses,use_count,expires_at,created_at) VALUES(?,?,?,?,0,?,?)`, out.ID, tokenSHA256, out.Pool, out.MaxUses, out.ExpiresAt, out.CreatedAt); err != nil {
			return err
		}
		after, err := marshalAuditJSON(out)
		if err != nil {
			return err
		}
		audit.AfterJSON = after
		return insertAuditRecordTx(ctx, tx, audit)
	})
	return out, err
}

// EnrollWorkerIdentity consumes one use of the enrollment token and records
// the new identity in the same transaction. The identity inherits the token's
// pool. The certificate described by id/serial/notAfter must only be handed to
// the worker if this call succeeds.
func (s *Store) EnrollWorkerIdentity(ctx context.Context, tokenSHA256, id, name, hostname, certSerial string, certNotAfter, now time.Time) (WorkerIdentity, error) {
	nowS := now.UTC().Format(time.RFC3339Nano)
	out := WorkerIdentity{ID: id, Name: name, Hostname: hostname, Status: WorkerIdentityActive, CertSerial: certSerial, CertNotAfter: certNotAfter.UTC().Format(time.RFC3339Nano), EnrolledAt: nowS, UpdatedAt: nowS}
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		err := tx.QueryRowContext(ctx, `SELECT id,pool FROM worker_enrollment_tokens WHERE token_sha256=? AND use_count<max_uses AND julianday(expires_at)>julianday(?)`, tokenSHA256, nowS).Scan(&out.EnrollmentTokenID, &out.Pool)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrEnrollmentTokenInvalid
		}
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE worker_enrollment_tokens SET use_count=use_count+1 WHERE id=? AND use_count<max_uses`, out.EnrollmentTokenID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n != 1 {
			return ErrEnrollmentTokenInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO worker_identities(id,name,hostname,pool,status,cert_serial,cert_not_after,enrollment_token_id,enrolled_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)`,
			out.ID, out.Name, out.Hostname, out.Pool, out.Status, out.CertSerial, out.CertNotAfter, out.EnrollmentTokenID, out.EnrolledAt, out.UpdatedAt)
		return err
	})
	if err != nil {
		return WorkerIdentity{}, err
	}
	return out, nil
}

func (s *Store) GetWorkerIdentity(ctx context.Context, id string) (WorkerIdentity, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id,name,hostname,pool,status,cert_serial,cert_not_after,enrollment_token_id,enrolled_at,updated_at,revoked_at FROM worker_identities WHERE id=?`, id)
	return scanWorkerIdentity(row)
}

func (s *Store) ListWorkerIdentities(ctx context.Context) ([]WorkerIdentity, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id,name,hostname,pool,status,cert_serial,cert_not_after,enrollment_token_id,enrolled_at,updated_at,revoked_at FROM worker_identities ORDER BY enrolled_at,id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]WorkerIdentity, 0)
	for rows.Next() {
		w, err := scanWorkerIdentity(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// RecordWorkerCertificate stores the serial of a renewed certificate for an
// active identity.
func (s *Store) RecordWorkerCertificate(ctx context.Context, id, certSerial string, certNotAfter, now time.Time) error {
	res, err := s.db.ExecContext(ctx, `UPDATE worker_identities SET cert_serial=?,cert_not_after=?,updated_at=? WHERE id=? AND status=?`,
		certSerial, certNotAfter.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), id, WorkerIdentityActive)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return sql.ErrNoRows
	}
	return nil
}

// RevokeWorkerIdentityAudited permanently revokes an identity. The master
// rejects every later RPC authenticated with any of its certificates.
func (s *Store) RevokeWorkerIdentityAudited(ctx context.Context, id string, now time.Time, audit AuditRecord) (WorkerIdentity, error) {
	var out WorkerIdentity
	err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		before, err := scanWorkerIdentity(tx.QueryRowContext(ctx, `SELECT id,name,hostname,pool,status,cert_serial,cert_not_after,enrollment_token_id,enrolled_at,updated_at,revoked_at FROM worker_identities WHERE id=?`, id))
		if err != nil {
			return err
		}
		out = before
		if before.Status == WorkerIdentityRevoked {
			return nil
		}
		nowS := now.UTC().Format(time.RFC3339Nano)
		if _, err := tx.ExecContext(ctx, `UPDATE worker_identities SET status=?,revoked_at=?,updated_at=? WHERE id=?`, WorkerIdentityRevoked, nowS, nowS, id); err != nil {
			return err
		}
		out.Status, out.RevokedAt, out.UpdatedAt = WorkerIdentityRevoked, &nowS, nowS
		if audit.BeforeJSON, err = marshalAuditJSON(before); err != nil {
			return err
		}
		if audit.AfterJSON, err = marshalAuditJSON(out); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	})
	return out, err
}

// VerifyTaskAttemptOwner reports whether workerID/bootID currently holds the
// unexpired lease of the given attempt. It returns the attempt's run ID, or
// ErrAttemptFenced.
func (s *Store) VerifyTaskAttemptOwner(ctx context.Context, bootID, taskID, attemptID, token, workerID string, now time.Time) (string, error) {
	var runID string
	err := s.db.QueryRowContext(ctx, `SELECT t.run_id FROM task_attempts a JOIN tasks t ON t.id=a.task_id
		WHERE a.id=? AND a.task_id=? AND a.fencing_token=? AND a.worker_id=? AND a.worker_boot_id=? AND a.status='ACTIVE'
		AND julianday(a.lease_deadline)>julianday(?) AND t.status='RUNNING' AND t.current_attempt_id=a.id`,
		attemptID, taskID, token, workerID, bootID, now.UTC().Format(time.RFC3339Nano)).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrAttemptFenced
	}
	return runID, err
}

func scanWorkerIdentity(row rowScanner) (WorkerIdentity, error) {
	var w WorkerIdentity
	var revokedAt sql.NullString
	if err := row.Scan(&w.ID, &w.Name, &w.Hostname, &w.Pool, &w.Status, &w.CertSerial, &w.CertNotAfter, &w.EnrollmentTokenID, &w.EnrolledAt, &w.UpdatedAt, &revokedAt); err != nil {
		return WorkerIdentity{}, err
	}
	if revokedAt.Valid {
		w.RevokedAt = &revokedAt.String
	}
	return w, nil
}

func marshalAuditJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal audit state: %w", err)
	}
	return b, nil
}
