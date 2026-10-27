package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// ErrCredentialNotFound is returned when no credential has the given ID.
var ErrCredentialNotFound = errors.New("api credential not found")

// APICredential is a named, scoped bearer credential. Only the SHA-256 of the
// token is stored; TokenSHA256 is populated by GetAPICredential (for
// verification) and always empty in ListAPICredentials.
type APICredential struct {
	ID          string   `json:"id"`
	Principal   string   `json:"principal"`
	Kind        string   `json:"kind"`
	Scopes      []string `json:"scopes"`
	TokenSHA256 string   `json:"-"`
	CreatedAt   string   `json:"created_at"`
	CreatedBy   string   `json:"created_by"`
	ExpiresAt   string   `json:"expires_at,omitempty"`
	RevokedAt   string   `json:"revoked_at,omitempty"`
	Description string   `json:"description,omitempty"`
}

// Active reports whether the credential is neither revoked nor expired at
// now. An unparseable expiry fails closed.
func (c APICredential) Active(now time.Time) bool {
	if c.RevokedAt != "" {
		return false
	}
	if c.ExpiresAt == "" {
		return true
	}
	exp, err := time.Parse(time.RFC3339Nano, c.ExpiresAt)
	return err == nil && now.Before(exp)
}

const apiCredentialColumns = `id, principal, kind, scopes, token_sha256, created_at, created_by, expires_at, revoked_at, description`

// CreateAPICredential stores c (stamping CreatedAt) and, when audit is
// non-nil, the audit record in the same transaction.
func (s *Store) CreateAPICredential(ctx context.Context, c APICredential, audit *AuditRecord) error {
	scopes, err := json.Marshal(c.Scopes)
	if err != nil {
		return err
	}
	var expires any
	if c.ExpiresAt != "" {
		expires = c.ExpiresAt
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO api_credentials(`+apiCredentialColumns+`)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?);`,
			c.ID, c.Principal, c.Kind, string(scopes), c.TokenSHA256, nowUTC(), c.CreatedBy, expires, c.Description); err != nil {
			return err
		}
		return insertOptionalAuditTx(ctx, tx, audit)
	})
}

// GetAPICredential loads one credential, including its token hash.
func (s *Store) GetAPICredential(ctx context.Context, id string) (APICredential, error) {
	row := s.rdb.QueryRowContext(ctx, `SELECT `+apiCredentialColumns+` FROM api_credentials WHERE id = ?;`, id)
	c, err := scanAPICredential(row)
	if errors.Is(err, sql.ErrNoRows) {
		return APICredential{}, ErrCredentialNotFound
	}
	return c, err
}

// ListAPICredentials returns every credential, newest first, without hashes.
func (s *Store) ListAPICredentials(ctx context.Context) ([]APICredential, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT `+apiCredentialColumns+` FROM api_credentials ORDER BY created_at DESC, id DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []APICredential
	for rows.Next() {
		c, err := scanAPICredential(rows)
		if err != nil {
			return nil, err
		}
		c.TokenSHA256 = ""
		out = append(out, c)
	}
	return out, rows.Err()
}

// RevokeAPICredential marks a credential revoked. Revoking an already
// revoked credential keeps the first revocation time and writes no audit.
func (s *Store) RevokeAPICredential(ctx context.Context, id string, audit *AuditRecord) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `UPDATE api_credentials SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL;`, nowUTC(), id)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			return insertOptionalAuditTx(ctx, tx, audit)
		}
		var one int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM api_credentials WHERE id = ?;`, id).Scan(&one)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrCredentialNotFound
		}
		return err
	})
}

// CountActiveAPICredentials counts credentials that are neither revoked nor
// expired. Zero means the deployment has not adopted scoped credentials.
func (s *Store) CountActiveAPICredentials(ctx context.Context) (int, error) {
	var n int
	err := s.rdb.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM api_credentials
		WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?);`, nowUTC()).Scan(&n)
	return n, err
}

func scanAPICredential(row rowScanner) (APICredential, error) {
	var (
		c                APICredential
		scopes           string
		expires, revoked sql.NullString
	)
	if err := row.Scan(&c.ID, &c.Principal, &c.Kind, &scopes, &c.TokenSHA256, &c.CreatedAt, &c.CreatedBy, &expires, &revoked, &c.Description); err != nil {
		return APICredential{}, err
	}
	if err := json.Unmarshal([]byte(scopes), &c.Scopes); err != nil {
		return APICredential{}, err
	}
	c.ExpiresAt, c.RevokedAt = expires.String, revoked.String
	return c, nil
}

func insertOptionalAuditTx(ctx context.Context, tx *sql.Tx, audit *AuditRecord) error {
	if audit == nil {
		return nil
	}
	return insertAuditRecordTx(ctx, tx, *audit)
}
