// internal/db/store.go

package db

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
	_ "modernc.org/sqlite"
)

// encryptStoredJSON seals a JSON document for a TEXT column as
// "enc:v1:<base64 blob>". Empty input stays empty.
func encryptStoredJSON(k secretcrypto.Key, plaintext, aad []byte) (string, error) {
	if len(plaintext) == 0 {
		return "", nil
	}
	if k.IsZero() {
		return "", ErrMasterKeyRequired
	}
	encrypted, err := secretcrypto.Encrypt(k, plaintext, aad)
	if err != nil {
		return "", err
	}
	return encryptedRegistrationConfigPrefix + base64.StdEncoding.EncodeToString(encrypted), nil
}

// decryptStoredJSON opens a value written by encryptStoredJSON. Unprefixed
// values are legacy plaintext that MigrateLegacySecrets should have sealed at
// startup, so they are refused rather than trusted.
func decryptStoredJSON(k secretcrypto.Key, stored string, aad []byte) ([]byte, error) {
	if strings.TrimSpace(stored) == "" {
		return nil, nil
	}
	if !strings.HasPrefix(stored, encryptedRegistrationConfigPrefix) {
		return nil, errors.New("value is stored unencrypted; run the legacy secret migration")
	}
	if k.IsZero() {
		return nil, ErrMasterKeyRequired
	}
	blob, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, encryptedRegistrationConfigPrefix))
	if err != nil {
		return nil, fmt.Errorf("decode encrypted value: %w", err)
	}
	plaintext, err := secretcrypto.Decrypt(k, blob, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt value: %w", err)
	}
	return plaintext, nil
}

type Store struct {
	// db is the single writer connection. It must stay a single connection:
	// the leadership fence installs per-connection TEMP triggers on it.
	db *sql.DB
	// rdb is a read-only pool for observability and list queries. With WAL,
	// readers never block the writer and always see committed data.
	rdb                     *sql.DB
	log                     *slog.Logger
	masterKey               secretcrypto.Key
	canceledObjectRetention time.Duration
	maxActiveRuns           int
}

type Config struct {
	Path string
	// ReadConns sizes the read-only connection pool. Zero means
	// defaultReadConns.
	ReadConns int
}

const defaultReadConns = 4

func (s *Store) SetMasterKey(k secretcrypto.Key) {
	s.masterKey = k
}

func Open(ctx context.Context, cfg Config, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}

	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, err
	}

	// SQLite is single-writer; keep one connection to avoid SQLITE_BUSY under concurrent gRPC.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// WAL + sane defaults.
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA foreign_keys=ON;",
		"PRAGMA busy_timeout=15000;",
	}
	for _, p := range pragmas {
		if _, err := db.ExecContext(ctx, p); err != nil {
			_ = db.Close()
			return nil, err
		}
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := Migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	rdb, err := openReadPool(ctx, cfg, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{db: db, rdb: rdb, log: log, canceledObjectRetention: 24 * time.Hour}, nil
}

// openReadPool opens a query_only pool on the same database file so reads do
// not queue behind the single writer connection. In-memory and URI-style
// paths cannot be shared across pools, so they reuse the writer.
func openReadPool(ctx context.Context, cfg Config, writer *sql.DB) (*sql.DB, error) {
	if cfg.Path == "" || strings.Contains(cfg.Path, ":memory:") || strings.HasPrefix(cfg.Path, "file:") || strings.Contains(cfg.Path, "?") {
		return writer, nil
	}
	conns := cfg.ReadConns
	if conns <= 0 {
		conns = defaultReadConns
	}
	dsn := "file:" + cfg.Path + "?_pragma=busy_timeout(15000)&_pragma=query_only(1)&_pragma=foreign_keys(1)"
	rdb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	rdb.SetMaxOpenConns(conns)
	rdb.SetMaxIdleConns(conns)
	if err := rdb.PingContext(ctx); err != nil {
		_ = rdb.Close()
		return nil, err
	}
	return rdb, nil
}

func (s *Store) SetCanceledObjectRetention(retention time.Duration) {
	if retention > 0 {
		s.canceledObjectRetention = retention
	}
}

func (s *Store) Close() error {
	if s.rdb != nil && s.rdb != s.db {
		_ = s.rdb.Close()
	}
	return s.db.Close()
}

// Ready reports whether the control-plane store is currently queryable.
// It uses a short, caller-bounded probe suitable for HTTP readiness checks.
func (s *Store) Ready(ctx context.Context) error {
	if s == nil || s.db == nil {
		return errors.New("store is not initialized")
	}

	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
	}

	if err := s.db.PingContext(ctx); err != nil {
		return err
	}

	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT 1;`).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("unexpected readiness probe result: %d", n)
	}
	return nil
}

// TimestampLayout is the one format for every timestamp the store writes: UTC
// with a fixed nine-digit fraction, so plain string comparison and ORDER BY
// match chronological order.
const TimestampLayout = "2006-01-02T15:04:05.000000000Z"

// FormatTimestamp formats t in TimestampLayout.
func FormatTimestamp(t time.Time) string { return t.UTC().Format(TimestampLayout) }

// normalizeTimestamp rewrites an RFC 3339 string in TimestampLayout. Values
// that do not parse are returned unchanged.
func normalizeTimestamp(v string) string {
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(v))
	if err != nil {
		return v
	}
	return FormatTimestamp(t)
}

func nowUTC() string { return FormatTimestamp(time.Now()) }

func isSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "SQLITE_LOCKED") || strings.Contains(msg, "database is locked")
}

func withBusyRetry(ctx context.Context, fn func() error) error {
	backoff := 25 * time.Millisecond
	deadline := time.Now().Add(5 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		err := fn()
		if err == nil {
			return nil
		}
		last = err
		if !isSQLiteBusy(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 500*time.Millisecond {
			backoff *= 2
			if backoff > 500*time.Millisecond {
				backoff = 500 * time.Millisecond
			}
		}
	}
	return last
}

// keysetPage orders base newest first by orderColumn then id and, when limit
// is positive, selects one extra row after cursor to detect a next page.
func keysetPage(base, orderColumn string, limit int, cursor string) (string, []any, error) {
	query, args := base, []any{}
	if strings.TrimSpace(cursor) != "" {
		if limit <= 0 {
			return "", nil, fmt.Errorf("cursor requires limit")
		}
		ts, id, err := parseEventCursor(cursor)
		if err != nil {
			return "", nil, err
		}
		query += " WHERE (" + orderColumn + " < ? OR (" + orderColumn + " = ? AND id < ?))"
		args = append(args, ts, ts, id)
	}
	query += " ORDER BY " + orderColumn + " DESC, id DESC"
	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit+1)
	}
	return query, args, nil
}
