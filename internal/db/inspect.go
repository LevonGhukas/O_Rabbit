package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

// DatabaseReport describes a master database copy, typically a restored
// backup, before a master is started on it. See InspectDatabase.
type DatabaseReport struct {
	Path string `json:"path"`
	// Integrity is the first line of PRAGMA integrity_check; "ok" when sound.
	Integrity           string `json:"integrity"`
	ForeignKeyViolation int    `json:"foreign_key_violations"`
	SchemaVersion       int    `json:"schema_version"`
	// SchemaVersionKnown is the newest migration this binary ships. A copy
	// with a newer schema was written by a newer master.
	SchemaVersionKnown int `json:"schema_version_known"`

	// The master renews its leadership lease every few seconds while it
	// runs, so the last renewal is the newest moment this copy reflects.
	// For a restored backup, its age is the data-loss window (RPO).
	LeadershipEpoch      int64      `json:"leadership_epoch"`
	LeadershipStatus     string     `json:"leadership_status"`
	LastLeadershipActive *time.Time `json:"last_leadership_activity,omitempty"`
	AgeSeconds           *float64   `json:"age_seconds,omitempty"`
	// LeaseRemainingSeconds is how long the copied lease still counts as
	// held. A master refuses to start until it has passed, so a restore
	// that is faster than the lease must wait it out.
	LeaseRemainingSeconds float64 `json:"leadership_lease_remaining_seconds"`

	RunsByStatus          map[string]int `json:"runs_by_status"`
	RegistrationsByStatus map[string]int `json:"registrations_by_status"`

	// SealedValues counts values encrypted with the master key, and
	// SealedValuesUnreadable those the given key cannot open. Both stay 0
	// when no key is given (KeyChecked false).
	KeyChecked             bool `json:"key_checked"`
	SealedValues           int  `json:"sealed_values"`
	SealedValuesUnreadable int  `json:"sealed_values_unreadable"`
}

// Problems lists the reasons a master should not be started on this copy.
// maxAge 0 skips the staleness check.
func (r DatabaseReport) Problems(maxAge time.Duration) []string {
	var out []string
	if r.Integrity != "ok" {
		out = append(out, "integrity check failed: "+r.Integrity)
	}
	if r.ForeignKeyViolation > 0 {
		out = append(out, fmt.Sprintf("%d foreign key violations", r.ForeignKeyViolation))
	}
	if r.SchemaVersion > r.SchemaVersionKnown {
		out = append(out, fmt.Sprintf("schema version %d is newer than this master supports (%d); use the matching master image", r.SchemaVersion, r.SchemaVersionKnown))
	}
	if r.KeyChecked && r.SealedValuesUnreadable > 0 {
		out = append(out, fmt.Sprintf("%d of %d stored secrets do not decrypt with ORABBIT_MASTER_KEY; supply the key this database was written with", r.SealedValuesUnreadable, r.SealedValues))
	}
	if maxAge > 0 {
		switch {
		case r.AgeSeconds == nil:
			out = append(out, "no leadership activity recorded; the copy's age is unknown")
		case *r.AgeSeconds > maxAge.Seconds():
			out = append(out, fmt.Sprintf("copy is %.0fs old, more than the allowed %s", *r.AgeSeconds, maxAge))
		}
	}
	return out
}

// InspectDatabase opens the master database at path read-only and checks it
// without migrating or changing it. With a non-zero key it also verifies that
// every stored secret decrypts, which catches restoring with the wrong
// ORABBIT_MASTER_KEY before a master starts and fails at run time.
func InspectDatabase(ctx context.Context, path string, key secretcrypto.Key, now time.Time) (DatabaseReport, error) {
	r := DatabaseReport{Path: path, RunsByStatus: map[string]int{}, RegistrationsByStatus: map[string]int{}}
	path = strings.TrimSpace(path)
	if path == "" {
		return r, errors.New("database path is required")
	}
	conn, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String())
	if err != nil {
		return r, err
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)
	if _, err := conn.ExecContext(ctx, "PRAGMA query_only=ON;"); err != nil {
		return r, fmt.Errorf("open %s read-only: %w", path, err)
	}

	if err := conn.QueryRowContext(ctx, "PRAGMA integrity_check;").Scan(&r.Integrity); err != nil {
		return r, fmt.Errorf("integrity check: %w", err)
	}
	if err := countRows(ctx, conn, "PRAGMA foreign_key_check;", &r.ForeignKeyViolation); err != nil {
		return r, fmt.Errorf("foreign key check: %w", err)
	}
	for _, m := range migrations {
		r.SchemaVersionKnown = max(r.SchemaVersionKnown, m.version)
	}
	if err := conn.QueryRowContext(ctx, "SELECT COALESCE(MAX(version),0) FROM schema_migrations").Scan(&r.SchemaVersion); err != nil {
		return r, fmt.Errorf("read schema version (is this a master database?): %w", err)
	}

	var renewedMS, acquiredMS, deadlineMS int64
	err = conn.QueryRowContext(ctx, `SELECT epoch,status,renewed_at_ms,acquired_at_ms,lease_deadline_ms FROM master_leadership WHERE leadership_name='master'`).Scan(&r.LeadershipEpoch, &r.LeadershipStatus, &renewedMS, &acquiredMS, &deadlineMS)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return r, fmt.Errorf("read leadership: %w", err)
	}
	if r.LeadershipStatus == "ACTIVE" {
		r.LeaseRemainingSeconds = max(0, time.UnixMilli(deadlineMS).Sub(now).Seconds())
	}
	if last := max(renewedMS, acquiredMS); last > 0 {
		at := time.UnixMilli(last).UTC()
		age := now.Sub(at).Seconds()
		r.LastLeadershipActive, r.AgeSeconds = &at, &age
	}

	if err := countByStatus(ctx, conn, "SELECT status,COUNT(*) FROM runs GROUP BY status", r.RunsByStatus); err != nil {
		return r, fmt.Errorf("count runs: %w", err)
	}
	if err := countByStatus(ctx, conn, "SELECT status,COUNT(*) FROM iceberg_registrations GROUP BY status", r.RegistrationsByStatus); err != nil {
		return r, fmt.Errorf("count registrations: %w", err)
	}

	if !key.IsZero() {
		r.KeyChecked = true
		for _, c := range sealedColumns {
			total, unreadable, err := checkSealedColumn(ctx, conn, c, key)
			if err != nil {
				return r, fmt.Errorf("check %s.%s: %w", c.table, c.column, err)
			}
			r.SealedValues += total
			r.SealedValuesUnreadable += unreadable
		}
	}
	return r, nil
}

func countRows(ctx context.Context, conn *sql.DB, query string, out *int) error {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		*out++
	}
	return rows.Err()
}

func countByStatus(ctx context.Context, conn *sql.DB, query string, out map[string]int) error {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return err
		}
		out[status] = n
	}
	return rows.Err()
}

// checkSealedColumn decrypts every value of one sealed column with key and
// counts those that do not open. It reads the same rows RotateMasterKey
// re-encrypts, so both cover the same secrets.
func checkSealedColumn(ctx context.Context, conn *sql.DB, c sealedColumn, key secretcrypto.Key) (total, unreadable int, err error) {
	cols := append([]string{c.key, c.column}, c.aadColumns...)
	query := "SELECT "
	for i, col := range cols {
		if i > 0 {
			query += ","
		}
		query += "CAST(" + col + " AS BLOB)"
	}
	query += " FROM " + c.table + " WHERE " + c.column + " IS NOT NULL AND length(" + c.column + ") > 0"
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		raw := make([][]byte, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return 0, 0, err
		}
		var extra []string
		for _, x := range raw[2:] {
			extra = append(extra, string(x))
		}
		aad := c.aad(string(raw[0]), extra)
		total++
		if c.text {
			if _, err := decryptStoredJSON(key, string(raw[1]), aad); err != nil {
				unreadable++
			}
		} else if _, err := secretcrypto.Decrypt(key, raw[1], aad); err != nil {
			unreadable++
		}
	}
	return total, unreadable, rows.Err()
}
