package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

// WorkerCAKeyAAD binds the sealed worker CA private key to its purpose.
var WorkerCAKeyAAD = []byte("orabbit-worker-ca-key")

// KeyRotationResult counts the values re-encrypted by RotateMasterKey.
type KeyRotationResult struct {
	ConnectionSecrets    int
	ServerCredentials    int
	ConfigVersions       int
	RunRegistrationConfs int
	RetryOverrides       int
	WorkerCAKeys         int
}

func (r KeyRotationResult) Total() int {
	return r.ConnectionSecrets + r.ServerCredentials + r.ConfigVersions + r.RunRegistrationConfs + r.RetryOverrides + r.WorkerCAKeys
}

// sealedColumn describes one column holding values sealed with the master
// key. Identifiers are fixed by this package, never user input.
type sealedColumn struct {
	table, column, key string
	// aadColumns are selected with the key and passed to aad.
	aadColumns []string
	aad        func(keyValue string, extra []string) []byte
	// text is true for TEXT columns written by encryptStoredJSON.
	text  bool
	count func(*KeyRotationResult) *int
}

var sealedColumns = []sealedColumn{
	{table: "connections", column: "secret_enc_blob", key: "id",
		aad:   func(id string, _ []string) []byte { return []byte(id) },
		count: func(r *KeyRotationResult) *int { return &r.ConnectionSecrets }},
	{table: "server_credentials", column: "private_key_enc", key: "server_id",
		aad:   func(id string, _ []string) []byte { return serverCredentialAAD(id, "private_key") },
		count: func(r *KeyRotationResult) *int { return &r.ServerCredentials }},
	{table: "server_credentials", column: "password_enc", key: "server_id",
		aad:   func(id string, _ []string) []byte { return serverCredentialAAD(id, "password") },
		count: func(r *KeyRotationResult) *int { return &r.ServerCredentials }},
	{table: "server_credentials", column: "passphrase_enc", key: "server_id",
		aad:   func(id string, _ []string) []byte { return serverCredentialAAD(id, "passphrase") },
		count: func(r *KeyRotationResult) *int { return &r.ServerCredentials }},
	{table: "config_versions", column: "content_enc", key: "id", aadColumns: []string{"server_id", "config_id"},
		aad:   func(_ string, x []string) []byte { return configVersionAAD(x[0], x[1]) },
		count: func(r *KeyRotationResult) *int { return &r.ConfigVersions }},
	{table: "runs", column: "registration_config_json", key: "id", text: true,
		aad:   func(id string, _ []string) []byte { return runRegistrationConfigAAD(id) },
		count: func(r *KeyRotationResult) *int { return &r.RunRegistrationConfs }},
	{table: "iceberg_registrations", column: "retry_override_config_json", key: "id", text: true,
		aad:   func(id string, _ []string) []byte { return registrationRetryOverrideAAD(id) },
		count: func(r *KeyRotationResult) *int { return &r.RetryOverrides }},
	{table: "worker_ca", column: "key_enc", key: "id",
		aad:   func(string, []string) []byte { return WorkerCAKeyAAD },
		count: func(r *KeyRotationResult) *int { return &r.WorkerCAKeys }},
}

// RotateMasterKey re-encrypts every value sealed with oldKey under newKey in
// one transaction: either all values move to the new key or none do. It
// fails without changes if any value does not decrypt with oldKey. Run it
// only while no master is serving from this database.
func (s *Store) RotateMasterKey(ctx context.Context, oldKey, newKey secretcrypto.Key) (KeyRotationResult, error) {
	var res KeyRotationResult
	if oldKey.IsZero() || newKey.IsZero() {
		return res, ErrMasterKeyRequired
	}
	err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		res = KeyRotationResult{}
		for _, c := range sealedColumns {
			n, err := rotateSealedColumn(ctx, tx, c, oldKey, newKey)
			if err != nil {
				return fmt.Errorf("%s.%s: %w", c.table, c.column, err)
			}
			*c.count(&res) += n
		}
		return nil
	})
	return res, err
}

func rotateSealedColumn(ctx context.Context, tx *sql.Tx, c sealedColumn, oldKey, newKey secretcrypto.Key) (int, error) {
	cols := append([]string{c.key, c.column}, c.aadColumns...)
	query := "SELECT "
	for i, col := range cols {
		if i > 0 {
			query += ","
		}
		query += "CAST(" + col + " AS BLOB)"
	}
	query += " FROM " + c.table + " WHERE " + c.column + " IS NOT NULL AND length(" + c.column + ") > 0"
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	type item struct {
		key   string
		value []byte
		extra []string
	}
	var items []item
	for rows.Next() {
		raw := make([][]byte, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			_ = rows.Close()
			return 0, err
		}
		it := item{key: string(raw[0]), value: raw[1]}
		for _, x := range raw[2:] {
			it.extra = append(it.extra, string(x))
		}
		items = append(items, it)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}

	update := "UPDATE " + c.table + " SET " + c.column + "=? WHERE " + c.key + "=?"
	for _, it := range items {
		aad := c.aad(it.key, it.extra)
		var sealed any
		if c.text {
			plain, err := decryptStoredJSON(oldKey, string(it.value), aad)
			if err != nil {
				return 0, fmt.Errorf("row %s: %w", it.key, err)
			}
			if sealed, err = encryptStoredJSON(newKey, plain, aad); err != nil {
				return 0, err
			}
		} else {
			plain, err := secretcrypto.Decrypt(oldKey, it.value, aad)
			if err != nil {
				return 0, fmt.Errorf("row %s: %w", it.key, errors.Join(ErrKeyRotationDecrypt, err))
			}
			if sealed, err = secretcrypto.Encrypt(newKey, plain, aad); err != nil {
				return 0, err
			}
		}
		if _, err := tx.ExecContext(ctx, update, sealed, it.key); err != nil {
			return 0, err
		}
	}
	return len(items), nil
}

// ErrKeyRotationDecrypt means a stored value did not open with the current
// key, so the rotation was abandoned without changes.
var ErrKeyRotationDecrypt = errors.New("stored value does not decrypt with the current master key")
