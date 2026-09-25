// internal/db/secret_migration.go

package db

import (
	"context"
	"database/sql"
	"fmt"

	secretcrypto "github.com/LevonGhukas/O_Rabbit/internal/crypto"
)

func (s *Store) MigrateLegacySecrets(ctx context.Context, k secretcrypto.Key) error {
	if k.IsZero() {
		return ErrMasterKeyRequired
	}

	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := migrateLegacyConnectionSecrets(ctx, tx, k); err != nil {
			return fmt.Errorf("migrate connection secrets: %w", err)
		}

		if err := migrateLegacyServerCredentials(ctx, tx, k); err != nil {
			return fmt.Errorf("migrate server credentials: %w", err)
		}

		return nil
	})
}

func migrateLegacyConnectionSecrets(
	ctx context.Context,
	tx *sql.Tx,
	k secretcrypto.Key,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, secret_enc_blob
		 FROM connections
		 WHERE length(secret_enc_blob) > 0`,
	)
	if err != nil {
		return err
	}

	type connectionSecret struct {
		id   string
		blob []byte
	}

	var items []connectionSecret

	for rows.Next() {
		var item connectionSecret

		if err := rows.Scan(&item.id, &item.blob); err != nil {
			rows.Close()
			return err
		}

		items = append(items, item)
	}

	if err := rows.Close(); err != nil {
		return err
	}

	if err := rows.Err(); err != nil {
		return err
	}

	for _, item := range items {
		encrypted, migrated, err := secretcrypto.ReencryptLegacy(
			k,
			item.blob,
			[]byte(item.id),
		)
		if err != nil {
			return fmt.Errorf("connection %s: %w", item.id, err)
		}

		if !migrated {
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE connections SET secret_enc_blob=? WHERE id=?`,
			encrypted,
			item.id,
		); err != nil {
			return err
		}
	}

	return nil
}

func migrateLegacyServerCredentials(
	ctx context.Context,
	tx *sql.Tx,
	k secretcrypto.Key,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT server_id, private_key_enc, password_enc, passphrase_enc
		 FROM server_credentials`,
	)
	if err != nil {
		return err
	}

	type credential struct {
		serverID   string
		privateKey []byte
		password   []byte
		passphrase []byte
	}

	var items []credential

	for rows.Next() {
		var item credential

		if err := rows.Scan(
			&item.serverID,
			&item.privateKey,
			&item.password,
			&item.passphrase,
		); err != nil {
			rows.Close()
			return err
		}

		items = append(items, item)
	}

	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, item := range items {
		privateKey, privateKeyMigrated, err := secretcrypto.ReencryptLegacy(
			k,
			item.privateKey,
			serverCredentialAAD(item.serverID, "private_key"),
		)
		if err != nil {
			return fmt.Errorf("server %s private key: %w", item.serverID, err)
		}

		password, passwordMigrated, err := secretcrypto.ReencryptLegacy(
			k,
			item.password,
			serverCredentialAAD(item.serverID, "password"),
		)
		if err != nil {
			return fmt.Errorf("server %s password: %w", item.serverID, err)
		}

		passphrase, passphraseMigrated, err := secretcrypto.ReencryptLegacy(
			k,
			item.passphrase,
			serverCredentialAAD(item.serverID, "passphrase"),
		)
		if err != nil {
			return fmt.Errorf("server %s passphrase: %w", item.serverID, err)
		}

		if !privateKeyMigrated &&
			!passwordMigrated &&
			!passphraseMigrated {
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE server_credentials
			 SET private_key_enc=?,
			     password_enc=?,
			     passphrase_enc=?
			 WHERE server_id=?`,
			privateKey,
			password,
			passphrase,
			item.serverID,
		); err != nil {
			return err
		}
	}

	return nil
}
