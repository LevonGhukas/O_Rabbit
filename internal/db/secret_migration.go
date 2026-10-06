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

		if err := migrateLegacyConfigVersions(ctx, tx, k); err != nil {
			return fmt.Errorf("migrate config versions: %w", err)
		}

		if err := migrateLegacyEncryptedJSONColumn(ctx, tx, k, "runs", "registration_config_json", runRegistrationConfigAAD); err != nil {
			return fmt.Errorf("migrate run registration configs: %w", err)
		}

		if err := migrateLegacyEncryptedJSONColumn(ctx, tx, k, "iceberg_registrations", "retry_override_config_json", registrationRetryOverrideAAD); err != nil {
			return fmt.Errorf("migrate registration retry overrides: %w", err)
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

func migrateLegacyConfigVersions(
	ctx context.Context,
	tx *sql.Tx,
	k secretcrypto.Key,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, server_id, config_id, content_enc
		 FROM config_versions
		 WHERE length(content_enc) > 0`,
	)
	if err != nil {
		return err
	}

	type configVersion struct {
		id       string
		serverID string
		configID string
		content  []byte
	}

	var items []configVersion

	for rows.Next() {
		var item configVersion

		if err := rows.Scan(
			&item.id,
			&item.serverID,
			&item.configID,
			&item.content,
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
		encrypted, migrated, err := secretcrypto.ReencryptLegacy(
			k,
			item.content,
			configVersionAAD(item.serverID, item.configID),
		)
		if err != nil {
			return fmt.Errorf("config version %s: %w", item.id, err)
		}

		if !migrated {
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE config_versions SET content_enc=? WHERE id=?`,
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

// migrateLegacyEncryptedJSONColumn seals plaintext JSON left in a TEXT column
// by releases that stored it unencrypted. table and column are fixed
// identifiers supplied by this package, never user input.
func migrateLegacyEncryptedJSONColumn(
	ctx context.Context,
	tx *sql.Tx,
	k secretcrypto.Key,
	table, column string,
	aad func(id string) []byte,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, `+column+` FROM `+table+`
		 WHERE trim(`+column+`) <> '' AND `+column+` NOT LIKE '`+encryptedRegistrationConfigPrefix+`%'`,
	)
	if err != nil {
		return err
	}

	type legacyValue struct {
		id    string
		value string
	}

	var items []legacyValue

	for rows.Next() {
		var item legacyValue
		if err := rows.Scan(&item.id, &item.value); err != nil {
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
		sealed, err := encryptStoredJSON(k, []byte(item.value), aad(item.id))
		if err != nil {
			return fmt.Errorf("%s %s: %w", table, item.id, err)
		}
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE `+table+` SET `+column+`=? WHERE id=?`,
			sealed,
			item.id,
		); err != nil {
			return err
		}
	}

	return nil
}
