package db

import (
	"context"
	"database/sql"
	"encoding/json"
)

type Connection struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Kind          string          `json:"kind"`
	Engine        string          `json:"engine"`
	MetadataJSON  json.RawMessage `json:"metadata_json"`
	SecretEncBlob []byte          `json:"-"`
	CreatedAt     string          `json:"created_at"`
	UpdatedAt     string          `json:"updated_at"`
}

func (s *Store) CreateConnection(ctx context.Context, c Connection) error {
	c = prepareConnectionForCreate(c)
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createConnectionTx(ctx, tx, c)
	})
}

func (s *Store) GetConnection(ctx context.Context, id string) (Connection, error) {
	var c Connection
	var meta string
	row := s.db.QueryRowContext(ctx, `SELECT id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at FROM connections WHERE id=?;`, id)
	if err := row.Scan(&c.ID, &c.Name, &c.Kind, &c.Engine, &meta, &c.SecretEncBlob, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return Connection{}, err
	}
	c.MetadataJSON = []byte(meta)
	return c, nil
}

// FindConnectionByName returns the connection named name (names are unique,
// idx_connections_name), or sql.ErrNoRows.
func (s *Store) FindConnectionByName(ctx context.Context, name string) (Connection, error) {
	var c Connection
	var meta string
	row := s.db.QueryRowContext(ctx, `SELECT id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at FROM connections WHERE name=? ORDER BY created_at DESC, id DESC LIMIT 1;`, name)
	if err := row.Scan(&c.ID, &c.Name, &c.Kind, &c.Engine, &meta, &c.SecretEncBlob, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return Connection{}, err
	}
	c.MetadataJSON = []byte(meta)
	return c, nil
}

func (s *Store) ListConnections(ctx context.Context) ([]Connection, error) {
	out, _, err := s.ListConnectionsPage(ctx, 0, "")
	return out, err
}

// ListConnectionsPage lists connections newest first. With limit > 0 it
// returns at most limit connections after cursor and the cursor of the next
// page ("" on the last page); limit 0 returns every connection.
func (s *Store) ListConnectionsPage(ctx context.Context, limit int, cursor string) ([]Connection, string, error) {
	query, args, err := keysetPage(`SELECT id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at FROM connections`, "created_at", limit, cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.rdb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []Connection
	for rows.Next() {
		var c Connection
		var meta string
		if err := rows.Scan(&c.ID, &c.Name, &c.Kind, &c.Engine, &meta, &c.SecretEncBlob, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, "", err
		}
		c.MetadataJSON = []byte(meta)
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if limit > 0 && len(out) > limit {
		out = out[:limit]
		last := out[len(out)-1]
		next = formatEventCursor(last.CreatedAt, last.ID)
	}
	return out, next, nil
}

func (s *Store) UpdateConnection(ctx context.Context, c Connection) error {
	if len(c.MetadataJSON) == 0 {
		c.MetadataJSON = []byte(`{}`)
	}
	c.UpdatedAt = nowUTC()
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateConnectionTx(ctx, tx, c)
	})
}

func (s *Store) DeleteConnection(ctx context.Context, id string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return deleteConnectionTx(ctx, tx, id)
	})
}

func (s *Store) CountJobsUsingConnection(ctx context.Context, connectionID string) (int, error) {
	row := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE source_connection_id=? OR target_connection_id=?;`, connectionID, connectionID)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
