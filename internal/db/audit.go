package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

type AuditRecord struct {
	ID           string          `json:"id"`
	TS           string          `json:"ts"`
	ActorType    string          `json:"actor_type"`
	ActorID      string          `json:"actor_id,omitempty"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	RequestID    string          `json:"request_id,omitempty"`
	BeforeJSON   json.RawMessage `json:"before_json,omitempty"`
	AfterJSON    json.RawMessage `json:"after_json,omitempty"`
	MetadataJSON json.RawMessage `json:"metadata_json,omitempty"`
}

func (s *Store) InsertAuditRecord(ctx context.Context, rec AuditRecord) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return insertAuditRecordTx(ctx, tx, rec)
	})
}

func (s *Store) ListAuditRecords(ctx context.Context, limit int) ([]AuditRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, ts, actor_type, actor_id, action, resource_type, resource_id, request_id, before_json, after_json, metadata_json
		FROM audit_log
		ORDER BY ts DESC, id DESC
		LIMIT ?;`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AuditRecord
	for rows.Next() {
		var (
			rec        AuditRecord
			beforeJSON sql.NullString
			afterJSON  sql.NullString
			metaJSON   string
		)
		if err := rows.Scan(
			&rec.ID,
			&rec.TS,
			&rec.ActorType,
			&rec.ActorID,
			&rec.Action,
			&rec.ResourceType,
			&rec.ResourceID,
			&rec.RequestID,
			&beforeJSON,
			&afterJSON,
			&metaJSON,
		); err != nil {
			return nil, err
		}
		if beforeJSON.Valid {
			rec.BeforeJSON = []byte(beforeJSON.String)
		}
		if afterJSON.Valid {
			rec.AfterJSON = []byte(afterJSON.String)
		}
		rec.MetadataJSON = []byte(metaJSON)
		out = append(out, rec)
	}
	return out, rows.Err()
}

func (s *Store) CreateConnectionAudited(ctx context.Context, c Connection, audit AuditRecord) (Connection, error) {
	c = prepareConnectionForCreate(c)
	audit, err := withAuditPayloads(audit, nil, c)
	if err != nil {
		return Connection{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := createConnectionTx(ctx, tx, c); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	}); err != nil {
		return Connection{}, err
	}
	return c, nil
}

func (s *Store) UpdateConnectionAudited(ctx context.Context, before Connection, after Connection, audit AuditRecord) (Connection, error) {
	after = prepareConnectionForUpdate(before, after)
	audit, err := withAuditPayloads(audit, before, after)
	if err != nil {
		return Connection{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := updateConnectionTx(ctx, tx, after); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	}); err != nil {
		return Connection{}, err
	}
	return after, nil
}

func (s *Store) DeleteConnectionAudited(ctx context.Context, before Connection, audit AuditRecord) error {
	audit, err := withAuditPayloads(audit, before, nil)
	if err != nil {
		return err
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := deleteConnectionTx(ctx, tx, before.ID); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	})
}

func (s *Store) CreateJobAudited(ctx context.Context, j Job, audit AuditRecord) (Job, error) {
	j = prepareJobForCreate(j)
	audit, err := withAuditPayloads(audit, nil, j)
	if err != nil {
		return Job{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := createJobTx(ctx, tx, j); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	}); err != nil {
		return Job{}, err
	}
	return j, nil
}

func (s *Store) UpdateJobAudited(ctx context.Context, before Job, after Job, audit AuditRecord) (Job, error) {
	after = prepareJobForUpdate(before, after)
	audit, err := withAuditPayloads(audit, before, after)
	if err != nil {
		return Job{}, err
	}
	if err := s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := updateJobTx(ctx, tx, after); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	}); err != nil {
		return Job{}, err
	}
	return after, nil
}

func (s *Store) DeleteJobAudited(ctx context.Context, before Job, audit AuditRecord) error {
	audit, err := withAuditPayloads(audit, before, nil)
	if err != nil {
		return err
	}
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		if err := deleteJobTx(ctx, tx, before.ID); err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, audit)
	})
}

func (s *Store) StartRunWithTasksAudited(ctx context.Context, run Run, tasks []TaskInsert, audit AuditRecord) (bool, error) {
	if strings.TrimSpace(run.ID) == "" {
		return false, fmt.Errorf("missing run id")
	}
	if len(tasks) == 0 {
		return false, fmt.Errorf("missing tasks")
	}
	if len(audit.MetadataJSON) == 0 {
		meta, err := marshalAuditPayload(map[string]any{
			"job_id":      run.JobID,
			"dataset_key": run.DatasetKey,
			"task_count":  len(tasks),
		})
		if err != nil {
			return false, err
		}
		audit.MetadataJSON = meta
	}
	audit.ResourceID = run.ID
	admitted := false
	err := s.withTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable}, func(tx *sql.Tx) error {
		if err := insertTasksTx(ctx, tx, tasks); err != nil {
			return err
		}
		var err error
		admitted, err = s.admitRunTx(ctx, tx, run.ID)
		if err != nil {
			return err
		}
		if admitted {
			run.Status = "RUNNING"
		} else {
			run.Status = "PLANNING"
		}
		preparedAudit, err := withAuditPayloads(audit, nil, run)
		if err != nil {
			return err
		}
		return insertAuditRecordTx(ctx, tx, preparedAudit)
	})
	return admitted, err
}

func (s *Store) withTx(ctx context.Context, opts *sql.TxOptions, fn func(*sql.Tx) error) error {
	return withBusyRetry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, opts)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit()
	})
}

func insertAuditRecordTx(ctx context.Context, tx *sql.Tx, rec AuditRecord) error {
	rec, err := prepareAuditRecord(rec)
	if err != nil {
		return err
	}
	var before any
	if len(rec.BeforeJSON) != 0 {
		before = string(rec.BeforeJSON)
	}
	var after any
	if len(rec.AfterJSON) != 0 {
		after = string(rec.AfterJSON)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_log(id, ts, actor_type, actor_id, action, resource_type, resource_id, request_id, before_json, after_json, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		rec.ID,
		rec.TS,
		rec.ActorType,
		rec.ActorID,
		rec.Action,
		rec.ResourceType,
		rec.ResourceID,
		rec.RequestID,
		before,
		after,
		string(rec.MetadataJSON),
	)
	return err
}

func prepareAuditRecord(rec AuditRecord) (AuditRecord, error) {
	if strings.TrimSpace(rec.ID) == "" {
		rec.ID = newStoreID()
	}
	if strings.TrimSpace(rec.TS) == "" {
		rec.TS = nowUTC()
	}
	if strings.TrimSpace(rec.ActorType) == "" {
		rec.ActorType = "system"
	}
	if strings.TrimSpace(rec.Action) == "" {
		return AuditRecord{}, fmt.Errorf("missing audit action")
	}
	if strings.TrimSpace(rec.ResourceType) == "" {
		return AuditRecord{}, fmt.Errorf("missing audit resource_type")
	}
	if strings.TrimSpace(rec.ResourceID) == "" {
		return AuditRecord{}, fmt.Errorf("missing audit resource_id")
	}
	if len(rec.MetadataJSON) == 0 {
		rec.MetadataJSON = []byte(`{}`)
	}
	return rec, nil
}

func withAuditPayloads(rec AuditRecord, before, after any) (AuditRecord, error) {
	var err error
	rec.BeforeJSON, err = marshalAuditPayload(before)
	if err != nil {
		return AuditRecord{}, err
	}
	rec.AfterJSON, err = marshalAuditPayload(after)
	if err != nil {
		return AuditRecord{}, err
	}
	return rec, nil
}

func marshalAuditPayload(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return nil, nil
		}
		return append(json.RawMessage(nil), raw...), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

func prepareConnectionForCreate(c Connection) Connection {
	if len(c.MetadataJSON) == 0 {
		c.MetadataJSON = []byte(`{}`)
	}
	ts := nowUTC()
	if c.CreatedAt == "" {
		c.CreatedAt = ts
	}
	c.UpdatedAt = ts
	return c
}

func prepareConnectionForUpdate(before, after Connection) Connection {
	if len(after.MetadataJSON) == 0 {
		after.MetadataJSON = []byte(`{}`)
	}
	after.ID = before.ID
	after.CreatedAt = before.CreatedAt
	after.UpdatedAt = nowUTC()
	return after
}

func prepareJobForCreate(j Job) Job {
	if len(j.OptionsJSON) == 0 {
		j.OptionsJSON = []byte(`{}`)
	}
	ts := nowUTC()
	if j.CreatedAt == "" {
		j.CreatedAt = ts
	}
	j.UpdatedAt = ts
	return j
}

func prepareJobForUpdate(before, after Job) Job {
	if len(after.OptionsJSON) == 0 {
		after.OptionsJSON = []byte(`{}`)
	}
	after.ID = before.ID
	after.CreatedAt = before.CreatedAt
	after.UpdatedAt = nowUTC()
	return after
}

func createConnectionTx(ctx context.Context, tx *sql.Tx, c Connection) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO connections(id, name, kind, engine, metadata_json, secret_enc_blob, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
		c.ID, c.Name, c.Kind, c.Engine, string(c.MetadataJSON), c.SecretEncBlob, c.CreatedAt, c.UpdatedAt)
	return err
}

func updateConnectionTx(ctx context.Context, tx *sql.Tx, c Connection) error {
	_, err := tx.ExecContext(ctx, `
		UPDATE connections
		SET name=?, kind=?, engine=?, metadata_json=?, secret_enc_blob=?, updated_at=?
		WHERE id=?;`,
		c.Name, c.Kind, c.Engine, string(c.MetadataJSON), c.SecretEncBlob, c.UpdatedAt, c.ID)
	return err
}

func deleteConnectionTx(ctx context.Context, tx *sql.Tx, id string) error {
	res, err := tx.ExecContext(ctx, `DELETE FROM connections WHERE id=?;`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func createJobTx(ctx context.Context, tx *sql.Tx, j Job) error {
	inc := 0
	if j.Incremental {
		inc = 1
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO jobs(id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);`,
		j.ID, j.Name, j.SourceConnectionID, j.TargetConnectionID, j.SourceSQL, j.TargetNamespace, j.TargetTable, j.WriteMode, inc, j.HWMColumn, string(j.OptionsJSON), j.CreatedAt, j.UpdatedAt)
	return err
}

func updateJobTx(ctx context.Context, tx *sql.Tx, j Job) error {
	inc := 0
	if j.Incremental {
		inc = 1
	}
	_, err := tx.ExecContext(ctx, `
		UPDATE jobs
		SET name=?, source_connection_id=?, target_connection_id=?, source_sql=?, target_namespace=?, target_table=?, write_mode=?, incremental=?, hwm_column=?, options_json=?, updated_at=?
		WHERE id=?;`,
		j.Name, j.SourceConnectionID, j.TargetConnectionID, j.SourceSQL, j.TargetNamespace, j.TargetTable, j.WriteMode, inc, j.HWMColumn, string(j.OptionsJSON), j.UpdatedAt, j.ID)
	return err
}

func deleteJobTx(ctx context.Context, tx *sql.Tx, id string) error {
	res, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id=?;`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func insertTasksTx(ctx context.Context, tx *sql.Tx, tasks []TaskInsert) error {
	stmt, err := tx.PrepareContext(ctx, `INSERT INTO tasks(id, run_id, task_index, partition_spec_json, worker_id, status, rows_read, bytes_read, bytes_written, parquet_objects_json, started_at, finished_at, error_message) VALUES (?, ?, ?, ?, NULL, ?, 0, 0, 0, ?, NULL, NULL, NULL);`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, t := range tasks {
		part := t.PartitionSpec
		if len(part) == 0 {
			part = []byte(`{}`)
		}
		objs := t.ParquetObjects
		if len(objs) == 0 {
			objs = []byte(`[]`)
		}
		status := t.Status
		if status == "" {
			status = "PENDING"
		}
		if _, err := stmt.ExecContext(ctx, t.ID, t.RunID, t.TaskIndex, string(part), status, string(objs)); err != nil {
			return err
		}
	}
	return nil
}

func updateRunStatusTx(ctx context.Context, tx *sql.Tx, runID, status string, finished bool, errSummary *string) error {
	if status == "" {
		return fmt.Errorf("missing run status")
	}
	if finished {
		f := nowUTC()
		_, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, finished_at=?, error_summary=? WHERE id=?;`, status, f, errSummary, runID)
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE runs SET status=?, error_summary=? WHERE id=?;`, status, errSummary, runID)
	return err
}

func newStoreID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
