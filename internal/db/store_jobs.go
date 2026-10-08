package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
)

type Job struct {
	ID                 string          `json:"id"`
	Name               string          `json:"name"`
	SourceConnectionID string          `json:"source_connection_id"`
	TargetConnectionID string          `json:"target_connection_id"`
	SourceSQL          string          `json:"source_sql"`
	TargetNamespace    string          `json:"target_namespace"`
	TargetTable        string          `json:"target_table"`
	WriteMode          string          `json:"write_mode"`
	Incremental        bool            `json:"incremental"`
	HWMColumn          *string         `json:"hwm_column"`
	OptionsJSON        json.RawMessage `json:"options_json"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
}

func (s *Store) CreateJob(ctx context.Context, j Job) error {
	j = prepareJobForCreate(j)
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return createJobTx(ctx, tx, j)
	})
}

func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	var j Job
	var options string
	var inc int
	row := s.db.QueryRowContext(ctx, `SELECT id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at FROM jobs WHERE id=?;`, id)
	if err := row.Scan(&j.ID, &j.Name, &j.SourceConnectionID, &j.TargetConnectionID, &j.SourceSQL, &j.TargetNamespace, &j.TargetTable, &j.WriteMode, &inc, &j.HWMColumn, &options, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return Job{}, err
	}
	j.Incremental = inc != 0
	j.OptionsJSON = []byte(options)
	return j, nil
}

// FindJobByName returns the newest job named name, or sql.ErrNoRows.
func (s *Store) FindJobByName(ctx context.Context, name string) (Job, error) {
	var j Job
	var options string
	var inc int
	row := s.db.QueryRowContext(ctx, `SELECT id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at FROM jobs WHERE name=? ORDER BY created_at DESC, id DESC LIMIT 1;`, name)
	if err := row.Scan(&j.ID, &j.Name, &j.SourceConnectionID, &j.TargetConnectionID, &j.SourceSQL, &j.TargetNamespace, &j.TargetTable, &j.WriteMode, &inc, &j.HWMColumn, &options, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return Job{}, err
	}
	j.Incremental = inc != 0
	j.OptionsJSON = []byte(options)
	return j, nil
}

func (s *Store) ListJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT id, name, source_connection_id, target_connection_id, source_sql, target_namespace, target_table, write_mode, incremental, hwm_column, options_json, created_at, updated_at FROM jobs ORDER BY created_at DESC;`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var options string
		var inc int
		if err := rows.Scan(&j.ID, &j.Name, &j.SourceConnectionID, &j.TargetConnectionID, &j.SourceSQL, &j.TargetNamespace, &j.TargetTable, &j.WriteMode, &inc, &j.HWMColumn, &options, &j.CreatedAt, &j.UpdatedAt); err != nil {
			return nil, err
		}
		j.Incremental = inc != 0
		j.OptionsJSON = []byte(options)
		out = append(out, j)
	}
	return out, rows.Err()
}

func (s *Store) UpdateJob(ctx context.Context, j Job) error {
	if len(j.OptionsJSON) == 0 {
		j.OptionsJSON = []byte(`{}`)
	}
	j.UpdatedAt = nowUTC()
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return updateJobTx(ctx, tx, j)
	})
}

func (s *Store) DeleteJob(ctx context.Context, id string) error {
	return s.withTx(ctx, nil, func(tx *sql.Tx) error {
		return deleteJobTx(ctx, tx, id)
	})
}

func (s *Store) CountRunsForJob(ctx context.Context, jobID string) (total int, active int, err error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			SUM(CASE WHEN status IN ('PLANNING','RUNNING','COMMITTING') THEN 1 ELSE 0 END) AS active
		FROM runs
		WHERE job_id=?;`, jobID)
	var activeN sql.NullInt64
	if err := row.Scan(&total, &activeN); err != nil {
		return 0, 0, err
	}
	if activeN.Valid {
		active = int(activeN.Int64)
	}
	return total, active, nil
}

// MaxPartIndexForJob scans completed task outputs for a job and returns the max part number
// found in object keys like ".../part-000123-000.parquet". It also accepts
// the legacy base-file form so existing completed runs remain visible.
//
// This is used to keep part numbering monotonically increasing when writing into a shared prefix.
func (s *Store) MaxPartIndexForJob(ctx context.Context, jobID string) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.parquet_objects_json
		FROM tasks t
		JOIN runs r ON r.id = t.run_id
		WHERE r.job_id=? AND t.status='SUCCEEDED';`, jobID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	re := regexp.MustCompile(`(?:^|/)part-(\d+)(?:-\d+)?\.parquet$`)
	max := 0
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return 0, err
		}
		var arr []map[string]any
		if err := json.Unmarshal([]byte(raw), &arr); err != nil {
			continue
		}
		for _, o := range arr {
			k, _ := o["key"].(string)
			if k == "" {
				continue
			}
			m := re.FindStringSubmatch(k)
			if len(m) != 2 {
				continue
			}
			v, err := strconv.Atoi(m[1])
			if err != nil {
				continue
			}
			if v > max {
				max = v
			}
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return max, nil
}

func (s *Store) GetHWM(ctx context.Context, jobID string) (string, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT hwm_value FROM hwm WHERE job_id=?;`, jobID)
	var v string
	if err := row.Scan(&v); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}
	return v, true, nil
}

func (s *Store) UpsertHWM(ctx context.Context, jobID, value string) error {
	var err error
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO hwm(job_id, hwm_value, updated_at) VALUES (?, ?, ?) ON CONFLICT(job_id) DO UPDATE SET hwm_value=excluded.hwm_value, updated_at=excluded.updated_at;`, jobID, value, nowUTC())
		return err
	})
	return err
}
