package db

import (
	"context"
	"encoding/json"
)

type Event struct {
	ID         string          `json:"id"`
	RunID      string          `json:"run_id"`
	TaskID     *string         `json:"task_id"`
	TS         string          `json:"ts"`
	Level      string          `json:"level"`
	Message    string          `json:"message"`
	FieldsJSON json.RawMessage `json:"fields_json"`
}

func (s *Store) InsertEvent(ctx context.Context, e Event) error {
	if len(e.FieldsJSON) == 0 {
		e.FieldsJSON = []byte(`{}`)
	}
	var err error
	err = withBusyRetry(ctx, func() error {
		_, err = s.db.ExecContext(ctx, `INSERT INTO events(id, run_id, task_id, ts, level, message, fields_json) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			e.ID, e.RunID, e.TaskID, normalizeTimestamp(e.TS), e.Level, e.Message, string(e.FieldsJSON))
		return err
	})
	return err
}

// InsertEventOnce records an idempotent lifecycle observation. Callers must use
// a stable event ID that identifies the logical transition or observation.
func (s *Store) InsertEventOnce(ctx context.Context, e Event) error {
	if len(e.FieldsJSON) == 0 {
		e.FieldsJSON = []byte(`{}`)
	}
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `INSERT OR IGNORE INTO events(id, run_id, task_id, ts, level, message, fields_json) VALUES (?, ?, ?, ?, ?, ?, ?);`,
			e.ID, e.RunID, e.TaskID, normalizeTimestamp(e.TS), e.Level, e.Message, string(e.FieldsJSON))
		return err
	})
}

func (s *Store) ListEventsForRun(ctx context.Context, runID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.rdb.QueryContext(ctx, `SELECT id, run_id, task_id, ts, level, message, fields_json FROM events WHERE run_id=? ORDER BY ts ASC LIMIT ?;`, runID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var fields string
		if err := rows.Scan(&e.ID, &e.RunID, &e.TaskID, &e.TS, &e.Level, &e.Message, &fields); err != nil {
			return nil, err
		}
		e.FieldsJSON = []byte(fields)
		out = append(out, e)
	}
	return out, rows.Err()
}
