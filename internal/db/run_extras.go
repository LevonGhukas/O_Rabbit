package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func (s *Store) ListEventsForRunPage(ctx context.Context, runID string, limit int, cursor string) ([]Event, string, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}

	var (
		rows *sql.Rows
		err  error
	)
	if strings.TrimSpace(cursor) == "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, run_id, task_id, ts, level, message, fields_json
			FROM events
			WHERE run_id=?
			ORDER BY ts ASC, id ASC
			LIMIT ?;`, runID, limit+1)
	} else {
		var ts, id string
		ts, id, err = parseEventCursor(cursor)
		if err != nil {
			return nil, "", err
		}
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, run_id, task_id, ts, level, message, fields_json
			FROM events
			WHERE run_id=? AND (ts > ? OR (ts = ? AND id > ?))
			ORDER BY ts ASC, id ASC
			LIMIT ?;`, runID, ts, ts, id, limit+1)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	out := make([]Event, 0, limit+1)
	for rows.Next() {
		var e Event
		var fields string
		if err := rows.Scan(&e.ID, &e.RunID, &e.TaskID, &e.TS, &e.Level, &e.Message, &fields); err != nil {
			return nil, "", err
		}
		e.FieldsJSON = json.RawMessage(fields)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	nextCursor := ""
	if len(out) > limit {
		last := out[limit-1]
		nextCursor = formatEventCursor(last.TS, last.ID)
		out = out[:limit]
	}
	return out, nextCursor, nil
}

func formatEventCursor(ts string, id string) string {
	return ts + "|" + id
}

func parseEventCursor(cursor string) (string, string, error) {
	parts := strings.SplitN(strings.TrimSpace(cursor), "|", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", fmt.Errorf("invalid cursor")
	}
	return parts[0], parts[1], nil
}
