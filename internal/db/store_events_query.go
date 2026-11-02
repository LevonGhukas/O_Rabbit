package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// EventFilter selects events across runs for the log viewer. Empty fields do
// not filter. Results are newest first; Before and After are event sequence
// numbers (the table's rowid, i.e. insertion order, which does not depend on
// worker clocks).
type EventFilter struct {
	Levels   []string // canonical: DEBUG, INFO, WARN, ERROR, CRITICAL
	RunID    string
	JobID    string
	TaskID   string
	WorkerID string
	Query    string // case-insensitive substring of the message or fields
	Since    string // inclusive, RFC 3339
	Until    string // inclusive, RFC 3339
	Before   int64  // only events with seq < Before (older page)
	After    int64  // only events with seq > After (live tail)
	Limit    int
}

// EventRecord is an event with the context the log viewer links to.
type EventRecord struct {
	Event
	Seq      int64  `json:"seq"`
	JobID    string `json:"job_id"`
	WorkerID string `json:"worker_id"`
}

const (
	maxEventQueryLimit = 1000
	// EventCountCap bounds COUNT(*) so a broad filter never scans the whole table.
	EventCountCap = 10000
)

// levelAliases maps a canonical level to the spellings stored in events.
var levelAliases = map[string][]string{
	"DEBUG":    {"DEBUG", "TRACE"},
	"INFO":     {"INFO"},
	"WARN":     {"WARN", "WARNING"},
	"ERROR":    {"ERROR"},
	"CRITICAL": {"CRITICAL", "FATAL", "PANIC"},
}

// NormalizeEventLevel maps any spelling (warning, Fatal, ...) to a canonical level, or "".
func NormalizeEventLevel(level string) string {
	up := strings.ToUpper(strings.TrimSpace(level))
	for canonical, aliases := range levelAliases {
		for _, a := range aliases {
			if up == a {
				return canonical
			}
		}
	}
	return ""
}

func (f EventFilter) where() (string, []any, error) {
	var (
		clauses []string
		args    []any
	)
	if len(f.Levels) > 0 {
		var stored []any
		for _, l := range f.Levels {
			canonical := NormalizeEventLevel(l)
			if canonical == "" {
				return "", nil, fmt.Errorf("unknown level %q", l)
			}
			for _, a := range levelAliases[canonical] {
				stored = append(stored, a)
			}
		}
		clauses = append(clauses, "UPPER(e.level) IN ("+strings.TrimSuffix(strings.Repeat("?,", len(stored)), ",")+")")
		args = append(args, stored...)
	}
	if v := strings.TrimSpace(f.RunID); v != "" {
		clauses = append(clauses, "e.run_id = ?")
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.JobID); v != "" {
		clauses = append(clauses, "r.job_id = ?")
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.TaskID); v != "" {
		clauses = append(clauses, "e.task_id = ?")
		args = append(args, v)
	}
	if v := strings.TrimSpace(f.WorkerID); v != "" {
		clauses = append(clauses, "(t.worker_id = ? OR (json_valid(e.fields_json) AND json_extract(e.fields_json, '$.worker_id') = ?))")
		args = append(args, v, v)
	}
	if v := strings.TrimSpace(f.Query); v != "" {
		like := "%" + escapeLike(strings.ToLower(v)) + "%"
		clauses = append(clauses, `(LOWER(e.message) LIKE ? ESCAPE '\' OR LOWER(e.fields_json) LIKE ? ESCAPE '\')`)
		args = append(args, like, like)
	}
	if v := strings.TrimSpace(f.Since); v != "" {
		clauses = append(clauses, "e.ts >= ?")
		args = append(args, normalizeTimestamp(v))
	}
	if v := strings.TrimSpace(f.Until); v != "" {
		clauses = append(clauses, "e.ts <= ?")
		args = append(args, normalizeTimestamp(v))
	}
	if f.Before > 0 {
		clauses = append(clauses, "e.rowid < ?")
		args = append(args, f.Before)
	}
	if f.After > 0 {
		clauses = append(clauses, "e.rowid > ?")
		args = append(args, f.After)
	}
	if len(clauses) == 0 {
		return "", nil, nil
	}
	return " WHERE " + strings.Join(clauses, " AND "), args, nil
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

const eventQueryFrom = `
	FROM events e
	LEFT JOIN runs r ON r.id = e.run_id
	LEFT JOIN tasks t ON t.id = e.task_id`

// QueryEvents returns one page of matching events, newest first.
// For a live tail (After > 0) it returns the oldest new events first instead,
// so a client can append them in order.
func (s *Store) QueryEvents(ctx context.Context, f EventFilter) ([]EventRecord, error) {
	limit := f.Limit
	if limit <= 0 || limit > maxEventQueryLimit {
		limit = 200
	}
	where, args, err := f.where()
	if err != nil {
		return nil, err
	}
	order := "DESC"
	if f.After > 0 {
		order = "ASC"
	}
	q := `SELECT e.rowid, e.id, e.run_id, e.task_id, e.ts, e.level, e.message, e.fields_json,
		COALESCE(r.job_id, ''),
		COALESCE(t.worker_id, CASE WHEN json_valid(e.fields_json) THEN json_extract(e.fields_json, '$.worker_id') END, '')` +
		eventQueryFrom + where + ` ORDER BY e.rowid ` + order + ` LIMIT ?`
	rows, err := s.rdb.QueryContext(ctx, q, append(args, limit)...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()
	out := make([]EventRecord, 0, limit)
	for rows.Next() {
		var (
			rec    EventRecord
			fields string
		)
		if err := rows.Scan(&rec.Seq, &rec.ID, &rec.RunID, &rec.TaskID, &rec.TS, &rec.Level, &rec.Message, &fields, &rec.JobID, &rec.WorkerID); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		rec.FieldsJSON = json.RawMessage(fields)
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read events: %w", err)
	}
	return out, nil
}

// CountEvents counts matching events up to EventCountCap; capped reports
// whether there may be more.
func (s *Store) CountEvents(ctx context.Context, f EventFilter) (count int, capped bool, err error) {
	f.Before, f.After = 0, 0
	where, args, err := f.where()
	if err != nil {
		return 0, false, err
	}
	q := `SELECT COUNT(*) FROM (SELECT 1` + eventQueryFrom + where + ` LIMIT ?)`
	if err := s.rdb.QueryRowContext(ctx, q, append(args, EventCountCap+1)...).Scan(&count); err != nil {
		return 0, false, fmt.Errorf("count events: %w", err)
	}
	if count > EventCountCap {
		return EventCountCap, true, nil
	}
	return count, false, nil
}

// LatestEventSeq is where a live tail starts when the client has nothing yet.
func (s *Store) LatestEventSeq(ctx context.Context) (int64, error) {
	var seq int64
	if err := s.rdb.QueryRowContext(ctx, `SELECT COALESCE(MAX(rowid), 0) FROM events`).Scan(&seq); err != nil {
		return 0, fmt.Errorf("latest event: %w", err)
	}
	return seq, nil
}
