package db

import (
	"context"
	"fmt"
)

// Source reconciliation statuses, one per task.
const (
	SourceReconciliationMatch         = "match"
	SourceReconciliationMismatch      = "mismatch"
	SourceReconciliationIndeterminate = "indeterminate"
	SourceReconciliationError         = "error"
)

// SourceReconciliationMethodCount compares a source COUNT(*) of the planned
// range with the rows the task wrote.
const SourceReconciliationMethodCount = "count"

// SourceReconciliation is the commit-time comparison of one task's planned
// source range with its output.
type SourceReconciliation struct {
	RunID          string `json:"run_id"`
	TaskID         string `json:"task_id"`
	Method         string `json:"method"`
	Consistency    string `json:"consistency"`
	ObservedAt     string `json:"observed_at"`
	LowerBound     string `json:"lower_bound,omitempty"`
	LowerExclusive bool   `json:"lower_exclusive"`
	UpperBound     string `json:"upper_bound,omitempty"`
	UpperInclusive bool   `json:"upper_inclusive"`
	SourceRows     int64  `json:"source_rows"`
	OutputRows     int64  `json:"output_rows"`
	Status         string `json:"status"`
	Detail         string `json:"detail,omitempty"`
}

// UpsertSourceReconciliation records the result for a task, replacing an
// earlier attempt's result.
func (s *Store) UpsertSourceReconciliation(ctx context.Context, rec SourceReconciliation) error {
	if rec.RunID == "" || rec.TaskID == "" {
		return fmt.Errorf("source reconciliation needs a run and task id")
	}
	return withBusyRetry(ctx, func() error {
		_, err := s.db.ExecContext(ctx, `INSERT INTO source_reconciliations(run_id,task_id,method,consistency,observed_at,lower_bound,lower_exclusive,upper_bound,upper_inclusive,source_rows,output_rows,status,detail)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(run_id,task_id) DO UPDATE SET method=excluded.method,consistency=excluded.consistency,observed_at=excluded.observed_at,
			lower_bound=excluded.lower_bound,lower_exclusive=excluded.lower_exclusive,upper_bound=excluded.upper_bound,upper_inclusive=excluded.upper_inclusive,
			source_rows=excluded.source_rows,output_rows=excluded.output_rows,status=excluded.status,detail=excluded.detail`,
			rec.RunID, rec.TaskID, rec.Method, rec.Consistency, rec.ObservedAt, rec.LowerBound, rec.LowerExclusive, rec.UpperBound, rec.UpperInclusive,
			rec.SourceRows, rec.OutputRows, rec.Status, rec.Detail)
		return err
	})
}

// ListSourceReconciliations returns a run's recorded results ordered by task.
func (s *Store) ListSourceReconciliations(ctx context.Context, runID string) ([]SourceReconciliation, error) {
	rows, err := s.rdb.QueryContext(ctx, `SELECT sr.run_id,sr.task_id,sr.method,sr.consistency,sr.observed_at,sr.lower_bound,sr.lower_exclusive,sr.upper_bound,sr.upper_inclusive,sr.source_rows,sr.output_rows,sr.status,sr.detail
		FROM source_reconciliations sr LEFT JOIN tasks t ON t.id=sr.task_id WHERE sr.run_id=? ORDER BY COALESCE(t.task_index,0),sr.task_id`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SourceReconciliation
	for rows.Next() {
		var rec SourceReconciliation
		if err := rows.Scan(&rec.RunID, &rec.TaskID, &rec.Method, &rec.Consistency, &rec.ObservedAt, &rec.LowerBound, &rec.LowerExclusive, &rec.UpperBound, &rec.UpperInclusive, &rec.SourceRows, &rec.OutputRows, &rec.Status, &rec.Detail); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// SourceReconciliationSummary aggregates a run's results for diagnosis.
type SourceReconciliationSummary struct {
	Method      string         `json:"method"`
	Consistency string         `json:"consistency"`
	Counts      map[string]int `json:"counts"`
	// Findings lists every task that did not match.
	Findings []SourceReconciliation `json:"findings,omitempty"`
}

// SummarizeSourceReconciliations returns nil when the run was not reconciled.
func SummarizeSourceReconciliations(recs []SourceReconciliation) *SourceReconciliationSummary {
	if len(recs) == 0 {
		return nil
	}
	out := &SourceReconciliationSummary{Method: recs[0].Method, Consistency: recs[0].Consistency, Counts: map[string]int{}}
	for _, rec := range recs {
		out.Counts[rec.Status]++
		if rec.Status != SourceReconciliationMatch {
			out.Findings = append(out.Findings, rec)
		}
	}
	return out
}
