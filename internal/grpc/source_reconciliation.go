package grpcapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/partitionspec"
)

const (
	sourceReconciliationComponent = "source_reconciliation"
	// sourceReconciliationCountTimeout bounds one task's COUNT(*) on the source.
	sourceReconciliationCountTimeout = 30 * time.Minute
	// sourceReconciliationDeadlineShare is the fraction of the time left on the
	// commit context one count may use, so a slow count fails on its own
	// (counted, retryable) timeout before the commit context expires.
	sourceReconciliationDeadlineShare = 2
)

// sourceRangeCounter is the narrow source surface reconciliation needs. A
// seam so tests can fake the source.
type sourceRangeCounter interface {
	connectors.RangeCounter
	Close() error
}

func openSourceRangeCounter(ctx context.Context, engine, dsn string) (sourceRangeCounter, error) {
	reader, err := connectors.OpenCursorReader(ctx, engine, dsn)
	if err != nil {
		return nil, err
	}
	counter, ok := reader.(sourceRangeCounter)
	if !ok {
		_ = reader.Close()
		return nil, fmt.Errorf("source engine %s cannot count a cursor range", engine)
	}
	return counter, nil
}

// reconcileSourceRanges compares, per task, a COUNT(*) of the planned source
// range with the rows the task wrote. It uses the planned bounds from each
// task's partition spec, never the worker-reported high-water mark, so a
// truncated tail is visible. It returns nil when reconciliation is off or when
// nothing blocks the commit; otherwise a classified commit error.
func (s *Server) reconcileSourceRanges(ctx context.Context, resolved resolvedRun, srcConn db.Connection, tasks []db.Task, hasPersistedIntent bool) error {
	settings := resolved.opts.EffectiveReconciliation()
	// A persisted intent means an earlier pass already reconciled and began
	// publishing; re-counting a source that has since grown must not fail it.
	if !settings.Enabled() || hasPersistedIntent {
		return nil
	}
	counter, err := s.openSourceCounter(ctx, srcConn)
	if err != nil {
		return err
	}
	defer counter.Close()

	sourceSQL := ""
	if resolved.opts.NormalizedSourceMode() == "query" {
		// The worker reads the job's source_sql; count the same text.
		sourceSQL = strings.TrimSpace(resolved.config.Job.SourceSQL)
	}
	persisted, err := s.st.ListSourceReconciliations(ctx, tasks[0].RunID)
	if err != nil {
		return err
	}
	byTask := make(map[string]db.SourceReconciliation, len(persisted))
	for _, rec := range persisted {
		byTask[rec.TaskID] = rec
	}
	var mismatches, indeterminate []db.SourceReconciliation
	for _, task := range tasks {
		rec, done := byTask[task.ID]
		if !done || !skipPersistedSourceCount(rec.Status) {
			rec, err = s.reconcileTask(ctx, counter, settings, sourceSQL, task)
			if err != nil {
				return err
			}
		}
		switch rec.Status {
		case db.SourceReconciliationMismatch:
			mismatches = append(mismatches, rec)
		case db.SourceReconciliationIndeterminate:
			indeterminate = append(indeterminate, rec)
		}
	}
	s.warnIndeterminateSourceCounts(ctx, resolved.config.Job.ID, tasks, indeterminate)
	return sourceMismatchError(settings, mismatches)
}

func (s *Server) openSourceCounter(ctx context.Context, srcConn db.Connection) (sourceRangeCounter, error) {
	secret, err := decryptSecretMap(s.k, srcConn)
	if err != nil {
		return nil, &classifiedCommitError{class: commitFailureValidation, component: sourceReconciliationComponent, err: fmt.Errorf("source reconciliation: source credentials unavailable: %w", err)}
	}
	dsn := stringMapValue(secret, "dsn")
	if strings.TrimSpace(dsn) == "" {
		return nil, &classifiedCommitError{class: commitFailureValidation, component: sourceReconciliationComponent, err: fmt.Errorf("source reconciliation: source connection secret missing dsn")}
	}
	counter, err := s.openSourceCounterFn(ctx, srcConn.Engine, dsn)
	if err != nil {
		// Connectivity problems are retryable like any other commit failure.
		return nil, fmt.Errorf("source reconciliation: open source (engine=%s): %w", srcConn.Engine, err)
	}
	return counter, nil
}

func (s *Server) reconcileTask(ctx context.Context, counter sourceRangeCounter, settings jobopts.Reconciliation, sourceSQL string, task db.Task) (db.SourceReconciliation, error) {
	spec, err := partitionspec.Parse(task.PartitionSpec)
	if err != nil || !countableSpecType(spec.Type) {
		return db.SourceReconciliation{}, &classifiedCommitError{class: commitFailureValidation, component: sourceReconciliationComponent, err: fmt.Errorf("source reconciliation: task %s has no countable cursor range (spec type %q): %v", task.ID, spec.Type, err)}
	}
	rec := db.SourceReconciliation{
		RunID:          task.RunID,
		TaskID:         task.ID,
		Method:         db.SourceReconciliationMethodCount,
		Consistency:    settings.Consistency,
		ObservedAt:     db.FormatTimestamp(s.nowFn()),
		LowerBound:     spec.Lower,
		LowerExclusive: spec.LowerExclusive,
		UpperBound:     spec.Upper,
		UpperInclusive: spec.UpperInclusive,
		OutputRows:     task.RowsRead,
	}
	countCtx, cancel := context.WithTimeout(ctx, sourceCountTimeout(ctx))
	defer cancel()
	sourceRows, countErr := counter.CountRange(countCtx, spec.CursorQuery(sourceSQL))
	if countErr != nil {
		rec.Status, rec.Detail = db.SourceReconciliationError, "source count failed: "+countErr.Error()
		if err := s.st.UpsertSourceReconciliation(ctx, rec); err != nil {
			return rec, err
		}
		// Never a mismatch: the source could not be read, so retry the commit.
		return rec, fmt.Errorf("source reconciliation: count for task %s failed: %w", task.ID, countErr)
	}
	rec.SourceRows = sourceRows
	rec.Status, rec.Detail = classifySourceCount(settings.Consistency, sourceRows, task.RowsRead)
	return rec, s.st.UpsertSourceReconciliation(ctx, rec)
}

// skipPersistedSourceCount reports whether a stored result is final: the
// bounds are fixed per spec, so a match or indeterminate row is not re-counted.
func skipPersistedSourceCount(status string) bool {
	return status == db.SourceReconciliationMatch || status == db.SourceReconciliationIndeterminate
}

// sourceCountTimeout bounds one count by the fixed limit and by a share of the
// time the commit context has left.
func sourceCountTimeout(ctx context.Context) time.Duration {
	timeout := sourceReconciliationCountTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if share := time.Until(deadline) / sourceReconciliationDeadlineShare; share < timeout {
			timeout = share
		}
	}
	return timeout
}

func countableSpecType(specType string) bool {
	switch specType {
	case "sql_cursor_range", "sql_cursor_single", "sql_int_range", "mssql_int_range":
		return true
	}
	return false
}

// classifySourceCount decides one task's outcome. A difference is proven data
// loss only when the operator asserted an immutable source; otherwise the
// source may have changed during the run and the result is indeterminate.
func classifySourceCount(consistency string, sourceRows, outputRows int64) (status, detail string) {
	if sourceRows == outputRows {
		return db.SourceReconciliationMatch, ""
	}
	detail = fmt.Sprintf("source holds %d rows in the planned range, %d rows were written (difference %+d)", sourceRows, outputRows, sourceRows-outputRows)
	if consistency == jobopts.ReconciliationConsistencyImmutable {
		return db.SourceReconciliationMismatch, detail
	}
	return db.SourceReconciliationIndeterminate, detail + "; the source may have changed during the run (consistency=none)"
}

func sourceMismatchError(settings jobopts.Reconciliation, mismatches []db.SourceReconciliation) error {
	if len(mismatches) == 0 {
		return nil
	}
	first := mismatches[0]
	summary := fmt.Sprintf("%d task(s) differ from the source count, first task %s: %s", len(mismatches), first.TaskID, first.Detail)
	if settings.OnMismatch == jobopts.ReconciliationOnMismatchReview {
		return commitOperatorError(sourceReconciliationComponent, "held for operator review: %s", summary)
	}
	return &classifiedCommitError{class: commitFailureValidation, component: sourceReconciliationComponent, err: fmt.Errorf("source reconciliation mismatch: %s", summary)}
}

// warnIndeterminateSourceCounts surfaces differences that are not proven loss.
// The commit proceeds; the persisted rows carry the detail.
func (s *Server) warnIndeterminateSourceCounts(ctx context.Context, jobID string, tasks []db.Task, indeterminate []db.SourceReconciliation) {
	if len(indeterminate) == 0 || len(tasks) == 0 {
		return
	}
	runID := tasks[0].RunID
	s.log.Warn("source reconciliation indeterminate", slog.String("run_id", runID), slog.String("job_id", jobID), slog.Int("tasks", len(indeterminate)), slog.String("first_task_id", indeterminate[0].TaskID), slog.String("detail", indeterminate[0].Detail))
	fields, _ := json.Marshal(map[string]any{"event_type": "SOURCE_RECONCILIATION_INDETERMINATE", "tasks": len(indeterminate), "first_task_id": indeterminate[0].TaskID, "detail": indeterminate[0].Detail})
	e := db.Event{ID: "source-reconciliation-indeterminate-" + runID, RunID: runID, TS: db.FormatTimestamp(s.nowFn()), Level: "WARN", Message: "source row count differs from rows written; source may have changed during the run", FieldsJSON: fields}
	if s.st.InsertEventOnce(ctx, e) == nil && s.bc != nil {
		s.bc.Publish(e)
	}
}
