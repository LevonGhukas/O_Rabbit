// internal/planner/planner.go
// this file contains the core planner logic for creating runs and tasks based on jobs and dataset state.

package planner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

const activeWorkerHeartbeatWindow = 30 * time.Second

// newID handles new id behavior.
// It exists to keep this logic isolated and reusable.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func emitPlanEvent(ctx context.Context, st *db.Store, runID, level, message string, fields map[string]any) {
	payload, _ := json.Marshal(fields)
	_ = st.InsertEvent(ctx, db.Event{
		ID:         newID(),
		RunID:      runID,
		TS:         db.FormatTimestamp(time.Now()),
		Level:      level,
		Message:    message,
		FieldsJSON: payload,
	})
}

// isLocalEndpoint handles is local endpoint behavior.
// It exists to keep this logic isolated and reusable.
func isLocalEndpoint(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	// Accept full URLs (http://host:port) or bare host:port.
	if !strings.Contains(raw, "//") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

type datasetState struct {
	MaxHWMValue string `json:"max_hwm_value"`
	MaxPart     int    `json:"max_part"`
	NextPart    int    `json:"next_part"`
	SourceMode  string `json:"source_mode"`
	QueryHash   string `json:"query_hash"`
}

// loadDatasetState handles load dataset state behavior.
// It exists to keep this logic isolated and reusable.
func loadDatasetState(ctx context.Context, st *db.Store, k crypto.Key, job db.Job, srcEngine string, opts jobopts.Options) (datasetState, bool, string, bool, error) {
	tgtConn, err := st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return datasetState{}, false, "", false, err
	}

	target, err := dataset.ParseTarget(tgtConn.MetadataJSON)
	if err != nil {
		return datasetState{}, false, "", false, err
	}
	localTarget := isLocalEndpoint(target.Endpoint)

	// Compute dataset prefix (derived if metadata.prefix empty).
	basePrefix := dataset.Prefix(target.Prefix, srcEngine, sourceDatasetName(job, opts))

	sec, err := crypto.Decrypt(k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		return datasetState{}, false, basePrefix, localTarget, err
	}
	var tgtSecret map[string]any
	if err := json.Unmarshal(sec, &tgtSecret); err != nil {
		return datasetState{}, false, basePrefix, localTarget, fmt.Errorf("target connection secret is not a JSON object: %w", err)
	}
	accessKey, _ := tgtSecret["access_key_id"].(string)
	secretKey, _ := tgtSecret["secret_access_key"].(string)
	sessionToken, _ := tgtSecret["session_token"].(string)

	u, err := s3io.New(ctx, s3io.Config{
		Endpoint:        target.Endpoint,
		Region:          target.Region,
		Bucket:          target.Bucket,
		ForcePathStyle:  target.ForcePathStyle,
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		SessionToken:    sessionToken,
	})
	if err != nil {
		return datasetState{}, false, basePrefix, localTarget, err
	}

	key := basePrefix + "/_state.json"
	b, ok, err := u.GetObjectBytes(ctx, key)
	if err != nil {
		return datasetState{}, false, basePrefix, localTarget, err
	}
	if !ok {
		return datasetState{}, false, basePrefix, localTarget, nil
	}
	var ds datasetState
	if err := json.Unmarshal(b, &ds); err != nil {
		return datasetState{}, false, basePrefix, localTarget, fmt.Errorf("parse dataset state: %w", err)
	}
	return ds, true, basePrefix, localTarget, nil
}

func sourceDatasetName(job db.Job, opts jobopts.Options) string {
	if name := strings.TrimSpace(opts.SourceName); name != "" {
		return name
	}
	if opts.NormalizedSourceMode() == "query" {
		if hash := strings.TrimSpace(opts.QueryHash); hash != "" {
			return "query_" + hash
		}
		return "query"
	}
	if table := strings.TrimSpace(opts.Table); table != "" {
		return table
	}
	return strings.TrimSpace(job.TargetTable)
}

func sourceQueryForJob(job db.Job, opts jobopts.Options) string {
	if query := strings.TrimSpace(opts.Query); query != "" {
		return query
	}
	return strings.TrimSpace(job.SourceSQL)
}

func validateDatasetSourceState(ds datasetState, dsOK bool, opts jobopts.Options) error {
	if !dsOK {
		return nil
	}
	mode := opts.NormalizedSourceMode()
	if storedMode := strings.TrimSpace(ds.SourceMode); storedMode != "" && storedMode != mode {
		return fmt.Errorf("dataset state source_mode=%q does not match requested source_mode=%q; use a new target prefix or reset the dataset state", storedMode, mode)
	}
	if mode == "query" {
		storedHash := strings.TrimSpace(ds.QueryHash)
		currentHash := strings.TrimSpace(opts.QueryHash)
		if storedHash != "" && currentHash != "" && storedHash != currentHash {
			return fmt.Errorf("query text changed for existing incremental dataset (stored query_hash=%s requested query_hash=%s); use a new target prefix or reset the dataset state", storedHash, currentHash)
		}
	}
	return nil
}

func activeWorkerCountBestEffort(ctx context.Context, st *db.Store) int {
	if st == nil {
		return 0
	}
	cutoff := db.FormatTimestamp(time.Now().UTC().Add(-activeWorkerHeartbeatWindow))
	workers, err := st.ListWorkersActive(ctx, cutoff)
	if err != nil {
		return 0
	}
	return len(workers)
}

// CreateRunAndTasks creates a run and inserts tasks.
//
// MVP strategies:
// - single: one task for the full job
// - ordered_cursor: one or more ordered cursor partitions for SQL sources.
//
// State model:
// - The dataset state file (<prefix>/_state.json) is treated as the source of truth.
// - If _state.json is missing, we treat the dataset as empty and plan a full export.
func CreateRunAndTasks(ctx context.Context, st *db.Store, k crypto.Key, job db.Job, registrationConfig json.RawMessage, audit *db.AuditRecord) (run db.Run, tasks []db.TaskInsert, err error) {
	o, err := jobopts.Parse(job.OptionsJSON)
	if err != nil {
		return db.Run{}, nil, err
	}

	// Load source engine (for derived dataset prefix).
	srcConn, err := st.GetConnection(ctx, job.SourceConnectionID)
	if err != nil {
		return db.Run{}, nil, err
	}
	srcEngine := connectors.NormalizeSourceEngine(srcConn.Engine)
	if srcEngine == "" {
		srcEngine = "db"
	}
	o.SourceMode = o.NormalizedSourceMode()
	if strings.TrimSpace(o.WhereClause) != "" {
		if err := connectors.ValidateWhereClause(o.WhereClause); err != nil {
			return db.Run{}, nil, fmt.Errorf("options_json.where_clause is invalid: %w", err)
		}
	}
	if o.SourceMode == "query" {
		sourceQuery, err := connectors.NormalizeReadOnlySQLQuery(sourceQueryForJob(job, o))
		if err != nil {
			return db.Run{}, nil, fmt.Errorf("options_json.query is invalid for query mode: %w", err)
		}
		o.Query = sourceQuery
		if strings.TrimSpace(o.QueryHash) == "" {
			o.QueryHash = connectors.QueryHash(sourceQuery)
		}
		if strings.TrimSpace(o.SourceName) == "" {
			o.SourceName = sourceDatasetName(job, o)
		}
	}

	tgtConn, err := st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return db.Run{}, nil, err
	}
	target, err := dataset.ParseTarget(tgtConn.MetadataJSON)
	if err != nil {
		return db.Run{}, nil, err
	}
	sourceName := sourceDatasetName(job, o)
	basePrefix := dataset.Prefix(target.Prefix, srcEngine, sourceName)
	datasetKey := dataset.StorageKey(target.Endpoint, target.Bucket, basePrefix)

	// A dataset has at most one active run. A new run for a busy dataset is
	// rejected with DatasetBusyError below; an unwanted active run must be
	// canceled explicitly (POST /runs/{id}/cancel), never superseded silently.
	run = db.Run{
		ID:                     newID(),
		JobID:                  job.ID,
		DatasetKey:             datasetKey,
		Status:                 "PLANNING",
		CorrelationID:          newID(),
		StartedAt:              db.FormatTimestamp(time.Now()),
		RegistrationConfigJSON: append(json.RawMessage(nil), registrationConfig...),
	}
	if err := st.CreateRun(ctx, run); err != nil {
		if errors.Is(err, db.ErrActiveDatasetRun) {
			if active, ok, aerr := st.FindActiveRunByDatasetKey(ctx, datasetKey); aerr == nil && ok {
				return db.Run{}, nil, &DatasetBusyError{
					DatasetKey:   datasetKey,
					BasePrefix:   basePrefix,
					ActiveRunID:  active.ID,
					ActiveJobID:  active.JobID,
					ActiveStatus: active.Status,
				}
			}
			return db.Run{}, nil, &DatasetBusyError{
				DatasetKey: datasetKey,
				BasePrefix: basePrefix,
			}
		}
		return db.Run{}, nil, err
	}
	runIDForFailure := run.ID
	defer func() {
		if err == nil || strings.TrimSpace(runIDForFailure) == "" {
			return
		}
		msg := strings.TrimSpace(db.RedactCredentials(err.Error()))
		if msg == "" {
			msg = "run planning failed"
		}
		failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = st.UpdateRunStatus(failCtx, runIDForFailure, "FAILED", true, &msg)
		_ = st.RecordRunFailure(failCtx, runIDForFailure, string(failure.ClassOf(err)), db.RunFailurePhasePlanning)
		emitPlanEvent(failCtx, st, runIDForFailure, "ERROR", "planner failed", map[string]any{"error": msg})
	}()
	startTasks := func(tasks []db.TaskInsert) (bool, error) {
		var admitted bool
		var err error
		if audit != nil {
			admitted, err = st.StartRunWithTasksAudited(ctx, run, tasks, *audit)
		} else {
			admitted, err = st.StartRunWithTasks(ctx, run, tasks)
		}
		if err == nil && !admitted {
			emitPlanEvent(ctx, st, run.ID, "INFO", "run queued by global active-run admission", map[string]any{
				"admission": "MAX_ACTIVE_RUNS",
				"status":    "PLANNING",
			})
		}
		return admitted, err
	}

	// Keep the user's requested types for persisted job options; o.ColumnTypes
	// becomes this run's verified effective types.
	userColumnTypes := o.ColumnTypes
	effectiveTypes, typeWarnings, err := resolveEffectiveColumnTypes(ctx, st, k, job, srcEngine, o, run.ID)
	if err != nil {
		return db.Run{}, nil, err
	}
	o.ColumnTypes = effectiveTypes
	if len(typeWarnings) > 0 {
		run.TypeWarnings = typeWarnings
		if err := st.SetRunTypeWarnings(ctx, run.ID, typeWarnings); err != nil {
			return db.Run{}, nil, err
		}
		for _, warning := range typeWarnings {
			slog.Warn("type mapping fallback", "run_id", run.ID, "source_engine", srcEngine, "column", warning.Column, "logical_type", warning.LogicalType, "storage_type", warning.StorageType, "mapping_class", warning.Class, "reason", warning.Reason)
		}
		emitPlanEvent(ctx, st, run.ID, "WARN", "column type fallbacks applied", map[string]any{"warnings": typeWarnings})
	}
	if usages := o.DeprecatedUsages(srcEngine); len(usages) > 0 {
		slog.Warn("job uses deprecated options", "run_id", run.ID, "job_id", job.ID, "usages", usages)
		emitPlanEvent(ctx, st, run.ID, "WARN", "deprecated job options used", map[string]any{"usages": usages})
	}

	switch o.NormalizedPartitionStrategy() {
	case "single":
		var part json.RawMessage
		if connectors.SupportsOrderedCursor(srcEngine) {
			part = partitionSpecSQLCursorSingle(o.Table, o.NormalizedSourceMode(), o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, o.EffectiveCursorColumn(), connectors.CursorDomainUnknown, "", false, "")
		} else {
			part = PartitionSpecSingleWithFileOptions(o.Table, o.NormalizedSourceMode(), o.QueryHash, o.RecordPath, o.FileFormat)
		}
		tasks := []db.TaskInsert{{
			ID:            newID(),
			RunID:         run.ID,
			TaskIndex:     1,
			PartitionSpec: part,
			Status:        "PENDING",
		}}
		admitted, err := startTasks(tasks)
		if err != nil {
			return db.Run{}, nil, err
		}
		if admitted {
			run.Status = "RUNNING"
		}
		return run, tasks, nil

	case "ordered_cursor":
		if !connectors.SupportsOrderedCursor(srcEngine) {
			return db.Run{}, nil, fmt.Errorf("partition_strategy=ordered_cursor is not supported for source engine %q", srcEngine)
		}
		sourceMode := o.NormalizedSourceMode()
		o.SourceMode = sourceMode
		sourceQuery := ""
		cursorColumn := o.EffectiveCursorColumn()
		switch sourceMode {
		case "table":
			if strings.TrimSpace(o.Table) == "" {
				return db.Run{}, nil, fmt.Errorf("options_json.table is required for ordered_cursor")
			}
		case "query":
			if !connectors.SupportsQueryMode(srcEngine) {
				return db.Run{}, nil, fmt.Errorf("query mode is not supported for %s", srcEngine)
			}
			var qerr error
			sourceQuery, qerr = connectors.NormalizeReadOnlySQLQuery(sourceQueryForJob(job, o))
			if qerr != nil {
				return db.Run{}, nil, fmt.Errorf("options_json.query is invalid for query mode: %w", qerr)
			}
			o.Query = sourceQuery
			if strings.TrimSpace(o.QueryHash) == "" {
				o.QueryHash = connectors.QueryHash(sourceQuery)
			}
			if strings.TrimSpace(o.SourceName) == "" {
				o.SourceName = sourceDatasetName(job, o)
			}
		default:
			return db.Run{}, nil, fmt.Errorf("options_json.source_mode must be table or query")
		}
		if strings.TrimSpace(o.Table) == "" && sourceMode == "table" {
			return db.Run{}, nil, fmt.Errorf("options_json.table is required for ordered_cursor")
		}
		if strings.TrimSpace(cursorColumn) == "" && job.Incremental {
			return db.Run{}, nil, fmt.Errorf("options_json.cursor_column is required for incremental mode")
		}

		ds, dsOK, basePrefix, localTarget, err := loadDatasetState(ctx, st, k, job, srcEngine, o)
		if err != nil {
			return db.Run{}, nil, err
		}
		if err := validateDatasetSourceState(ds, dsOK, o); err != nil {
			return db.Run{}, nil, err
		}

		fromHWM := ""
		basePart := 0
		if dsOK {
			fromHWM = strings.TrimSpace(ds.MaxHWMValue)
			if ds.NextPart > 0 {
				basePart = ds.NextPart - 1
			} else {
				basePart = ds.MaxPart
			}
		} else {
			if v, ok, err := st.GetHWM(ctx, job.ID); err == nil && ok && strings.TrimSpace(v) != "" {
				emitPlanEvent(ctx, st, run.ID, "WARN", "_state.json missing; resetting HWM to empty", map[string]any{"dataset_prefix": basePrefix, "sqlite_hwm": v})
				_ = st.UpsertHWM(ctx, job.ID, "")
			}
		}
		if !job.Incremental {
			fromHWM = ""
		}

		planStart := time.Now()
		emitPlanEvent(ctx, st, run.ID, "INFO", "planner ordered_cursor", map[string]any{
			"stage":         "start",
			"source_engine": srcEngine,
			"source_mode":   sourceMode,
			"table":         o.Table,
			"query_hash":    o.QueryHash,
			"cursor_column": cursorColumn,
			"from_hwm":      fromHWM,
			"ctx_deadline":  ctxDeadlineRFC3339(ctx),
		})

		var reader any
		var closeReader func() error
		if connectors.SupportsDocumentReader(srcEngine) {
			r, err := openDocumentReader(ctx, st, k, job.SourceConnectionID, srcEngine)
			if err != nil {
				return db.Run{}, nil, err
			}
			reader = r
			closeReader = r.Close
		} else {
			r, err := openCursorReader(ctx, st, k, job.SourceConnectionID, srcEngine)
			if err != nil {
				return db.Run{}, nil, err
			}
			reader = r
			closeReader = r.Close
		}
		defer closeReader()
		validationStart := time.Now()
		cv := connectors.CursorColumnValidation{}
		if sourceMode == "query" {
			emitPlanEvent(ctx, st, run.ID, "INFO", "planner query_mode validation", map[string]any{
				"stage":         "validation_start",
				"source_engine": srcEngine,
				"query_hash":    o.QueryHash,
				"cursor_column": cursorColumn,
			})
			queryReader, ok := reader.(connectors.SourceQueryReader)
			if !ok {
				return db.Run{}, nil, fmt.Errorf("query mode is not supported for %s", srcEngine)
			}
			cv, err = validateQueryCursorColumn(ctx, queryReader, srcEngine, sourceQuery, cursorColumn, o.QueryHash)
			if err != nil {
				emitPlanEvent(ctx, st, run.ID, "ERROR", "planner query_mode validation", map[string]any{
					"stage":         "validation_failed",
					"source_engine": srcEngine,
					"query_hash":    o.QueryHash,
					"cursor_column": cursorColumn,
					"error":         err.Error(),
					"duration_ms":   time.Since(validationStart).Milliseconds(),
				})
				return db.Run{}, nil, err
			}
		} else {
			if v, ok := reader.(cursorValidator); ok {
				if strings.TrimSpace(cursorColumn) != "" {
					cv, err = validateCursorColumn(ctx, v, srcEngine, o.Table, cursorColumn)
				}
				if (!cv.Found || cv.Domain == connectors.CursorDomainUnknown) && (!job.Incremental || strings.TrimSpace(cursorColumn) == "") {
					if td, tdOK := reader.(tableDescriber); tdOK {
						if cols, _, derr := td.DescribeTable(ctx, o.Table); derr == nil {
							for _, col := range cols {
								res, verr := validateCursorColumn(ctx, v, srcEngine, o.Table, col)
								if verr == nil && res.Found && res.Orderable && res.Domain != connectors.CursorDomainUnknown {
									cursorColumn = col
									cv = res
									err = nil
									break
								}
							}
						}
					}
				}
			} else {
				return db.Run{}, nil, fmt.Errorf("source engine %s does not support cursor validation", srcEngine)
			}
			if err != nil && job.Incremental {
				return db.Run{}, nil, err
			}
		}
		if !cv.Found {
			if job.Incremental {
				if sourceMode == "query" {
					return db.Run{}, nil, fmt.Errorf("cursor column %q was not found in query result", cursorColumn)
				}
				return db.Run{}, nil, fmt.Errorf("cursor column %q was not found in table %q", cursorColumn, o.Table)
			}
			part := partitionSpecSQLCursorSingle(o.Table, sourceMode, o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, "", connectors.CursorDomainInt64, "", false, "")
			tasks := []db.TaskInsert{{
				ID:            newID(),
				RunID:         run.ID,
				TaskIndex:     1,
				PartitionSpec: part,
				Status:        "PENDING",
			}}
			admitted, err := startTasks(tasks)
			if err != nil {
				return db.Run{}, nil, err
			}
			if admitted {
				run.Status = "RUNNING"
			}
			return run, tasks, nil
		}
		// Query result identifiers may be quoted and case-sensitive. The reader
		// has already resolved the exact output name, so use it for stats and all
		// worker cursor queries instead of the user-provided spelling.
		if sourceMode == "query" && strings.TrimSpace(cv.ResolvedName) != "" {
			cursorColumn = cv.ResolvedName
		}
		if !cv.Orderable || cv.Domain == connectors.CursorDomainUnknown {
			return db.Run{}, nil, fmt.Errorf("cursor column %q has unsupported type %q; choose an orderable numeric, decimal, date, timestamp, or text column", cursorColumn, cv.DataType)
		}
		if cv.NullableKnown && cv.Nullable {
			return db.Run{}, nil, fmt.Errorf("cursor column %q is nullable; nullable cursor columns can skip rows in incremental mode. Choose a NOT NULL ordered key or run full load", cursorColumn)
		}
		if cv.ResolvedName != "" {
			cursorColumn = cv.ResolvedName
		}
		o.CursorColumn = cursorColumn
		o.IDColumn = cursorColumn
		o.CursorDomain = string(cv.Domain)
		if sourceMode == "query" {
			emitPlanEvent(ctx, st, run.ID, "INFO", "planner query_mode validation", map[string]any{
				"stage":         "validation_success",
				"source_engine": srcEngine,
				"query_hash":    o.QueryHash,
				"cursor_column": cursorColumn,
				"cursor_type":   cv.DataType,
				"cursor_domain": cv.Domain,
				"duration_ms":   time.Since(validationStart).Milliseconds(),
				"range_capable": cv.RangeCapable,
			})
		} else if cv.IndexedKnown && !cv.Indexed {
			emitPlanEvent(ctx, st, run.ID, "WARN", "planner ordered_cursor validation", map[string]any{
				"stage":          "validation",
				"table":          o.Table,
				"cursor_column":  cursorColumn,
				"cursor_type":    cv.DataType,
				"cursor_domain":  cv.Domain,
				"cursor_indexed": false,
				"duration_ms":    time.Since(validationStart).Milliseconds(),
				"range_capable":  cv.RangeCapable,
				"note":           "non-indexed cursor columns can make ordered scans and MIN/MAX discovery slow; prefer an indexed or sort-keyed cursor column",
			})
		} else {
			emitPlanEvent(ctx, st, run.ID, "INFO", "planner ordered_cursor validation", map[string]any{
				"stage":          "validation",
				"table":          o.Table,
				"cursor_column":  cursorColumn,
				"cursor_type":    cv.DataType,
				"cursor_domain":  cv.Domain,
				"cursor_indexed": cv.Indexed,
				"duration_ms":    time.Since(validationStart).Milliseconds(),
				"range_capable":  cv.RangeCapable,
			})
		}

		stats := connectors.CursorStats{}
		statsStart := time.Now()
		if cv.RangeCapable {
			if sourceMode == "query" {
				queryReader, ok := reader.(connectors.SourceQueryReader)
				if !ok {
					return db.Run{}, nil, fmt.Errorf("query mode is not supported for %s", srcEngine)
				}
				stats, err = discoverQueryCursorStats(ctx, queryReader, srcEngine, sourceQuery, cursorColumn, cv.Domain, o.QueryHash)
			} else {
				if d, ok := reader.(cursorStatDiscoverer); ok {
					stats, err = discoverCursorStats(ctx, d, srcEngine, o.Table, cursorColumn, cv.Domain)
				} else {
					return db.Run{}, nil, fmt.Errorf("source engine %s does not support cursor stats discovery", srcEngine)
				}
			}
			if err != nil {
				return db.Run{}, nil, err
			}
			fields := map[string]any{
				"stage":                "stats",
				"source_mode":          sourceMode,
				"table":                o.Table,
				"query_hash":           o.QueryHash,
				"cursor_column":        cursorColumn,
				"cursor_domain":        cv.Domain,
				"min_cursor":           stats.MinValue,
				"max_cursor":           stats.MaxValue,
				"row_count_estimate":   stats.RowCount,
				"table_bytes_estimate": stats.TableBytes,
				"duration_ms":          time.Since(statsStart).Milliseconds(),
			}
			message := "planner ordered_cursor stats"
			if sourceMode == "query" {
				message = "planner query_mode stats"
			}
			emitPlanEvent(ctx, st, run.ID, "INFO", message, fields)
		}

		// Both modes infer omitted task ranges and concurrency from the same safe
		// planner heuristics. auto_tune controls whether the caller delegates the
		// whole performance policy, not whether a manual file-size request must
		// expose scheduler internals.
		activeWorkers := activeWorkerCountBestEffort(ctx, st)
		o, autoTuneDetails := autoTuneCursorPlanWithDecision(o, cv.Domain, stats, localTarget, activeWorkers)
		persisted := o
		persisted.ColumnTypes = userColumnTypes
		_ = persistTunedOptionsBestEffort(ctx, st, job, persisted)

		{
			emitPlanEvent(ctx, st, run.ID, "INFO", "performance_plan", map[string]any{
				"source_mode":                     sourceMode,
				"table":                           o.Table,
				"query_hash":                      o.QueryHash,
				"cursor_column":                   cursorColumn,
				"cursor_domain":                   cv.Domain,
				"from_hwm":                        fromHWM,
				"min_cursor":                      stats.MinValue,
				"max_cursor":                      stats.MaxValue,
				"auto_tune":                       o.AutoTune,
				"row_count_estimate":              stats.RowCount,
				"table_bytes_estimate":            stats.TableBytes,
				"estimated_rows":                  autoTuneDetails.EstimatedRows,
				"active_workers":                  autoTuneDetails.ActiveWorkers,
				"table_bytes":                     autoTuneDetails.TableBytes,
				"target_file_bytes":               autoTuneDetails.TargetFileBytes,
				"task_target_bytes":               autoTuneDetails.TaskTargetBytes,
				"files_per_task":                  autoTuneDetails.FilesPerTask,
				"planning_max_in_flight_tasks":    autoTuneDetails.PlanningMaxInFlightTasks,
				"effective_concurrency":           autoTuneDetails.EffectiveMinTaskConcurrency,
				"max_in_flight_tasks":             autoTuneDetails.MaxInFlightTasks,
				"minimum_tasks":                   autoTuneDetails.MinimumTasks,
				"planned_tasks":                   o.PlannedTasks,
				"final_planned_tasks":             autoTuneDetails.FinalPlannedTasks,
				"planned_tasks_by_bytes":          autoTuneDetails.PlannedTasksByBytes,
				"planned_tasks_by_rows":           autoTuneDetails.PlannedTasksByRows,
				"selected_max_in_flight_reason":   autoTuneDetails.SelectedMaxInFlightReason,
				"target_rows_per_task":            autoTuneDetails.TargetRowsPerTask,
				"selected_fallback_rows_per_task": autoTuneDetails.SelectedFallbackRowsPerTask,
				"selected_reason":                 autoTuneDetails.SelectedReason,
				"task_count_explicit":             autoTuneDetails.SelectedReason == "user_override",
				"concurrency_explicit":            autoTuneDetails.SelectedMaxInFlightReason == "user_override",
				"min_tasks_multiplier":            o.MinTasksMultiplier,
			})
		}

		var snapshotCtx string
		if o.ConsistencyMode == "STRONG_SNAPSHOT" {
			if exporter, ok := reader.(connectors.SnapshotExporter); ok {
				var err error
				snapshotCtx, err = exporter.ExportSnapshot(ctx)
				if err != nil {
					return db.Run{}, nil, fmt.Errorf("failed to export snapshot for STRONG_SNAPSHOT consistency: %w", err)
				}
			} else {
				return db.Run{}, nil, fmt.Errorf("STRONG_SNAPSHOT consistency mode is not supported for engine %q", srcEngine)
			}
		}

		// Resume after the high-water mark, or, with cursor_lookback, at an
		// inclusive bound below it so late-committed rows are re-read.
		lowerBound, lowerExclusive := fromHWM, strings.TrimSpace(fromHWM) != ""
		if lowerExclusive && strings.TrimSpace(o.CursorLookback) != "" {
			start, err := connectors.CursorLookbackStart(cv.Domain, fromHWM, o.CursorLookback)
			if err != nil {
				return db.Run{}, nil, err
			}
			lowerBound, lowerExclusive = start, false
			emitPlanEvent(ctx, st, run.ID, "INFO", "cursor lookback applied", map[string]any{"from_hwm": fromHWM, "cursor_lookback": o.CursorLookback, "lower_bound_inclusive": start})
		}

		idx := basePart + 1
		var tasks []db.TaskInsert
		if cv.RangeCapable {
			tasks, err = buildOrderedCursorRangeTasks(run.ID, idx, o.Table, cursorColumn, cv.Domain, lowerBound, lowerExclusive, stats, o, snapshotCtx)
			if err != nil {
				return db.Run{}, nil, err
			}
		} else {
			part := partitionSpecSQLCursorSingle(o.Table, sourceMode, o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, cursorColumn, cv.Domain, lowerBound, lowerExclusive, snapshotCtx)
			tasks = []db.TaskInsert{{ID: newID(), RunID: run.ID, TaskIndex: idx, PartitionSpec: part, Status: "PENDING"}}
		}
		if len(tasks) == 0 {
			part := partitionSpecSQLCursorSingle(o.Table, sourceMode, o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, cursorColumn, cv.Domain, lowerBound, lowerExclusive, snapshotCtx)
			tasks = []db.TaskInsert{{ID: newID(), RunID: run.ID, TaskIndex: idx, PartitionSpec: part, Status: "PENDING"}}
		}

		admitted, err := startTasks(tasks)
		if err != nil {
			return db.Run{}, nil, err
		}
		if admitted {
			run.Status = "RUNNING"
		}
		emitPlanEvent(ctx, st, run.ID, "INFO", "planner ordered_cursor done", map[string]any{
			"stage":          "tasks_created",
			"tasks":          len(tasks),
			"planned_tasks":  o.PlannedTasks,
			"duration_ms":    time.Since(planStart).Milliseconds(),
			"max_in_flight":  o.MaxInFlightTasks,
			"target_file_mb": o.TargetFileBytes / (1024 * 1024),
			"cursor_domain":  cv.Domain,
			"source_mode":    sourceMode,
			"query_hash":     o.QueryHash,
		})
		return run, tasks, nil

	case "cdc_stream":
		part := PartitionSpecCDCStream(o.Table, o.NormalizedSourceMode(), o.QueryHash)
		tasks := []db.TaskInsert{{
			ID:            newID(),
			RunID:         run.ID,
			TaskIndex:     1,
			PartitionSpec: part,
			Status:        "PENDING",
		}}
		admitted, err := startTasks(tasks)
		if err != nil {
			return db.Run{}, nil, err
		}
		if admitted {
			run.Status = "RUNNING"
		}
		emitPlanEvent(ctx, st, run.ID, "INFO", "planner cdc_stream done", map[string]any{
			"stage": "tasks_created",
			"tasks": 1,
		})
		return run, tasks, nil

	default:
		return db.Run{}, nil, fmt.Errorf("unknown partition_strategy %q", o.PartitionStrategy)
	}
}
