// internal/planner/planner.go
// this file contains the core planner logic for creating runs and tasks based on jobs and dataset state.

package planner

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"runtime"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/sysinfo"
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
		TS:         time.Now().UTC().Format(time.RFC3339Nano),
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

	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)

	endpoint, _ := tgtMeta["endpoint"].(string)
	region, _ := tgtMeta["region"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	forcePathStyle := true
	if v, ok := tgtMeta["force_path_style"].(bool); ok {
		forcePathStyle = v
	}
	prefix, _ := tgtMeta["prefix"].(string)

	localTarget := isLocalEndpoint(endpoint)

	// Compute dataset prefix (derived if metadata.prefix empty).
	basePrefix := dataset.Prefix(prefix, srcEngine, sourceDatasetName(job, opts))

	if strings.TrimSpace(bucket) == "" {
		// Can't read state without a bucket.
		return datasetState{}, false, basePrefix, localTarget, nil
	}
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	if region == "" {
		region = "us-east-1"
	}

	sec, err := crypto.Decrypt(k, tgtConn.SecretEncBlob, []byte(tgtConn.ID))
	if err != nil {
		return datasetState{}, false, basePrefix, localTarget, err
	}
	var tgtSecret map[string]any
	_ = json.Unmarshal(sec, &tgtSecret)
	accessKey, _ := tgtSecret["access_key_id"].(string)
	secretKey, _ := tgtSecret["secret_access_key"].(string)
	sessionToken, _ := tgtSecret["session_token"].(string)

	u, err := s3io.New(ctx, s3io.Config{
		Endpoint:        endpoint,
		Region:          region,
		Bucket:          bucket,
		ForcePathStyle:  forcePathStyle,
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
	cutoff := time.Now().UTC().Add(-activeWorkerHeartbeatWindow).Format(time.RFC3339Nano)
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
	var tgtMeta map[string]any
	_ = json.Unmarshal(tgtConn.MetadataJSON, &tgtMeta)
	targetPrefix, _ := tgtMeta["prefix"].(string)
	endpoint, _ := tgtMeta["endpoint"].(string)
	bucket, _ := tgtMeta["bucket"].(string)
	sourceName := sourceDatasetName(job, o)
	basePrefix := dataset.Prefix(targetPrefix, srcEngine, sourceName)
	datasetKey := dataset.StorageKey(endpoint, bucket, basePrefix)

	// Ensure a single active RUNNING run per job.
	// If a previous CLI run was interrupted, its run/tasks may still be RUNNING and can steal worker capacity.
	_, _ = st.FailRunningRunsForJob(ctx, job.ID, "superseded by new run")

	run = db.Run{
		ID:                     newID(),
		JobID:                  job.ID,
		DatasetKey:             datasetKey,
		Status:                 "PLANNING",
		CorrelationID:          newID(),
		StartedAt:              time.Now().UTC().Format(time.RFC3339Nano),
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
		msg := strings.TrimSpace(err.Error())
		if msg == "" {
			msg = "run planning failed"
		}
		failCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_ = st.UpdateRunStatus(failCtx, runIDForFailure, "FAILED", true, &msg)
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
		_ = persistTunedOptionsBestEffort(ctx, st, job, o)

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

		idx := basePart + 1
		var tasks []db.TaskInsert
		if cv.RangeCapable {
			tasks, err = buildOrderedCursorRangeTasks(run.ID, idx, o.Table, cursorColumn, cv.Domain, fromHWM, stats, o, snapshotCtx)
			if err != nil {
				return db.Run{}, nil, err
			}
		} else {
			part := partitionSpecSQLCursorSingle(o.Table, sourceMode, o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, cursorColumn, cv.Domain, fromHWM, strings.TrimSpace(fromHWM) != "", snapshotCtx)
			tasks = []db.TaskInsert{{ID: newID(), RunID: run.ID, TaskIndex: idx, PartitionSpec: part, Status: "PENDING"}}
		}
		if len(tasks) == 0 {
			part := partitionSpecSQLCursorSingle(o.Table, sourceMode, o.QueryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, cursorColumn, cv.Domain, fromHWM, strings.TrimSpace(fromHWM) != "", snapshotCtx)
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

// PartitionSpecSingle constructs a partition specification payload.
// It exists to keep worker partition contracts explicit and stable.
func PartitionSpecSingle(table, sourceMode, queryHash string) json.RawMessage {
	return PartitionSpecSingleWithRecordPath(table, sourceMode, queryHash, "")
}

// PartitionSpecSingleWithRecordPath constructs a single-task partition
// specification with optional JSON record selection metadata.
func PartitionSpecSingleWithRecordPath(table, sourceMode, queryHash, recordPath string) json.RawMessage {
	return PartitionSpecSingleWithFileOptions(table, sourceMode, queryHash, recordPath, "")
}

// PartitionSpecSingleWithFileOptions constructs a single-task partition
// specification with optional file-source metadata.
func PartitionSpecSingleWithFileOptions(table, sourceMode, queryHash, recordPath, fileFormat string) json.RawMessage {
	part := map[string]any{
		"type":        "single",
		"source_mode": sourceMode,
		"table":       table,
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(recordPath) != "" {
		part["record_path"] = strings.TrimSpace(recordPath)
	}
	if strings.TrimSpace(fileFormat) != "" {
		part["format"] = strings.TrimSpace(fileFormat)
	}
	b, _ := json.Marshal(part)
	return json.RawMessage(b)
}

func PartitionSpecCDCStream(table, sourceMode, queryHash string) json.RawMessage {
	part := map[string]any{
		"type":        "cdc_stream",
		"source_mode": sourceMode,
		"table":       table,
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	b, _ := json.Marshal(part)
	return b
}

// PartitionSpecSQLCursorRange constructs a partition specification payload for a bounded ordered-cursor task.
func PartitionSpecSQLCursorRange(table, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, upper string, upperInclusive bool, outputPart int, snapshotCtx string) json.RawMessage {
	return partitionSpecSQLCursorRange(table, "table", "", "", nil, nil, column, domain, lower, lowerExclusive, upper, upperInclusive, outputPart, snapshotCtx)
}

func partitionSpecSQLCursorRange(table, sourceMode, queryHash, whereClause string, selectColumns []string, columnTypes map[string]string, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, upper string, upperInclusive bool, outputPart int, snapshotCtx string) json.RawMessage {
	part := map[string]any{
		"type":            "sql_cursor_range",
		"source_mode":     sourceMode,
		"table":           table,
		"cursor_column":   column,
		"cursor_domain":   domain,
		"output_part":     outputPart,
		"lower":           strings.TrimSpace(lower),
		"lower_exclusive": lowerExclusive,
		"upper":           strings.TrimSpace(upper),
		"upper_inclusive": upperInclusive,
	}
	if strings.TrimSpace(whereClause) != "" {
		part["where_clause"] = strings.TrimSpace(whereClause)
	}
	if len(selectColumns) > 0 {
		part["select_columns"] = selectColumns
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	if len(selectColumns) > 0 {
		part["select_columns"] = selectColumns
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(snapshotCtx) != "" {
		part["snapshot_context"] = strings.TrimSpace(snapshotCtx)
	}
	b, _ := json.Marshal(part)
	return b
}

// PartitionSpecSQLCursorSingle constructs a partition specification payload for a single ordered-cursor task.
func PartitionSpecSQLCursorSingle(table, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, snapshotCtx string) json.RawMessage {
	return partitionSpecSQLCursorSingle(table, "table", "", "", nil, nil, column, domain, lower, lowerExclusive, snapshotCtx)
}

func partitionSpecSQLCursorSingle(table, sourceMode, queryHash, whereClause string, selectColumns []string, columnTypes map[string]string, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, snapshotCtx string) json.RawMessage {
	part := map[string]any{
		"type":            "sql_cursor_single",
		"source_mode":     sourceMode,
		"table":           table,
		"cursor_column":   column,
		"cursor_domain":   domain,
		"lower":           strings.TrimSpace(lower),
		"lower_exclusive": lowerExclusive,
	}
	if strings.TrimSpace(whereClause) != "" {
		part["where_clause"] = strings.TrimSpace(whereClause)
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(snapshotCtx) != "" {
		part["snapshot_context"] = strings.TrimSpace(snapshotCtx)
	}
	b, _ := json.Marshal(part)
	return b
}

const (
	defaultTargetRowsPerTask  int64 = 200_000
	mediumFallbackRowsPerTask int64 = 500_000
	largeFallbackRowsPerTask  int64 = 1_000_000
	smallTableRowsThreshold   int64 = 1_000_000
	mediumTableRowsThreshold  int64 = 10_000_000
	defaultTargetFileBytes    int64 = 256 * 1024 * 1024
	filesPerPlannedTask             = 4
)

type autoTuneDecision struct {
	EstimatedRows               int64
	ActiveWorkers               int
	TableBytes                  int64
	TargetFileBytes             int64
	TaskTargetBytes             int64
	FilesPerTask                int
	TargetRowsPerTask           int64
	SelectedFallbackRowsPerTask int64
	PlannedTasksByBytes         int
	PlannedTasksByRows          int
	FinalPlannedTasks           int
	PlanningMaxInFlightTasks    int
	EffectiveMinTaskConcurrency int
	MaxInFlightTasks            int
	MinimumTasks                int64
	SelectedMaxInFlightReason   string
	SelectedReason              string
}

func autoTuneCursorPlanWithDecision(o jobopts.Options, domain connectors.CursorDomain, st connectors.CursorStats, localTarget bool, activeWorkers int) (jobopts.Options, autoTuneDecision) {
	return autoTuneCursorPlanWithDecisionUsingHeuristic(o, domain, st, localTarget, activeWorkers, heuristicMaxInFlightTasks)
}

// autoTuneCursorPlanWithDecisionUsingHeuristic keeps the host heuristic injectable
// for deterministic planner tests. Production always uses heuristicMaxInFlightTasks.
func autoTuneCursorPlanWithDecisionUsingHeuristic(o jobopts.Options, domain connectors.CursorDomain, st connectors.CursorStats, localTarget bool, activeWorkers int, inferredMaxInFlight func(connectors.CursorStats, bool) int) (jobopts.Options, autoTuneDecision) {
	// Resolved values remain in job options for current-run scheduling, but are
	// marked so a later run recalculates them from current source statistics and
	// target-file policy instead of mistaking them for caller overrides.
	if o.PlannedTasksWasInferred() {
		o.PlannedTasks = 0
	}
	if o.MaxInFlightTasksWasInferred() {
		o.MaxInFlightTasks = 0
	}
	explicitMaxInFlight := o.MaxInFlightTasks > 0
	explicitPlannedTasks := o.PlannedTasks > 0
	decision := autoTuneDecision{
		EstimatedRows:     st.RowCount,
		ActiveWorkers:     activeWorkers,
		TableBytes:        st.TableBytes,
		TargetFileBytes:   o.TargetFileBytes,
		TargetRowsPerTask: o.TargetRowsPerTask,
		MaxInFlightTasks:  o.MaxInFlightTasks,
	}
	// Concurrency cap (deterministic).
	// NOTE: MaxInFlightTasks controls *end-to-end* task concurrency (DB read + convert + upload).
	// On local laptop stacks (SQL source + MinIO in Docker/Colima), high concurrency is often slower and less stable.
	if o.MaxInFlightTasks <= 0 {
		o.MaxInFlightTasks = inferredMaxInFlight(st, localTarget)
		decision.SelectedMaxInFlightReason = "host_heuristic"
	} else {
		decision.SelectedMaxInFlightReason = "user_override"
	}
	decision.PlanningMaxInFlightTasks = o.MaxInFlightTasks
	decision.MaxInFlightTasks = o.MaxInFlightTasks

	// Only inferred concurrency is constrained by the workers presently available.
	// Keep the host-derived value for target-file policy and the max-task cap, but
	// use the effective scheduler concurrency for the minimum task lower bound.
	// An explicit user concurrency value remains authoritative.
	effectiveMinTaskConcurrency := o.MaxInFlightTasks
	if !explicitMaxInFlight && activeWorkers > 0 {
		effectiveMinTaskConcurrency = minInt(effectiveMinTaskConcurrency, activeWorkers)
	}
	decision.EffectiveMinTaskConcurrency = effectiveMinTaskConcurrency

	// PlannedTasks controls independent leased source ranges. It is a distinct
	// advanced override from MaxInFlightTasks (scheduler concurrency) and from
	// TargetFileBytes (physical Parquet file goal).
	if explicitPlannedTasks {
		decision.SelectedReason = "user_override"
		decision.FinalPlannedTasks = o.PlannedTasks
		decision.TargetFileBytes = o.TargetFileBytes
		o.PlannedTasksSource = jobopts.PerformanceValueSourceExplicit
		if explicitMaxInFlight {
			o.MaxInFlightTasksSource = jobopts.PerformanceValueSourceExplicit
		} else {
			o.MaxInFlightTasksSource = jobopts.PerformanceValueSourceInferred
		}
		return o, decision
	}

	// ---- NEW: estimate bytes per row and total bytes to export ----
	var bytesPerRow int64 = 0
	if st.TableBytes > 0 && st.RowCount > 0 {
		// allocated bytes / rows is crude but good enough for tuning
		bpr := st.TableBytes / st.RowCount
		if bpr > 0 {
			bytesPerRow = bpr
		}
	}

	estRows := st.RowCount
	var estBytes int64 = 0
	if bytesPerRow > 0 && estRows > 0 {
		// avoid overflow paranoia: clamp
		if bytesPerRow > 0 && estRows > 0 {
			if estRows > (math.MaxInt64 / bytesPerRow) {
				estBytes = math.MaxInt64
			} else {
				estBytes = bytesPerRow * estRows
			}
		}
	}

	// Decide planned task count.
	targetRowsPerTask := o.TargetRowsPerTask
	if targetRowsPerTask <= 0 {
		targetRowsPerTask = defaultTargetRowsPerTask
	}
	decision.TargetRowsPerTask = targetRowsPerTask
	selectedFallbackRowsPerTask := adaptiveFallbackRowsPerTask(targetRowsPerTask, estRows)
	decision.SelectedFallbackRowsPerTask = selectedFallbackRowsPerTask
	targetFileBytes := o.TargetFileBytes
	if targetFileBytes <= 0 {
		targetFileBytes = defaultTargetFileBytes
	}
	// Prefer fewer, larger files when concurrency is low; prefer smaller files
	// when concurrency is high. This keeps throughput good across single-node and
	// multi-worker setups without hard-coding environment-specific defaults.
	if targetFileBytes == defaultTargetFileBytes && st.TableBytes > 0 {
		const gib = 1024 * 1024 * 1024
		// Scale desired part size by planned end-to-end concurrency.
		switch {
		case o.MaxInFlightTasks <= 2:
			if st.TableBytes <= 8*gib {
				targetFileBytes = 1 * gib
			} else {
				targetFileBytes = 512 * 1024 * 1024
			}
		case o.MaxInFlightTasks <= 8:
			targetFileBytes = 512 * 1024 * 1024
		default:
			// Keep the default 256MiB target for high parallelism.
			targetFileBytes = defaultTargetFileBytes
		}
		// Persist so the CLI prints the effective plan.
		o.TargetFileBytes = targetFileBytes
	}
	decision.TargetFileBytes = targetFileBytes
	taskTargetBytes := targetFileBytes * filesPerPlannedTask
	if taskTargetBytes <= 0 || taskTargetBytes/filesPerPlannedTask != targetFileBytes {
		taskTargetBytes = targetFileBytes
	}
	decision.TaskTargetBytes = taskTargetBytes
	decision.FilesPerTask = filesPerPlannedTask

	minTasks := int64(effectiveMinTaskConcurrency) * int64(o.MinTasksMultiplier)
	if minTasks < int64(effectiveMinTaskConcurrency) {
		minTasks = int64(effectiveMinTaskConcurrency)
	}
	if minTasks < 1 {
		minTasks = 1
	}
	decision.MinimumTasks = minTasks

	var tasks int64 = 0

	// Primary: bytes-based
	if estBytes > 0 {
		tasks = int64(math.Ceil(float64(estBytes) / float64(taskTargetBytes)))
		decision.PlannedTasksByBytes = int(tasks)
	}

	// Fallback: rows-based
	if estRows > 0 && selectedFallbackRowsPerTask > 0 {
		decision.PlannedTasksByRows = int(math.Ceil(float64(estRows) / float64(selectedFallbackRowsPerTask)))
	}
	if tasks <= 0 && estRows > 0 && selectedFallbackRowsPerTask > 0 {
		tasks = int64(decision.PlannedTasksByRows)
		decision.SelectedReason = "adaptive_rows_fallback"
	}

	// Final fallback
	if tasks < 1 {
		tasks = minTasks
		decision.SelectedReason = "min_tasks_fallback"
	}

	if decision.SelectedReason == "" && decision.PlannedTasksByBytes > 0 {
		decision.SelectedReason = "bytes_based"
	}

	// Ensure enough tasks to keep workers busy
	if tasks < minTasks {
		tasks = minTasks
	}

	// Hard cap to avoid millions of tasks
	maxTasks := int64(o.MaxInFlightTasks * 64)
	if maxTasks < 128 {
		maxTasks = 128
	}
	if maxTasks > 4096 {
		maxTasks = 4096
	}
	if tasks > maxTasks {
		tasks = maxTasks
	}

	if connectors.SupportsCursorRangeSplit(domain) {
		o.PlannedTasks = int(tasks)
		if o.PlannedTasks < 1 {
			o.PlannedTasks = 1
		}
	} else {
		o.PlannedTasks = 1
	}

	// Keep target_file_bytes behavior stable by computing it against the existing
	// heuristic. Once the final task count is known, align scheduler concurrency
	// to active workers when the caller did not set max_in_flight_tasks explicitly.
	if !explicitMaxInFlight && activeWorkers > 0 && o.PlannedTasks > 0 {
		o.MaxInFlightTasks = minInt(o.PlannedTasks, activeWorkers)
		decision.SelectedMaxInFlightReason = "active_workers"
	}

	decision.FinalPlannedTasks = o.PlannedTasks
	decision.MaxInFlightTasks = o.MaxInFlightTasks
	o.PlannedTasksSource = jobopts.PerformanceValueSourceInferred
	if explicitMaxInFlight {
		o.MaxInFlightTasksSource = jobopts.PerformanceValueSourceExplicit
	} else {
		o.MaxInFlightTasksSource = jobopts.PerformanceValueSourceInferred
	}

	return o, decision
}

func heuristicMaxInFlightTasks(st connectors.CursorStats, localTarget bool) int {
	cpuCap := runtime.NumCPU()
	if cpuCap < 2 {
		cpuCap = 2
	}
	ramCap := 32
	if memBytes, ok := sysinfo.TotalMemoryBytes(); ok {
		const perTask = 1536 * 1024 * 1024 // ~1.5GiB
		ramCap = int(memBytes / perTask)
		if ramCap < 2 {
			ramCap = 2
		}
	}
	n := cpuCap
	if ramCap < n {
		n = ramCap
	}
	if n > 32 {
		n = 32
	}
	// Extra safety cap for single-host/local endpoints.
	// When the DB and object store are on the same machine/VM, very high concurrency
	// can reduce throughput (disk contention, page cache churn). Keep it moderate
	// unless the machine has plenty of memory.
	if localTarget || st.SourceIsLocal {
		if memBytes, ok := sysinfo.TotalMemoryBytes(); ok {
			const gib = 1024 * 1024 * 1024
			switch {
			case memBytes <= 12*gib:
				if n > 2 {
					n = 2
				}
			case memBytes <= 24*gib:
				if n > 4 {
					n = 4
				}
			case memBytes <= 64*gib:
				if n > 8 {
					n = 8
				}
			default:
				if n > 16 {
					n = 16
				}
			}
		} else if n > 8 {
			n = 8
		}
	}
	return n
}

func adaptiveFallbackRowsPerTask(targetRowsPerTask, estRows int64) int64 {
	if targetRowsPerTask <= 0 {
		targetRowsPerTask = defaultTargetRowsPerTask
	}
	// Preserve explicit non-default target_rows_per_task overrides.
	if targetRowsPerTask != defaultTargetRowsPerTask {
		return targetRowsPerTask
	}
	switch {
	case estRows <= 0:
		return targetRowsPerTask
	case estRows <= smallTableRowsThreshold:
		return defaultTargetRowsPerTask
	case estRows <= mediumTableRowsThreshold:
		return mediumFallbackRowsPerTask
	default:
		return largeFallbackRowsPerTask
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func buildOrderedCursorRangeTasks(runID string, baseIndex int, table, column string, domain connectors.CursorDomain, fromHWM string, stats connectors.CursorStats, o jobopts.Options, snapshotCtx string) ([]db.TaskInsert, error) {
	maxValue := strings.TrimSpace(stats.MaxValue)
	minValue := strings.TrimSpace(stats.MinValue)
	if maxValue == "" || minValue == "" {
		return nil, nil
	}

	startInclusive := minValue
	if strings.TrimSpace(fromHWM) != "" {
		next, ok := connectors.CursorSuccessor(domain, fromHWM)
		if !ok {
			return nil, nil
		}
		startInclusive = next
	}
	if startInclusive == "" || connectors.CompareCursorValues(domain, startInclusive, maxValue) > 0 {
		return nil, nil
	}

	plannedTasks := o.PlannedTasks
	if plannedTasks <= 0 {
		plannedTasks = 1
		if o.ChunkSize > 0 {
			if span, ok := connectors.ClosedCursorSpanUnits(domain, startInclusive, maxValue); ok && span > 0 {
				plannedTasks = int(math.Ceil(float64(span) / float64(o.ChunkSize)))
			}
		}
	}
	if plannedTasks < 1 {
		plannedTasks = 1
	}

	uppers, err := connectors.SplitCursorRange(domain, startInclusive, maxValue, plannedTasks)
	if err != nil {
		return nil, err
	}
	lower := strings.TrimSpace(fromHWM)
	lowerExclusive := lower != ""
	idx := baseIndex
	sourceMode := o.NormalizedSourceMode()
	queryHash := strings.TrimSpace(o.QueryHash)
	tasks := make([]db.TaskInsert, 0, len(uppers))
	for _, upper := range uppers {
		part := partitionSpecSQLCursorRange(table, sourceMode, queryHash, o.WhereClause, o.SelectColumns, o.ColumnTypes, column, domain, lower, lowerExclusive, upper, true, idx, snapshotCtx)
		tasks = append(tasks, db.TaskInsert{
			ID:            newID(),
			RunID:         runID,
			TaskIndex:     idx,
			PartitionSpec: part,
			Status:        "PENDING",
		})
		lower = upper
		lowerExclusive = true
		idx++
	}
	return tasks, nil
}

// persistTunedOptionsBestEffort handles persist tuned options best effort behavior.
// It exists to keep this logic isolated and reusable.
func persistTunedOptionsBestEffort(ctx context.Context, st *db.Store, job db.Job, o jobopts.Options) error {
	var m map[string]any
	_ = json.Unmarshal(job.OptionsJSON, &m)
	m = o.MergeInto(m)

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	job.OptionsJSON = b
	return st.UpdateJob(ctx, job)
}

type cursorValidator interface {
	ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (connectors.CursorColumnValidation, error)
}

type tableDescriber interface {
	DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error)
}

type cursorStatDiscoverer interface {
	DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain connectors.CursorDomain) (connectors.CursorStats, error)
}

// discoverCursorStats handles source statistics discovery for ordered-cursor capable engines.
func discoverCursorStats(ctx context.Context, r cursorStatDiscoverer, sourceEngine, table, cursorCol string, domain connectors.CursorDomain) (connectors.CursorStats, error) {
	stats, err := r.DiscoverCursorStats(ctx, table, cursorCol, domain)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query timed out (engine=%s table=%s cursor_column=%s deadline=%s): %w", sourceEngine, table, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query was canceled (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
		}
		return connectors.CursorStats{}, fmt.Errorf("ordered-cursor stats query failed (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
	}
	return stats, nil
}

func discoverQueryCursorStats(ctx context.Context, r connectors.SourceQueryReader, sourceEngine, query, cursorCol string, domain connectors.CursorDomain, queryHash string) (connectors.CursorStats, error) {
	stats, err := r.DiscoverQueryCursorStats(ctx, query, cursorCol, domain)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorStats{}, fmt.Errorf("query-mode stats query timed out (engine=%s query_hash=%s cursor_column=%s deadline=%s): %w", sourceEngine, queryHash, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorStats{}, fmt.Errorf("query-mode stats query was canceled (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
		}
		return connectors.CursorStats{}, fmt.Errorf("query-mode stats query failed (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
	}
	return stats, nil
}

func validateCursorColumn(ctx context.Context, r cursorValidator, sourceEngine, table, cursorCol string) (connectors.CursorColumnValidation, error) {
	cv, err := r.ValidateCursorColumn(ctx, table, cursorCol)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation timed out (engine=%s table=%s cursor_column=%s deadline=%s): %w", sourceEngine, table, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation was canceled (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
		}
		return connectors.CursorColumnValidation{}, fmt.Errorf("ordered-cursor column validation failed (engine=%s table=%s cursor_column=%s): %w", sourceEngine, table, cursorCol, err)
	}
	return cv, nil
}

func validateQueryCursorColumn(ctx context.Context, r connectors.SourceQueryReader, sourceEngine, query, cursorCol, queryHash string) (connectors.CursorColumnValidation, error) {
	cv, err := r.ValidateQueryCursorColumn(ctx, query, cursorCol)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation timed out (engine=%s query_hash=%s cursor_column=%s deadline=%s): %w", sourceEngine, queryHash, cursorCol, ctxDeadlineRFC3339(ctx), err)
		}
		if errors.Is(err, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
			return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation was canceled (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
		}
		return connectors.CursorColumnValidation{}, fmt.Errorf("query-mode column validation failed (engine=%s query_hash=%s cursor_column=%s): %w", sourceEngine, queryHash, cursorCol, err)
	}
	return cv, nil
}

func openCursorReader(ctx context.Context, st *db.Store, k crypto.Key, connID, sourceEngine string) (connectors.TableReader, error) {
	src, err := st.GetConnection(ctx, connID)
	if err != nil {
		return nil, err
	}
	sec, err := crypto.Decrypt(k, src.SecretEncBlob, []byte(src.ID))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(sec, &m); err != nil {
		return nil, err
	}
	dsn, _ := m["dsn"].(string)
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("source connection secret missing dsn")
	}
	r, err := connectors.OpenCursorReader(ctx, sourceEngine, dsn)
	if err != nil {
		return nil, fmt.Errorf("open source reader (engine=%s): %w", sourceEngine, err)
	}
	return r, nil
}

func openDocumentReader(ctx context.Context, st *db.Store, k crypto.Key, connID, sourceEngine string) (connectors.DocumentReader, error) {
	src, err := st.GetConnection(ctx, connID)
	if err != nil {
		return nil, err
	}
	sec, err := crypto.Decrypt(k, src.SecretEncBlob, []byte(src.ID))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(sec, &m); err != nil {
		return nil, err
	}
	dsn, _ := m["dsn"].(string)
	if strings.TrimSpace(dsn) == "" {
		return nil, fmt.Errorf("source connection secret missing dsn")
	}
	r, err := connectors.OpenDocumentReader(ctx, sourceEngine, dsn)
	if err != nil {
		return nil, fmt.Errorf("open source reader (engine=%s): %w", sourceEngine, err)
	}
	return r, nil
}

func ctxDeadlineRFC3339(ctx context.Context) string {
	if d, ok := ctx.Deadline(); ok {
		return d.UTC().Format(time.RFC3339Nano)
	}
	return "none"
}
