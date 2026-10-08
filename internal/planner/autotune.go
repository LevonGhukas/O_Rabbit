package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/sysinfo"
)

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

// buildOrderedCursorRangeTasks splits (lower, max] into range tasks, or
// [lower, max] when lowerExclusive is false. An empty lower starts at the
// source minimum.
func buildOrderedCursorRangeTasks(runID string, baseIndex int, table, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, stats connectors.CursorStats, o jobopts.Options, snapshotCtx string) ([]db.TaskInsert, error) {
	maxValue := strings.TrimSpace(stats.MaxValue)
	minValue := strings.TrimSpace(stats.MinValue)
	if maxValue == "" || minValue == "" {
		return nil, nil
	}

	lower = strings.TrimSpace(lower)
	startInclusive := minValue
	if lower != "" {
		startInclusive = lower
		if lowerExclusive {
			next, ok := connectors.CursorSuccessor(domain, lower)
			if !ok {
				return nil, nil
			}
			startInclusive = next
		}
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
	if err := json.Unmarshal(job.OptionsJSON, &m); err != nil {
		// Never overwrite options that could not be read.
		return fmt.Errorf("job options are not a JSON object: %w", err)
	}
	m = o.MergeInto(m)

	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	job.OptionsJSON = b
	return st.UpdateJob(ctx, job)
}
