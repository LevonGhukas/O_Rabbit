package planner

import (
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

func TestAutoTuneCursorPlanWithDecisionAdaptiveRowFallback(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		rows                 int64
		wantTasks            int
		wantFallbackRowsTask int64
		wantFetchLimit       int
	}{
		{
			name:                 "small table 100k rows stays at one task",
			rows:                 100_000,
			wantTasks:            1,
			wantFallbackRowsTask: 200_000,
		},
		{
			name:                 "medium table 5m rows uses 500k fallback",
			rows:                 5_000_000,
			wantTasks:            10,
			wantFallbackRowsTask: 500_000,
		},
		{
			name:                 "large table 16.6m rows uses 1m fallback",
			rows:                 16_600_000,
			wantTasks:            17,
			wantFallbackRowsTask: 1_000_000,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
				AutoTune:           true,
				MaxInFlightTasks:   1,
				MinTasksMultiplier: 1,
				TargetRowsPerTask:  defaultTargetRowsPerTask,
			}, connectors.CursorDomainInt64, connectors.CursorStats{
				RowCount: tt.rows,
			}, false, 0)

			if got.PlannedTasks != tt.wantTasks {
				t.Fatalf("planned_tasks=%d want=%d", got.PlannedTasks, tt.wantTasks)
			}
			if decision.PlannedTasksByRows != tt.wantTasks {
				t.Fatalf("planned_tasks_by_rows=%d want=%d", decision.PlannedTasksByRows, tt.wantTasks)
			}
			if decision.PlannedTasksByBytes != 0 {
				t.Fatalf("planned_tasks_by_bytes=%d want=0", decision.PlannedTasksByBytes)
			}
			if decision.SelectedFallbackRowsPerTask != tt.wantFallbackRowsTask {
				t.Fatalf("selected_fallback_rows_per_task=%d want=%d", decision.SelectedFallbackRowsPerTask, tt.wantFallbackRowsTask)
			}
			if decision.SelectedReason != "adaptive_rows_fallback" {
				t.Fatalf("selected_reason=%q want %q", decision.SelectedReason, "adaptive_rows_fallback")
			}
		})
	}
}

func TestAutoTuneCursorPlanWithDecisionBytesBasedPlanningWins(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   1,
		MinTasksMultiplier: 1,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
		TargetFileBytes:    100_000_000,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount:   5_000_000,
		TableBytes: 500_000_000,
	}, false, 0)

	if got.PlannedTasks != 2 {
		t.Fatalf("planned_tasks=%d want=2", got.PlannedTasks)
	}
	if decision.PlannedTasksByBytes != 2 {
		t.Fatalf("planned_tasks_by_bytes=%d want=2", decision.PlannedTasksByBytes)
	}
	if decision.TaskTargetBytes != 400_000_000 || decision.FilesPerTask != 4 {
		t.Fatalf("task target=%d files_per_task=%d", decision.TaskTargetBytes, decision.FilesPerTask)
	}
	if decision.PlannedTasksByRows != 10 {
		t.Fatalf("planned_tasks_by_rows=%d want=10", decision.PlannedTasksByRows)
	}
	if decision.SelectedReason != "bytes_based" {
		t.Fatalf("selected_reason=%q want %q", decision.SelectedReason, "bytes_based")
	}
}

func TestManualFileSizePlanningInfersConcurrencyAndKeepsItIndependent(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           false,
		MinTasksMultiplier: 1,
		TargetFileBytes:    30 * 1024 * 1024,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount:   1_000_000,
		TableBytes: 480 * 1024 * 1024,
	}, true, 3)

	if decision.TaskTargetBytes != 120*1024*1024 || decision.FilesPerTask != 4 {
		t.Fatalf("task target=%d files_per_task=%d", decision.TaskTargetBytes, decision.FilesPerTask)
	}
	if got.PlannedTasks < 4 {
		t.Fatalf("planned_tasks=%d want at least bytes-derived count 4", got.PlannedTasks)
	}
	if got.MaxInFlightTasks != 3 || decision.SelectedMaxInFlightReason != "active_workers" {
		t.Fatalf("inferred concurrency=%d reason=%q", got.MaxInFlightTasks, decision.SelectedMaxInFlightReason)
	}
}

func TestAutoTuneCursorPlanWithDecisionPreservesCurrentOverrideContract(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   4,
		MinTasksMultiplier: 2,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
		PlannedTasks:       7,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 16_600_000,
	}, false, 6)

	if got.PlannedTasks != 7 {
		t.Fatalf("planned_tasks=%d want=7", got.PlannedTasks)
	}
	if decision.SelectedReason != "user_override" {
		t.Fatalf("selected_reason=%q want %q", decision.SelectedReason, "user_override")
	}
}

func TestAutoTuneCursorPlanWithDecisionMinClampStillApplies(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   4,
		MinTasksMultiplier: 2,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 100_000,
	}, false, 0)

	// Row fallback would choose one task, but the existing minTasks clamp raises it to 8.
	if decision.PlannedTasksByRows != 1 {
		t.Fatalf("planned_tasks_by_rows=%d want=1 before clamp", decision.PlannedTasksByRows)
	}
	if got.PlannedTasks != 8 {
		t.Fatalf("planned_tasks=%d want=8 after min clamp", got.PlannedTasks)
	}
}

func TestAutoTuneCursorPlanWithDecisionMaxClampStillApplies(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   1,
		MinTasksMultiplier: 1,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 200_000_000,
	}, false, 0)

	// Adaptive row fallback yields 200 tasks, but the existing maxTasks clamp lowers it to 128.
	if decision.PlannedTasksByRows != 200 {
		t.Fatalf("planned_tasks_by_rows=%d want=200 before clamp", decision.PlannedTasksByRows)
	}
	if got.PlannedTasks != 128 {
		t.Fatalf("planned_tasks=%d want=128 after max clamp", got.PlannedTasks)
	}
}

func TestAdaptiveFallbackRowsPerTaskPreservesExplicitNonDefaultOverride(t *testing.T) {
	t.Parallel()

	got := adaptiveFallbackRowsPerTask(300_000, 16_600_000)
	if got != 300_000 {
		t.Fatalf("adaptiveFallbackRowsPerTask explicit override=%d want=300000", got)
	}
}

func TestAutoTuneCursorPlanWithDecisionUsesActiveWorkersForFinalMaxInFlight(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   0,
		MinTasksMultiplier: 1,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 16_600_000,
	}, true, 6)

	if got.PlannedTasks != 17 {
		t.Fatalf("planned_tasks=%d want=17", got.PlannedTasks)
	}
	if got.MaxInFlightTasks != 6 {
		t.Fatalf("max_in_flight_tasks=%d want=6", got.MaxInFlightTasks)
	}
	if decision.MaxInFlightTasks != 6 {
		t.Fatalf("decision.max_in_flight_tasks=%d want=6", decision.MaxInFlightTasks)
	}
	if decision.ActiveWorkers != 6 {
		t.Fatalf("active_workers=%d want=6", decision.ActiveWorkers)
	}
	if decision.SelectedMaxInFlightReason != "active_workers" {
		t.Fatalf("selected_max_in_flight_reason=%q want=%q", decision.SelectedMaxInFlightReason, "active_workers")
	}
	if decision.PlanningMaxInFlightTasks <= 0 {
		t.Fatalf("planning_max_in_flight_tasks=%d want > 0", decision.PlanningMaxInFlightTasks)
	}
}

func TestAutoTuneCursorPlanWithDecisionCapsFinalMaxInFlightToPlannedTasks(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   0,
		MinTasksMultiplier: 1,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 16_600_000,
	}, true, 40)

	if got.PlannedTasks != 17 {
		t.Fatalf("planned_tasks=%d want=17", got.PlannedTasks)
	}
	if got.MaxInFlightTasks != 17 {
		t.Fatalf("max_in_flight_tasks=%d want=17", got.MaxInFlightTasks)
	}
	if decision.SelectedMaxInFlightReason != "active_workers" {
		t.Fatalf("selected_max_in_flight_reason=%q want=%q", decision.SelectedMaxInFlightReason, "active_workers")
	}
}

func TestAutoTuneCursorPlanWithDecisionPreservesExplicitMaxInFlight(t *testing.T) {
	t.Parallel()

	got, decision := autoTuneCursorPlanWithDecision(jobopts.Options{
		AutoTune:           true,
		MaxInFlightTasks:   4,
		MinTasksMultiplier: 1,
		TargetRowsPerTask:  defaultTargetRowsPerTask,
	}, connectors.CursorDomainInt64, connectors.CursorStats{
		RowCount: 16_600_000,
	}, false, 6)

	if got.MaxInFlightTasks != 4 {
		t.Fatalf("max_in_flight_tasks=%d want=4", got.MaxInFlightTasks)
	}
	if decision.SelectedMaxInFlightReason != "user_override" {
		t.Fatalf("selected_max_in_flight_reason=%q want=%q", decision.SelectedMaxInFlightReason, "user_override")
	}
}
