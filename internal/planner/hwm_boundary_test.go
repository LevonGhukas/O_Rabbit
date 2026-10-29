package planner

import (
	"encoding/json"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

const tsDomain = connectors.CursorDomainTimestamp

func TestPlanHWMResumeReadsRowsAtTheMarkAgain(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	state := &hwmboundary.Boundary{CursorValue: hwm, RowHashes: map[string]int64{"aa": 1}}
	r, err := planHWMResume(tsDomain, hwm, "", state)
	if err != nil {
		t.Fatal(err)
	}
	if r.lower != hwm || r.lowerExclusive {
		t.Fatalf("resume lower=%q exclusive=%v, want inclusive at the HWM", r.lower, r.lowerExclusive)
	}
	if r.skip == nil || r.skip.Seed || r.skip.RowHashes["aa"] != 1 || r.eventLevel != "INFO" {
		t.Fatalf("resume=%+v, want the stored rows as skip set", r)
	}
}

func TestPlanHWMResumeSeedsLegacyState(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	r, err := planHWMResume(tsDomain, hwm, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if r.lowerExclusive || r.skip == nil || !r.skip.Seed {
		t.Fatalf("resume=%+v, want an inclusive seed run", r)
	}
}

func TestPlanHWMResumeFallsBackToExclusiveOnOverflow(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	r, err := planHWMResume(tsDomain, hwm, "", &hwmboundary.Boundary{CursorValue: hwm, Overflow: true})
	if err != nil {
		t.Fatal(err)
	}
	if !r.lowerExclusive || r.skip != nil || r.eventLevel != "WARN" || r.eventReason == "" {
		t.Fatalf("resume=%+v, want an exclusive resume with a warning", r)
	}
}

func TestPlanHWMResumeKeepsLookbackAndFreshAndUnknownDomain(t *testing.T) {
	r, err := planHWMResume(connectors.CursorDomainInt64, "1000", "250", &hwmboundary.Boundary{CursorValue: "1000", RowHashes: map[string]int64{"aa": 1}})
	if err != nil || r.lower != "750" || r.lowerExclusive || r.skip != nil {
		t.Fatalf("lookback resume=%+v err=%v", r, err)
	}
	if r, err := planHWMResume(connectors.CursorDomainInt64, "", "", nil); err != nil || r.lower != "" || r.lowerExclusive || r.skip != nil {
		t.Fatalf("first run resume=%+v err=%v", r, err)
	}
	if r, err := planHWMResume(connectors.CursorDomainUnknown, "x", "", nil); err != nil || r.lower != "x" || !r.lowerExclusive || r.skip != nil {
		t.Fatalf("unknown domain resume=%+v err=%v", r, err)
	}
	if _, err := planHWMResume(connectors.CursorDomainInt64, "1000", "bad", nil); err == nil {
		t.Fatal("invalid lookback accepted")
	}
}

func TestWithHWMBoundarySpecMarksTasksAndSkipsOnlyInTheFirst(t *testing.T) {
	stats := connectors.CursorStats{MinValue: "1", MaxValue: "1000"}
	tasks, err := buildOrderedCursorRangeTasks("run", 1, "t", "id", connectors.CursorDomainInt64, "900", false, stats, jobopts.Options{PlannedTasks: 2}, "")
	if err != nil || len(tasks) != 2 {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	original := string(tasks[0].PartitionSpec)
	skip := &hwmboundary.Skip{CursorValue: "900", RowHashes: map[string]int64{"aa": 1}}
	marked, err := withHWMBoundarySpec(tasks, skip)
	if err != nil {
		t.Fatal(err)
	}
	if string(tasks[0].PartitionSpec) != original {
		t.Fatal("input tasks were modified")
	}
	for i, task := range marked {
		var spec map[string]json.RawMessage
		if err := json.Unmarshal(task.PartitionSpec, &spec); err != nil {
			t.Fatal(err)
		}
		if string(spec[hwmboundary.SpecTrackKey]) != "true" {
			t.Fatalf("task %d spec=%s, want boundary tracking", i, task.PartitionSpec)
		}
		_, hasSkip := spec[hwmboundary.SpecSkipKey]
		if hasSkip != (i == 0) {
			t.Fatalf("task %d skip present=%v", i, hasSkip)
		}
		if string(spec["lower"]) == "" {
			t.Fatalf("task %d lost its range: %s", i, task.PartitionSpec)
		}
	}
	var first struct {
		Skip *hwmboundary.Skip `json:"hwm_boundary_skip"`
	}
	if err := json.Unmarshal(marked[0].PartitionSpec, &first); err != nil || first.Skip == nil || first.Skip.RowHashes["aa"] != 1 {
		t.Fatalf("first task skip=%+v err=%v", first.Skip, err)
	}
}

func TestWithHWMBoundarySpecWithoutSkipStillTracks(t *testing.T) {
	tasks := []db.TaskInsert{{ID: "a", PartitionSpec: json.RawMessage(`{"type":"sql_cursor_single"}`)}}
	marked, err := withHWMBoundarySpec(tasks, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(marked[0].PartitionSpec); got != `{"track_hwm_boundary":true,"type":"sql_cursor_single"}` {
		t.Fatalf("spec=%s", got)
	}
}

func TestInclusiveResumeAtMaxPlansOneTask(t *testing.T) {
	// No row above the mark: the run still re-reads the rows at the mark.
	stats := connectors.CursorStats{MinValue: "1", MaxValue: "900"}
	tasks, err := buildOrderedCursorRangeTasks("run", 1, "t", "id", connectors.CursorDomainInt64, "900", false, stats, jobopts.Options{PlannedTasks: 4}, "")
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks=%v err=%v", tasks, err)
	}
	var spec map[string]any
	if err := json.Unmarshal(tasks[0].PartitionSpec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec["lower"] != "900" || spec["lower_exclusive"] != false || spec["upper"] != "900" || spec["upper_inclusive"] != true {
		t.Fatalf("spec=%v, want [900, 900]", spec)
	}
}

func TestDatasetStateReadsTheCommittedBoundary(t *testing.T) {
	committed, err := json.Marshal(map[string]any{
		"max_hwm_value":      "42",
		hwmboundary.StateKey: hwmboundary.Boundary{CursorValue: "42", RowHashes: map[string]int64{"aa": 2}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ds datasetState
	if err := json.Unmarshal(committed, &ds); err != nil {
		t.Fatal(err)
	}
	if ds.HWMBoundary == nil || ds.HWMBoundary.CursorValue != "42" || ds.HWMBoundary.RowHashes["aa"] != 2 {
		t.Fatalf("planner read boundary %+v from %s", ds.HWMBoundary, committed)
	}
}
