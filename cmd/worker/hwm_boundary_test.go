package main

import (
	"encoding/json"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

func TestPartitionSpecHWMBoundaryKeysDriveTheRowFilter(t *testing.T) {
	hwm := "2026-01-01T00:00:03Z"
	exported := []any{int64(1), hwm}
	raw, err := json.Marshal(map[string]any{
		"type":                   "sql_cursor_range",
		"cursor_domain":          "timestamp",
		"lower":                  hwm,
		"lower_exclusive":        false,
		hwmboundary.SpecTrackKey: true,
		hwmboundary.SpecSkipKey:  hwmboundary.Skip{CursorValue: hwm, RowHashes: map[string]int64{hwmboundary.RowHash(exported): 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var ps partitionSpec
	if err := json.Unmarshal(raw, &ps); err != nil {
		t.Fatal(err)
	}
	tracker, keep := newHWMBoundaryFilter(ps)
	if tracker == nil || keep == nil {
		t.Fatalf("spec %s did not enable boundary tracking", raw)
	}
	if keep(hwm, true, exported) {
		t.Fatal("row exported by the previous run was kept")
	}
	if !keep(hwm, true, []any{int64(2), hwm}) {
		t.Fatal("late row at the HWM was dropped")
	}
	got := hwmBoundaryProto(boundaryPtr(tracker.Boundary()))
	if got.CursorValue != hwm || len(got.RowHashes) != 2 || got.Overflow {
		t.Fatalf("reported boundary=%+v", got)
	}
}

func TestPartitionSpecWithoutHWMBoundaryHasNoFilter(t *testing.T) {
	var ps partitionSpec
	if err := json.Unmarshal([]byte(`{"type":"sql_cursor_single","lower":"5","lower_exclusive":true}`), &ps); err != nil {
		t.Fatal(err)
	}
	if tracker, keep := newHWMBoundaryFilter(ps); tracker != nil || keep != nil {
		t.Fatal("untracked spec got a row filter")
	}
	if hwmBoundaryProto(nil) != nil {
		t.Fatal("untracked task must not report a boundary")
	}
}

func boundaryPtr(b hwmboundary.Boundary) *hwmboundary.Boundary { return &b }
