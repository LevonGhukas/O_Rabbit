package planner

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/partitionspec"
)

const testSnapshotID = "00000003-0000001B-1"

type fakeSnapshotExporter struct {
	snapshot string
	err      error
	calls    int
}

func (f *fakeSnapshotExporter) ExportSnapshot(context.Context) (string, error) {
	f.calls++
	return f.snapshot, f.err
}

func TestPlannedSpecsCarrySnapshotToWorkerCursorQuery(t *testing.T) {
	specs := map[string][]byte{
		"range":  partitionSpecSQLCursorRange("orders", "table", "", "", nil, nil, "id", connectors.CursorDomainInt64, "0", true, "9", true, 1, testSnapshotID),
		"single": partitionSpecSQLCursorSingle("orders", "table", "", "", nil, nil, "id", connectors.CursorDomainInt64, "0", true, testSnapshotID),
	}
	for name, raw := range specs {
		ps, err := partitionspec.Parse(raw)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := ps.CursorQuery("").SnapshotContext; got != testSnapshotID {
			t.Fatalf("%s: worker SnapshotContext=%q, want %q", name, got, testSnapshotID)
		}
	}
}

func TestEveryPlannedTaskReadsTheRunSnapshot(t *testing.T) {
	stats := connectors.CursorStats{MinValue: "1", MaxValue: "1000"}
	o := jobopts.Options{PlannedTasks: 4, FetchSize: 500}
	tasks, err := buildOrderedCursorRangeTasks("run", 1, "t", "id", connectors.CursorDomainInt64, "900", false, stats, o, testSnapshotID)
	if err != nil || len(tasks) != 4 {
		t.Fatalf("tasks=%d err=%v", len(tasks), err)
	}
	tasks, err = withHWMBoundarySpec(tasks, &hwmboundary.Skip{CursorValue: "900", RowHashes: map[string]int64{"aa": 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		ps, err := partitionspec.Parse(task.PartitionSpec)
		if err != nil {
			t.Fatal(err)
		}
		if got := ps.CursorQuery("").SnapshotContext; got != testSnapshotID {
			t.Fatalf("task %d: SnapshotContext=%q in %s", task.TaskIndex, got, task.PartitionSpec)
		}
	}
}

func TestExportRunSnapshotOnlyForStrongSnapshot(t *testing.T) {
	exporter := &fakeSnapshotExporter{snapshot: testSnapshotID}
	for _, mode := range []string{"", "EVENTUAL"} {
		got, err := exportRunSnapshot(context.Background(), exporter, "postgres", "table", mode)
		if err != nil || got != "" {
			t.Fatalf("mode %q: snapshot=%q err=%v", mode, got, err)
		}
	}
	if exporter.calls != 0 {
		t.Fatalf("exporter called %d times without STRONG_SNAPSHOT", exporter.calls)
	}
}

func TestExportRunSnapshotReturnsExportedSnapshot(t *testing.T) {
	for _, mode := range []string{"table", "query"} {
		got, err := exportRunSnapshot(context.Background(), &fakeSnapshotExporter{snapshot: " " + testSnapshotID + " "}, "postgres", mode, jobopts.ConsistencyStrongSnapshot)
		if err != nil || got != testSnapshotID {
			t.Fatalf("%s: snapshot=%q err=%v", mode, got, err)
		}
	}
}

func TestExportRunSnapshotRejectsUnsupportedSources(t *testing.T) {
	cases := map[string]struct {
		reader     any
		engine     string
		sourceMode string
	}{
		"engine without exporter": {reader: struct{}{}, engine: "mysql", sourceMode: "table"},
		"oracle query mode":       {reader: &fakeSnapshotExporter{snapshot: "123"}, engine: "oracle", sourceMode: "query"},
	}
	for name, tc := range cases {
		_, err := exportRunSnapshot(context.Background(), tc.reader, tc.engine, tc.sourceMode, jobopts.ConsistencyStrongSnapshot)
		if !failure.IsFailure(err, failure.FailureConfigurationInvalid) {
			t.Fatalf("%s: err=%v, want configuration invalid", name, err)
		}
	}
}

func TestExportRunSnapshotFailsWhenExportFailsOrIsEmpty(t *testing.T) {
	for name, exporter := range map[string]*fakeSnapshotExporter{
		"error": {err: errors.New("boom")},
		"empty": {snapshot: "  "},
	} {
		_, err := exportRunSnapshot(context.Background(), exporter, "postgres", "table", jobopts.ConsistencyStrongSnapshot)
		if err == nil || !strings.Contains(err.Error(), "STRONG_SNAPSHOT") {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
}
