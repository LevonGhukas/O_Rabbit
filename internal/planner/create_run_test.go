package planner

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func openPlannerStore(t *testing.T) (*db.Store, crypto.Key) {
	t.Helper()
	st, err := db.Open(context.Background(), db.Config{Path: filepath.Join(t.TempDir(), "planner.sqlite")}, nil)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	t.Setenv("ORABBIT_MASTER_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	k, err := crypto.LoadMasterKeyFromEnv()
	if err != nil {
		t.Fatalf("master key: %v", err)
	}
	st.SetMasterKey(k)
	return st, k
}

// fileJob stores an S3 file source, an S3 target and a job reading one file.
// S3 file sources are planned as one task without contacting the source; the
// target endpoint is unroutable so any unexpected network call fails fast.
func fileJob(t *testing.T, st *db.Store, k crypto.Key, name string, options map[string]any) db.Job {
	t.Helper()
	ctx := context.Background()
	secret, err := crypto.Encrypt(k, []byte("s3://uploads/"+name+".csv"), []byte("src-"+name))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "src-" + name, Name: "src-" + name, Kind: "source", Engine: "s3", MetadataJSON: []byte(`{}`), SecretEncBlob: secret}); err != nil {
		t.Fatalf("source connection: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "tgt-" + name, Name: "tgt-" + name, Kind: "target", Engine: "s3", MetadataJSON: []byte(`{"endpoint":"http://127.0.0.1:1","bucket":"b","prefix":"p"}`), SecretEncBlob: secret}); err != nil {
		t.Fatalf("target connection: %v", err)
	}
	opts := map[string]any{"table": name + ".csv"}
	for key, v := range options {
		opts[key] = v
	}
	raw, _ := json.Marshal(opts)
	job := db.Job{ID: "job-" + name, Name: name, SourceConnectionID: "src-" + name, TargetConnectionID: "tgt-" + name, TargetNamespace: "ns", TargetTable: name, WriteMode: "overwrite", OptionsJSON: raw}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func TestFileSourceIsPlannedAsOneSingleTask(t *testing.T) {
	st, k := openPlannerStore(t)
	job := fileJob(t, st, k, "orders", map[string]any{"format": "csv"})

	run, tasks, err := CreateRunAndTasks(context.Background(), st, k, job, nil, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("tasks=%d, want 1", len(tasks))
	}
	var spec map[string]any
	if err := json.Unmarshal(tasks[0].PartitionSpec, &spec); err != nil {
		t.Fatalf("partition spec: %v", err)
	}
	if spec["type"] != "single" || spec["format"] != "csv" {
		t.Fatalf("partition spec=%v, want single csv", spec)
	}
	if run.Status != "RUNNING" {
		t.Fatalf("run status=%s, want RUNNING", run.Status)
	}
}

func TestSecondRunForBusyDatasetIsRejected(t *testing.T) {
	st, k := openPlannerStore(t)
	job := fileJob(t, st, k, "orders", map[string]any{"format": "csv"})
	ctx := context.Background()
	if _, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil); err != nil {
		t.Fatalf("first plan: %v", err)
	}

	_, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)

	var busy *DatasetBusyError
	if !errors.As(err, &busy) || busy.ActiveRunID == "" {
		t.Fatalf("err=%v, want DatasetBusyError naming the active run", err)
	}
}

func TestPlanningFailureMarksTheRunFailedInPlanningPhase(t *testing.T) {
	st, k := openPlannerStore(t)
	// An S3 file has no ordered cursor to split by.
	job := fileJob(t, st, k, "orders", map[string]any{"format": "csv", "partition_strategy": "ordered_cursor", "cursor_column": "id"})
	ctx := context.Background()

	_, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("err=%v, want ordered_cursor not supported", err)
	}
	runs, err := st.ListRuns(ctx)
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%v err=%v, want the one failed run", runs, err)
	}
	if runs[0].Status != "FAILED" || runs[0].FailurePhase != db.RunFailurePhasePlanning {
		t.Fatalf("run status=%s phase=%s, want FAILED/planning", runs[0].Status, runs[0].FailurePhase)
	}
}

func TestInvalidWhereClauseIsRejectedBeforeARunExists(t *testing.T) {
	st, k := openPlannerStore(t)
	job := fileJob(t, st, k, "orders", map[string]any{"format": "csv", "where_clause": "1=1; DROP TABLE x"})
	ctx := context.Background()

	if _, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil); err == nil || !strings.Contains(err.Error(), "where_clause") {
		t.Fatalf("err=%v, want where_clause rejection", err)
	}
	if runs, _ := st.ListRuns(ctx); len(runs) != 0 {
		t.Fatalf("runs=%d, want none", len(runs))
	}
}

func TestDeprecatedOptionsAreRecordedAsRunEvent(t *testing.T) {
	st, k := openPlannerStore(t)
	job := fileJob(t, st, k, "legacy", nil) // no format: inferred from extension
	ctx := context.Background()

	run, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	events, err := st.ListEventsForRun(ctx, run.ID, 100)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	for _, e := range events {
		if strings.Contains(e.Message, "deprecated job options used") {
			return
		}
	}
	t.Fatalf("no deprecation event in %d events", len(events))
}
