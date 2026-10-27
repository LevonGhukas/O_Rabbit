package db

import (
	"context"
	"testing"
)

func TestListWorkersCountsRunningTasks(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()

	for _, id := range []string{"worker-busy", "worker-idle"} {
		if err := st.UpdateWorkerHeartbeat(ctx, "", id, "", `{}`, "", "", 0); err != nil {
			t.Fatalf("heartbeat %s: %v", id, err)
		}
	}
	// Assignment joins runs to jobs, so the run needs a real job.
	if err := st.CreateJob(ctx, Job{
		ID: "job-count", Name: "job-count", SourceConnectionID: "src", TargetConnectionID: "tgt",
		SourceSQL: "select 1", TargetNamespace: "ns", TargetTable: "tbl", WriteMode: "append", OptionsJSON: []byte(`{}`),
	}); err != nil {
		t.Fatalf("create job: %v", err)
	}
	run := Run{ID: "run-count", JobID: "job-count", DatasetKey: "k", Status: "RUNNING", CorrelationID: "c", StartedAt: nowUTC()}
	if err := st.CreateRun(ctx, run); err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := st.InsertTasks(ctx, []TaskInsert{
		{ID: "task-a", RunID: run.ID, TaskIndex: 1, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"},
		{ID: "task-b", RunID: run.ID, TaskIndex: 2, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"},
		{ID: "task-c", RunID: run.ID, TaskIndex: 3, PartitionSpec: []byte(`{"type":"single"}`), Status: "PENDING"},
	}); err != nil {
		t.Fatalf("insert tasks: %v", err)
	}
	// Two tasks leased by the busy worker; the third stays PENDING and must not count.
	for i := 0; i < 2; i++ {
		if _, ok, err := st.AssignNextPendingTask(ctx, "worker-busy"); err != nil || !ok {
			t.Fatalf("assign task %d: ok=%v err=%v", i, ok, err)
		}
	}

	workers, err := st.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list workers: %v", err)
	}
	got := map[string]int{}
	for _, w := range workers {
		got[w.ID] = w.RunningTasks
	}
	if got["worker-busy"] != 2 || got["worker-idle"] != 0 {
		t.Fatalf("running tasks = %v, want worker-busy=2 worker-idle=0", got)
	}

	active, err := st.ListWorkersActive(ctx, "2000-01-01T00:00:00Z")
	if err != nil {
		t.Fatalf("list active workers: %v", err)
	}
	for _, w := range active {
		if w.ID == "worker-busy" && w.RunningTasks != 2 {
			t.Fatalf("active listing running tasks = %d, want 2", w.RunningTasks)
		}
	}
}
