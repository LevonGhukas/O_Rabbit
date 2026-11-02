package db

import (
	"context"
	"testing"
)

func seedEvents(t *testing.T) (*Store, context.Context) {
	t.Helper()
	st := openTestStore(t)
	ctx := context.Background()
	if err := st.CreateJob(ctx, Job{ID: "job-a", Name: "job-a", SourceConnectionID: "s", TargetConnectionID: "t", SourceSQL: "select 1", TargetNamespace: "n", TargetTable: "t", WriteMode: "append", OptionsJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"run-a", "run-b"} {
		if err := st.CreateRun(ctx, Run{ID: id, JobID: "job-a", DatasetKey: id, Status: "RUNNING", CorrelationID: id, StartedAt: nowUTC()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.InsertTasks(ctx, []TaskInsert{{ID: "task-1", RunID: "run-a", TaskIndex: 1, PartitionSpec: []byte(`{}`), Status: "PENDING"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := st.AssignNextPendingTask(ctx, "worker-x"); err != nil || !ok {
		t.Fatalf("assign: ok=%v err=%v", ok, err)
	}
	task := "task-1"
	events := []Event{
		{ID: "e1", RunID: "run-a", TS: "2026-10-01T10:00:00Z", Level: "INFO", Message: "planning started", FieldsJSON: []byte(`{}`)},
		{ID: "e2", RunID: "run-a", TaskID: &task, TS: "2026-10-01T10:01:00Z", Level: "WARN", Message: "slow source", FieldsJSON: []byte(`{}`)},
		{ID: "e3", RunID: "run-a", TaskID: &task, TS: "2026-10-01T10:02:00Z", Level: "ERROR", Message: "extract failed: 100% broken", FieldsJSON: []byte(`{}`)},
		{ID: "e4", RunID: "run-b", TS: "2026-10-01T11:00:00Z", Level: "debug", Message: "heartbeat", FieldsJSON: []byte(`{"worker_id":"worker-y"}`)},
		{ID: "e5", RunID: "run-b", TS: "2026-10-01T12:00:00Z", Level: "INFO", Message: "committed", FieldsJSON: []byte(`not-json`)},
	}
	for _, e := range events {
		if err := st.InsertEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	return st, ctx
}

func ids(recs []EventRecord) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.ID
	}
	return out
}

func TestQueryEventsFilters(t *testing.T) {
	st, ctx := seedEvents(t)
	cases := []struct {
		name string
		f    EventFilter
		want []string
	}{
		{"all newest first", EventFilter{}, []string{"e5", "e4", "e3", "e2", "e1"}},
		{"level aliases and case", EventFilter{Levels: []string{"warning", "DEBUG"}}, []string{"e4", "e2"}},
		{"run", EventFilter{RunID: "run-b"}, []string{"e5", "e4"}},
		{"job via runs", EventFilter{JobID: "job-a"}, []string{"e5", "e4", "e3", "e2", "e1"}},
		{"task", EventFilter{TaskID: "task-1"}, []string{"e3", "e2"}},
		{"worker via task lease", EventFilter{WorkerID: "worker-x"}, []string{"e3", "e2"}},
		{"worker via fields", EventFilter{WorkerID: "worker-y"}, []string{"e4"}},
		{"keyword is case-insensitive", EventFilter{Query: "EXTRACT"}, []string{"e3"}},
		{"keyword percent is literal", EventFilter{Query: "100%"}, []string{"e3"}},
		{"time range", EventFilter{Since: "2026-10-01T10:01:00Z", Until: "2026-10-01T11:00:00Z"}, []string{"e4", "e3", "e2"}},
		{"combined", EventFilter{RunID: "run-a", Levels: []string{"ERROR", "WARN"}, Query: "slow"}, []string{"e2"}},
		{"limit", EventFilter{Limit: 2}, []string{"e5", "e4"}},
	}
	for _, c := range cases {
		got, err := st.QueryEvents(ctx, c.f)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if g := ids(got); len(g) != len(c.want) || (len(g) > 0 && g[0] != c.want[0]) || (len(g) > 0 && g[len(g)-1] != c.want[len(c.want)-1]) {
			t.Errorf("%s: got %v want %v", c.name, g, c.want)
		}
	}
	if _, err := st.QueryEvents(ctx, EventFilter{Levels: []string{"LOUD"}}); err == nil {
		t.Error("unknown level must be rejected")
	}
}

func TestQueryEventsPagingTailAndCount(t *testing.T) {
	st, ctx := seedEvents(t)
	first, _ := st.QueryEvents(ctx, EventFilter{Limit: 2})
	older, _ := st.QueryEvents(ctx, EventFilter{Limit: 2, Before: first[len(first)-1].Seq})
	if g := ids(older); len(g) != 2 || g[0] != "e3" || g[1] != "e2" {
		t.Fatalf("older page = %v", g)
	}
	tail, _ := st.QueryEvents(ctx, EventFilter{After: older[0].Seq})
	if g := ids(tail); len(g) != 2 || g[0] != "e4" || g[1] != "e5" {
		t.Fatalf("tail must be oldest-first after the cursor, got %v", g)
	}
	if first[0].JobID != "job-a" {
		t.Errorf("job id not joined: %+v", first[0])
	}
	n, capped, err := st.CountEvents(ctx, EventFilter{RunID: "run-a", Before: 999})
	if err != nil || n != 3 || capped {
		t.Fatalf("count = %d capped=%v err=%v (cursor must be ignored)", n, capped, err)
	}
	latest, err := st.LatestEventSeq(ctx)
	if err != nil || latest != first[0].Seq {
		t.Fatalf("latest = %d err=%v want %d", latest, err, first[0].Seq)
	}
}
