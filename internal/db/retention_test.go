package db

import (
	"context"
	"testing"
	"time"
)

func TestPruneHistoryRemovesOnlyOldTerminalRunHistory(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour).Format(time.RFC3339Nano)
	recent := now.Add(-time.Minute).Format(time.RFC3339Nano)

	mustExec := func(q string, args ...any) {
		t.Helper()
		if _, err := st.db.ExecContext(ctx, q, args...); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, r := range []struct{ id, status, finished string }{
		{"old-failed", "FAILED", old},
		{"old-succeeded", "SUCCEEDED", old},
		{"recent-failed", "FAILED", recent},
		{"running", "RUNNING", ""},
	} {
		var finished any
		if r.finished != "" {
			finished = r.finished
		}
		mustExec(`INSERT INTO runs(id,job_id,status,correlation_id,started_at,finished_at) VALUES(?,?,?,?,?,?)`, r.id, "job", r.status, "c", old, finished)
		mustExec(`INSERT INTO events(id,run_id,ts,level,message,fields_json) VALUES(?,?,?,?,?,?)`, "ev-"+r.id, r.id, old, "INFO", "m", "{}")
		mustExec(`INSERT INTO tasks(id,run_id,task_index,partition_spec_json,status,rows_read,bytes_read,bytes_written,parquet_objects_json) VALUES(?,?,0,'{}','FAILED',0,0,0,'[]')`, "task-"+r.id, r.id)
		mustExec(`INSERT INTO task_attempts(id,task_id,attempt_number,worker_id,fencing_token,status,assigned_at,lease_deadline,last_renewed_at,finished_at,created_at,updated_at) VALUES(?,?,1,'w',?,'FAILED',?,?,?,?,?,?)`,
			"att-"+r.id, "task-"+r.id, "tok-"+r.id, old, old, old, old, old, old)
	}
	mustExec(`INSERT INTO master_leadership_history(id,instance_id,epoch,event_type,occurred_at_ms) VALUES('h1','i',1,'ACQUIRED',?)`, now.Add(-48*time.Hour).UnixMilli())

	res, err := st.PruneHistory(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneHistory: %v", err)
	}
	if res.Events != 2 || res.TaskAttempts != 1 || res.LeadershipHistory != 1 {
		t.Fatalf("unexpected prune result: %+v", res)
	}

	count := func(q string) int {
		var n int
		if err := st.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := count(`SELECT COUNT(*) FROM events WHERE run_id IN ('recent-failed','running')`); n != 2 {
		t.Fatalf("recent/active events pruned: remaining=%d", n)
	}
	if n := count(`SELECT COUNT(*) FROM task_attempts WHERE id='att-old-succeeded'`); n != 1 {
		t.Fatal("attempts of succeeded runs must be kept")
	}
	if n := count(`SELECT COUNT(*) FROM runs`); n != 4 {
		t.Fatalf("runs must never be pruned: %d", n)
	}
}

func TestReadPoolIsQueryOnly(t *testing.T) {
	st := openTestStore(t)
	if st.rdb == st.db {
		t.Fatal("file-backed store should use a separate read pool")
	}
	if _, err := st.rdb.ExecContext(context.Background(), `INSERT INTO hwm(job_id,hwm_value,updated_at) VALUES('j','v','t')`); err == nil {
		t.Fatal("read pool accepted a write")
	}
}
