package planner

// Ordered-cursor planning against a real PostgreSQL source and S3 target
// (planning reads the dataset's _state.json). Runs only when these are set:
//
//	ORABBIT_IT_POSTGRES_DSN, ORABBIT_IT_S3_ENDPOINT, ORABBIT_IT_S3_BUCKET,
//	ORABBIT_IT_S3_ACCESS_KEY_ID, ORABBIT_IT_S3_SECRET_ACCESS_KEY

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
)

func postgresSourceJob(t *testing.T, st *db.Store, k crypto.Key, dsn, name string, options map[string]any) db.Job {
	t.Helper()
	ctx := context.Background()
	srcSecret, _ := json.Marshal(map[string]string{"dsn": dsn})
	secret, err := crypto.Encrypt(k, srcSecret, []byte("src-"+name))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := st.CreateConnection(ctx, db.Connection{ID: "src-" + name, Name: "src-" + name, Kind: "source", Engine: "postgres", MetadataJSON: []byte(`{}`), SecretEncBlob: secret}); err != nil {
		t.Fatalf("source connection: %v", err)
	}
	creds, _ := json.Marshal(map[string]string{"access_key_id": os.Getenv("ORABBIT_IT_S3_ACCESS_KEY_ID"), "secret_access_key": os.Getenv("ORABBIT_IT_S3_SECRET_ACCESS_KEY")})
	tsecret, _ := crypto.Encrypt(k, creds, []byte("tgt-"+name))
	meta, _ := json.Marshal(map[string]any{"endpoint": os.Getenv("ORABBIT_IT_S3_ENDPOINT"), "bucket": os.Getenv("ORABBIT_IT_S3_BUCKET"), "prefix": "planner-it/" + t.Name(), "force_path_style": true})
	if err := st.CreateConnection(ctx, db.Connection{ID: "tgt-" + name, Name: "tgt-" + name, Kind: "target", Engine: "s3", MetadataJSON: meta, SecretEncBlob: tsecret}); err != nil {
		t.Fatalf("target connection: %v", err)
	}
	raw, _ := json.Marshal(options)
	job := db.Job{ID: "job-" + name, Name: name, SourceConnectionID: "src-" + name, TargetConnectionID: "tgt-" + name, TargetNamespace: "ns", TargetTable: name, WriteMode: "overwrite", OptionsJSON: raw}
	if err := st.CreateJob(ctx, job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func seedPostgresPlanningTable(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	stmts := []string{
		"DROP TABLE IF EXISTS orabbit_it_plan",
		"CREATE TABLE orabbit_it_plan (id BIGINT NOT NULL PRIMARY KEY, maybe BIGINT NULL, v TEXT)",
		"INSERT INTO orabbit_it_plan SELECT g, g, 'x' FROM generate_series(1, 1000) g",
		"ANALYZE orabbit_it_plan",
	}
	for _, s := range stmts {
		if _, err := pg.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() { _, _ = pg.ExecContext(context.Background(), "DROP TABLE IF EXISTS orabbit_it_plan") })
}

func TestOrderedCursorPlanningIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_POSTGRES_DSN")
	if dsn == "" || os.Getenv("ORABBIT_IT_S3_ENDPOINT") == "" {
		t.Skip("set ORABBIT_IT_POSTGRES_DSN and ORABBIT_IT_S3_* to plan against PostgreSQL and S3")
	}
	seedPostgresPlanningTable(t, dsn)
	ctx := context.Background()

	t.Run("splits the table into contiguous cursor ranges", func(t *testing.T) {
		st, k := openPlannerStore(t)
		job := postgresSourceJob(t, st, k, dsn, "split", map[string]any{
			"table": "public.orabbit_it_plan", "partition_strategy": "ordered_cursor", "cursor_column": "id",
			"planned_tasks": 4, "auto_tune": false,
		})
		run, tasks, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		if run.Status != "RUNNING" || len(tasks) < 2 {
			t.Fatalf("status=%s tasks=%d, want RUNNING with several range tasks", run.Status, len(tasks))
		}
		prevUpper := ""
		for i, task := range tasks {
			var spec map[string]any
			if err := json.Unmarshal(task.PartitionSpec, &spec); err != nil {
				t.Fatalf("spec %d: %v", i, err)
			}
			if spec["type"] != "sql_cursor_range" || spec["cursor_column"] != "id" {
				t.Fatalf("spec %d=%v", i, spec)
			}
			lower := fmt.Sprint(spec["lower"])
			if i > 0 && lower != prevUpper {
				t.Fatalf("task %d starts at %s, previous ended at %s: ranges must be contiguous", i, lower, prevUpper)
			}
			prevUpper = fmt.Sprint(spec["upper"])
		}
		if prevUpper != "1000" {
			t.Fatalf("last range ends at %s, want the max id 1000", prevUpper)
		}
	})

	t.Run("incremental runs refuse a nullable cursor", func(t *testing.T) {
		st, k := openPlannerStore(t)
		job := postgresSourceJob(t, st, k, dsn, "nullable", map[string]any{
			"table": "public.orabbit_it_plan", "partition_strategy": "ordered_cursor", "cursor_column": "maybe",
		})
		job.Incremental = true
		hwm := "maybe"
		job.HWMColumn = &hwm
		if err := st.UpdateJob(ctx, job); err != nil {
			t.Fatalf("update job: %v", err)
		}
		_, _, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)
		if err == nil || !strings.Contains(err.Error(), "nullable") {
			t.Fatalf("err=%v, want nullable cursor rejection", err)
		}
	})

	t.Run("query mode plans from the query's own bounds", func(t *testing.T) {
		st, k := openPlannerStore(t)
		job := postgresSourceJob(t, st, k, dsn, "query", map[string]any{
			"source_mode": "query", "query": "SELECT id, v FROM public.orabbit_it_plan WHERE id <= 500",
			"partition_strategy": "ordered_cursor", "cursor_column": "id", "planned_tasks": 2, "auto_tune": false,
		})
		_, tasks, err := CreateRunAndTasks(ctx, st, k, job, nil, nil)
		if err != nil {
			t.Fatalf("plan: %v", err)
		}
		var last map[string]any
		_ = json.Unmarshal(tasks[len(tasks)-1].PartitionSpec, &last)
		if fmt.Sprint(last["upper"]) != "500" || last["source_mode"] != "query" {
			t.Fatalf("last spec=%v, want query mode ending at 500", last)
		}
	})
}
