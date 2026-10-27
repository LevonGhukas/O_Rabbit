package main

// TID-908 against a real PostgreSQL source: a row committed after a run with
// the same cursor value as that run's high-water mark is exported by the next
// run, and rows already exported are not exported again. Runs only when
// ORABBIT_IT_POSTGRES_DSN is set.

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

func TestExtractReadsLateRowsAtTheHighWaterMarkIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set ORABBIT_IT_POSTGRES_DSN to extract from PostgreSQL")
	}
	ctx := context.Background()
	pg, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer pg.Close()
	exec := func(s string) {
		t.Helper()
		if _, err := pg.ExecContext(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	exec("DROP TABLE IF EXISTS orabbit_it_hwm_boundary")
	exec("CREATE TABLE orabbit_it_hwm_boundary (id BIGINT NOT NULL PRIMARY KEY, name TEXT, updated_at TIMESTAMPTZ NOT NULL)")
	exec("INSERT INTO orabbit_it_hwm_boundary VALUES (1, 'a', '2026-01-01T00:00:01Z'), (2, 'b', '2026-01-01T00:00:02Z'), (3, 'c', '2026-01-01T00:00:03Z')")
	t.Cleanup(func() { _, _ = pg.ExecContext(context.Background(), "DROP TABLE IF EXISTS orabbit_it_hwm_boundary") })

	clients := &clientCache{}
	defer clients.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	taskCtx := withTaskCredentials(ctx, taskCredentials{SourceDSN: dsn})
	extract := func(run string, lower string, skip *hwmboundary.Skip) sourceExtract {
		t.Helper()
		spec := normalizePartitionSpec(partitionSpec{
			Type: "sql_cursor_single", Table: "public.orabbit_it_hwm_boundary", CursorColumn: "updated_at", CursorDomain: "timestamp",
			Lower: lower, TrackHWMBoundary: true, HWMBoundarySkip: skip,
		})
		task := &grpcpb.TaskAssignment{TaskId: "task-" + run, RunId: "run-" + run, TaskIndex: 1}
		res, err := extractSQLCursorTask(taskCtx, log, &progressSink{}, "worker-1", task, spec, clients, "postgres")
		if err != nil {
			t.Fatalf("run %s: extract: %v", run, err)
		}
		for _, f := range res.ParquetFiles {
			path := f.Path
			t.Cleanup(func() { _ = os.Remove(path) })
		}
		if res.HWMBoundary == nil {
			t.Fatalf("run %s: no boundary reported", run)
		}
		return res
	}
	skipFrom := func(b *hwmboundary.Boundary) *hwmboundary.Skip {
		return &hwmboundary.Skip{CursorValue: b.CursorValue, RowHashes: b.RowHashes}
	}

	first := extract("1", "", nil)
	if first.Rows != 3 || len(first.HWMBoundary.RowHashes) != 1 {
		t.Fatalf("run 1: rows=%d boundary=%+v, want 3 rows and one row at the mark", first.Rows, first.HWMBoundary)
	}
	hwm := first.MaxCursor

	// The ticket: a row committed later with the same cursor value as the mark.
	exec("INSERT INTO orabbit_it_hwm_boundary VALUES (4, 'late', '2026-01-01T00:00:03Z')")
	second := extract("2", hwm, skipFrom(first.HWMBoundary))
	if second.Rows != 1 {
		t.Fatalf("run 2: rows=%d, want only the late row", second.Rows)
	}
	if len(second.HWMBoundary.RowHashes) != 2 {
		t.Fatalf("run 2: boundary=%+v, want both rows at the mark", second.HWMBoundary)
	}

	idle := extract("3", hwm, skipFrom(second.HWMBoundary))
	if idle.Rows != 0 {
		t.Fatalf("run 3: rows=%d, want 0 (no duplicates)", idle.Rows)
	}

	// A dataset whose state predates the boundary seeds it without re-exporting.
	seed := extract("4", hwm, &hwmboundary.Skip{CursorValue: hwm, Seed: true})
	if seed.Rows != 0 || len(seed.HWMBoundary.RowHashes) != 2 {
		t.Fatalf("seed run: rows=%d boundary=%+v, want 0 rows and both rows recorded", seed.Rows, seed.HWMBoundary)
	}
}
