package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// TestPostgresSnapshotReadsIgnoreLaterWritesIntegration runs with
// ORABBIT_IT_POSTGRES_DSN set. Rows inserted after the planner exported the
// snapshot must stay invisible to every task reading at that snapshot, in table
// and query mode, and snapshot reads must not leak pooled connections.
func TestPostgresSnapshotReadsIgnoreLaterWritesIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ORABBIT_IT_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	e := sqlConformanceEngines()[0]
	seed, err := e.seedDB(dsn)
	if err != nil {
		t.Fatalf("open seed connection: %v", err)
	}
	defer seed.Close()
	seedConformanceTable(t, ctx, seed, e)

	planner, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer planner.Close()
	snapshot, err := planner.ExportSnapshot(ctx)
	if err != nil {
		t.Fatalf("export snapshot: %v", err)
	}

	values := make([]string, 0, 10)
	for i := 101; i <= 110; i++ {
		values = append(values, fmt.Sprintf("(%d, 'late', %d, '2026-01-01 00:00:00')", i, i))
	}
	if _, err := seed.ExecContext(ctx, "INSERT INTO "+conformanceTable+" (id, name, amount, created) VALUES "+strings.Join(values, ", ")); err != nil {
		t.Fatalf("insert late rows: %v", err)
	}

	worker, err := OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	tableQuery := CursorQuery{Table: conformanceTable, CursorColumn: "id", CursorDomain: CursorDomainInt64}
	queryMode := CursorQuery{SourceQuery: "SELECT id, amount FROM " + conformanceTable, CursorColumn: "id", CursorDomain: CursorDomainInt64}
	for name, tc := range map[string]struct {
		q    CursorQuery
		want int
	}{
		"table live":        {q: tableQuery, want: 110},
		"table snapshot":    {q: withSnapshot(tableQuery, snapshot), want: 100},
		"query live":        {q: queryMode, want: 110},
		"query snapshot":    {q: withSnapshot(queryMode, snapshot), want: 100},
		"partial range":     {q: withSnapshot(withRange(tableQuery, "50", "200"), snapshot), want: 51},
		"partial range raw": {q: withRange(tableQuery, "50", "200"), want: 61},
	} {
		if got := readCursorRows(t, ctx, worker, tc.q); got != tc.want {
			t.Fatalf("%s: rows=%d, want %d", name, got, tc.want)
		}
	}

	if inUse := worker.db.Stats().InUse; inUse != 0 {
		t.Fatalf("snapshot reads left %d pooled connections in use", inUse)
	}
}

func withSnapshot(q CursorQuery, snapshot string) CursorQuery {
	q.SnapshotContext = snapshot
	return q
}

func withRange(q CursorQuery, lower, upper string) CursorQuery {
	q.LowerBound, q.UpperBound, q.UpperInclusive = lower, upper, true
	return q
}

// readCursorRows reads one task the way the worker does: on its own context,
// cancelled once the rows are closed.
func readCursorRows(t *testing.T, parent context.Context, p *Postgres, q CursorQuery) int {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	rows, _, _, _, err := p.QueryCursor(ctx, q)
	if err != nil {
		t.Fatalf("query cursor: %v", err)
	}
	n := countRows(t, rows)
	cancel()
	waitForIdle(t, p.db)
	return n
}

// waitForIdle waits for database/sql to roll back a context-cancelled
// transaction, which happens on a background goroutine.
func waitForIdle(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for db.Stats().InUse != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
}
