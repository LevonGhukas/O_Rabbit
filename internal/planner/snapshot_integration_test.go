package planner

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

// TestExportRunSnapshotThroughOpenCursorReaderIntegration runs with
// ORABBIT_IT_POSTGRES_DSN set. The planner exports the snapshot from the
// reader OpenCursorReader returns, so the connector wrappers must keep
// PostgreSQL's snapshot support visible in table and query mode, and the
// snapshot must stay importable by workers after the planning context ends.
func TestExportRunSnapshotThroughOpenCursorReaderIntegration(t *testing.T) {
	dsn := os.Getenv("ORABBIT_IT_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("ORABBIT_IT_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	check, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open check connection: %v", err)
	}
	defer check.Close()

	for _, mode := range []string{"table", "query"} {
		snapshot := exportWithPlanningContext(t, ctx, dsn, mode)
		if err := importSnapshot(ctx, check, snapshot); err != nil {
			t.Fatalf("%s mode: snapshot %q not importable after planning ended: %v", mode, snapshot, err)
		}
	}
}

// exportWithPlanningContext exports like the run planner does: through a
// reader and context that are both closed once planning returns.
func exportWithPlanningContext(t *testing.T, parent context.Context, dsn, mode string) string {
	t.Helper()
	planCtx, cancel := context.WithCancel(parent)
	defer cancel()
	reader, err := connectors.OpenCursorReader(planCtx, "postgres", dsn)
	if err != nil {
		t.Fatalf("%s mode: open cursor reader: %v", mode, err)
	}
	defer reader.Close()
	snapshot, err := exportRunSnapshot(planCtx, reader, "postgres", mode, jobopts.ConsistencyStrongSnapshot)
	if err != nil {
		t.Fatalf("%s mode: export run snapshot: %v", mode, err)
	}
	return snapshot
}

func importSnapshot(ctx context.Context, db *sql.DB, snapshot string) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", snapshot))
	return err
}
