package arrowio

import (
	"database/sql"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/apache/arrow-go/v18/arrow"
	_ "modernc.org/sqlite"
)

func TestRowsToRecordBatchesFilteredSkipsRejectedRowsBeforeCounting(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT 1 AS id, 10 AS seq UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30 ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	colTypes, _ := rows.ColumnTypes()

	var seen []string
	keep := func(cursor string, cursorOK bool, vals []any) bool {
		seen = append(seen, cursor)
		return cursorOK && cursor != "20"
	}
	var written int64
	total, maxCursor, err := RowsToRecordBatchesEngineFiltered("", rows, cols, colTypes, nil, 0, nil, 1, connectors.CursorDomainInt64, keep, func(_ *arrow.Schema, rec arrow.RecordBatch) error {
		written += rec.NumRows()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || written != 2 {
		t.Fatalf("total=%d written=%d, want 2 rows kept", total, written)
	}
	if maxCursor != "30" {
		t.Fatalf("maxCursor=%q, want 30", maxCursor)
	}
	if len(seen) != 3 || seen[1] != "20" {
		t.Fatalf("filter saw cursors %v, want every row", seen)
	}
}
