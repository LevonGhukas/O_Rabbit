package connectors

import (
	"context"
	"strings"
	"testing"
)

func TestValidPostgresSnapshotID(t *testing.T) {
	for _, id := range []string{"00000003-0000001B-1", "000003A1-1", "00000004-00000002-3"} {
		if err := validatePostgresSnapshotID(id); err != nil {
			t.Fatalf("%q: %v", id, err)
		}
	}
	for _, id := range []string{"", "abc", "1'; DROP TABLE t; --", "0000-", "-0001", "00000003-0000001B-1 ", "00000003 0000001B"} {
		if err := validatePostgresSnapshotID(id); err == nil {
			t.Fatalf("%q: expected error", id)
		}
	}
}

func TestValidOracleSCN(t *testing.T) {
	for _, scn := range []string{"0", "1234567890123"} {
		if err := validateOracleSCN(scn); err != nil {
			t.Fatalf("%q: %v", scn, err)
		}
	}
	for _, scn := range []string{"", "12a", "1 OR 1=1", "-5", "1.5"} {
		if err := validateOracleSCN(scn); err == nil {
			t.Fatalf("%q: expected error", scn)
		}
	}
}

func TestOracleCursorQueryReadsAsOfSCN(t *testing.T) {
	q, _, err := buildOracleCursorQuery(CursorQuery{Table: "ORDERS", CursorColumn: "ID", CursorDomain: CursorDomainInt64, SnapshotContext: " 4242 "})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, `"ORDERS" AS OF SCN 4242`) {
		t.Fatalf("query=%s", q)
	}
}

func TestOracleCursorQueryRejectsInvalidSCN(t *testing.T) {
	_, _, err := buildOracleCursorQuery(CursorQuery{Table: "ORDERS", CursorColumn: "ID", CursorDomain: CursorDomainInt64, SnapshotContext: "1 OR 1=1"})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestOracleQueryModeRefusesSnapshot(t *testing.T) {
	o := &Oracle{}
	_, _, _, _, err := o.QueryCursor(context.Background(), CursorQuery{SourceQuery: "SELECT 1 AS ID FROM DUAL", CursorColumn: "ID", SnapshotContext: "4242"})
	if err == nil || !strings.Contains(err.Error(), "snapshot") {
		t.Fatalf("err=%v", err)
	}
}

func TestPostgresQueryCursorRejectsInvalidSnapshotBeforeQuerying(t *testing.T) {
	p := &Postgres{}
	for _, q := range []CursorQuery{
		{Table: "orders", CursorColumn: "id", CursorDomain: CursorDomainInt64, SnapshotContext: "x'; SELECT 1; --"},
		{SourceQuery: "SELECT 1 AS id", CursorColumn: "id", SnapshotContext: "x'; SELECT 1; --"},
	} {
		if _, _, _, _, err := p.QueryCursor(context.Background(), q); err == nil {
			t.Fatalf("expected error for %+v", q)
		}
	}
}
