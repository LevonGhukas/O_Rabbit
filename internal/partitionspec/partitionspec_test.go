package partitionspec

import (
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

func TestParseNormalizesLegacyIntRangeLikeTheWorker(t *testing.T) {
	ps, err := Parse([]byte(`{"type":"sql_int_range","id_column":" id ","from":5,"to":9}`))
	if err != nil {
		t.Fatal(err)
	}
	q := ps.CursorQuery("")
	if q.LowerBound != "5" || !q.LowerExclusive || q.UpperBound != "9" || !q.UpperInclusive || q.CursorDomain != connectors.CursorDomainInt64 {
		t.Fatalf("query=%+v", q)
	}
}

func TestCursorQueryUsesPlannedBoundsAndWhere(t *testing.T) {
	ps, err := Parse([]byte(`{"type":"sql_cursor_range","table":"t","cursor_column":"id","cursor_domain":"int64","lower":"1","upper":"2","upper_inclusive":true,"where_clause":"a=1"}`))
	if err != nil {
		t.Fatal(err)
	}
	q := ps.CursorQuery("select 1")
	if q.SourceQuery != "select 1" || q.LowerBound != "1" || q.LowerExclusive || q.UpperBound != "2" || !q.UpperInclusive || q.WhereClause != "a=1" {
		t.Fatalf("query=%+v", q)
	}
}

func TestParseRejectsMalformedSpec(t *testing.T) {
	if _, err := Parse([]byte(`{`)); err == nil {
		t.Fatal("expected error")
	}
}
