package connectors

import (
	"os"
	"strings"
	"sync"
	"testing"
)

func TestBuildMSSQLSelectClauseProjectsXMLAsUnboundedText(t *testing.T) {
	got := buildMSSQLSelectClause([]string{"xml_col", "id"}, map[string]string{"xml_col": "XML", "id": "INT"})
	if !strings.Contains(got, "CONVERT(NVARCHAR(MAX), [xml_col]) AS [xml_col]") {
		t.Fatalf("XML projection missing or bounded: %s", got)
	}
}

func TestSplitMSSQLTableIdent(t *testing.T) {
	tests := []struct {
		in         string
		wantDB     string
		wantSchema string
		wantTable  string
	}{
		{in: "SalesDB.dbo.Orders", wantDB: "SalesDB", wantSchema: "dbo", wantTable: "Orders"},
		{in: "dbo.Orders", wantDB: "", wantSchema: "dbo", wantTable: "Orders"},
		{in: "Orders", wantDB: "", wantSchema: "dbo", wantTable: "Orders"},
	}
	for _, tc := range tests {
		db, schema, table := splitMSSQLTableIdent(tc.in)
		if db != tc.wantDB || schema != tc.wantSchema || table != tc.wantTable {
			t.Fatalf("splitMSSQLTableIdent(%q)=(%q,%q,%q), want (%q,%q,%q)", tc.in, db, schema, table, tc.wantDB, tc.wantSchema, tc.wantTable)
		}
	}
}

func TestMSSQLDSNIsLocal(t *testing.T) {
	local := []string{
		"sqlserver://sa:pass@localhost:1433?database=master&encrypt=true",
		"sqlserver://sa:pass@127.0.0.1:1433?database=master&encrypt=false",
		"server=tcp:localhost,1433;user id=sa;password=p;database=master;encrypt=true",
		"server=127.0.0.1;user id=sa;password=p;database=master",
	}
	for _, dsn := range local {
		if !mssqlDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"sqlserver://sa:pass@db.internal:1433?database=master&encrypt=true",
		"server=tcp:db.internal,1433;user id=sa;password=p;database=master",
	}
	for _, dsn := range remote {
		if mssqlDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestMSSQLOrderedRangeReadsEnabled(t *testing.T) {
	prev, hadPrev := os.LookupEnv(envMSSQLOrderedReads)
	defer func() {
		if hadPrev {
			_ = os.Setenv(envMSSQLOrderedReads, prev)
		} else {
			_ = os.Unsetenv(envMSSQLOrderedReads)
		}
		mssqlOrderedReadsOnce = sync.Once{}
		mssqlOrderedReads = true
	}()

	tests := []struct {
		raw  *string
		want bool
	}{
		{raw: nil, want: true},
		{raw: strPtr("off"), want: false},
		{raw: strPtr("On"), want: true},
		{raw: strPtr("unknown"), want: true},
	}

	for _, tc := range tests {
		mssqlOrderedReadsOnce = sync.Once{}
		mssqlOrderedReads = true
		if tc.raw == nil {
			_ = os.Unsetenv(envMSSQLOrderedReads)
		} else {
			_ = os.Setenv(envMSSQLOrderedReads, *tc.raw)
		}
		if got := mssqlOrderedRangeReadsEnabled(); got != tc.want {
			val := "<unset>"
			if tc.raw != nil {
				val = *tc.raw
			}
			t.Fatalf("mssqlOrderedRangeReadsEnabled(%q)=%v want %v", val, got, tc.want)
		}
	}
}
