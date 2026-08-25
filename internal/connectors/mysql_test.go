package connectors

import (
	"os"
	"sync"
	"testing"
)

func TestSplitMySQLTableIdent(t *testing.T) {
	tests := []struct {
		in        string
		wantDB    string
		wantTable string
	}{
		{in: "mydb.orders", wantDB: "mydb", wantTable: "orders"},
		{in: "orders", wantDB: "", wantTable: "orders"},
		{in: "`mydb`.`orders`", wantDB: "mydb", wantTable: "orders"},
		{in: "mydb.`orders`", wantDB: "mydb", wantTable: "orders"},
	}
	for _, tc := range tests {
		db, table := splitMySQLTableIdent(tc.in)
		if db != tc.wantDB || table != tc.wantTable {
			t.Fatalf("splitMySQLTableIdent(%q)=(%q,%q), want (%q,%q)", tc.in, db, table, tc.wantDB, tc.wantTable)
		}
	}
}

func TestMySQLDSNIsLocal(t *testing.T) {
	local := []string{
		"user:pass@tcp(localhost:3306)/db",
		"user:pass@tcp(127.0.0.1:3306)/db",
	}
	for _, dsn := range local {
		if !mysqlDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"user:pass@tcp(db.internal:3306)/db",
		"user:pass@tcp(10.0.0.1:3306)/db",
	}
	for _, dsn := range remote {
		if mysqlDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestMySQLOrderedRangeReadsEnabled(t *testing.T) {
	prev, hadPrev := os.LookupEnv(envMySQLOrderedReads)
	defer func() {
		if hadPrev {
			_ = os.Setenv(envMySQLOrderedReads, prev)
		} else {
			_ = os.Unsetenv(envMySQLOrderedReads)
		}
		mysqlOrderedReadsOnce = sync.Once{}
		mysqlOrderedReads = true
	}()

	tests := []struct {
		raw  *string
		want bool
	}{
		{raw: nil, want: true},
		{raw: strPtr("off"), want: false},
		{raw: strPtr("On"), want: true},
	}

	for _, tc := range tests {
		mysqlOrderedReadsOnce = sync.Once{}
		mysqlOrderedReads = true
		if tc.raw == nil {
			_ = os.Unsetenv(envMySQLOrderedReads)
		} else {
			_ = os.Setenv(envMySQLOrderedReads, *tc.raw)
		}
		if got := mysqlOrderedRangeReadsEnabled(); got != tc.want {
			val := "<unset>"
			if tc.raw != nil {
				val = *tc.raw
			}
			t.Fatalf("mysqlOrderedRangeReadsEnabled(%q)=%v want %v", val, got, tc.want)
		}
	}
}

func TestQuoteMySQLMultipartIdent(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "table", want: "`table`"},
		{in: "db.table", want: "`db`.`table`"},
		{in: "`db`.`table`", want: "`db`.`table`"},
		{in: "db.`some``column`", want: "`db`.`some``column`"},
	}
	for _, tc := range tests {
		got, err := quoteMySQLMultipartIdent(tc.in)
		if err != nil {
			t.Fatalf("quoteMySQLMultipartIdent(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("quoteMySQLMultipartIdent(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}

	if _, err := quoteMySQLMultipartIdent(""); err == nil {
		t.Fatal("expected error for empty ident")
	}
	if _, err := quoteMySQLMultipartIdent(""); err == nil {
		t.Fatal("expected error for unsafe ident")
	}
}
