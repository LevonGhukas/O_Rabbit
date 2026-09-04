package connectors

import "testing"

func TestQuoteClickHouseMultipartIdent(t *testing.T) {
	got, err := quoteClickHouseMultipartIdent("analytics.events")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != "`analytics`.`events`" {
		t.Fatalf("unexpected quoted ident: %q", got)
	}

	if _, err := quoteClickHouseMultipartIdent("analytics.event-log"); err == nil {
		t.Fatalf("expected unsafe identifier error")
	}
}

func TestSplitClickHouseTableIdent(t *testing.T) {
	tests := []struct {
		in        string
		wantDB    string
		wantTable string
	}{
		{in: "analytics.events", wantDB: "analytics", wantTable: "events"},
		{in: "`analytics`.`events`", wantDB: "analytics", wantTable: "events"},
		{in: "events", wantDB: "", wantTable: "events"},
	}
	for _, tc := range tests {
		db, table := splitClickHouseTableIdent(tc.in)
		if db != tc.wantDB || table != tc.wantTable {
			t.Fatalf("splitClickHouseTableIdent(%q)=(%q,%q), want (%q,%q)", tc.in, db, table, tc.wantDB, tc.wantTable)
		}
	}
}

func TestClickHouseDSNIsLocal(t *testing.T) {
	local := []string{
		"clickhouse://localhost:19000/default?username=myuser&password=mypassword",
		"clickhouse://127.0.0.1:19000/default",
		"http://localhost:18123/?username=myuser&password=mypassword",
	}
	for _, dsn := range local {
		if !clickhouseDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"clickhouse://clickhouse.internal:9000/default?username=myuser&password=mypassword",
		"https://clickhouse.internal:8443/?username=myuser&password=mypassword",
	}
	for _, dsn := range remote {
		if clickhouseDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestClickHouseSortingKeyMentionsColumn(t *testing.T) {
	if !clickhouseSortingKeyMentionsColumn("tuple(event_date, user_id)", "user_id") {
		t.Fatalf("expected sorting key to mention user_id")
	}
	if clickhouseSortingKeyMentionsColumn("tuple(event_date, session_id)", "user_id") {
		t.Fatalf("did not expect sorting key to mention user_id")
	}
}

func TestValidateClickHouseQueryCursorColumn(t *testing.T) {
	columns := []string{"u.id", "o.id", "o.amount", "u.created_at"}
	types := []string{"UInt64", "Int64", "Decimal(10, 2)", "Nullable(DateTime64(3))"}

	// 1. Qualified cursor column matching first joined table
	v1 := validateClickHouseQueryCursorColumn(columns, types, "u.id")
	if !v1.Found || v1.ResolvedName != "u.id" || v1.Domain != CursorDomainUInt64 || !v1.Orderable || v1.Nullable {
		t.Fatalf("expected u.id to be found with UInt64, got %+v", v1)
	}

	// 2. Qualified cursor column matching second joined table
	v2 := validateClickHouseQueryCursorColumn(columns, types, "o.id")
	if !v2.Found || v2.ResolvedName != "o.id" || v2.Domain != CursorDomainInt64 || !v2.Orderable {
		t.Fatalf("expected o.id to be found with Int64, got %+v", v2)
	}

	// 3. Case-insensitive match
	v3 := validateClickHouseQueryCursorColumn(columns, types, "U.ID")
	if !v3.Found || v3.ResolvedName != "u.id" {
		t.Fatalf("expected case-insensitive u.id match, got %+v", v3)
	}

	// 4. Backtick-quoted cursor column
	v4 := validateClickHouseQueryCursorColumn(columns, types, "`u.id`")
	if !v4.Found || v4.ResolvedName != "u.id" {
		t.Fatalf("expected backtick-quoted u.id match, got %+v", v4)
	}

	// 5. Double-quoted cursor column
	v5 := validateClickHouseQueryCursorColumn(columns, types, `"o.id"`)
	if !v5.Found || v5.ResolvedName != "o.id" {
		t.Fatalf("expected double-quoted o.id match, got %+v", v5)
	}

	// 6. Nullable timestamp classification
	v6 := validateClickHouseQueryCursorColumn(columns, types, "u.created_at")
	if !v6.Found || v6.ResolvedName != "u.created_at" || v6.Domain != CursorDomainTimestamp || !v6.Nullable {
		t.Fatalf("expected u.created_at to be found as nullable timestamp, got %+v", v6)
	}

	// 7. Backwards compatibility: unqualified cursor with unqualified columns
	unqualifiedCols := []string{"id", "amount"}
	unqualifiedTypes := []string{"UInt64", "Decimal(10, 2)"}
	v7 := validateClickHouseQueryCursorColumn(unqualifiedCols, unqualifiedTypes, "users.id")
	if !v7.Found || v7.ResolvedName != "id" {
		t.Fatalf("expected leaf fallback match for users.id -> id, got %+v", v7)
	}

	// 8. Non-existent cursor column
	v8 := validateClickHouseQueryCursorColumn(columns, types, "non_existent")
	if v8.Found {
		t.Fatalf("expected non_existent to not be found, got %+v", v8)
	}
}
