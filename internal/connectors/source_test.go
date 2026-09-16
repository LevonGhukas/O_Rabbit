package connectors

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeSourceEngine(t *testing.T) {
	tests := map[string]string{
		"mssql":          "mssql",
		"SQLSERVER":      "mssql",
		"oracle":         "oracle",
		"ORA":            "oracle",
		"postgres":       "postgres",
		"PostgreSQL":     "postgres",
		"pg":             "postgres",
		"clickhouse":     "clickhouse",
		"click-house":    "clickhouse",
		"ch":             "clickhouse",
		"flightsql":      "flightsql",
		"flight-sql":     "flightsql",
		"adbc":           "flightsql",
		"mysql":          "mysql",
		"mariadb":        "mariadb",
		"mariadb-server": "mariadb",
		"trino":          "trino",
		"mongodb":        "mongodb",
		"mongo":          "mongodb",
		"unknown":        "unknown",
		"  flightsql ":   "flightsql",
		"  clickhouse ":  "clickhouse",
	}
	for in, want := range tests {
		if got := NormalizeSourceEngine(in); got != want {
			t.Fatalf("NormalizeSourceEngine(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestKnownSourceEngines(t *testing.T) {
	got := KnownSourceEngines()
	want := []string{"cassandra", "clickhouse", "flightsql", "mariadb", "mongodb", "mssql", "mysql", "oracle", "postgres", "s3", "trino"}
	if len(got) != len(want) {
		t.Fatalf("KnownSourceEngines()=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("KnownSourceEngines()[%d]=%q want %q", i, got[i], want[i])
		}
	}
	if !IsKnownSourceEngine("pg") {
		t.Fatalf("pg alias should be recognized as a known source engine")
	}
	if IsKnownSourceEngine("unknown-engine") {
		t.Fatalf("unknown engine should not be recognized")
	}
}

func TestSupportsOrderedCursor(t *testing.T) {
	if !SupportsOrderedCursor("mssql") {
		t.Fatalf("mssql should support ordered_cursor")
	}
	if !SupportsOrderedCursor("oracle") {
		t.Fatalf("oracle should support ordered_cursor")
	}
	if !SupportsOrderedCursor("postgres") {
		t.Fatalf("postgres should support ordered_cursor")
	}
	if !SupportsOrderedCursor("clickhouse") {
		t.Fatalf("clickhouse should support ordered_cursor")
	}
	if !SupportsOrderedCursor("mysql") {
		t.Fatalf("mysql should support ordered_cursor")
	}
	if !SupportsOrderedCursor("mariadb") {
		t.Fatalf("mariadb should support ordered_cursor")
	}
	if !SupportsOrderedCursor("trino") {
		t.Fatalf("trino should support ordered_cursor")
	}
	if !SupportsOrderedCursor("mongodb") {
		t.Fatalf("mongodb should support ordered_cursor")
	}
	if SupportsOrderedCursor("flightsql") {
		t.Fatalf("flightsql should not support ordered_cursor")
	}
}

func TestSupportsQueryMode(t *testing.T) {
	for _, engine := range []string{"postgres", "mysql", "mariadb", "mssql", "oracle", "clickhouse", "trino", "cassandra"} {
		if !SupportsQueryMode(engine) {
			t.Fatalf("%s should support query mode", engine)
		}
	}
	for _, engine := range []string{"mongodb", "flightsql"} {
		if SupportsQueryMode(engine) {
			t.Fatalf("%s should not support query mode", engine)
		}
	}
}

func TestQueryCapabilitiesForEngine(t *testing.T) {
	supported := map[string]QueryLanguage{
		"postgres":   QueryLanguagePostgresSQL,
		"mysql":      QueryLanguageMySQLSQL,
		"mariadb":    QueryLanguageMariaDBSQL,
		"mssql":      QueryLanguageTSQL,
		"oracle":     QueryLanguageOracleSQL,
		"clickhouse": QueryLanguageClickHouseSQL,
		"trino":      QueryLanguageTrinoSQL,
		"cassandra":  QueryLanguageCQL,
	}
	for engine, wantLanguage := range supported {
		capabilities := QueryCapabilitiesForEngine(engine)
		if !capabilities.Supported || !capabilities.IncrementalSupported || !capabilities.SchemaInferenceSupported {
			t.Fatalf("%s capabilities=%+v want complete query support", engine, capabilities)
		}
		if len(capabilities.Languages) != 1 || capabilities.Languages[0] != wantLanguage {
			t.Fatalf("%s languages=%v want [%s]", engine, capabilities.Languages, wantLanguage)
		}
		if language, ok := DefaultQueryLanguageForEngine(engine); !ok || language != wantLanguage {
			t.Fatalf("%s default language=(%q,%v) want (%q,true)", engine, language, ok, wantLanguage)
		}
	}

	for _, engine := range []string{"mongodb", "flightsql", "unknown"} {
		capabilities := QueryCapabilitiesForEngine(engine)
		if capabilities.Supported || capabilities.IncrementalSupported || capabilities.SchemaInferenceSupported {
			t.Fatalf("%s capabilities=%+v want unsupported", engine, capabilities)
		}
		if len(capabilities.Languages) != 0 {
			t.Fatalf("%s languages=%v want empty", engine, capabilities.Languages)
		}
		if language, ok := DefaultQueryLanguageForEngine(engine); ok || language != "" {
			t.Fatalf("%s default language=(%q,%v) want empty,false", engine, language, ok)
		}
	}
}

func TestSupportsDocumentReader(t *testing.T) {
	if !SupportsDocumentReader("mongodb") {
		t.Fatalf("mongodb should support document reader")
	}
	if !SupportsDocumentReader("mongo") {
		t.Fatalf("mongo alias should support document reader")
	}
	if SupportsDocumentReader("mssql") {
		t.Fatalf("mssql should not support document reader")
	}
	if SupportsDocumentReader("flightsql") {
		t.Fatalf("flightsql should not support document reader")
	}
	if SupportsDocumentReader("unknown") {
		t.Fatalf("unknown engine should not support document reader")
	}
}

func TestNormalizeReadOnlySQLQueryValidation(t *testing.T) {
	valid := "WITH active_orders AS (SELECT id FROM public.orders) SELECT id FROM active_orders;"
	got, err := NormalizeReadOnlySQLQuery(valid)
	if err != nil {
		t.Fatalf("NormalizeReadOnlySQLQuery(valid): %v", err)
	}
	if strings.HasSuffix(got, ";") {
		t.Fatalf("normalized query kept trailing semicolon: %q", got)
	}

	rejects := []string{
		"SELECT id FROM public.orders; SELECT id FROM public.customers",
		"DELETE FROM public.orders",
		"SELECT id INTO copied_orders FROM public.orders",
	}
	for _, q := range rejects {
		if _, err := NormalizeReadOnlySQLQuery(q); err == nil {
			t.Fatalf("expected query to be rejected: %s", q)
		}
	}
}

func TestBuildQueryModeCursorSQLPostgres(t *testing.T) {
	gotSQL, gotArgs, err := buildQueryModeCursorSQL("postgres", CursorQuery{
		SourceQuery:    "SELECT id, name FROM public.orders WHERE status = 'active'",
		CursorColumn:   "id",
		CursorDomain:   CursorDomainInt64,
		LowerBound:     "10",
		UpperBound:     "20",
		LowerExclusive: true,
		UpperInclusive: true,
	})
	if err != nil {
		t.Fatalf("buildQueryModeCursorSQL: %v", err)
	}
	wantSQL := `SELECT * FROM (SELECT id, name FROM public.orders WHERE status = 'active') AS orabbit_query WHERE "orabbit_query"."id" > $1 AND "orabbit_query"."id" <= $2 ORDER BY "orabbit_query"."id" ASC;`
	if gotSQL != wantSQL {
		t.Fatalf("sql=%q want %q", gotSQL, wantSQL)
	}
	if len(gotArgs) != 2 || gotArgs[0] != int64(10) || gotArgs[1] != int64(20) {
		t.Fatalf("args=%#v want [10 20]", gotArgs)
	}
}

func TestBuildQueryModeCursorSQLOraclePreservesQuotedResultCase(t *testing.T) {
	gotSQL, _, err := buildQueryModeCursorSQL("oracle", CursorQuery{
		SourceQuery:  `SELECT "normal_column" FROM "APP"."ui_stress_test"`,
		CursorColumn: "normal_column",
		CursorDomain: CursorDomainInt64,
	})
	if err != nil {
		t.Fatalf("buildQueryModeCursorSQL: %v", err)
	}
	wantSQL := `SELECT * FROM (SELECT "normal_column" FROM "APP"."ui_stress_test") orabbit_query ORDER BY orabbit_query."normal_column" ASC`
	if gotSQL != wantSQL {
		t.Fatalf("sql=%q want %q", gotSQL, wantSQL)
	}
}

func TestBuildQueryModeCursorSQLOracleQuotesSpecialResultName(t *testing.T) {
	gotSQL, _, err := buildQueryModeCursorSQL("oracle", CursorQuery{
		SourceQuery:  `SELECT "column.with.dot" FROM "APP"."ui_stress_test"`,
		CursorColumn: "column.with.dot",
		CursorDomain: CursorDomainString,
	})
	if err != nil {
		t.Fatalf("buildQueryModeCursorSQL: %v", err)
	}
	if !strings.Contains(gotSQL, `orabbit_query."column.with.dot"`) {
		t.Fatalf("query cursor identifier was not preserved: %q", gotSQL)
	}
}

func TestQueryResultColumnIndexPrefersExactCase(t *testing.T) {
	columns := []string{"NORMAL_COLUMN", "normal_column"}
	if got := queryResultColumnIndex(columns, "normal_column"); got != 1 {
		t.Fatalf("column index=%d want exact-case index 1", got)
	}
}

func TestIDColumnMatches(t *testing.T) {
	if !idColumnMatches("RowId", "[dbo].[RowId]") {
		t.Fatalf("expected RowId to match [dbo].[RowId]")
	}
	if !idColumnMatches("id", `"public"."id"`) {
		t.Fatalf("expected id to match public.id")
	}
	if idColumnMatches("id2", "id") {
		t.Fatalf("id2 should not match id")
	}
}

func TestClassifySQLCursorType(t *testing.T) {
	tests := []struct {
		typeName     string
		wantDomain   CursorDomain
		wantOrder    bool
		wantRange    bool
		wantUnsigned bool
		wantBits     int
	}{
		{typeName: "BIGINT", wantDomain: CursorDomainInt64, wantOrder: true, wantRange: true, wantBits: 64},
		{typeName: "UInt64", wantDomain: CursorDomainUInt64, wantOrder: true, wantRange: true, wantUnsigned: true, wantBits: 64},
		{typeName: "Decimal(18,2)", wantDomain: CursorDomainDecimal, wantOrder: true},
		{typeName: "DATE", wantDomain: CursorDomainDate, wantOrder: true, wantRange: true},
		{typeName: "TIMESTAMPTZ", wantDomain: CursorDomainTimestamp, wantOrder: true, wantRange: true},
		{typeName: "LowCardinality(Nullable(String))", wantDomain: CursorDomainString, wantOrder: true},
		{typeName: "UUID", wantDomain: CursorDomainUUID, wantOrder: true},
		{typeName: "JSONB", wantDomain: CursorDomainUnknown, wantOrder: false},
		{typeName: "Int128", wantDomain: CursorDomainUnknown, wantOrder: false},
	}
	for _, tc := range tests {
		got := ClassifySQLCursorType(tc.typeName)
		if got.Domain != tc.wantDomain || got.Orderable != tc.wantOrder || got.RangeCapable != tc.wantRange || got.Unsigned != tc.wantUnsigned || got.Bits != tc.wantBits {
			t.Fatalf("ClassifySQLCursorType(%q)=%+v", tc.typeName, got)
		}
	}
}

func TestEncodeCursorValue(t *testing.T) {
	now := time.Date(2026, 3, 16, 12, 30, 45, 123456789, time.UTC)
	tests := []struct {
		domain CursorDomain
		in     any
		want   string
	}{
		{domain: CursorDomainInt64, in: int64(42), want: "42"},
		{domain: CursorDomainUInt64, in: uint64(99), want: "99"},
		{domain: CursorDomainDate, in: now, want: "2026-03-16"},
		{domain: CursorDomainTimestamp, in: now, want: now.Format(time.RFC3339Nano)},
		{domain: CursorDomainDecimal, in: "123.4500", want: "123.4500"},
		{domain: CursorDomainString, in: []byte("abc"), want: "abc"},
	}
	for _, tc := range tests {
		got, ok := EncodeCursorValue(tc.domain, tc.in)
		if !ok || got != tc.want {
			t.Fatalf("EncodeCursorValue(%q,%T)=(%q,%v), want (%q,true)", tc.domain, tc.in, got, ok, tc.want)
		}
	}
}

func TestSplitCursorRange(t *testing.T) {
	parts, err := SplitCursorRange(CursorDomainUInt64, "1", "10", 3)
	if err != nil {
		t.Fatalf("SplitCursorRange(uint64): %v", err)
	}
	want := []string{"4", "7", "10"}
	if strings.Join(parts, ",") != strings.Join(want, ",") {
		t.Fatalf("SplitCursorRange(uint64)=%v want %v", parts, want)
	}

	parts, err = SplitCursorRange(CursorDomainDate, "2026-03-01", "2026-03-06", 2)
	if err != nil {
		t.Fatalf("SplitCursorRange(date): %v", err)
	}
	want = []string{"2026-03-03", "2026-03-06"}
	if strings.Join(parts, ",") != strings.Join(want, ",") {
		t.Fatalf("SplitCursorRange(date)=%v want %v", parts, want)
	}
}

func TestClosedCursorSpanUnits(t *testing.T) {
	if got, ok := ClosedCursorSpanUnits(CursorDomainInt64, "11", "20"); !ok || got != 10 {
		t.Fatalf("ClosedCursorSpanUnits(int64)=(%d,%v), want (10,true)", got, ok)
	}
	if got, ok := CursorSpanUnits(CursorDomainInt64, "10", "20"); !ok || got != 10 {
		t.Fatalf("CursorSpanUnits(int64)=(%d,%v), want (10,true)", got, ok)
	}
}

func TestValidateWhereClause(t *testing.T) {
	valid := []string{
		"",
		"   ",
		"id > 100",
		"status = 'ACTIVE' AND created_at >= '2026-01-01'",
		"price BETWEEN 10 AND 50",
		"category IN ('A', 'B')",
	}
	for _, v := range valid {
		if err := ValidateWhereClause(v); err != nil {
			t.Fatalf("expected valid where clause %q, got err: %v", v, err)
		}
	}

	invalid := []string{
		"id = 1; DROP TABLE users",
		"id = 1 -- comment",
		"id = 1 /* block comment */",
		"id = 1; DELETE FROM orders",
		"id = 1 UNION SELECT * FROM passwords", // UNION is blocked if followed by destructive or multiple statements
		"id = 1; UPDATE users SET admin=1",
		"EXEC sp_executesql",
	}
	for _, inv := range invalid {
		if err := ValidateWhereClause(inv); err == nil {
			t.Fatalf("expected invalid where clause for %q, but passed", inv)
		}
	}
}

func TestValidateSelectColumns(t *testing.T) {
	valid := [][]string{
		nil,
		{},
		{"id", "name", "created_at"},
		{"o.id", "o.name"},
		{"[id]", "[order_date]"},
		{"\"id\"", "\"user_name\""},
		{"`id`", "`price`"},
	}
	for _, v := range valid {
		if err := ValidateSelectColumns(v); err != nil {
			t.Fatalf("expected valid columns %v, got err: %v", v, err)
		}
	}

	invalid := [][]string{
		{""},
		{"   "},
		{"id; DROP TABLE users"},
		{"id -- comment"},
		{"id /* comment */"},
		{"id, (SELECT 1 FROM secret)"},
	}
	for _, inv := range invalid {
		if err := ValidateSelectColumns(inv); err == nil {
			t.Fatalf("expected invalid columns %v, but passed", inv)
		}
	}
}
