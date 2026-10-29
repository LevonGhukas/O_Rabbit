package connectors

import (
	"strings"
	"testing"
)

// fuzzMaxSQLBytes bounds one fuzz input. Real queries and filters are far
// smaller; larger inputs only slow the fuzzer without new code paths.
const fuzzMaxSQLBytes = 16 << 10

// fuzzCursorMarker ends the cursor range appended after a where clause or a
// wrapped query. It must stay visible to every dialect.
const fuzzCursorMarker = "__orabbit_cursor__"

// fuzzCursorRange mirrors the cursor predicates the connectors append: column
// names quoted per engine and bound parameters. Its quote characters could
// close a literal the validated text left open.
const fuzzCursorRange = "\"orabbit_query\".\"id\" > $1 AND `id` > ? AND [id] > @p1 AND \"ID\" <= :2 AND " + fuzzCursorMarker + " > 1"

var fuzzSQLSeeds = []string{
	"",
	"SELECT 1",
	"SELECT id FROM t;",
	"WITH a AS (SELECT 1) SELECT * FROM a",
	"select 'it''s' from t -- note",
	"SELECT /* c */ id FROM t; /* tail */",
	"SELECT data #>> '{a,b}' FROM t",
	"SELECT [Order Date] FROM [dbo].[Orders]",
	"SELECT `order` FROM t",
	"SELECT $$it's$$, $1, E'a\\'b'",
	"SELECT 'Ünïcødé ✓', \"ид\" FROM t",
	"SELECT ((((((((((1))))))))))",
	"SELECT 1; SELECT 2",
	"DELETE FROM t",
	"SELECT id INTO x FROM t",
	`SELECT '\'; DROP TABLE t; --'`,
	`SELECT $$'$$; DROP TABLE t; -- '`,
	`SELECT 1 /* /* */ ' */; DROP TABLE t; -- '`,
	`SELECT a[' ]'; DROP TABLE t; -- ']`,
	"SELECT 1 # '\n; DROP TABLE t; -- '",
	"SELECT 1) UNION (SELECT 2",
	"SELECT [a'b]; DROP TABLE t",
	"SELECT [[a]] FROM t",
	"SELECT [Update (UTC)] FROM t",
	`SELECT q'[ ' ]'; DROP TABLE t; -- '`,
	"SELECT " + strings.Repeat("a", 4096),
	"SELECT '" + strings.Repeat("'", 4096),
	strings.Repeat("/*", 2048),
}

func FuzzNormalizeReadOnlySQLQuery(f *testing.F) {
	for _, seed := range fuzzSQLSeeds {
		f.Add(seed)
	}
	pgDialect, ok := queryModeDialectForEngine("postgres")
	if !ok {
		f.Fatal("postgres query-mode dialect missing")
	}
	f.Fuzz(func(t *testing.T, raw string) {
		if len(raw) > fuzzMaxSQLBytes {
			t.Skip()
		}
		got, err := NormalizeReadOnlySQLQuery(raw)
		if err != nil {
			return
		}
		again, err := NormalizeReadOnlySQLQuery(got)
		if err != nil || again != got {
			t.Fatalf("normalize not idempotent: %q -> %q -> %q (%v)", raw, got, again, err)
		}
		wrapped := "SELECT * FROM " + pgDialect.sourceExpr(got) + " WHERE " + fuzzCursorRange
		assertSingleReadOnlyStatement(t, raw, wrapped)
	})
}

func FuzzValidateWhereClause(f *testing.F) {
	for _, seed := range []string{
		"",
		"status = 'active'",
		"(region IN ('eu', 'us')) AND deleted_at IS NULL",
		"note <> 'it''s -- fine; (really'",
		`"Order Date" > DATE '2026-01-01'`,
		"[Order Date] > '2026-01-01'",
		"data['key'] = 'x'",
		"name = 'Ünïcødé'",
		"1=1; DROP TABLE users",
		"1=1 --",
		"1=1 #",
		"1=1) OR (1=1",
		"[a'b] = 1 --'",
		`x = '\' OR 1=1 --'`,
		`x = $$'$$) OR 1=1 --'`,
		"x = 1 // ')",
		"[[a]] = 1",
		"[Load-Date] > 0",
		strings.Repeat("(", 4096) + strings.Repeat(")", 4096),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, clause string) {
		if len(clause) > fuzzMaxSQLBytes {
			t.Skip()
		}
		if ValidateWhereClause(clause) != nil {
			return
		}
		query := "SELECT * FROM t WHERE (" + clause + ") AND " + fuzzCursorRange
		assertSingleReadOnlyStatement(t, clause, query)
	})
}

// assertSingleReadOnlyStatement checks, under every modelled dialect that can
// lex the text, that it is one statement with balanced parentheses, no
// data-changing keyword, and the cursor marker outside literals and comments.
func assertSingleReadOnlyStatement(t *testing.T, input, sql string) {
	t.Helper()
	for _, dialect := range sqlDialects {
		res, err := lexSQL(sql, dialect)
		if err != nil {
			continue
		}
		if res.statementEnd >= 0 || res.unbalanced || res.forbidden != "" {
			t.Fatalf("%s: accepted %q builds %q: %+v", dialect.name, input, sql, res)
		}
		if !strings.EqualFold(res.lastWord, fuzzCursorMarker) {
			t.Fatalf("%s: accepted %q hides the cursor range in %q (last word %q)", dialect.name, input, sql, res.lastWord)
		}
		if res.keyword != "" {
			t.Fatalf("%s: accepted %q contains %q", dialect.name, input, res.keyword)
		}
	}
}
