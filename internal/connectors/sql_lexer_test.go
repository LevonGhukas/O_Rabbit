package connectors

import (
	"strings"
	"testing"
	"time"
)

// Each query reads as one harmless statement under one SQL dialect but hides
// a second statement under another dialect's quoting or comment rules.
func TestNormalizeReadOnlySQLQueryRejectsDialectAmbiguity(t *testing.T) {
	for name, query := range map[string]string{
		"postgres backslash is literal":    `SELECT '\'; DROP TABLE t; --'`,
		"postgres dollar quote":            `SELECT $$'$$; DROP TABLE t; -- '`,
		"postgres tagged dollar quote":     `SELECT $x$'$x$; DROP TABLE t; -- '`,
		"postgres nested block comment":    `SELECT 1 /* /* */ ' */; DROP TABLE t; -- '`,
		"postgres array subscript":         `SELECT a[' ]'; DROP TABLE t; -- ']`,
		"mysql hash comment":               "SELECT 1 # '\n; DROP TABLE t; -- '",
		"mysql backslash in double quotes": `SELECT "\"; DROP TABLE t; --"`,
		"mysql dash without space":         "SELECT 1 --'\n; DROP TABLE t; -- '",
		"mysql executable comment":         `SELECT 1 /*! ; DROP TABLE t */`,
		"mssql bracket hides quote":        "SELECT [a'b]; DROP TABLE t; -- '",
		"cassandra slash comment":          "SELECT 1 // '\n; DROP TABLE t; -- '",
		"oracle q quote":                   `SELECT q'[ ' ]'; DROP TABLE t; -- '`,
		"unbalanced close escapes wrapper": "SELECT 1) UNION (SELECT 2",
		"unbalanced open":                  "SELECT (1",
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := NormalizeReadOnlySQLQuery(query); err == nil {
				t.Fatalf("NormalizeReadOnlySQLQuery(%q) accepted as %q", query, got)
			}
		})
	}
}

func TestNormalizeReadOnlySQLQueryAcceptsDialectSpecificReads(t *testing.T) {
	for name, query := range map[string]string{
		"postgres json operator":      `SELECT data #>> '{a,b}' AS v FROM t`,
		"postgres jsonb subscript":    `SELECT data['key'] FROM t`,
		"postgres array slice":        `SELECT arr[1:2] FROM t`,
		"postgres escape string":      `SELECT E'it\'s' FROM t`,
		"postgres dollar quote":       `SELECT $$it's$$ AS v FROM t`,
		"postgres positional param":   `SELECT $1::int AS v`,
		"mssql bracket identifiers":   `SELECT [Order Date], [Update] FROM [dbo].[Orders]`,
		"mssql keyword bracket names": `SELECT [Load-Date], [Update (UTC)] FROM t`,
		"mysql backtick identifier":   "SELECT `order` FROM `t`",
		"doubled quote":               `SELECT 'it''s' FROM t`,
		"comment before select":       "-- daily orders\nSELECT id FROM t",
		"trailing semicolon":          "SELECT id FROM t;",
		"trailing comment after semi": "SELECT id FROM t; -- done",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NormalizeReadOnlySQLQuery(query); err != nil {
				t.Fatalf("NormalizeReadOnlySQLQuery(%q): %v", query, err)
			}
		})
	}
}

// A query ending in a line comment is wrapped as "(<query>) alias"; the
// comment must not swallow the closing parenthesis.
func TestNormalizeReadOnlySQLQueryEndsLineCommentBeforeWrapping(t *testing.T) {
	got, err := NormalizeReadOnlySQLQuery("SELECT id FROM t -- note")
	if err != nil {
		t.Fatal(err)
	}
	sql, err := buildQueryModeDescribeSQL("postgres", got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "-- note\n)") {
		t.Fatalf("wrapped query keeps ')' inside the comment: %q", sql)
	}
}

func TestValidateWhereClauseRejectsDialectAmbiguity(t *testing.T) {
	for name, clause := range map[string]string{
		"mysql hash comment":            "1=1 #",
		"mssql bracket then comment":    "[a'b] = 1 --'",
		"postgres backslash is literal": `x = '\' OR 1=1 --'`,
		"postgres dollar quote":         `x = $$'$$) OR 1=1 --'`,
		"mysql backslash double quote":  `x = "\") OR 1=1 --"`,
		"oracle q quote":                `x = q'[ ' ]') OR 1=1 --'`,
		"cassandra slash comment":       "x = 1 // ')",
		"mysql executable comment":      "1=1 /*! OR 1=1 */",
		"unbalanced bracket":            "[a = 1",
		"bracket closed by cursor SQL":  "[[a]] = 1",
	} {
		t.Run(name, func(t *testing.T) {
			if err := ValidateWhereClause(clause); err == nil {
				t.Fatalf("ValidateWhereClause(%q) accepted", clause)
			}
		})
	}
}

func TestValidateWhereClauseAcceptsDialectSpecificReads(t *testing.T) {
	for _, clause := range []string{
		`[Order Date] > '2026-01-01'`,
		`[Exec-Time] > 0`,
		`data['key'] = 'x'`,
		`E'a\'b' = name`,
		"`order` = 1",
		`price > $1`,
	} {
		if err := ValidateWhereClause(clause); err != nil {
			t.Fatalf("ValidateWhereClause(%q): %v", clause, err)
		}
	}
}

// Linear validators take about 4x as long on 4x the input; a scan that
// restarts for every opening delimiter takes about 16x. Timing ratios rather
// than absolute deadlines keep the test meaningful under -race.
const (
	lexerGrowthBaseBytes = 64 << 10
	lexerGrowthFactor    = 4
	lexerMaxGrowthRatio  = 10
	lexerTimingSlack     = 20 * time.Millisecond
	lexerTimingRuns      = 3
)

func TestSQLValidatorsStayLinearOnAdversarialInput(t *testing.T) {
	for name, build := range map[string]func(n int) string{
		"open brackets":         func(n int) string { return strings.Repeat("[", n) },
		"brackets before close": func(n int) string { return strings.Repeat("[(", n/2) + "]" },
		"dollar tags":           func(n int) string { return strings.Repeat("$a", n/2) },
		"q quotes":              func(n int) string { return strings.Repeat("q'[", n/3) },
		"block comments":        func(n int) string { return strings.Repeat("/*", n/2) },
		"quotes":                func(n int) string { return strings.Repeat("'", n) },
		"parentheses":           func(n int) string { return strings.Repeat("(", n) },
		"words":                 func(n int) string { return strings.Repeat("a ", n/2) },
	} {
		t.Run(name, func(t *testing.T) {
			small := fastestValidation(build(lexerGrowthBaseBytes))
			large := fastestValidation(build(lexerGrowthBaseBytes * lexerGrowthFactor))
			if large > small*lexerMaxGrowthRatio+lexerTimingSlack {
				t.Fatalf("%dx input took %s vs %s: validators are not linear", lexerGrowthFactor, large, small)
			}
		})
	}
}

func fastestValidation(input string) time.Duration {
	best := time.Duration(1<<63 - 1)
	for range lexerTimingRuns {
		start := time.Now()
		_, _ = NormalizeReadOnlySQLQuery("SELECT " + input)
		_ = ValidateWhereClause(input)
		best = min(best, time.Since(start))
	}
	return best
}
