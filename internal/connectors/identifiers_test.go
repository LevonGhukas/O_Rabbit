package connectors

import "testing"

func TestIdentifierRenderingPreservesLiteralDotsAndEscapesDelimiters(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
		dialect         identifierDialect
	}{
		{"postgres", `a.b`, `"a.b"`, doubleQuoteDialect()},
		{"mysql", "some`column", "`some``column`", backtickDialect()},
		{"mssql", "some]column", "[some]]column]", bracketDialect()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := quoteIdentifierPart(tc.raw, tc.dialect)
			if err != nil || got != tc.want {
				t.Fatalf("quoteIdentifierPart(%q)=%q, %v; want %q", tc.raw, got, err, tc.want)
			}
		})
	}

	got, err := quoteQualifiedIdentifier(`"schema.name"."table.name"`, doubleQuoteDialect())
	if err != nil || got != `"schema.name"."table.name"` {
		t.Fatalf("qualified literal dots=%q, %v", got, err)
	}
}

func TestConnectorRenderersAcceptUnusualIdentifierParts(t *testing.T) {
	identifiers := []string{
		"normal", "first name", "customer-id", "O'Reilly", "O`Rayelly",
		`O"Rayelly`, "O’Rayelly", "O‘Rayelly", "OʼRayelly", "O«Rayelly",
		"O「Rayelly", "日本語「列」", "123column", "select",
	}
	for _, identifier := range identifiers {
		for _, tc := range []struct {
			name  string
			quote func(string) (string, error)
		}{
			{"postgres", func(s string) (string, error) { return quoteIdentifierPart(s, doubleQuoteDialect()) }},
			{"mysql", func(s string) (string, error) { return quoteIdentifierPart(s, backtickDialect()) }},
			{"mariadb", func(s string) (string, error) { return quoteIdentifierPart(s, backtickDialect()) }},
			{"mssql", func(s string) (string, error) { return quoteIdentifierPart(s, bracketDialect()) }},
			{"oracle", quoteOracleCursorIdent},
			{"clickhouse", func(s string) (string, error) { return quoteIdentifierPart(s, backtickDialect()) }},
			{"trino", func(s string) (string, error) { return quoteIdentifierPart(s, doubleQuoteDialect()) }},
			{"cassandra", quoteCassandraIdent},
		} {
			t.Run(tc.name+"/"+identifier, func(t *testing.T) {
				if _, err := tc.quote(identifier); err != nil {
					t.Fatalf("failed to render %q: %v", identifier, err)
				}
			})
		}
	}
}

func TestGeneratedSelectClausesNeverUseRawUnusualColumns(t *testing.T) {
	columns := []string{`O"Rayelly`, "O`Rayelly", "first name", "日本語「列」"}
	for _, tc := range []struct {
		name  string
		build func([]string) string
		want  []string
	}{
		{"postgres", buildPostgresSelectClause, []string{`"O""Rayelly"`, `"O` + "`" + `Rayelly"`}},
		{"mysql", buildMySQLSelectClause, []string{"`O\"Rayelly`", "`O``Rayelly`"}},
		{"mariadb", buildMariaDBSelectClause, []string{"`O\"Rayelly`", "`O``Rayelly`"}},
		{"mssql", buildMSSQLSelectClause, []string{`[O"Rayelly]`, "[O`Rayelly]"}},
		{"oracle", buildOracleSelectClause, []string{`"O""Rayelly"`, `"O` + "`" + `Rayelly"`}},
		{"clickhouse", buildClickHouseSelectClause, []string{"`O\"Rayelly`", "`O``Rayelly`"}},
		{"trino", buildTrinoSelectClause, []string{`"O""Rayelly"`, `"O` + "`" + `Rayelly"`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.build(columns)
			for _, want := range tc.want {
				if !contains(got, want) {
					t.Fatalf("generated select clause %q does not contain %q", got, want)
				}
			}
		})
	}
}

func contains(value, substring string) bool {
	for i := 0; i+len(substring) <= len(value); i++ {
		if value[i:i+len(substring)] == substring {
			return true
		}
	}
	return false
}
