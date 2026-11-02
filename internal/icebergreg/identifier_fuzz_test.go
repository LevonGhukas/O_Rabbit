package icebergreg

import (
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

const fuzzMaxIdentifierBytes = 4 << 10

func TestValidateTableIdentifierRejectsEscapes(t *testing.T) {
	for _, table := range []string{
		"",
		"orders",
		"ns.",
		".orders",
		"ns..orders",
		"ns/../x.orders",
		`ns\x.orders`,
		"ns.orders\x00",
		"ns\x1fother.orders",
		"ns.or\nders",
		"ns.\xff",
	} {
		if err := validateTableIdentifier(table); err == nil {
			t.Fatalf("validateTableIdentifier(%q) accepted", table)
		}
	}
	for _, table := range []string{"mssql.orders", " analytics . orders ", "a.b.c", "ns.Заказы", "ns.order-items"} {
		if err := validateTableIdentifier(table); err != nil {
			t.Fatalf("validateTableIdentifier(%q): %v", table, err)
		}
	}
}

func FuzzValidateTableIdentifier(f *testing.F) {
	for _, seed := range []string{
		"mssql.orders", "a.b.c", "ns.", "..", "ns/../x.t", `a\b.c`, "a.b\x00",
		"a\x1fb.c", "ns.Заказы", "ns.\xff", " a . b ", strings.Repeat("a.", 1024),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, table string) {
		if len(table) > fuzzMaxIdentifierBytes {
			t.Skip()
		}
		if validateTableIdentifier(table) != nil {
			return
		}
		parts := strings.Split(strings.TrimSpace(table), ".")
		if len(parts) < 2 {
			t.Fatalf("accepted %q without a namespace", table)
		}
		for _, part := range parts {
			if strings.TrimSpace(part) == "" {
				t.Fatalf("accepted %q with an empty segment", table)
			}
		}
		if !utf8.ValidString(table) || strings.ContainsAny(table, `/\`) || strings.IndexFunc(table, unicode.IsControl) >= 0 {
			t.Fatalf("accepted unsafe identifier %q", table)
		}
	})
}

// FuzzDefaultTableIsValid guards against tightening the identifier check
// beyond what O_Rabbit itself generates for a source table.
func FuzzDefaultTableIsValid(f *testing.F) {
	for _, seed := range []struct{ engine, table string }{
		{"mssql", "SalesDB.dbo.Orders"}, {"postgres", "[a].[b]"}, {"mysql", "`x`.`y`"},
		{"", ""}, {"oracle", "warehouse/ns/table/metadata"}, {"s3", "bucket/ü ✓/t"},
	} {
		f.Add(seed.engine, seed.table)
	}
	f.Fuzz(func(t *testing.T, engine, table string) {
		if len(engine)+len(table) > fuzzMaxIdentifierBytes {
			t.Skip()
		}
		got := DefaultTable(canonicalFuzzEngine(engine), table)
		if err := validateTableIdentifier(got); err != nil {
			t.Fatalf("DefaultTable(%q, %q)=%q rejected: %v", engine, table, got, err)
		}
	})
}

// canonicalFuzzEngine maps arbitrary fuzz text onto a real engine name: the
// API rejects unknown engines before a table name is ever derived.
func canonicalFuzzEngine(raw string) string {
	engines := []string{"mssql", "postgres", "mysql", "mariadb", "oracle", "clickhouse", "trino", "s3", ""}
	return engines[len(raw)%len(engines)]
}
