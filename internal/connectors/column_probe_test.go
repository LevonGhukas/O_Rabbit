package connectors

import (
	"strings"
	"testing"
)

func TestBuildColumnProbeSQLPostgres(t *testing.T) {
	got, err := buildColumnProbeSQL("postgres", "SELECT id, amount FROM orders", []ColumnProbe{{Column: "amount", Range: true, Fraction: true, Scale: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`MIN("orabbit_query"."amount")`,
		`COUNT(*) - COUNT("orabbit_query"."amount")`,
		`FLOOR(("orabbit_query"."amount" * 100))`,
		`FROM (SELECT id, amount FROM orders) AS orabbit_query`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("sql %q missing %q", got, want)
		}
	}
}

func TestProbeRatParsesDriverValues(t *testing.T) {
	cases := map[string]any{"42": int64(42), "12345678901234567890": []byte("12345678901234567890"), "1.5": "1.5", "0.25": float64(0.25)}
	for want, in := range cases {
		r := probeRat(in)
		if r == nil || r.RatString() != mustRat(t, want) {
			t.Fatalf("probeRat(%v) = %v, want %s", in, r, want)
		}
	}
	if probeRat(nil) != nil {
		t.Fatal("nil should stay nil")
	}
}

func mustRat(t *testing.T, s string) string {
	t.Helper()
	r := probeRat(s)
	if r == nil {
		t.Fatalf("bad rat %s", s)
	}
	return r.RatString()
}
