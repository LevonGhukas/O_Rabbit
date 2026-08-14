package connectors

import (
	"os"
	"sync"
	"testing"
)

func TestQuotePostgresMultipartIdent(t *testing.T) {
	got, err := quotePostgresMultipartIdent(`public.wide_table`)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if got != `"public"."wide_table"` {
		t.Fatalf("unexpected quoted ident: %q", got)
	}

	if _, err := quotePostgresMultipartIdent(`public.wide-table`); err == nil {
		t.Fatalf("expected unsafe identifier error")
	}
}

func TestSplitPostgresTableIdent(t *testing.T) {
	tests := []struct {
		in         string
		wantSchema string
		wantTable  string
	}{
		{in: "public.wide_table", wantSchema: "public", wantTable: "wide_table"},
		{in: `"public"."wide_table"`, wantSchema: "public", wantTable: "wide_table"},
		{in: "wide_table", wantSchema: "public", wantTable: "wide_table"},
	}
	for _, tc := range tests {
		s, tname := splitPostgresTableIdent(tc.in)
		if s != tc.wantSchema || tname != tc.wantTable {
			t.Fatalf("splitPostgresTableIdent(%q)=(%q,%q), want (%q,%q)", tc.in, s, tname, tc.wantSchema, tc.wantTable)
		}
	}
}

func TestPostgresDSNIsLocal(t *testing.T) {
	local := []string{
		"postgresql://postgres:postgres@localhost:5432/appdb?sslmode=disable",
		"postgres://postgres:postgres@127.0.0.1:5432/appdb",
		"host=localhost port=5432 user=postgres password=postgres dbname=appdb sslmode=disable",
	}
	for _, dsn := range local {
		if !postgresDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"postgresql://postgres:postgres@db.internal:5432/appdb?sslmode=disable",
		"host=db.internal port=5432 user=postgres password=postgres dbname=appdb sslmode=disable",
	}
	for _, dsn := range remote {
		if postgresDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestPostgresOrderedRangeReadsEnabled(t *testing.T) {
	prev, hadPrev := os.LookupEnv(envPostgresOrderedReads)
	defer func() {
		if hadPrev {
			_ = os.Setenv(envPostgresOrderedReads, prev)
		} else {
			_ = os.Unsetenv(envPostgresOrderedReads)
		}
		postgresOrderedReadsOnce = sync.Once{}
		postgresOrderedReads = true
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
		postgresOrderedReadsOnce = sync.Once{}
		postgresOrderedReads = true
		if tc.raw == nil {
			_ = os.Unsetenv(envPostgresOrderedReads)
		} else {
			_ = os.Setenv(envPostgresOrderedReads, *tc.raw)
		}
		if got := postgresOrderedRangeReadsEnabled(); got != tc.want {
			val := "<unset>"
			if tc.raw != nil {
				val = *tc.raw
			}
			t.Fatalf("postgresOrderedRangeReadsEnabled(%q)=%v want %v", val, got, tc.want)
		}
	}
}

func strPtr(v string) *string { return &v }
