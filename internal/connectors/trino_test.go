package connectors

import (
	"os"
	"sync"
	"testing"
)

func TestQuoteTrinoMultipartIdent(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "table", want: `"table"`},
		{in: "catalog.schema.table", want: `"catalog"."schema"."table"`},
		{in: `"schema"."table"`, want: `"schema"."table"`},
	}
	for _, tc := range tests {
		got, err := quoteTrinoMultipartIdent(tc.in)
		if err != nil {
			t.Fatalf("quoteTrinoMultipartIdent(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("quoteTrinoMultipartIdent(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}

	if _, err := quoteTrinoMultipartIdent(""); err == nil {
		t.Fatal("expected error for empty ident")
	}
}

func TestTrinoDSNIsLocal(t *testing.T) {
	local := []string{
		"http://user@localhost:8080?catalog=test",
		"https://user@127.0.0.1:8443",
	}
	for _, dsn := range local {
		if !trinoDSNIsLocal(dsn) {
			t.Fatalf("expected local dsn: %q", dsn)
		}
	}

	remote := []string{
		"http://user@trino.internal:8080",
		"https://user@10.0.0.1:8443",
	}
	for _, dsn := range remote {
		if trinoDSNIsLocal(dsn) {
			t.Fatalf("expected non-local dsn: %q", dsn)
		}
	}
}

func TestTrinoOrderedRangeReadsEnabled(t *testing.T) {
	prev, hadPrev := os.LookupEnv(envTrinoOrderedReads)
	defer func() {
		if hadPrev {
			_ = os.Setenv(envTrinoOrderedReads, prev)
		} else {
			_ = os.Unsetenv(envTrinoOrderedReads)
		}
		trinoOrderedReadsOnce = sync.Once{}
		trinoOrderedReads = true
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
		trinoOrderedReadsOnce = sync.Once{}
		trinoOrderedReads = true
		if tc.raw == nil {
			_ = os.Unsetenv(envTrinoOrderedReads)
		} else {
			_ = os.Setenv(envTrinoOrderedReads, *tc.raw)
		}
		if got := trinoOrderedRangeReadsEnabled(); got != tc.want {
			val := "<unset>"
			if tc.raw != nil {
				val = *tc.raw
			}
			t.Fatalf("trinoOrderedRangeReadsEnabled(%q)=%v want %v", val, got, tc.want)
		}
	}
}
