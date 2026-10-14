package connectors

import (
	"net/url"
	"testing"
)

func dsnParam(t *testing.T, dsn, key string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse %q: %v", dsn, err)
	}
	return u.Query().Get(key)
}

func TestWithFetchSizeSetsEngineParameter(t *testing.T) {
	tests := []struct {
		engine, dsn, param string
	}{
		{"oracle", "oracle://app:p%40ss@db:1521/FREEPDB1", "PREFETCH_ROWS"},
		{"ora", "oracle://app:pass@db:1521/FREEPDB1?TIMEOUT=30", "PREFETCH_ROWS"},
		{"cassandra", "cassandra://db:9042/ks", "page_size"},
		{"clickhouse", "clickhouse://default:pw@db:9000/default", "max_block_size"},
	}
	for _, tt := range tests {
		got := WithFetchSize(tt.engine, tt.dsn, 20000)
		if v := dsnParam(t, got, tt.param); v != "20000" {
			t.Errorf("%s: %s=%q in %q, want 20000", tt.engine, tt.param, v, got)
		}
	}
	// The password survives re-encoding.
	if u, _ := url.Parse(WithFetchSize("oracle", "oracle://app:p%40ss@db:1521/X", 10)); u.User.String() != "app:p%40ss" {
		t.Errorf("userinfo=%q, want app:p%%40ss", u.User.String())
	}
}

func TestWithFetchSizeReplacesExistingValueCaseInsensitively(t *testing.T) {
	got := WithFetchSize("oracle", "oracle://app:pass@db:1521/X?prefetch_rows=25", 5000)
	u, _ := url.Parse(got)
	if len(u.Query()) != 1 || u.Query().Get("PREFETCH_ROWS") != "5000" {
		t.Fatalf("got %q, want only PREFETCH_ROWS=5000", got)
	}
}

func TestWithFetchSizeLeavesOtherCasesUnchanged(t *testing.T) {
	for _, tc := range []struct {
		engine, dsn string
		n           int
	}{
		{"postgres", "postgres://u:p@db/app", 5000},            // streams; no fetch size
		{"oracle", "oracle://app:pass@db:1521/X", 0},           // not set
		{"oracle", "app/pass@db:1521/X", 5000},                 // not a URL
		{"mongodb", "mongodb://db:27017/app", 5000},            // batch size comes from the worker
		{"cassandra", "cassandra://db:9042/ks?page_size=7", 0}, // keep the DSN's own
	} {
		if got := WithFetchSize(tc.engine, tc.dsn, tc.n); got != tc.dsn {
			t.Errorf("%s n=%d: got %q, want unchanged %q", tc.engine, tc.n, got, tc.dsn)
		}
	}
}

func TestOracleDefaultPrefetchKeepsExplicitValue(t *testing.T) {
	if v := dsnParam(t, oracleDSNWithDefaultPrefetch("oracle://app:pass@db:1521/X"), "PREFETCH_ROWS"); v != "5000" {
		t.Errorf("default PREFETCH_ROWS=%q, want 5000", v)
	}
	explicit := "oracle://app:pass@db:1521/X?Prefetch_Rows=100"
	if got := oracleDSNWithDefaultPrefetch(explicit); got != explicit {
		t.Errorf("got %q, want the DSN's own value kept", got)
	}
}

func TestCassandraDSNPageSize(t *testing.T) {
	parsed, err := parseCassandraDSN("cassandra://db:9042/ks?page_size=20000")
	if err != nil || parsed.pageSize != 20000 {
		t.Fatalf("pageSize=%d err=%v, want 20000", parsed.pageSize, err)
	}
	for _, bad := range []string{"0", "-5", "many"} {
		if _, err := parseCassandraDSN("cassandra://db:9042/ks?page_size=" + bad); err == nil {
			t.Errorf("page_size=%s: want an error", bad)
		}
	}
}

func TestSupportsFetchSize(t *testing.T) {
	for engine, want := range map[string]bool{"oracle": true, "cassandra": true, "clickhouse": true, "mongo": true, "postgres": false, "mysql": false} {
		if got := SupportsFetchSize(engine); got != want {
			t.Errorf("SupportsFetchSize(%s)=%t, want %t", engine, got, want)
		}
	}
}
