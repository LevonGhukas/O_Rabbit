package icebergreg

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

func TestParseIceYAMLIncludesS3Config(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ice.yaml")
	if err := os.WriteFile(path, []byte(`
uri: http://localhost:5001
bearerToken: foo
s3:
  endpoint: http://127.0.0.1:9000
  region: us-east-1
  pathStyleAccess: true
  accessKeyID: minioadmin
  secretAccessKey: minioadmin
`), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := ParseIceYAML(path)
	if err != nil {
		t.Fatalf("ParseIceYAML() error = %v", err)
	}
	if cfg.URI != "http://localhost:5001" {
		t.Fatalf("URI = %q want %q", cfg.URI, "http://localhost:5001")
	}
	if cfg.BearerToken != "foo" {
		t.Fatalf("BearerToken = %q want %q", cfg.BearerToken, "foo")
	}
	if cfg.S3.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("S3.Endpoint = %q want %q", cfg.S3.Endpoint, "http://127.0.0.1:9000")
	}
	if cfg.S3.Region != "us-east-1" {
		t.Fatalf("S3.Region = %q want %q", cfg.S3.Region, "us-east-1")
	}
	if cfg.S3.PathStyleAccess == nil || !*cfg.S3.PathStyleAccess {
		t.Fatalf("S3.PathStyleAccess = %v want true", cfg.S3.PathStyleAccess)
	}
	if cfg.S3.AccessKeyID != "minioadmin" {
		t.Fatalf("S3.AccessKeyID = %q want %q", cfg.S3.AccessKeyID, "minioadmin")
	}
	if cfg.S3.SecretAccessKey != "minioadmin" {
		t.Fatalf("S3.SecretAccessKey = %q want %q", cfg.S3.SecretAccessKey, "minioadmin")
	}
}

func TestParseIceYAMLLeavesOptionalS3PathStyleUnsetWhenMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ice.yaml")
	if err := os.WriteFile(path, []byte(`
uri: http://localhost:5001
s3:
  endpoint: http://127.0.0.1:9000
`), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}

	cfg, err := ParseIceYAML(path)
	if err != nil {
		t.Fatalf("ParseIceYAML() error = %v", err)
	}
	if cfg.S3.PathStyleAccess != nil {
		t.Fatalf("S3.PathStyleAccess = %v want nil", cfg.S3.PathStyleAccess)
	}
}

func TestParseJobConfigReadsNestedJobOptions(t *testing.T) {
	raw, err := json.Marshal(map[string]any{
		"table": "SalesDB.dbo.Orders",
		JobOptionsKey: map[string]any{
			"enabled": true,
			"engine":  "rest-go",
			"table":   "mssql.Orders",
		},
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	cfg, err := ParseJobConfig(raw)
	if err != nil {
		t.Fatalf("ParseJobConfig: %v", err)
	}
	if !cfg.Enabled {
		t.Fatalf("Enabled=%v want true", cfg.Enabled)
	}
	if cfg.Engine != "rest-go" {
		t.Fatalf("Engine=%q want %q", cfg.Engine, "rest-go")
	}
	if cfg.Table != "mssql.Orders" {
		t.Fatalf("Table=%q want %q", cfg.Table, "mssql.Orders")
	}
}

func TestParseRunConfigReadsResolvedSnapshot(t *testing.T) {
	raw := json.RawMessage(`{
		"enabled": true,
		"engine": "rest-go",
		"table": "mssql.orders",
		"uri": "http://catalog:8181",
		"bearer_token": "token",
		"config_yaml": "uri: http://catalog:8181\nbearerToken: token\n",
		"s3": {
			"endpoint": "http://minio:9000",
			"region": "us-east-1",
			"path_style_access": true,
			"access_key_id": "minioadmin",
			"secret_access_key": "minioadmin"
		}
	}`)
	cfg, err := ParseRunConfig(raw)
	if err != nil {
		t.Fatalf("ParseRunConfig: %v", err)
	}
	if cfg.URI != "http://catalog:8181" {
		t.Fatalf("URI=%q want %q", cfg.URI, "http://catalog:8181")
	}
	if cfg.ConfigYAML != "uri: http://catalog:8181\nbearerToken: token\n" {
		t.Fatalf("ConfigYAML=%q", cfg.ConfigYAML)
	}
	if cfg.S3.Endpoint != "http://minio:9000" {
		t.Fatalf("S3.Endpoint=%q want %q", cfg.S3.Endpoint, "http://minio:9000")
	}
}

func TestResolveRunConfigUsesResolvedRegistrationS3Overrides(t *testing.T) {
	pathStyle := true
	cfg := ResolveRunConfig(true, "rest-go", "mssql.orders", s3io.Config{
		Endpoint:        "http://minio:9000",
		Region:          "docker-region",
		ForcePathStyle:  false,
		AccessKeyID:     "docker-user",
		SecretAccessKey: "docker-secret",
	}, IceYAML{
		URI:         "http://catalog:8181",
		BearerToken: "token",
		S3: IceS3{
			Endpoint:        "http://127.0.0.1:9000",
			Region:          "us-east-1",
			PathStyleAccess: &pathStyle,
			AccessKeyID:     "host-user",
			SecretAccessKey: "host-secret",
		},
	})
	if cfg.S3.Endpoint != "http://127.0.0.1:9000" {
		t.Fatalf("S3.Endpoint=%q want %q", cfg.S3.Endpoint, "http://127.0.0.1:9000")
	}
	if cfg.S3.AccessKeyID != "host-user" {
		t.Fatalf("S3.AccessKeyID=%q want %q", cfg.S3.AccessKeyID, "host-user")
	}
	if !cfg.S3.PathStyleAccess {
		t.Fatalf("S3.PathStyleAccess=%v want true", cfg.S3.PathStyleAccess)
	}
}

func TestMergeJobConfigDeletesDisabledConfig(t *testing.T) {
	options := map[string]any{
		JobOptionsKey: map[string]any{
			"enabled": true,
		},
	}
	got := MergeJobConfig(options, JobConfig{})
	if _, ok := got[JobOptionsKey]; ok {
		t.Fatalf("expected disabled config to be removed, got %#v", got[JobOptionsKey])
	}
}

func TestDefaultTableUsesSourceEngineAndLeafTableName(t *testing.T) {
	if got := DefaultTable("mssql", "SalesDB.dbo.Orders"); got != "mssql.SalesDB__dbo__Orders" {
		t.Fatalf("DefaultTable()=%q want %q", got, "mssql.SalesDB__dbo__Orders")
	}
}

func TestDefaultTableFromDiscoveredStoragePrefixStripsFinalMetadata(t *testing.T) {
	if got := DefaultTable("postgres", "postgres/public__demo_orders/metadata/"); got != "postgres.public__demo_orders" {
		t.Fatalf("DefaultTable()=%q want %q", got, "postgres.public__demo_orders")
	}
}

func TestDefaultTableFromDiscoveredStoragePrefixKeepsMetadataTableNames(t *testing.T) {
	if got := DefaultTable("postgres", "postgres/metadata_orders"); got != "postgres.metadata_orders" {
		t.Fatalf("DefaultTable()=%q want %q", got, "postgres.metadata_orders")
	}
}

func TestParsePartNumSupportsRolledTaskSuffixes(t *testing.T) {
	tests := []struct {
		key  string
		want int
		ok   bool
	}{
		{key: "exports/orders/part-000123.parquet", want: 123, ok: true},
		{key: "exports/orders/part-000123-001.parquet", want: 123, ok: true},
		{key: "exports/orders/part-000123-bad.parquet", want: 0, ok: false},
	}
	for _, tt := range tests {
		got, ok := parsePartNum(tt.key)
		if ok != tt.ok || got != tt.want {
			t.Fatalf("parsePartNum(%q)=(%d,%v) want (%d,%v)", tt.key, got, ok, tt.want, tt.ok)
		}
	}
}

func TestParsePartFileIndexSupportsBaseAndRolledFiles(t *testing.T) {
	tests := []struct {
		key  string
		want int
		ok   bool
	}{
		{key: "exports/orders/part-000123.parquet", want: 0, ok: true},
		{key: "exports/orders/part-000123-001.parquet", want: 1, ok: true},
		{key: "exports/orders/part-000123-010.parquet", want: 10, ok: true},
		{key: "exports/orders/part-000123-bad.parquet", want: 0, ok: false},
	}
	for _, tt := range tests {
		got, ok := parsePartFileIndex(tt.key)
		if ok != tt.ok || got != tt.want {
			t.Fatalf("parsePartFileIndex(%q)=(%d,%v) want (%d,%v)", tt.key, got, ok, tt.want, tt.ok)
		}
	}
}

func TestRestGoTableLocation(t *testing.T) {
	got := restGoTableLocation("bucket1", "/postgres/public__demo_orders/")
	want := "s3://bucket1/postgres/public__demo_orders"
	if got != want {
		t.Fatalf("restGoTableLocation()=%q want %q", got, want)
	}
}

func TestIsBrokenRESTMetadataErr(t *testing.T) {
	tableLoc := "s3://bucket1/postgres/public__demo_orders"
	// err := errors.New("NotFoundException: Location does not exist: s3://bucket1/postgres/public__demo_orders/metadata/00001-abc.metadata.json")
	err := errors.New("NotFoundException: Location does not exist: s3://bucket1/postgres/public__demo_orders/metadata/00001-abc.json")

	if !isBrokenRESTMetadataErr(err, tableLoc) {
		t.Fatalf("expected broken metadata error to match")
	}
}

func TestDescribeSourceSchemaForAutoCreateTableModeUsesSourceTable(t *testing.T) {
	reader := &fakeAutoCreateSchemaReader{tableCols: []string{"id", "name"}}
	cols, _, err := describeSourceSchemaForAutoCreate(context.Background(), reader, RunRequest{
		SourceMode:  "table",
		SourceTable: "public.orders",
		SourceQuery: "SELECT id, name FROM public.orders",
	})
	if err != nil {
		t.Fatalf("describeSourceSchemaForAutoCreate: %v", err)
	}
	if strings.Join(cols, ",") != "id,name" {
		t.Fatalf("cols=%v", cols)
	}
	if reader.describeTableCalls != 1 || reader.lastTable != "public.orders" {
		t.Fatalf("DescribeTable calls=%d table=%q", reader.describeTableCalls, reader.lastTable)
	}
	if reader.describeQueryCalls != 0 {
		t.Fatalf("DescribeQuery calls=%d want 0", reader.describeQueryCalls)
	}
}

func TestDescribeSourceSchemaForAutoCreateQueryModeUsesSourceQuery(t *testing.T) {
	query := "SELECT id, name FROM public.customers"
	reader := &fakeAutoCreateSchemaReader{queryCols: []string{"id", "name"}}
	cols, _, err := describeSourceSchemaForAutoCreate(context.Background(), reader, RunRequest{
		SourceMode:  "query",
		SourceTable: "",
		SourceQuery: query,
		QueryHash:   connectors.QueryHash(query),
	})
	if err != nil {
		t.Fatalf("describeSourceSchemaForAutoCreate: %v", err)
	}
	if strings.Join(cols, ",") != "id,name" {
		t.Fatalf("cols=%v", cols)
	}
	if reader.describeQueryCalls != 1 || reader.lastQuery != query {
		t.Fatalf("DescribeQuery calls=%d query=%q", reader.describeQueryCalls, reader.lastQuery)
	}
	if reader.describeTableCalls != 0 {
		t.Fatalf("DescribeTable calls=%d want 0", reader.describeTableCalls)
	}
}

func TestDescribeSourceSchemaForAutoCreateQueryModeEmptyQueryFailsClearly(t *testing.T) {
	reader := &fakeAutoCreateSchemaReader{}
	_, _, err := describeSourceSchemaForAutoCreate(context.Background(), reader, RunRequest{
		SourceMode:  "query",
		SourceTable: "",
	})
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "cannot infer Iceberg schema for query-mode run") {
		t.Fatalf("error=%q", msg)
	}
	if strings.Contains(msg, "empty identifier") {
		t.Fatalf("error should not expose table identifier failure: %q", msg)
	}
	if reader.describeTableCalls != 0 {
		t.Fatalf("DescribeTable calls=%d want 0", reader.describeTableCalls)
	}
}

type fakeAutoCreateSchemaReader struct {
	tableCols          []string
	queryCols          []string
	describeTableCalls int
	describeQueryCalls int
	lastTable          string
	lastQuery          string
}

func (f *fakeAutoCreateSchemaReader) Close() error { return nil }

func (f *fakeAutoCreateSchemaReader) DescribeTable(_ context.Context, table string) ([]string, []*sql.ColumnType, error) {
	f.describeTableCalls++
	f.lastTable = table
	return f.tableCols, nil, nil
}

func (f *fakeAutoCreateSchemaReader) DescribeQuery(_ context.Context, query string) ([]string, []*sql.ColumnType, error) {
	f.describeQueryCalls++
	f.lastQuery = query
	return f.queryCols, nil, nil
}

func (f *fakeAutoCreateSchemaReader) QueryCursor(context.Context, connectors.CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	return nil, nil, nil, -1, nil
}

func (f *fakeAutoCreateSchemaReader) DiscoverCursorStats(context.Context, string, string, connectors.CursorDomain) (connectors.CursorStats, error) {
	return connectors.CursorStats{}, nil
}

func (f *fakeAutoCreateSchemaReader) ValidateCursorColumn(context.Context, string, string) (connectors.CursorColumnValidation, error) {
	return connectors.CursorColumnValidation{}, nil
}

func (f *fakeAutoCreateSchemaReader) DiscoverQueryCursorStats(context.Context, string, string, connectors.CursorDomain) (connectors.CursorStats, error) {
	return connectors.CursorStats{}, nil
}

func (f *fakeAutoCreateSchemaReader) ValidateQueryCursorColumn(context.Context, string, string) (connectors.CursorColumnValidation, error) {
	return connectors.CursorColumnValidation{}, nil
}
