//go:build integration

package icebergreg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// TestTypeCompatibilityRESTAndClickHouse is opt-in because it mutates the
// dedicated compatibility catalog. It uses the same REST-Go registration
// function as Manager and the same DataLakeCatalog configuration as compose.
func TestTypeCompatibilityRESTAndClickHouse(t *testing.T) {
	if os.Getenv("ORABBIT_RUN_INTEGRATION") != "1" {
		t.Skip("set ORABBIT_RUN_INTEGRATION=1 and use -tags=integration")
	}
	ctx := context.Background()
	s3cfg := s3io.Config{Endpoint: "http://127.0.0.1:9000", Region: "us-east-1", Bucket: "bucket1", ForcePathStyle: true, AccessKeyID: "minioadmin", SecretAccessKey: "minioadmin"}
	uploader, err := s3io.New(ctx, s3cfg)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		dt     arrow.DataType
		values string
	}{
		{"bool", arrow.FixedWidthTypes.Boolean, `[true,false]`},
		{"int32", arrow.PrimitiveTypes.Int32, `[-2147483648,2147483647]`},
		{"int64", arrow.PrimitiveTypes.Int64, `[-9223372036854775808,9223372036854775807]`},
		{"uint32", arrow.PrimitiveTypes.Uint32, `[4294967295]`},
		{"uint64", arrow.PrimitiveTypes.Uint64, `[18446744073709551615]`},
		{"float32", arrow.PrimitiveTypes.Float32, `[-1.5,3.25]`},
		{"float64", arrow.PrimitiveTypes.Float64, `[-1.5,3.25]`},
		{"string", arrow.BinaryTypes.String, `["postgres-uuid-text:550e8400-e29b-41d4-a716-446655440000"]`},
		{"binary", arrow.BinaryTypes.Binary, `["AAH/gP8="]`},
		{"decimal10_2", &arrow.Decimal128Type{Precision: 10, Scale: 2}, `["-99999999.99"]`},
		{"decimal38_0", &arrow.Decimal128Type{Precision: 38, Scale: 0}, `["-99999999999999999999999999999999999999"]`},
		{"date32", arrow.PrimitiveTypes.Date32, `["1800-01-02"]`},
		{"timestamp_us", &arrow.TimestampType{Unit: arrow.Microsecond}, `["2300-12-30T23:59:59.654321"]`},
		{"timestamp_ms", &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, `["1970-01-01T00:00:00.123Z"]`},
		{"time64_us", arrow.FixedWidthTypes.Time64us, `["23:59:59.999999"]`},
		{"list", arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.PrimitiveTypes.Int32, Nullable: true}), `[[1,null,3],[]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arr, _, err := array.FromJSON(memory.DefaultAllocator, tc.dt, bytes.NewBufferString(tc.values))
			if err != nil {
				t.Fatal(err)
			}
			defer arr.Release()
			schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: tc.dt, Nullable: true}}, nil)
			record := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
			defer record.Release()
			writer, path, err := parquetio.NewTempFileWriterInDir(schema, parquetio.Options{}, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := writer.Write(record); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			prefix := fmt.Sprintf("type-compatibility/%s/%d", tc.name, time.Now().UnixNano())
			key := prefix + "/part-000001-000.parquet"
			if err := uploader.PutObjectBytes(ctx, key, data, "application/vnd.apache.parquet", nil); err != nil {
				t.Fatal(err)
			}
			iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
			if err != nil {
				t.Fatal(err)
			}
			durable, err := json.Marshal(iceSchema)
			if err != nil {
				t.Fatal(err)
			}
			table := "compat." + tc.name
			req := RunRequest{RunID: "type-compatibility-" + tc.name, RegistrationID: "type-compatibility", CommitID: "type-compatibility-" + tc.name, ArtifactSetDigest: strings.Repeat("a", 64), SourceEngine: "postgres", SourceMode: "query", Incremental: true, DatasetPrefix: prefix, DatasetS3: s3cfg, DurableIcebergSchema: durable}
			reg := RunConfig{Enabled: true, Engine: "rest-go", Table: table, URI: "http://127.0.0.1:5001", BearerToken: "foo"}
			if err := runRESTGoRegister(ctx, slog.Default(), req, reg, table, s3cfg, prefix, []icebergObj{{key: key, part: 1, rows: int64(arr.Len()), bytes: int64(len(data))}}); err != nil {
				t.Fatalf("REST registration: %v", err)
			}
			cat, ident, err := openRESTCatalog(ctx, req, reg, s3cfg, table)
			if err != nil {
				t.Fatal(err)
			}
			loaded, err := cat.LoadTable(ctx, ident)
			if err != nil {
				t.Fatalf("read registered table: %v", err)
			}
			if _, ok := loaded.Schema().FindFieldByName("value"); !ok {
				t.Fatal("registered schema lost value")
			}

			chTable := "ice.`compat." + tc.name + "`"
			describe := clickHouseQuery(t, "DESCRIBE TABLE "+chTable+" FORMAT TSV")
			values := clickHouseQuery(t, "SELECT value FROM "+chTable+" FORMAT TSV")
			t.Logf("DESCRIBE: %s", describe)
			t.Logf("VALUES: %s", values)
		})
	}
}

func clickHouseQuery(t *testing.T, query string) string {
	t.Helper()
	u := "http://default:default@127.0.0.1:8123/?query=" + url.QueryEscape(query)
	resp, err := http.Get(u)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("ClickHouse %s: %s", resp.Status, body)
	}
	return strings.TrimSpace(string(body))
}
