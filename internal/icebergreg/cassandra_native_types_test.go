package icebergreg

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// cassandraColumn is one column as the worker plans it: from the CQL type,
// or from the effective type the planner chose (decimal scale, native uuid).
type cassandraColumn struct {
	name, cqlType, effective string
	values                   []any
	iceberg                  string
}

// TID-945: Cassandra decimal, varint, uuid, timeuuid, map and tuple columns
// are written as native Parquet types, become native Iceberg types, and keep
// every value, including nulls, boundaries and nesting.
func TestCassandraNativeTypesReachIcebergLosslessly(t *testing.T) {
	maxVarint := strings.Repeat("9", 38)
	cols := []cassandraColumn{
		{"balance", "decimal", "nullable<decimal(38,3)>",
			[]any{"123.45", "-12345678901234567890123456789012345.678", nil, "0.001"}, "decimal(38, 3)"},
		{"big", "varint", "",
			[]any{maxVarint, "-" + maxVarint, nil, "0"}, "decimal(38, 0)"},
		{"id", "uuid", "nullable<uuid>",
			[]any{"123e4567-e89b-12d3-a456-426614174000", "00000000-0000-0000-0000-000000000000", nil, "ffffffff-ffff-ffff-ffff-ffffffffffff"}, "uuid"},
		{"event", "timeuuid", "nullable<uuid>",
			[]any{"5b6962dd-3f90-11ee-8000-000000000000", nil, nil, "5b6962dd-3f90-11ee-8000-000000000001"}, "uuid"},
		{"scores", "map<text, int>", "",
			[]any{typesystem.MapValue{{Key: "a", Value: 1}, {Key: "b", Value: nil}}, typesystem.MapValue{}, nil, typesystem.MapValue{{Key: "max", Value: 2147483647}}}, "map<string, int>"},
		{"pair", "tuple<int, text>", "",
			[]any{typesystem.TupleValue{1, "x"}, typesystem.TupleValue{nil, "only text"}, nil, typesystem.TupleValue{-2147483648, nil}}, "struct<f1: int, f2: string>"},
		{"history", "list<frozen<map<text, varint>>>", "",
			[]any{[]any{typesystem.MapValue{{Key: "k", Value: "1"}}}, []any{}, nil, []any{typesystem.MapValue{{Key: "k", Value: "-" + maxVarint}}, nil}}, "list<map<string, decimal(38, 0)>>"},
	}

	fields := make([]arrow.Field, len(cols))
	plans := make([]arrowio.ColumnPlan, len(cols))
	for i, c := range cols {
		logical, err := arrowio.LogicalTypeForCassandraColumn(c.cqlType, 0, 0, false)
		require.NoError(t, err)
		logical.Nullable = true
		if c.effective != "" {
			logical, err = typesystem.ResolveOverride(c.effective, logical)
			require.NoError(t, err)
		}
		plan, _, err := arrowio.PlanForOverride(c.name, c.effective, logical)
		require.NoError(t, err, c.name)
		plans[i] = plan
		fields[i] = arrow.Field{Name: c.name, Type: plan.DataType, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	for row := 0; row < 4; row++ {
		for i, c := range cols {
			require.NoError(t, plans[i].Append(builder.Field(i), c.values[row]), "%s row %d", c.name, row)
		}
	}
	rec := builder.NewRecordBatch()
	defer rec.Release()

	// Write the file the way the worker does, then read it back.
	w, path, err := parquetio.NewTempFileWriterInDir(schema, parquetio.Options{Compression: compress.Codecs.Snappy}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, w.Write(rec))
	require.NoError(t, w.Close())
	reader, err := file.OpenParquetFile(path, false)
	require.NoError(t, err)
	defer reader.Close()
	fr, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	require.NoError(t, err)
	tbl, err := fr.ReadTable(context.Background())
	require.NoError(t, err)
	defer tbl.Release()

	// Registration derives the Iceberg schema from the file's Arrow schema.
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(tbl.Schema(), false)
	require.NoError(t, err)
	for i, c := range cols {
		require.Equal(t, c.iceberg, stripFieldIDs(iceSchema.Fields()[i].Type.String()), c.name)
	}

	// Every value survives the round trip unchanged.
	readBack := array.NewTableReader(tbl, 4)
	defer readBack.Release()
	require.True(t, readBack.Next())
	got := readBack.RecordBatch()
	for i, c := range cols {
		require.Equal(t, rec.Column(i).String(), got.Column(i).String(), c.name)
	}
	balance := got.Column(0)
	require.True(t, balance.IsNull(2))
	require.Equal(t, "-12345678901234567890123456789012345.678", balance.ValueStr(1))
	require.Equal(t, maxVarint, got.Column(1).ValueStr(0))
}

// fieldIDPattern matches the "2: " field ids and optional/required markers
// iceberg-go prints inside nested types.
var fieldIDPattern = regexp.MustCompile(`([<,] ?)\d+: (\w+): (?:optional |required )?`)

// stripFieldIDs turns "struct<2: f1: optional int>" into "struct<f1: int>" so
// the expectation reads like a type.
func stripFieldIDs(s string) string { return fieldIDPattern.ReplaceAllString(s, "${1}${2}: ") }

// Keeping an existing table's column type only works if writing that type
// produces exactly the table's Iceberg type again.
func TestLogicalTypeForIcebergWritesTheSameIcebergType(t *testing.T) {
	for _, spelling := range []string{
		"string", "nullable<string>", "int32", "int64", "float64", "bool", "date", "binary",
		"timestamp_tz[UTC]", "decimal(38,3)", "decimal(38,0)",
		"array<nullable<string>>", "map<string,nullable<int32>>",
		"struct<f1:nullable<int32>,f2:nullable<string>>",
		"array<nullable<map<string,nullable<decimal(38,0)>>>>",
	} {
		logical, err := typesystem.ParseType(spelling)
		require.NoError(t, err)
		plan, _, err := arrowio.PlanForOverride("c", spelling, logical)
		require.NoError(t, err)
		schema := arrow.NewSchema([]arrow.Field{{Name: "c", Type: plan.DataType, Nullable: logical.Nullable}}, nil)
		ice, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
		require.NoError(t, err)
		field := ice.Fields()[0]

		back, ok := LogicalTypeForIceberg(field.Type, !field.Required)
		require.True(t, ok, spelling)
		backPlan, _, err := arrowio.PlanForOverride("c", back.String(), back)
		require.NoError(t, err)
		again, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema([]arrow.Field{{Name: "c", Type: backPlan.DataType, Nullable: back.Nullable}}, nil), false)
		require.NoError(t, err)
		require.Equal(t, field.Type.String(), again.Fields()[0].Type.String(), spelling)
		require.Equal(t, field.Required, again.Fields()[0].Required, spelling)
	}

	// A native UUID column round-trips as a native UUID.
	uuid := typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindUUID})
	plan, _, err := arrowio.PlanForOverride("c", uuid.String(), uuid)
	require.NoError(t, err)
	ice, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema([]arrow.Field{{Name: "c", Type: plan.DataType, Nullable: true}}, nil), false)
	require.NoError(t, err)
	back, ok := LogicalTypeForIceberg(ice.Fields()[0].Type, true)
	require.True(t, ok)
	require.Equal(t, "nullable<uuid>", back.String())
}
