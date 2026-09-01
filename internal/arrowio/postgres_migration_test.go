package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestLogicalTypeForPostgresColumn(t *testing.T) {
	for _, test := range []struct {
		source           string
		precision, scale int64
		hasDecimal       bool
		want             typesystem.LogicalType
	}{
		{"INT2", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt16}},
		{"INT4", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt32}},
		{"INT8", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt64}},
		{"FLOAT4", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat32}},
		{"FLOAT8", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat64}},
		{"NUMERIC", 18, 2, true, typesystem.Decimal(18, 2)},
		{"MONEY", 0, 0, false, typesystem.Decimal(19, 2)},
		{"BOOL", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBool}},
		{"DATE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindDate}},
		{"TIMESTAMP", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}},
		{"TIMESTAMPTZ", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}},
		{"BYTEA", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}},
		{"UUID", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindUUID}},
		{"JSONB", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindJSON}},
		{"TEXT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}},
		{"INTEGER[]", 0, 0, false, typesystem.ArrayOf(typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindInt32}))},
		{"UUID[]", 0, 0, false, typesystem.ArrayOf(typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindUUID}))},
	} {
		t.Run(test.source, func(t *testing.T) {
			got, err := LogicalTypeForPostgresColumn(test.source, test.precision, test.scale, test.hasDecimal)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want))
		})
	}
	for _, source := range []string{"INET", "TIMETZ", "VARBIT", "MY_EXTENSION_TYPE"} {
		got, err := LogicalTypeForPostgresColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, got.Kind)
		require.Equal(t, source, got.SourceTypeName)
	}
	got, err := LogicalTypeForPostgresColumn("NUMERIC", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, typesystem.KindUnknown, got.Kind)
}

func TestPostgresPlansUseSharedConversionAndFallbacks(t *testing.T) {
	for _, test := range []struct {
		source string
		value  any
	}{
		{"INT2", int32(32768)}, {"INT4", int64(2147483648)}, {"BOOL", "not-a-boolean"},
		{"DATE", "not-a-date"}, {"TIME", "25:00:00"}, {"TIMESTAMP", "not-a-timestamp"}, {"NUMERIC", "not-a-decimal"},
	} {
		t.Run(test.source, func(t *testing.T) {
			plan := PlanForSQLColumn("postgres", "value", test.source, 18, 2, test.source == "NUMERIC")
			builder := plan.Builder(memory.DefaultAllocator)
			defer builder.Release()
			require.Error(t, plan.Append(builder, test.value))
		})
	}

	arrayPlan := PlanForSQLColumn("postgres", "items", "INTEGER[]", 0, 0, false)
	builder := arrayPlan.Builder(memory.DefaultAllocator)
	defer builder.Release()
	require.NoError(t, arrayPlan.Append(builder, "{-2147483648,NULL,2147483647}"))
	require.Error(t, arrayPlan.Append(builder, "{1,2147483648}"))

	for _, test := range []struct {
		source string
		raw    any
		want   string
		class  typesystem.MappingClass
	}{
		{"UUID", "A0B1C2D3-E4F5-6789-ABCD-0123456789AB", "a0b1c2d3-e4f5-6789-abcd-0123456789ab", typesystem.MappingSemanticFallback},
		{"MY_EXTENSION_TYPE", "opaque", "opaque", typesystem.MappingUnsupportedFallback},
	} {
		t.Run(test.source, func(t *testing.T) {
			logical, err := LogicalTypeForPostgresColumn(test.source, 0, 0, false)
			require.NoError(t, err)
			plan, mapping, err := PlanForLogicalType("value", logical)
			require.NoError(t, err)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
			require.True(t, mapping.Fallback)
			require.Equal(t, test.class, mapping.Class)
			builder := plan.Builder(memory.DefaultAllocator)
			defer builder.Release()
			require.NoError(t, plan.Append(builder, test.raw))
			values := builder.NewArray().(*array.String)
			defer values.Release()
			require.Equal(t, test.want, values.Value(0))
		})
	}
}

func TestPostgresGeneratedSchemaMapsToIceberg(t *testing.T) {
	columns := []struct {
		name, source     string
		precision, scale int64
		decimal          bool
	}{
		{"i32", "INT4", 0, 0, false}, {"i64", "INT8", 0, 0, false}, {"dec", "NUMERIC", 18, 2, true},
		{"ts", "TIMESTAMP", 0, 0, false}, {"tstz", "TIMESTAMPTZ", 0, 0, false}, {"uuid", "UUID", 0, 0, false},
		{"json", "JSONB", 0, 0, false}, {"unknown", "MY_EXTENSION_TYPE", 0, 0, false}, {"items", "INTEGER[]", 0, 0, false},
	}
	fields := make([]arrow.Field, 0, len(columns))
	for _, column := range columns {
		plan := PlanForSQLColumn("postgres", column.name, column.source, column.precision, column.scale, column.decimal)
		fields = append(fields, arrow.Field{Name: plan.Name, Type: plan.DataType, Nullable: true})
	}
	schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema(fields, nil), false)
	require.NoError(t, err)
	got := make(map[string]string)
	for _, field := range schema.Fields() {
		got[field.Name] = field.Type.String()
	}
	require.Equal(t, "int", got["i32"])
	require.Equal(t, "long", got["i64"])
	require.Equal(t, "decimal(18, 2)", got["dec"])
	require.Equal(t, "timestamp", got["ts"])
	require.Equal(t, "timestamptz", got["tstz"])
	require.Equal(t, "string", got["uuid"])
	require.Equal(t, "string", got["json"])
	require.Equal(t, "string", got["unknown"])
	require.Equal(t, "list<int>", got["items"])

}
