package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestLogicalTypeForClickHouseColumn(t *testing.T) {
	for _, test := range []struct {
		source string
		want   typesystem.LogicalType
	}{
		{"UInt8", typesystem.LogicalType{Kind: typesystem.KindUInt8}}, {"UInt16", typesystem.LogicalType{Kind: typesystem.KindUInt16}}, {"UInt32", typesystem.LogicalType{Kind: typesystem.KindUInt32}}, {"UInt64", typesystem.LogicalType{Kind: typesystem.KindUInt64}}, {"Int8", typesystem.LogicalType{Kind: typesystem.KindInt8}}, {"Int16", typesystem.LogicalType{Kind: typesystem.KindInt16}}, {"Int32", typesystem.LogicalType{Kind: typesystem.KindInt32}}, {"Int64", typesystem.LogicalType{Kind: typesystem.KindInt64}}, {"Float32", typesystem.LogicalType{Kind: typesystem.KindFloat32}}, {"Float64", typesystem.LogicalType{Kind: typesystem.KindFloat64}}, {"BFloat16", typesystem.LogicalType{Kind: typesystem.KindFloat32}}, {"Bool", typesystem.LogicalType{Kind: typesystem.KindBool}},
		{"Decimal(38,10)", typesystem.Decimal(38, 10)}, {"Decimal32(2)", typesystem.Decimal(9, 2)}, {"Decimal64(4)", typesystem.Decimal(18, 4)}, {"Decimal128(6)", typesystem.Decimal(38, 6)}, {"Decimal256(10)", typesystem.Decimal(76, 10)}, {"Date", typesystem.LogicalType{Kind: typesystem.KindDate}}, {"Date32", typesystem.LogicalType{Kind: typesystem.KindDate}}, {"DateTime", typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"DateTime64(6)", typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"DateTime64(6, 'UTC')", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}}, {"DateTime64(6, 'Europe/Yerevan')", typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "Europe/Yerevan"}},
		{"String", typesystem.LogicalType{Kind: typesystem.KindString}}, {"FixedString(16)", typesystem.LogicalType{Kind: typesystem.KindString}}, {"UUID", typesystem.LogicalType{Kind: typesystem.KindUUID}}, {"JSON", typesystem.LogicalType{Kind: typesystem.KindJSON}}, {"Nullable(UInt64)", typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindUInt64})}, {"LowCardinality(String)", typesystem.LogicalType{Kind: typesystem.KindString}}, {"Array(Nullable(UInt64))", typesystem.ArrayOf(typesystem.Nullable(typesystem.LogicalType{Kind: typesystem.KindUInt64}))}, {"Array(Array(String))", typesystem.ArrayOf(typesystem.ArrayOf(typesystem.LogicalType{Kind: typesystem.KindString}))},
	} {
		t.Run(test.source, func(t *testing.T) {
			got, err := LogicalTypeForClickHouseColumn(test.source, 0, 0, false)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want))
		})
	}
	for _, source := range []string{"Time", "Time64(3)", "IPv4", "IPv6", "Enum8('a'=1)", "Enum16('a'=1)", "Tuple(String)", "Map(String, UInt64)", "Dynamic", "Variant(String)", "Int128", "UInt128", "Point", "MY_EXTENSION"} {
		got, err := LogicalTypeForClickHouseColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, got.Kind)
	}
	_, err := LogicalTypeForClickHouseColumn("Decimal(38)", 0, 0, false)
	require.Error(t, err)
}

func TestClickHouseTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"UInt64", 0, 0, false, arrow.BinaryTypes.String},
		{"UInt32", 0, 0, false, arrow.PrimitiveTypes.Uint32},
		{"UInt16", 0, 0, false, arrow.PrimitiveTypes.Uint16},
		{"UInt8", 0, 0, false, arrow.PrimitiveTypes.Uint8},
		{"Int64", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"Int32", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"Int16", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"Int8", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"Float32", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"Float64", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"Bool", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"Decimal(38, 10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"Decimal32(2)", 0, 2, false, &arrow.Decimal128Type{Precision: 9, Scale: 2}},
		{"Decimal64(4)", 0, 4, false, &arrow.Decimal128Type{Precision: 18, Scale: 4}},
		{"Decimal128(6)", 0, 6, false, &arrow.Decimal128Type{Precision: 38, Scale: 6}},
		{"Date", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"Date32", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"DateTime64(6)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DateTime64(6, 'UTC')", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"UUID", 0, 0, false, arrow.BinaryTypes.String},
		{"Nullable(UInt64)", 0, 0, false, arrow.BinaryTypes.String},
		{"LowCardinality(String)", 0, 0, false, arrow.BinaryTypes.String},
		{"Array(Int32)", 0, 0, false, arrow.ListOf(arrow.PrimitiveTypes.Int32)},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("clickhouse", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestClickHousePointerDereferencing(t *testing.T) {
	plan := PlanForSQLColumn("clickhouse", "col", "LowCardinality(String)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	val := "hello_world"
	var ptr *string = &val

	err := plan.Append(builder, ptr)
	require.NoError(t, err)

	var nilPtr *string = nil
	err = plan.Append(builder, nilPtr)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "hello_world", arr.Value(0))
	require.True(t, arr.IsNull(1))
}

func TestClickHouseSharedSafetyAndStorage(t *testing.T) {
	for _, test := range []struct {
		source string
		value  any
	}{{"Int8", 128}, {"UInt8", 256}, {"Bool", "bad"}, {"Decimal(38,10)", "bad"}, {"Date", "bad"}, {"DateTime", "bad"}} {
		t.Run(test.source, func(t *testing.T) {
			plan := PlanForSQLColumn("clickhouse", "v", test.source, 0, 0, false)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.Error(t, plan.Append(b, test.value))
		})
	}
	uintPlan := PlanForSQLColumn("clickhouse", "v", "UInt64", 0, 0, false)
	b := uintPlan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, uintPlan.Append(b, "18446744073709551615"))
	values := b.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, "18446744073709551615", values.Value(0))
	arrayPlan := PlanForSQLColumn("clickhouse", "v", "Array(Nullable(UInt64))", 0, 0, false)
	require.Equal(t, arrow.ListOf(arrow.BinaryTypes.String), arrayPlan.DataType)
	invalidArray := PlanForSQLColumn("clickhouse", "v", "Array(Int8)", 0, 0, false)
	invalidArrayBuilder := invalidArray.Builder(memory.DefaultAllocator)
	defer invalidArrayBuilder.Release()
	require.Error(t, invalidArray.Append(invalidArrayBuilder, []any{128}))
	datePlan := PlanForSQLColumn("clickhouse", "v", "Date", 0, 0, false)
	dateBuilder := datePlan.Builder(memory.DefaultAllocator)
	defer dateBuilder.Release()
	require.NoError(t, datePlan.Append(dateBuilder, "0001-01-01"))
	dates := dateBuilder.NewArray().(*array.Date32)
	defer dates.Release()
	require.Equal(t, "0001-01-01", dates.Value(0).FormattedString())
	timestampPlan := PlanForSQLColumn("clickhouse", "v", "DateTime", 0, 0, false)
	timestampBuilder := timestampPlan.Builder(memory.DefaultAllocator)
	defer timestampBuilder.Release()
	future := time.Date(2300, time.January, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, timestampPlan.Append(timestampBuilder, future))
	timestamps := timestampBuilder.NewArray().(*array.Timestamp)
	defer timestamps.Release()
	require.Equal(t, arrow.Timestamp(future.UnixMicro()), timestamps.Value(0))
	uuidPlan := PlanForSQLColumn("clickhouse", "v", "UUID", 0, 0, false)
	uuidBuilder := uuidPlan.Builder(memory.DefaultAllocator)
	defer uuidBuilder.Release()
	rawUUID := []byte{0x12, 0x3e, 0x45, 0x67, 0xe8, 0x9b, 0x12, 0xd3, 0xa4, 0x56, 0x42, 0x66, 0x14, 0x17, 0x40, 0x00}
	require.NoError(t, uuidPlan.Append(uuidBuilder, rawUUID))
	require.NoError(t, uuidPlan.Append(uuidBuilder, "123e4567-e89b-12d3-a456-426614174000"))
	uuidValues := uuidBuilder.NewArray().(*array.String)
	defer uuidValues.Release()
	require.Equal(t, uuidValues.Value(0), uuidValues.Value(1))
	utc, err := LogicalTypeForClickHouseColumn("DateTime64(6, 'UTC')", 0, 0, false)
	require.NoError(t, err)
	utcPlan, utcMapping, err := PlanForLogicalType("v", utc)
	require.NoError(t, err)
	require.False(t, utcMapping.Fallback)
	require.Equal(t, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, utcPlan.DataType)
	nonUTC, err := LogicalTypeForClickHouseColumn("DateTime64(6, 'Europe/Yerevan')", 0, 0, false)
	require.NoError(t, err)
	nonUTCPlan, nonUTCMapping, err := PlanForLogicalType("v", nonUTC)
	require.NoError(t, err)
	require.True(t, nonUTCMapping.Fallback)
	require.Equal(t, arrow.BinaryTypes.String, nonUTCPlan.DataType)
	decimal256, err := LogicalTypeForClickHouseColumn("Decimal256(10)", 0, 0, false)
	require.NoError(t, err)
	_, mapping, err := PlanForLogicalType("v", decimal256)
	require.NoError(t, err)
	require.Equal(t, typesystem.MappingSemanticFallback, mapping.Class)
	schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema([]arrow.Field{{Name: "v", Type: uintPlan.DataType}}, nil), false)
	require.NoError(t, err)
	require.Equal(t, "string", schema.Fields()[0].Type.String())
}
