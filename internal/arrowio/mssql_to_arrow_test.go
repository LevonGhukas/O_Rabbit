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

func TestLogicalTypeForMSSQLColumn(t *testing.T) {
	for _, test := range []struct {
		source           string
		precision, scale int64
		hasDecimal       bool
		want             typesystem.LogicalType
	}{
		{"TINYINT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindUInt8}}, {"SMALLINT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt16}}, {"INT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt32}}, {"BIGINT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindInt64}}, {"BIT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBool}},
		{"FLOAT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat64}}, {"FLOAT(24)", 24, 0, true, typesystem.LogicalType{Kind: typesystem.KindFloat32}}, {"FLOAT(53)", 53, 0, true, typesystem.LogicalType{Kind: typesystem.KindFloat64}}, {"REAL", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat32}},
		{"DOUBLE PRECISION", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat64}},
		{"DECIMAL(18,2)", 18, 2, true, typesystem.Decimal(18, 2)}, {"DECIMAL(50,10)", 50, 10, true, typesystem.Decimal(50, 10)}, {"MONEY", 0, 0, false, typesystem.Decimal(19, 4)}, {"SMALLMONEY", 0, 0, false, typesystem.Decimal(10, 4)},
		{"DATE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindDate}}, {"DATETIME", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"DATETIME2(7)", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"SMALLDATETIME", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"DATETIMEOFFSET(7)", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}}, {"TIME(7)", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTime}},
		{"UNIQUEIDENTIFIER", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindUUID}}, {"BINARY", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"VARBINARY", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"IMAGE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"ROWVERSION", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"TIMESTAMP", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}},
		{"VARCHAR", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}}, {"NVARCHAR", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}}, {"JSON", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindJSON}},
	} {
		t.Run(test.source, func(t *testing.T) {
			got, err := LogicalTypeForMSSQLColumn(test.source, test.precision, test.scale, test.hasDecimal)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want))
		})
	}
	for _, source := range []string{"XML", "SQL_VARIANT", "HIERARCHYID", "GEOMETRY", "GEOGRAPHY", "MY_EXTENSION_TYPE"} {
		got, err := LogicalTypeForMSSQLColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, got.Kind)
		require.Equal(t, source, got.SourceTypeName)
	}
	got, err := LogicalTypeForMSSQLColumn("DECIMAL", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, typesystem.KindUnknown, got.Kind)
}

func TestMSSQLTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"TINYINT", 0, 0, false, arrow.PrimitiveTypes.Uint8},
		{"SMALLINT", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"INT", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"BIGINT", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"BIT", 0, 0, false, arrow.FixedWidthTypes.Boolean},
		{"FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"FLOAT(53)", 53, 0, true, arrow.PrimitiveTypes.Float64},
		{"FLOAT(24)", 24, 0, true, arrow.PrimitiveTypes.Float32},
		{"FLOAT24", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"FLOAT53", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"REAL", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"DECIMAL(38,18)", 38, 18, true, &arrow.Decimal128Type{Precision: 38, Scale: 18}},
		{"MONEY", 0, 0, false, &arrow.Decimal128Type{Precision: 19, Scale: 4}},
		{"SMALLMONEY", 0, 0, false, &arrow.Decimal128Type{Precision: 10, Scale: 4}},
		{"DATE", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"DATETIME", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DATETIME2(7)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"DATETIMEOFFSET(7)", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"TIME(7)", 0, 0, false, arrow.FixedWidthTypes.Time64us},
		{"UNIQUEIDENTIFIER", 0, 0, false, arrow.BinaryTypes.String},
		{"XML", 0, 0, false, arrow.BinaryTypes.String},
		{"VARCHAR(100)", 0, 0, false, arrow.BinaryTypes.String},
		{"NVARCHAR(MAX)", 0, 0, false, arrow.BinaryTypes.String},
		{"TEXT", 0, 0, false, arrow.BinaryTypes.String},
		{"NTEXT", 0, 0, false, arrow.BinaryTypes.String},
		{"BINARY(16)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"VARBINARY(MAX)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"IMAGE", 0, 0, false, arrow.BinaryTypes.Binary},
		{"ROWVERSION", 0, 0, false, arrow.BinaryTypes.Binary},
		{"TIMESTAMP", 0, 0, false, arrow.BinaryTypes.Binary},
		{"SQL_VARIANT", 0, 0, false, arrow.BinaryTypes.String},
		{"HIERARCHYID", 0, 0, false, arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("mssql", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestMSSQLUniqueidentifierFormat(t *testing.T) {
	logical, err := LogicalTypeForMSSQLColumn("UNIQUEIDENTIFIER", 0, 0, false)
	require.NoError(t, err)
	require.Equal(t, typesystem.KindUUID, logical.Kind)
	plan, mapping, err := PlanForLogicalType("col", logical)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
	require.Equal(t, typesystem.MappingSemanticFallback, mapping.Class)
	schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema([]arrow.Field{{Name: "col", Type: plan.DataType}}, nil), false)
	require.NoError(t, err)
	require.Equal(t, "string", schema.Fields()[0].Type.String())
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	// 16 bytes raw UUID
	rawUUID := []byte{
		0x12, 0x3e, 0x45, 0x67,
		0xe8, 0x9b,
		0x12, 0xd3,
		0xa4, 0x56,
		0x42, 0x66, 0x14, 0x17, 0x40, 0x00,
	}

	err = plan.Append(builder, rawUUID)
	require.NoError(t, err)

	err = plan.Append(builder, "123e4567-e89b-12d3-a456-426614174000")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", arr.Value(0))
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", arr.Value(1))
}

func TestMSSQLPre1970Dates(t *testing.T) {
	plan := PlanForSQLColumn("mssql", "col", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	err := plan.Append(builder, "1969-12-31")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Date32)
	defer arr.Release()

	require.Equal(t, 1, arr.Len())
	require.Equal(t, "1969-12-31", arr.Value(0).FormattedString())

	// MSSQL no longer inherits ClickHouse's date range clamping.
	builder2 := plan.Builder(memory.DefaultAllocator)
	defer builder2.Release()

	err = plan.Append(builder2, "0001-01-01")
	require.NoError(t, err)

	arr2 := builder2.NewArray().(*array.Date32)
	defer arr2.Release()
	require.Equal(t, "0001-01-01", arr2.Value(0).FormattedString())
}

func TestMSSQLSharedConversionSafetyAndFallbacks(t *testing.T) {
	for _, test := range []struct {
		source           string
		precision, scale int64
		decimal          bool
		value            any
	}{
		{"TINYINT", 0, 0, false, 256}, {"TINYINT", 0, 0, false, -1}, {"SMALLINT", 0, 0, false, 32768}, {"INT", 0, 0, false, int64(2147483648)}, {"BIT", 0, 0, false, "bad"}, {"DECIMAL", 18, 2, true, "bad"}, {"DATE", 0, 0, false, "bad"}, {"TIME", 0, 0, false, "25:00:00"}, {"DATETIME", 0, 0, false, "bad"},
	} {
		t.Run(test.source, func(t *testing.T) {
			plan := PlanForSQLColumn("mssql", "v", test.source, test.precision, test.scale, test.decimal)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.Error(t, plan.Append(b, test.value))
		})
	}
	plan := PlanForSQLColumn("mssql", "v", "TINYINT", 0, 0, false)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, 255))
	logical, err := LogicalTypeForMSSQLColumn("MY_EXTENSION_TYPE", 0, 0, false)
	require.NoError(t, err)
	fallback, mapping, err := PlanForLogicalType("v", logical)
	require.NoError(t, err)
	require.Equal(t, typesystem.MappingUnsupportedFallback, mapping.Class)
	fb := fallback.Builder(memory.DefaultAllocator)
	defer fb.Release()
	require.NoError(t, fallback.Append(fb, []byte{1, 2}))
	values := fb.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, "base64:AQI=", values.Value(0))
	wide, err := LogicalTypeForMSSQLColumn("DECIMAL", 50, 10, true)
	require.NoError(t, err)
	_, decimalMapping, err := PlanForLogicalType("v", wide)
	require.NoError(t, err)
	require.Equal(t, typesystem.MappingSemanticFallback, decimalMapping.Class)
}
