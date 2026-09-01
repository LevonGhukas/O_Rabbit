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

func TestLogicalTypeForOracleColumn(t *testing.T) {
	for _, test := range []struct {
		source           string
		precision, scale int64
		decimal          bool
		want             typesystem.LogicalType
	}{
		{"NUMBER(4,0)", 4, 0, true, typesystem.LogicalType{Kind: typesystem.KindInt16}}, {"NUMBER(9,0)", 9, 0, true, typesystem.LogicalType{Kind: typesystem.KindInt32}}, {"NUMBER(18,0)", 18, 0, true, typesystem.LogicalType{Kind: typesystem.KindInt64}}, {"NUMBER(38,0)", 38, 0, true, typesystem.Decimal(38, 0)}, {"NUMBER(38,10)", 38, 10, true, typesystem.Decimal(38, 10)}, {"NUMBER(50,10)", 50, 10, true, typesystem.Decimal(50, 10)},
		{"FLOAT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat32}}, {"BINARY_FLOAT", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat32}}, {"BINARY_DOUBLE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat64}}, {"DOUBLE PRECISION", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindFloat64}},
		{"DATE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"TIMESTAMP", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"TIMESTAMP(6)", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"TIMESTAMP WITH TIME ZONE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}}, {"TIMESTAMP WITH LOCAL TIME ZONE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}}, {"TIMESTAMP WITH LOCAL TIMEZONE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}},
		{"RAW", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"LONG RAW", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"BLOB", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindBinary}}, {"VARCHAR2", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}}, {"NVARCHAR2", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}}, {"CLOB", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindString}}, {"DB_TYPE_TIMESTAMP", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestamp}}, {"DB_TYPE_TIMESTAMP_WITH_TIME_ZONE", 0, 0, false, typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}},
	} {
		t.Run(test.source, func(t *testing.T) {
			got, err := LogicalTypeForOracleColumn(test.source, test.precision, test.scale, test.decimal)
			require.NoError(t, err)
			require.True(t, got.Equal(test.want))
		})
	}
	for _, source := range []string{"NUMBER", "BFILE", "ROWID", "UROWID", "XMLTYPE", "INTERVAL DAY TO SECOND", "MY_OBJECT_TYPE"} {
		got, err := LogicalTypeForOracleColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, got.Kind)
		require.Equal(t, source, got.SourceTypeName)
	}
}

func TestOracleTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"NUMBER(4,0)", 4, 0, true, arrow.PrimitiveTypes.Int16},
		{"NUMBER(9,0)", 9, 0, true, arrow.PrimitiveTypes.Int32},
		{"NUMBER(18,0)", 18, 0, true, arrow.PrimitiveTypes.Int64},
		{"NUMBER(38,0)", 38, 0, true, &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{"NUMBER(38,10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"NUMBER", 0, 0, false, arrow.BinaryTypes.String},
		{"FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"BINARY_FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"BINARY_DOUBLE", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"DATE", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMP", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMP WITH TIME ZONE", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}},
		{"VARCHAR2(255)", 0, 0, false, arrow.BinaryTypes.String},
		{"RAW(16)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"BLOB", 0, 0, false, arrow.BinaryTypes.Binary},
		{"BFILE", 0, 0, false, arrow.BinaryTypes.String},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("oracle", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestOracleDateIncludesTime(t *testing.T) {
	plan := PlanForSQLColumn("oracle", "col", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	t1, err := time.Parse("2006-01-02 15:04:05", "2026-08-20 14:30:45")
	require.NoError(t, err)

	err = plan.Append(builder, t1)
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Timestamp)
	defer arr.Release()

	require.Equal(t, 1, arr.Len())
	require.Equal(t, arrow.Timestamp(t1.UnixMicro()), arr.Value(0))

	boundary := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	boundaryBuilder := plan.Builder(memory.DefaultAllocator)
	defer boundaryBuilder.Release()
	require.NoError(t, plan.Append(boundaryBuilder, boundary))
	boundaryValues := boundaryBuilder.NewArray().(*array.Timestamp)
	defer boundaryValues.Release()
	require.Equal(t, arrow.Timestamp(boundary.UnixMicro()), boundaryValues.Value(0))
}

func TestOracleSharedConversionStorageAndTimezone(t *testing.T) {
	for _, test := range []struct {
		source           string
		precision, scale int64
		decimal          bool
		value            any
	}{
		{"NUMBER", 4, 0, true, 32768}, {"NUMBER", 9, 0, true, int64(2147483648)}, {"NUMBER", 38, 10, true, "bad"}, {"DATE", 0, 0, false, "bad"}, {"FLOAT", 0, 0, false, "bad"},
	} {
		t.Run(test.source, func(t *testing.T) {
			plan := PlanForSQLColumn("oracle", "v", test.source, test.precision, test.scale, test.decimal)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.Error(t, plan.Append(b, test.value))
		})
	}

	for _, test := range []struct {
		source           string
		precision, scale int64
		decimal          bool
		arrowType        arrow.DataType
		iceberg          string
	}{
		{"NUMBER", 18, 0, true, arrow.PrimitiveTypes.Int64, "long"}, {"NUMBER", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}, "decimal(38, 10)"}, {"NUMBER", 50, 10, true, arrow.BinaryTypes.String, "string"}, {"NUMBER", 0, 0, false, arrow.BinaryTypes.String, "string"},
	} {
		t.Run(test.iceberg, func(t *testing.T) {
			plan := PlanForSQLColumn("oracle", "v", test.source, test.precision, test.scale, test.decimal)
			require.Equal(t, test.arrowType, plan.DataType)
			schema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrow.NewSchema([]arrow.Field{{Name: "v", Type: plan.DataType}}, nil), false)
			require.NoError(t, err)
			require.Equal(t, test.iceberg, schema.Fields()[0].Type.String())
		})
	}

	for _, source := range []string{"BFILE", "ROWID", "XMLTYPE", "INTERVAL DAY TO SECOND", "MY_OBJECT_TYPE"} {
		logical, err := LogicalTypeForOracleColumn(source, 0, 0, false)
		require.NoError(t, err)
		_, mapping, err := PlanForLogicalType("v", logical)
		require.NoError(t, err)
		require.True(t, mapping.Fallback)
		require.Equal(t, typesystem.MappingUnsupportedFallback, mapping.Class)
	}
	for _, source := range []string{"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE"} {
		plan := PlanForSQLColumn("oracle", "v", source, 0, 0, false)
		b := plan.Builder(memory.DefaultAllocator)
		instant := time.Date(2026, 8, 20, 14, 30, 45, 0, time.FixedZone("plus2", 2*3600))
		require.NoError(t, plan.Append(b, instant))
		values := b.NewArray().(*array.Timestamp)
		require.Equal(t, arrow.Timestamp(instant.UTC().UnixMicro()), values.Value(0))
		values.Release()
		b.Release()
	}
}
