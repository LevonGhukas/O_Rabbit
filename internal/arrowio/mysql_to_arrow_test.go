package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestMySQLTypeMapping(t *testing.T) {
	tests := []struct {
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{"BIGINT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint64},
		{"BIGINT", 0, 0, false, arrow.PrimitiveTypes.Int64},
		{"INT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint32},
		{"INT", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"MEDIUMINT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint32},
		{"MEDIUMINT", 0, 0, false, arrow.PrimitiveTypes.Int32},
		{"SMALLINT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint16},
		{"SMALLINT", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"TINYINT UNSIGNED", 0, 0, false, arrow.PrimitiveTypes.Uint8},
		{"TINYINT", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"TINYINT(1)", 0, 0, false, arrow.PrimitiveTypes.Int8},
		{"BIT(1)", 0, 0, false, arrow.BinaryTypes.String},
		{"BIT(64)", 0, 0, false, arrow.BinaryTypes.String},
		{"FLOAT", 0, 0, false, arrow.PrimitiveTypes.Float32},
		{"DOUBLE", 0, 0, false, arrow.PrimitiveTypes.Float64},
		{"DECIMAL(38,10)", 38, 10, true, &arrow.Decimal128Type{Precision: 38, Scale: 10}},
		{"NUMERIC(18,4)", 18, 4, true, &arrow.Decimal128Type{Precision: 18, Scale: 4}},
		{"DATE", 0, 0, false, arrow.PrimitiveTypes.Date32},
		{"DATETIME", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIMESTAMP", 0, 0, false, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{"TIME", 0, 0, false, arrow.BinaryTypes.String},
		{"YEAR", 0, 0, false, arrow.PrimitiveTypes.Int16},
		{"JSON", 0, 0, false, arrow.BinaryTypes.String},
		{"VARCHAR(255)", 0, 0, false, arrow.BinaryTypes.String},
		{"BINARY(16)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"VARBINARY(255)", 0, 0, false, arrow.BinaryTypes.Binary},
		{"BLOB", 0, 0, false, arrow.BinaryTypes.Binary},
		{"POINT", 0, 0, false, arrow.BinaryTypes.Binary},
		{"MULTIPOINT", 0, 0, false, arrow.BinaryTypes.Binary},
	}

	for _, tt := range tests {
		t.Run(tt.dbType, func(t *testing.T) {
			plan := PlanForSQLColumn("mysql", "col", tt.dbType, tt.precision, tt.scale, tt.hasDecimal)
			require.Equal(t, tt.wantType, plan.DataType)
		})
	}
}

func TestMySQLUint64MaxRoundtrip(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "col", "BIGINT UNSIGNED", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	// 18446744073709551615 (MaxUint64)
	err := plan.Append(builder, uint64(18446744073709551615))
	require.NoError(t, err)

	err = plan.Append(builder, "18446744073709551615")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Uint64)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, uint64(18446744073709551615), arr.Value(0))
	require.Equal(t, uint64(18446744073709551615), arr.Value(1))
}

func TestMySQLTimePreservesSignedDurationText(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "col", "TIME(6)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	err := plan.Append(builder, "-123:45:56.123456")
	require.NoError(t, err)
	err = plan.Append(builder, "838:59:59")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.String)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "-123:45:56.123456", arr.Value(0))
	require.Equal(t, "838:59:59", arr.Value(1))
}

func TestMySQLBitTextPreservesWidthAndLeadingZeroes(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "bits", "BIT(8)", 0, 0, false)
	require.Equal(t, MappingFallback, plan.Policy.MappingKind)
	require.Equal(t, mysqlBitTextCodec, plan.Policy.Fallback.Name)
	require.True(t, plan.Policy.Metadata.BitWidthKnown)
	require.Equal(t, int64(8), plan.Policy.Metadata.BitWidth)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()
	require.NoError(t, plan.Append(builder, "00000001"))
	require.NoError(t, plan.Append(builder, []byte{1}))
	require.Error(t, plan.Append(builder, "1"))
	arr := builder.NewArray().(*array.String)
	defer arr.Release()
	require.Equal(t, "00000001", arr.Value(0))
	require.Equal(t, "00000001", arr.Value(1))
}

func TestMySQLSemanticFallbackPolicies(t *testing.T) {
	for _, tt := range []struct{ typ, codec string }{
		{"JSON", mysqlJSONTextCodec}, {"ENUM", mysqlEnumTextCodec}, {"SET", mysqlSetTextCodec}, {"POINT", mysqlGeometryBinaryCodec}, {"unrecognized_extension", mysqlExtensionTextCodec},
	} {
		t.Run(tt.typ, func(t *testing.T) {
			plan := PlanForSQLColumn("mysql", "value", tt.typ, 0, 0, false)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, tt.codec, plan.Policy.Fallback.Name)
		})
	}
}

func TestMySQLJSONDoesNotRemarshalOrAcceptMalformedText(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "payload", "JSON", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()
	value := []byte(`{"b":1,"a":2}`)
	require.NoError(t, plan.Append(builder, value))
	require.Error(t, plan.Append(builder, `{`))
	arr := builder.NewArray().(*array.String)
	defer arr.Release()
	require.Equal(t, string(value), arr.Value(0))
}

func TestMariaDBKeepsSeparatePolicies(t *testing.T) {
	jsonPlan := PlanForSQLColumn("mariadb", "payload", "JSON", 0, 0, false)
	require.Equal(t, "mariadb", jsonPlan.Policy.SourceEngine)
	require.Equal(t, mariadbJSONTextCodec, jsonPlan.Policy.Fallback.Name)
	timePlan := PlanForSQLColumn("mariadb", "duration", "TIME", 0, 0, false)
	require.Equal(t, mariadbTimeTextCodec, timePlan.Policy.Fallback.Name)
	bitPlan := PlanForSQLColumn("mariadb", "bits", "BIT(64)", 0, 0, false)
	require.Equal(t, mariadbBitTextCodec, bitPlan.Policy.Fallback.Name)
}

func TestMySQLDate32Preservation(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "col", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	d1, err := time.Parse("2006-01-02", "1960-02-29")
	require.NoError(t, err)
	err = plan.Append(builder, d1)
	require.NoError(t, err)

	err = plan.Append(builder, "9999-12-31")
	require.NoError(t, err)

	arr := builder.NewArray().(*array.Date32)
	defer arr.Release()

	require.Equal(t, 2, arr.Len())
	require.Equal(t, "1960-02-29", arr.Value(0).FormattedString())
	require.Equal(t, "9999-12-31", arr.Value(1).FormattedString())
}
