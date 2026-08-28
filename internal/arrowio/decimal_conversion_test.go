package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requireDecimalConversionError(t *testing.T, err error, target, reason string) {
	t.Helper()
	var conversionErr *DecimalConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, target, conversionErr.Target)
	require.Equal(t, reason, conversionErr.Reason)
}

func TestDeclaredDecimalNativePlanning(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		dbType string
		want   arrow.DataType
	}{
		{name: "postgres decimal", engine: "postgres", dbType: "NUMERIC(10,2)", want: &arrow.Decimal128Type{Precision: 10, Scale: 2}},
		{name: "postgres decimal max precision", engine: "postgres", dbType: "DECIMAL(38,18)", want: &arrow.Decimal128Type{Precision: 38, Scale: 18}},
		{name: "mysql decimal", engine: "mysql", dbType: "DECIMAL(18,6)", want: &arrow.Decimal128Type{Precision: 18, Scale: 6}},
		{name: "mariadb decimal", engine: "mariadb", dbType: "NUMERIC(38,0)", want: &arrow.Decimal128Type{Precision: 38, Scale: 0}},
		{name: "negative scale", engine: "postgres", dbType: "NUMERIC(10,-2)", want: &arrow.Decimal128Type{Precision: 10, Scale: -2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanForSQLColumn(tt.engine, "amount", tt.dbType, 0, 0, false)
			require.True(t, arrow.TypeEqual(tt.want, plan.DataType))
			require.NotNil(t, plan.Policy)
			require.Equal(t, MappingNative, plan.Policy.MappingKind)
			require.Nil(t, plan.Policy.Fallback)
		})
	}
}

func TestDeclaredDecimalFallbackPlanning(t *testing.T) {
	tests := []struct {
		name   string
		engine string
		dbType string
	}{
		{name: "precision 39", engine: "postgres", dbType: "NUMERIC(39,10)"},
		{name: "precision 50", engine: "mysql", dbType: "DECIMAL(50,10)"},
		{name: "unknown postgres numeric", engine: "postgres", dbType: "NUMERIC"},
		{name: "scale exceeds precision", engine: "mariadb", dbType: "DECIMAL(10,11)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := PlanForSQLColumn(tt.engine, "amount", tt.dbType, 0, 0, false)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
			require.NotNil(t, plan.Policy)
			require.Equal(t, MappingFallback, plan.Policy.MappingKind)
			require.Equal(t, &FallbackCodec{Name: canonicalDecimalTextFallbackCodec, Version: 1}, plan.Policy.Fallback)
			require.NoError(t, plan.Policy.Validate())
		})
	}
}

func TestExactDecimalConversion(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "amount", "NUMERIC(10,2)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	for _, value := range []any{"0", "123.45", "-123.45", "99999999.99", []byte("1.20"), int64(7), nil} {
		require.NoError(t, plan.Append(builder, value))
	}
	requireDecimalConversionError(t, plan.Append(builder, "100000000.00"), "Decimal128(10,2)", "precision overflow")
	requireDecimalConversionError(t, plan.Append(builder, "1.234"), "Decimal128(10,2)", "scale mismatch")
	requireDecimalConversionError(t, plan.Append(builder, "not-a-decimal"), "Decimal128(10,2)", "invalid decimal representation")
	requireDecimalConversionError(t, plan.Append(builder, float64(1.25)), "Decimal128(10,2)", "unsupported decimal representation")

	values := builder.NewArray().(*array.Decimal128)
	defer values.Release()
	require.Equal(t, 7, values.Len(), "failed non-null decimals must not become Arrow nulls")
	require.Equal(t, "0.00", values.Value(0).ToString(2))
	require.Equal(t, "123.45", values.Value(1).ToString(2))
	require.Equal(t, "-123.45", values.Value(2).ToString(2))
	require.Equal(t, "99999999.99", values.Value(3).ToString(2))
	require.Equal(t, "1.20", values.Value(4).ToString(2))
	require.Equal(t, "7.00", values.Value(5).ToString(2))
	require.True(t, values.IsNull(6))
}

func TestExactDecimalNegativeScale(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "amount", "NUMERIC(10,-2)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, "1200"))
	requireDecimalConversionError(t, plan.Append(builder, "123"), "Decimal128(10,-2)", "scale mismatch")

	values := builder.NewArray().(*array.Decimal128)
	defer values.Release()
	require.Equal(t, "1200", values.Value(0).ToString(-2))
}

func TestDecimalTextFallbackPreservesExactText(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "amount", "NUMERIC(50,10)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	positive := "12345678901234567890123456789012345678901234567890.1234567890"
	negative := "-12345678901234567890123456789012345678901234567890.1234567890"
	require.NoError(t, plan.Append(builder, positive))
	require.NoError(t, plan.Append(builder, []byte(negative)))

	values := builder.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, positive, values.Value(0))
	require.Equal(t, negative, values.Value(1))
}

func TestSQLColumnPlanDecimalErrorPropagation(t *testing.T) {
	plan := PlanForSQLColumn("mysql", "amount", "DECIMAL(3,1)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, "99.9"))
	requireDecimalConversionError(t, plan.Append(builder, "100.0"), "Decimal128(3,1)", "precision overflow")

	values := builder.NewArray().(*array.Decimal128)
	defer values.Release()
	require.Equal(t, 1, values.Len())
	require.Equal(t, "99.9", values.Value(0).ToString(1))
}
