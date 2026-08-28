package arrowio

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requireIntegerConversionError(t *testing.T, err error, target, reason string) {
	t.Helper()
	var conversionErr *IntegerConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, target, conversionErr.Target)
	require.Equal(t, reason, conversionErr.Reason)
}

func TestCheckedSignedIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name      string
		plan      ColumnPlan
		min       any
		max       any
		underflow any
		overflow  any
	}{
		{name: "Int8", plan: planInt8("value"), min: int64(-128), max: int64(127), underflow: int64(-129), overflow: int64(128)},
		{name: "Int16", plan: planInt16("value"), min: "-32768", max: []byte("32767"), underflow: "-32769", overflow: []byte("32768")},
		{name: "Int32", plan: planInt32("value"), min: int64(-2147483648), max: int64(2147483647), underflow: int64(-2147483649), overflow: int64(2147483648)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := tt.plan.Builder(memory.DefaultAllocator)
			defer builder.Release()

			require.NoError(t, tt.plan.Append(builder, tt.min))
			require.NoError(t, tt.plan.Append(builder, tt.max))
			requireIntegerConversionError(t, tt.plan.Append(builder, tt.underflow), tt.name, "underflow")
			requireIntegerConversionError(t, tt.plan.Append(builder, tt.overflow), tt.name, "overflow")

			values := builder.NewArray()
			defer values.Release()
			require.Equal(t, 2, values.Len(), "failed non-null values must not become Arrow nulls")
		})
	}
}

func TestCheckedInt64Boundaries(t *testing.T) {
	plan := planInt64("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, int64(math.MinInt64)))
	require.NoError(t, plan.Append(builder, int64(math.MaxInt64)))
	require.NoError(t, plan.Append(builder, uint64(math.MaxInt64)))
	require.NoError(t, plan.Append(builder, "9223372036854775807"))
	requireIntegerConversionError(t, plan.Append(builder, uint64(math.MaxInt64)+1), "Int64", "overflow")
	requireIntegerConversionError(t, plan.Append(builder, "9223372036854775808"), "Int64", "overflow")
}

func TestCheckedUnsignedIntegerBoundaries(t *testing.T) {
	tests := []struct {
		name     string
		plan     ColumnPlan
		max      any
		overflow any
	}{
		{name: "UInt8", plan: planUint8("value"), max: uint64(255), overflow: uint64(256)},
		{name: "UInt16", plan: planUint16("value"), max: []byte("65535"), overflow: "65536"},
		{name: "UInt32", plan: planUint32("value"), max: uint64(4294967295), overflow: uint64(4294967296)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := tt.plan.Builder(memory.DefaultAllocator)
			defer builder.Release()

			require.NoError(t, tt.plan.Append(builder, uint64(0)))
			require.NoError(t, tt.plan.Append(builder, tt.max))
			requireIntegerConversionError(t, tt.plan.Append(builder, tt.overflow), tt.name, "overflow")
			requireIntegerConversionError(t, tt.plan.Append(builder, int64(-1)), tt.name, "negative value")

			values := builder.NewArray()
			defer values.Release()
			require.Equal(t, 2, values.Len(), "failed non-null values must not become Arrow nulls")
		})
	}
}

func TestCheckedUInt64Boundaries(t *testing.T) {
	plan := planUint64("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, uint64(0)))
	require.NoError(t, plan.Append(builder, ^uint64(0)))
	require.NoError(t, plan.Append(builder, "18446744073709551615"))
	require.NoError(t, plan.Append(builder, []byte("18446744073709551615")))
	require.NoError(t, plan.Append(builder, []byte("12345678")))
	requireIntegerConversionError(t, plan.Append(builder, int64(-1)), "UInt64", "negative value")
	requireIntegerConversionError(t, plan.Append(builder, "18446744073709551616"), "UInt64", "overflow")

	values := builder.NewArray().(*array.Uint64)
	defer values.Release()
	require.Equal(t, 5, values.Len())
	require.Equal(t, ^uint64(0), values.Value(1))
	require.Equal(t, ^uint64(0), values.Value(2))
	require.Equal(t, ^uint64(0), values.Value(3))
	require.Equal(t, uint64(12345678), values.Value(4))
}

func TestSQLColumnPlanIntegerErrorPropagation(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "small_value", "SMALLINT", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, int64(32767)))
	err := plan.Append(builder, int64(32768))
	requireIntegerConversionError(t, err, "Int16", "overflow")

	values := builder.NewArray().(*array.Int16)
	defer values.Release()
	require.Equal(t, 1, values.Len())
	require.Equal(t, int16(32767), values.Value(0))
}

func TestCheckedIntegerNullStillAppendsNull(t *testing.T) {
	plan := planInt16("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, nil))
	values := builder.NewArray().(*array.Int16)
	defer values.Release()
	require.True(t, values.IsNull(0))
}
