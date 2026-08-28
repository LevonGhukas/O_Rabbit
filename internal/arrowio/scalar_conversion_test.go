package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requireScalarConversionError(t *testing.T, err error, target, reason string) {
	t.Helper()
	var conversionErr *ScalarConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, target, conversionErr.Target)
	require.Equal(t, reason, conversionErr.Reason)
}

func TestFloatConversionErrorsDoNotAppendNullOrInfinity(t *testing.T) {
	float32Plan := planFloat32("value")
	float32Builder := float32Plan.Builder(memory.DefaultAllocator)
	defer float32Builder.Release()

	require.NoError(t, float32Plan.Append(float32Builder, float64(1.25)))
	requireScalarConversionError(t, float32Plan.Append(float32Builder, "not-a-float"), "Float32", "invalid float representation")
	requireScalarConversionError(t, float32Plan.Append(float32Builder, float64(1e100)), "Float32", "overflow")

	float32Values := float32Builder.NewArray().(*array.Float32)
	defer float32Values.Release()
	require.Equal(t, 1, float32Values.Len())
	require.Equal(t, float32(1.25), float32Values.Value(0))

	float64Plan := planFloat64("value")
	float64Builder := float64Plan.Builder(memory.DefaultAllocator)
	defer float64Builder.Release()
	requireScalarConversionError(t, float64Plan.Append(float64Builder, struct{}{}), "Float64", "invalid float representation")
	require.Zero(t, float64Builder.NewArray().Len())
}

func TestStringRejectsArbitraryObjectsButPreservesText(t *testing.T) {
	plan := planString("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, "text"))
	require.NoError(t, plan.Append(builder, []byte("bytes")))
	requireScalarConversionError(t, plan.Append(builder, struct{ Value int }{Value: 1}), "String", "unsupported string representation")

	values := builder.NewArray().(*array.String)
	defer values.Release()
	require.Equal(t, 2, values.Len())
	require.Equal(t, "text", values.Value(0))
	require.Equal(t, "bytes", values.Value(1))
}

func TestLegacyDecimalAndListConversionErrorsDoNotAppendNull(t *testing.T) {
	decimalPlan := planDecimal128("value", 10, 2)
	decimalBuilder := decimalPlan.Builder(memory.DefaultAllocator)
	defer decimalBuilder.Release()
	var decimalErr *DecimalConversionError
	require.ErrorAs(t, decimalPlan.Append(decimalBuilder, "not-a-decimal"), &decimalErr)
	require.Zero(t, decimalBuilder.NewArray().Len())

	listPlan := planList("value", planInt32("item"))
	listBuilder := listPlan.Builder(memory.DefaultAllocator)
	defer listBuilder.Release()
	requireScalarConversionError(t, listPlan.Append(listBuilder, 123), "List", "unsupported list representation")
	require.Zero(t, listBuilder.NewArray().Len())
}
