package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requireTemporalConversionError(t *testing.T, err error, target, reason string) {
	t.Helper()
	var conversionErr *TemporalConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, target, conversionErr.Target)
	require.Equal(t, reason, conversionErr.Reason)
}

func TestDate32PreservesValuesOutsideFormerClickHouseRange(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "value", "DATE", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, "1800-01-01"))
	require.NoError(t, plan.Append(builder, "2400-01-01"))
	require.NoError(t, plan.Append(builder, nil))
	requireTemporalConversionError(t, plan.Append(builder, "0000-00-00"), "Date32", "invalid date value")

	values := builder.NewArray().(*array.Date32)
	defer values.Release()
	require.Equal(t, 3, values.Len())
	require.Equal(t, "1800-01-01", values.Value(0).FormattedString())
	require.Equal(t, "2400-01-01", values.Value(1).FormattedString())
	require.True(t, values.IsNull(2))
}

func TestDate32PreservesCalendarDateWithoutTimezoneShift(t *testing.T) {
	plan := planDate32("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	value := time.Date(1800, time.January, 1, 0, 30, 0, 0, time.FixedZone("east", 2*60*60))
	require.NoError(t, plan.Append(builder, value))

	values := builder.NewArray().(*array.Date32)
	defer values.Release()
	require.Equal(t, "1800-01-01", values.Value(0).FormattedString())
}

func TestTimestampMicrosecondSafety(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "value", "TIMESTAMP(6)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	before := time.Date(1800, time.January, 1, 0, 0, 0, 123456000, time.UTC)
	after := time.Date(2400, time.January, 1, 0, 0, 0, 123456000, time.UTC)
	require.NoError(t, plan.Append(builder, before))
	require.NoError(t, plan.Append(builder, after))
	requireTemporalConversionError(t, plan.Append(builder, time.Date(2026, time.August, 20, 12, 34, 56, 123456789, time.UTC)), "Timestamp[us]", "sub-microsecond precision")
	requireTemporalConversionError(t, plan.Append(builder, time.Unix(1<<62, 0)), "Timestamp[us]", "outside Timestamp[us] range")
	require.NoError(t, plan.Append(builder, nil))
	requireTemporalConversionError(t, plan.Append(builder, "not-a-timestamp"), "Timestamp[us]", "unsupported timestamp representation")

	values := builder.NewArray().(*array.Timestamp)
	defer values.Release()
	require.Equal(t, 3, values.Len(), "failed non-null values must not become Arrow nulls")
	require.Equal(t, arrow.Timestamp(before.UnixMicro()), values.Value(0))
	require.Equal(t, arrow.Timestamp(after.UnixMicro()), values.Value(1))
	require.True(t, values.IsNull(2))
}

func TestTime64MicrosecondSafety(t *testing.T) {
	plan := planTime64("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	require.NoError(t, plan.Append(builder, "00:00:00"))
	require.NoError(t, plan.Append(builder, "23:59:59.999999"))
	requireTemporalConversionError(t, plan.Append(builder, "23:59:59.999999001"), "Time64[us]", "sub-microsecond precision")
	requireTemporalConversionError(t, plan.Append(builder, "24:00:00"), "Time64[us]", "outside time-of-day range")
	requireTemporalConversionError(t, plan.Append(builder, "-00:00:01"), "Time64[us]", "outside time-of-day range")
	require.NoError(t, plan.Append(builder, nil))

	values := builder.NewArray().(*array.Time64)
	defer values.Release()
	require.Equal(t, 3, values.Len(), "failed non-null values must not become Arrow nulls")
	require.Equal(t, arrow.Time64(0), values.Value(0))
	require.Equal(t, arrow.Time64(dayMicroseconds-1), values.Value(1))
	require.True(t, values.IsNull(2))
}

func TestSQLColumnPlanTemporalErrorPropagation(t *testing.T) {
	plan := PlanForSQLColumn("postgres", "created_at", "TIMESTAMP(6)", 0, 0, false)
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	valid := time.Date(2026, time.August, 20, 12, 34, 56, 123456000, time.UTC)
	require.NoError(t, plan.Append(builder, valid))
	err := plan.Append(builder, time.Date(2026, time.August, 20, 12, 34, 56, 123456789, time.UTC))
	requireTemporalConversionError(t, err, "Timestamp[us]", "sub-microsecond precision")

	values := builder.NewArray().(*array.Timestamp)
	defer values.Release()
	require.Equal(t, 1, values.Len())
	require.Equal(t, arrow.Timestamp(valid.UnixMicro()), values.Value(0))
}
