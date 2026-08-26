package arrowio

import (
	"math"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestStrictIntegerBoundariesNeverNullOrWrap(t *testing.T) {
	cases := []struct {
		name         string
		plan         ColumnPlan
		min, max     any
		below, above any
	}{
		{"int8", planInt8("c"), int64(-128), int64(127), int64(-129), int64(128)},
		{"int16", planInt16("c"), int64(-32768), int64(32767), int64(-32769), int64(32768)},
		{"int32", planInt32("c"), int64(-2147483648), int64(2147483647), int64(-2147483649), int64(2147483648)},
		{"uint8", planUint8("c"), uint64(0), uint64(255), int64(-1), uint64(256)},
		{"uint16", planUint16("c"), uint64(0), uint64(65535), int64(-1), uint64(65536)},
		{"uint32", planUint32("c"), uint64(0), uint64(4294967295), int64(-1), uint64(4294967296)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, tc.plan.Append(b, tc.min))
			require.NoError(t, tc.plan.Append(b, tc.max))
			require.Error(t, tc.plan.Append(b, tc.below))
			require.Error(t, tc.plan.Append(b, tc.above))
			a := b.NewArray()
			defer a.Release()
			require.Equal(t, 2, a.Len())
			require.False(t, a.IsNull(0))
			require.False(t, a.IsNull(1))
		})
	}
	for _, tc := range []struct {
		name  string
		plan  ColumnPlan
		value any
	}{{"int64 uint max", planInt64("c"), uint64(math.MaxUint64)}} {
		t.Run(tc.name, func(t *testing.T) {
			b := tc.plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.Error(t, tc.plan.Append(b, tc.value))
			a := b.NewArray()
			defer a.Release()
			require.Equal(t, 0, a.Len())
		})
	}
	uint64Plan := planUint64("c")
	b := uint64Plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, uint64Plan.Append(b, uint64(math.MaxUint64)))
	a := b.NewArray().(*array.Decimal128)
	defer a.Release()
	require.Equal(t, "18446744073709551615", a.Value(0).ToString(0))
}

func TestStrictDecimalExactness(t *testing.T) {
	p := planDecimal128("amount", 38, 18)
	b := p.Builder(memory.DefaultAllocator)
	defer b.Release()
	for _, v := range []any{"0", "1", "-1", "123.45", "123.4500", "0.000000000000000001", "12345678901234567890.123456789012345678", "99999999999999999999.999999999999999999"} {
		require.NoError(t, p.Append(b, v), v)
	}
	require.Error(t, p.Append(b, "123.4500000000000000001"))
	require.Error(t, p.Append(b, float64(123.45)))
	require.Error(t, p.Append(b, "123456789012345678901.123456789012345678"))
	a := b.NewArray().(*array.Decimal128)
	defer a.Release()
	require.Equal(t, 8, a.Len())
	require.False(t, a.IsNull(6))

	p4 := planDecimal128("amount", 20, 4)
	b4 := p4.Builder(memory.DefaultAllocator)
	defer b4.Release()
	require.NoError(t, p4.Append(b4, "123.45"))
	require.NoError(t, p4.Append(b4, "123.4500"))
	require.Error(t, p4.Append(b4, "123.45001"))
	a4 := b4.NewArray()
	defer a4.Release()
	require.Equal(t, 2, a4.Len())
}

func TestStrictNullBinaryAndTemporal(t *testing.T) {
	binaryPlan := planBinary("payload")
	b := binaryPlan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := []byte{0, 255, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
	require.NoError(t, binaryPlan.Append(b, raw))
	require.NoError(t, binaryPlan.Append(b, nil))
	require.Error(t, binaryPlan.Append(b, "not bytes"))
	a := b.NewArray().(*array.Binary)
	defer a.Release()
	require.Equal(t, raw, a.Value(0))
	require.True(t, a.IsNull(1))

	datePlan := planDate32("d")
	db := datePlan.Builder(memory.DefaultAllocator)
	defer db.Release()
	for _, v := range []string{"0001-01-01", "1900-01-01", "2299-12-31", "2300-01-01"} {
		require.NoError(t, datePlan.Append(db, v))
	}
	da := db.NewArray()
	defer da.Release()
	require.Equal(t, 4, da.Len())
	ts := planTimestampUs("ts", "")
	tb := ts.Builder(memory.DefaultAllocator)
	defer tb.Release()
	require.NoError(t, ts.Append(tb, time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.Error(t, ts.Append(tb, time.Date(2020, 1, 1, 0, 0, 0, 1, time.UTC)))
}
