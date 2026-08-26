package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestTemporalDescriptorMappings(t *testing.T) {
	cases := []struct {
		engine, typ string
		semantics   TemporalSemantics
		precision   int
		zone        string
	}{
		{"postgres", "DATE", TemporalDate, 0, ""},
		{"postgres", "TIME(6) WITHOUT TIME ZONE", TemporalTime, 6, ""},
		{"postgres", "TIMESTAMP(6) WITHOUT TIME ZONE", TemporalLocalTimestamp, 6, ""},
		{"postgres", "TIMESTAMPTZ", TemporalInstant, 6, ""},
		{"postgres", "TIME WITH TIME ZONE", TemporalZonedTime, 6, ""},
		{"mssql", "DATETIME2(7)", TemporalLocalTimestamp, 7, ""},
		{"mssql", "DATETIMEOFFSET(7)", TemporalInstant, 7, ""},
		{"mssql", "SMALLDATETIME", TemporalLocalTimestamp, 0, ""},
		{"oracle", "DATE", TemporalLocalTimestamp, 0, ""},
		{"oracle", "TIMESTAMP(9)", TemporalLocalTimestamp, 9, ""},
		{"oracle", "TIMESTAMP WITH LOCAL TIME ZONE", TemporalZonedTime, 6, ""},
		{"clickhouse", "Nullable(DateTime64(9, 'Europe/Yerevan'))", TemporalInstant, 9, "Europe/Yerevan"},
		{"clickhouse", "DateTime64(6, 'UTC')", TemporalInstant, 6, "UTC"},
	}
	for _, tc := range cases {
		t.Run(tc.engine+"/"+tc.typ, func(t *testing.T) {
			d := SourceFieldDescriptor{Engine: tc.engine, SourceType: tc.typ}
			classifyTemporalDescriptor(&d)
			require.Equal(t, tc.semantics, d.TemporalSemantics)
			require.Equal(t, tc.precision, d.TemporalPrecision)
			require.True(t, d.TemporalPrecisionKnown)
			require.Equal(t, tc.zone, d.SourceTimezone)
		})
	}
}

func TestLocalTimestampPreservesCivilFieldsAndInstantPreservesInstant(t *testing.T) {
	loc := time.FixedZone("+04", 4*60*60)
	value := time.Date(2026, 8, 26, 14, 0, 0, 123456000, loc)
	local := planTimestampUs("local", "")
	lb := local.Builder(memory.DefaultAllocator)
	defer lb.Release()
	require.NoError(t, local.Append(lb, value))
	la := lb.NewArray().(*array.Timestamp)
	defer la.Release()
	// Civil 14:00 stays 14:00, represented as a timezone-free Arrow timestamp.
	require.Equal(t, arrow.Timestamp(time.Date(2026, 8, 26, 14, 0, 0, 123456000, time.UTC).UnixMicro()), la.Value(0))

	instant := planTimestampUs("instant", "UTC")
	ib := instant.Builder(memory.DefaultAllocator)
	defer ib.Release()
	require.NoError(t, instant.Append(ib, value))
	require.NoError(t, instant.Append(ib, "2026-08-26T10:00:00.123456Z"))
	ia := ib.NewArray().(*array.Timestamp)
	defer ia.Release()
	require.Equal(t, ia.Value(0), ia.Value(1))
	require.Error(t, instant.Append(ib, "2026-08-26 10:00:00"), "instant text must carry offset")
}

func TestTemporalPrecisionAndDateAreStrict(t *testing.T) {
	local := planTimestampUs("local", "")
	b := local.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, local.Append(b, "2026-08-26 10:30:15.123456"))
	require.Error(t, local.Append(b, "2026-08-26 10:30:15.1234567"))

	date := planDate32("date")
	db := date.Builder(memory.DefaultAllocator)
	defer db.Release()
	for _, raw := range []string{"0001-01-01", "1900-01-01", "1970-01-01", "2299-12-31"} {
		require.NoError(t, date.Append(db, raw))
	}
	// Date is calendar-only; zoned timestamp text is not a date input.
	require.Error(t, date.Append(db, "2026-08-26T00:00:00+04:00"))
}

func TestFlightSQLTemporalSchemaCapability(t *testing.T) {
	require.NoError(t, ValidateArrowSchemaForIcebergV2(arrow.NewSchema([]arrow.Field{{Name: "local", Type: &arrow.TimestampType{Unit: arrow.Microsecond}}, {Name: "instant", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}}}, nil)))
	require.Error(t, ValidateArrowSchemaForIcebergV2(arrow.NewSchema([]arrow.Field{{Name: "ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond}}}, nil)))
	require.Error(t, ValidateArrowSchemaForIcebergV2(arrow.NewSchema([]arrow.Field{{Name: "zone", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "Europe/Yerevan"}}}, nil)))
}

func TestConfiguredTargetCapabilityIsSingleTemporalDecisionPoint(t *testing.T) {
	target := ConfiguredTargetCapabilities()
	require.Equal(t, 2, target.IcebergFormatVersion)
	require.Equal(t, 6, target.MaxTemporalPrecision)

	capability, err := target.ValidateTemporalDescriptor(SourceFieldDescriptor{Name: "us", SourceType: "TIMESTAMP(6)", TemporalSemantics: TemporalLocalTimestamp, TemporalPrecision: 6, TemporalPrecisionKnown: true})
	require.NoError(t, err)
	require.True(t, capability.ArrowExact)
	require.True(t, capability.ParquetExact)
	require.True(t, capability.IcebergExact)
	require.True(t, capability.ClickHouseExact)

	_, err = target.ValidateTemporalDescriptor(SourceFieldDescriptor{Name: "ns", SourceType: "TIMESTAMP(9)", TemporalSemantics: TemporalLocalTimestamp, TemporalPrecision: 9, TemporalPrecisionKnown: true})
	require.ErrorContains(t, err, "maximum microseconds")
	_, err = target.ValidateTemporalDescriptor(SourceFieldDescriptor{Name: "timetz", SourceType: "TIMETZ", TemporalSemantics: TemporalZonedTime, TemporalPrecision: 6, TemporalPrecisionKnown: true})
	require.ErrorContains(t, err, "no lossless")
}
