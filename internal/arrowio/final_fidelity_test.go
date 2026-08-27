package arrowio

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestUInt64FullDomainMapsToIcebergDecimal20(t *testing.T) {
	plan := planUint64("id")
	require.Equal(t, &arrow.Decimal128Type{Precision: 20, Scale: 0}, plan.DataType)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, uint64(math.MaxUint64)))
	a := b.NewArray().(*array.Decimal128)
	defer a.Release()
	require.Equal(t, "18446744073709551615", a.Value(0).ToString(0))

	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: plan.DataType, Nullable: false}}, nil)
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	require.NoError(t, err)
	field, ok := iceSchema.FindFieldByName("id")
	require.True(t, ok)
	require.Equal(t, iceberg.DecimalTypeOf(20, 0), field.Type)
}

func TestFixedBinaryPreservesPaddingAndIcebergFixedSchema(t *testing.T) {
	plan := planFixedBinary("payload", 4)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := []byte{0x61, 0x00, 0x00, 0xff}
	require.NoError(t, plan.Append(b, raw))
	require.Error(t, plan.Append(b, raw[:3]))
	a := b.NewArray().(*array.FixedSizeBinary)
	defer a.Release()
	require.Equal(t, raw, a.Value(0))

	schema := arrow.NewSchema([]arrow.Field{{Name: "payload", Type: plan.DataType, Nullable: false}}, nil)
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	require.NoError(t, err)
	field, ok := iceSchema.FindFieldByName("payload")
	require.True(t, ok)
	require.Equal(t, iceberg.FixedTypeOf(4), field.Type)
}

func TestUUIDUsesExplicitCanonicalTextFallback(t *testing.T) {
	d := SourceFieldDescriptor{Engine: "postgres", SourceType: "UUID"}
	classifySourceRepresentation(&d)
	require.Equal(t, LogicalUUID, d.LogicalFamily)
	require.Equal(t, "canonical_uuid_text_v1", d.FallbackEncoding)

	plan := planUUID("id")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, "123E4567-E89B-12D3-A456-426614174000"))
	require.Error(t, plan.Append(b, "not-a-uuid"))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, "123e4567-e89b-12d3-a456-426614174000", a.Value(0))
}

func TestJSONTextFallbackPreservesValidSourceText(t *testing.T) {
	plan := planJSONText("document")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := `{"n":1.2300,"null":null,"word":"\u2603"}`
	require.NoError(t, plan.Append(b, raw))
	require.Error(t, plan.Append(b, `{"broken":`))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, raw, a.Value(0))
}

func TestHighPrecisionTemporalFallbacksRequireOriginalText(t *testing.T) {
	cases := []struct {
		name, typ, value, encoding string
	}{
		{"datetime2", "DATETIME2(7)", "2026-08-26 11:23:54.1234567", "mssql_datetime2_text_v1"},
		{"time", "TIME(7)", "11:23:54.1234567", "mssql_time_text_v1"},
		{"offset", "DATETIMEOFFSET(7)", "2026-08-26T15:23:54.1234567+04:00", "mssql_datetimeoffset_text_v1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := SourceFieldDescriptor{Name: tc.name, Engine: "mssql", SourceType: tc.typ}
			classifyTemporalDescriptor(&d)
			classifyFallbackRepresentation(&d)
			require.Equal(t, RepresentationFallback, d.Representation)
			require.Equal(t, tc.encoding, d.FallbackEncoding)
			plan, err := fallbackPlanForDescriptor(d)
			require.NoError(t, err)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, tc.value))
			require.NoError(t, plan.Append(b, time.Date(2026, 8, 26, 11, 23, 54, 123456000, time.UTC)))
			require.NoError(t, plan.Append(b, nil))
			a := b.NewArray().(*array.String)
			defer a.Release()
			require.Equal(t, tc.value, a.Value(0))
			require.False(t, a.IsNull(1))
			require.True(t, a.IsNull(2))
		})
	}
}

func TestMSSQLDatetime2FallbackFormatsExactDriverNanoseconds(t *testing.T) {
	d := SourceFieldDescriptor{Name: "value", Engine: "mssql", SourceType: "DATETIME2(7)", TemporalPrecision: 7, TemporalPrecisionKnown: true, FallbackEncoding: "mssql_datetime2_text_v1"}
	plan, err := fallbackPlanForDescriptor(d)
	require.NoError(t, err)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	require.NoError(t, plan.Append(b, time.Date(2026, 8, 26, 13, 7, 28, 123456700, time.UTC)))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, "2026-08-26 13:07:28.1234567", a.Value(0))
}

func TestOracleAndClickHouseTemporalFallbackFormatExactNanoseconds(t *testing.T) {
	value := time.Date(2026, 8, 27, 12, 3, 4, 123456789, time.FixedZone("source", 4*3600))
	cases := []struct {
		name, encoding, want string
	}{
		{"oracle", "oracle_timestamp_text_v1", "2026-08-27 12:03:04.123456789"},
		{"oracle_tz", "oracle_timestamptz_text_v1", "2026-08-27T12:03:04.123456789+04:00"},
		{"clickhouse", "clickhouse_datetime64_text_v1", "2026-08-27 12:03:04.123456789"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := fallbackPlanForDescriptor(SourceFieldDescriptor{Name: "value", TemporalPrecision: 9, FallbackEncoding: tc.encoding})
			require.NoError(t, err)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, value))
			a := b.NewArray().(*array.String)
			defer a.Release()
			require.Equal(t, tc.want, a.Value(0))
		})
	}
}

func TestSelectableFallbackEncodingsHaveCodecs(t *testing.T) {
	encodings := []string{
		"canonical_uuid_text_v1", "json_utf8_text_v1", "utf8_text_v1", "xml_utf8_text_v1", "oracle_rowid_text_v1", "source_text_v1", "hex_v1",
		"mssql_time_text_v1", "mssql_datetime2_text_v1", "mssql_datetimeoffset_text_v1", "oracle_timestamp_text_v1", "oracle_timestamptz_text_v1", "clickhouse_datetime64_text_v1", "postgres_timetz_text_v1", "decimal_text_v1", "integer_text_v1",
	}
	for _, encoding := range encodings {
		_, err := fallbackPlanForDescriptor(SourceFieldDescriptor{Name: "value", FallbackEncoding: encoding})
		require.NoErrorf(t, err, "encoding %s", encoding)
	}
}

func TestTemporalNativeCapabilitySelectsFallbackBeforeConversion(t *testing.T) {
	target := ConfiguredTargetCapabilities()
	cases := []struct {
		typ, encoding string
		wantNative    bool
	}{
		{"DATETIME2(6)", "", true},
		{"DATETIME2(7)", "mssql_datetime2_text_v1", false},
		{"TIME(7)", "mssql_time_text_v1", false},
		{"DATETIMEOFFSET(7)", "mssql_datetimeoffset_text_v1", false},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			d := SourceFieldDescriptor{Name: "value", Engine: "mssql", SourceType: tc.typ}
			classifyTemporalDescriptor(&d)
			classifyFallbackRepresentation(&d)
			fallbackForNativeIncompatibility(&d, target)
			if tc.wantNative {
				require.Equal(t, RepresentationNative, d.Representation)
				return
			}
			require.Equal(t, RepresentationFallback, d.Representation)
			require.Equal(t, tc.encoding, d.FallbackEncoding)
			plan, err := fallbackPlanForDescriptor(d)
			require.NoError(t, err)
			require.Equal(t, arrow.BinaryTypes.String, plan.DataType)
		})
	}
}

func TestUnknownTemporalPrecisionPlansSourceText(t *testing.T) {
	d := SourceFieldDescriptor{Name: "value", Engine: "mssql", SourceType: "DATETIME2", TemporalSemantics: TemporalLocalTimestamp, TemporalPrecision: -1, TemporalPrecisionKnown: false, Representation: RepresentationNative}
	fallbackForNativeIncompatibility(&d, ConfiguredTargetCapabilities())
	require.Equal(t, RepresentationFallback, d.Representation)
	require.Equal(t, "source_text_v1", d.FallbackEncoding)
}

func TestDecimalTextFallbackAndMetadata(t *testing.T) {
	d := SourceFieldDescriptor{Name: "amount", Engine: "clickhouse", SourceType: "DECIMAL256(5)", Precision: 76, Scale: 5, PrecisionKnown: true, ScaleKnown: true}
	classifyFallbackRepresentation(&d)
	require.Equal(t, RepresentationFallback, d.Representation)
	require.Equal(t, "decimal_text_v1", d.FallbackEncoding)
	plan, err := fallbackPlanForDescriptor(d)
	require.NoError(t, err)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	value := "123456789012345678901234567890123456789.12345"
	require.NoError(t, plan.Append(b, value))
	require.Error(t, plan.Append(b, "123456789012345678901234567890123456789.123456"))
	d.ArrowType = plan.DataType
	field := arrowFieldFromDescriptor(d)
	representation, ok := field.Metadata.GetValue("orabbit.representation")
	require.True(t, ok)
	require.Equal(t, "fallback", representation)
	encoding, ok := field.Metadata.GetValue("orabbit.fallback.encoding")
	require.True(t, ok)
	require.Equal(t, "decimal_text_v1", encoding)
}

func TestUniversalSourceTextFallbackForUnmappedTypes(t *testing.T) {
	cases := []struct{ engine, typ string }{
		{"mssql", "HIERARCHYID"},
		{"postgres", "MY_ENUM"},
		{"oracle", "CUSTOM_SCALAR"},
		{"clickhouse", "ENUM8('open'=1)"},
	}
	for _, tc := range cases {
		t.Run(tc.engine+"/"+tc.typ, func(t *testing.T) {
			d := SourceFieldDescriptor{Name: "value", Engine: tc.engine, SourceType: tc.typ, Representation: RepresentationNative}
			universalFallbackDescriptor(&d)
			require.Equal(t, RepresentationFallback, d.Representation)
			require.Equal(t, "source_text_v1", d.FallbackEncoding)
			plan, err := fallbackPlanForDescriptor(d)
			require.NoError(t, err)
			b := plan.Builder(memory.DefaultAllocator)
			defer b.Release()
			require.NoError(t, plan.Append(b, "value with Unicode 雪"))
			require.Error(t, plan.Append(b, 17))
		})
	}
}

func TestMSSQLXMLFallbackKeepsLargeExactText(t *testing.T) {
	d := SourceFieldDescriptor{Name: "xml_col", Engine: "mssql", SourceType: "XML"}
	classifySourceRepresentation(&d)
	classifyFallbackRepresentation(&d)
	require.Equal(t, "xml_utf8_text_v1", d.FallbackEncoding)
	plan, err := fallbackPlanForDescriptor(d)
	require.NoError(t, err)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	xml := `<root snow="☃">&amp;<nested>` + string(make([]byte, 9000)) + `</nested></root>`
	xml = strings.ReplaceAll(xml, "\x00", "x")
	require.NoError(t, plan.Append(b, xml))
	require.NoError(t, plan.Append(b, nil))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, xml, a.Value(0))
	require.True(t, a.IsNull(1))
}

func TestHexFallbackIsReversible(t *testing.T) {
	d := SourceFieldDescriptor{Name: "blob", Engine: "oracle", SourceType: "BLOB", FallbackEncoding: "hex_v1", Representation: RepresentationFallback}
	plan, err := fallbackPlanForDescriptor(d)
	require.NoError(t, err)
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()
	raw := []byte{0, 0xff, 0x80, 'x'}
	require.NoError(t, plan.Append(b, raw))
	require.Error(t, plan.Append(b, string(raw)))
	a := b.NewArray().(*array.String)
	defer a.Release()
	require.Equal(t, "00ff8078", a.Value(0))
}
