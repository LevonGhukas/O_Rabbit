package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func TestFlightSQLTimestampNanosecondsFallsBackToExactText(t *testing.T) {
	typ := &arrow.TimestampType{Unit: arrow.Nanosecond}
	b := array.NewTimestampBuilder(memory.DefaultAllocator, typ)
	defer b.Release()
	b.Append(arrow.Timestamp(1787572984123456789))
	rec := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "created_at", Type: typ, Nullable: true}}, nil), []arrow.Array{b.NewArray()}, 1)
	defer rec.Release()
	schema, out, err := RecordForConfiguredTarget(rec)
	require.NoError(t, err)
	defer out.Release()
	require.Equal(t, arrow.BinaryTypes.String, schema.Field(0).Type)
	values := out.Column(0).(*array.String)
	require.Equal(t, "2026-08-24 12:03:04.123456789", values.Value(0))
	encoding, ok := schema.Field(0).Metadata.GetValue("orabbit.fallback.encoding")
	require.True(t, ok)
	require.Equal(t, "arrow_timestamp_ns_text_v1", encoding)
}
