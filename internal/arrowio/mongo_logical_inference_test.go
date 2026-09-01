package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestMongoLogicalInferencePromotionsAndWarnings(t *testing.T) {
	result, err := InferMongoSchemaResult([]map[string]any{{"i": int32(1), "safe": int64(2), "large": int64(1 << 54), "nullable": nil, "mixed": 7}, {"i": int64(2), "safe": 2.5, "large": 2.5, "nullable": int64(3), "mixed": "seven"}}, nil)
	require.NoError(t, err)
	logical := map[string]typesystem.LogicalType{}
	for _, f := range result.Fields {
		logical[f.Name] = f.LogicalType
	}
	require.Equal(t, typesystem.KindInt64, logical["i"].Kind)
	require.Equal(t, typesystem.KindFloat64, logical["safe"].Kind)
	require.Equal(t, typesystem.KindUnknown, logical["large"].Kind)
	require.True(t, logical["nullable"].Nullable)
	require.Equal(t, typesystem.KindUnknown, logical["mixed"].Kind)
	require.NotEmpty(t, result.Warnings)
}

func TestMongoLogicalInferenceBSONArraysAndRecords(t *testing.T) {
	oid := primitive.NewObjectID()
	ts := primitive.Timestamp{T: 7, I: 9}
	decimal, err := primitive.ParseDecimal128("12.34")
	require.NoError(t, err)
	docs := []map[string]any{{"array": []any{int64(1), int64(2)}, "nested": []any{[]any{"a"}}, "empty": []any{}, "oid": oid, "date": primitive.NewDateTimeFromTime(time.Now()), "stamp": ts, "binary": primitive.Binary{Data: []byte{1}}, "decimal": decimal}, {"array": []any{int64(3)}, "empty": []any{"x"}, "binary": []byte{2}, "decimal": mustDecimal(t, "100.00")}}
	result, err := InferMongoSchemaResult(docs, nil)
	require.NoError(t, err)
	logical := map[string]typesystem.LogicalType{}
	for _, f := range result.Fields {
		logical[f.Name] = f.LogicalType
	}
	require.Equal(t, typesystem.KindArray, logical["array"].Kind)
	require.Equal(t, typesystem.KindInt64, logical["array"].Element.Kind)
	require.Equal(t, typesystem.KindUnknown, logical["oid"].Kind)
	require.Equal(t, typesystem.KindTimestampTZ, logical["date"].Kind)
	require.Equal(t, typesystem.KindUnknown, logical["stamp"].Kind)
	require.Equal(t, typesystem.KindBinary, logical["binary"].Kind)
	require.True(t, logical["decimal"].Equal(typesystem.Decimal(5, 2)))
	record, err := MongoDocsToRecord(memory.DefaultAllocator, result.Schema, docs)
	require.NoError(t, err)
	defer record.Release()
	oidCol := record.Column(indexOf(result.Schema, "oid")).(*array.String)
	require.Equal(t, oid.Hex(), oidCol.Value(0))
	stampCol := record.Column(indexOf(result.Schema, "stamp")).(*array.String)
	require.Contains(t, stampCol.Value(0), "$timestamp")
	fields := result.Schema.Fields()
	require.Equal(t, arrow.ListOf(arrow.PrimitiveTypes.Int64), fields[indexOf(result.Schema, "array")].Type)
}
func mustDecimal(t *testing.T, s string) primitive.Decimal128 {
	d, err := primitive.ParseDecimal128(s)
	require.NoError(t, err)
	return d
}
func indexOf(s *arrow.Schema, name string) int {
	for i := 0; i < s.NumFields(); i++ {
		if s.Field(i).Name == name {
			return i
		}
	}
	return -1
}
