package arrowio

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestInferMongoSchema(t *testing.T) {
	docs := []map[string]any{
		{
			"name": "alice",
			"age":  30,
			"ok":   true,
		},
	}

	schema, err := InferMongoSchema(docs)

	require.NoError(t, err)
	require.NotNil(t, schema)

	require.Equal(t, 3, schema.NumFields())
	require.Equal(t, "age", schema.Field(0).Name)
	require.Equal(t, "name", schema.Field(1).Name)
	require.Equal(t, "ok", schema.Field(2).Name)
}

func TestInferMongoSchemaEmpty(t *testing.T) {
	_, err := InferMongoSchema(nil)

	require.Error(t, err)
}

func TestInferMongoSchemaWithFieldOrderPreservesSourceOrder(t *testing.T) {
	docs := []map[string]any{{
		"id":   int64(1),
		"date": "2026-01-01",
		"age":  int64(30),
	}}

	schema, err := InferMongoSchemaWithFieldOrder(docs, []string{"id", "date", "age"})
	require.NoError(t, err)
	require.Equal(t, "id", schema.Field(0).Name)
	require.Equal(t, "date", schema.Field(1).Name)
	require.Equal(t, "age", schema.Field(2).Name)
}

func TestInferMongoSchemaWithFieldOrderPreservesParquetOrder(t *testing.T) {
	docs := []map[string]any{{
		"id":   int64(1),
		"date": "2026-01-01",
		"age":  int64(30),
	}}
	schema, err := InferMongoSchemaWithFieldOrder(docs, []string{"id", "date", "age"})
	require.NoError(t, err)
	record, err := MongoDocsToRecord(memory.NewGoAllocator(), schema, docs)
	require.NoError(t, err)
	defer record.Release()

	var data bytes.Buffer
	writer, err := pqarrow.NewFileWriter(schema, &data, nil, pqarrow.NewArrowWriterProperties())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	parquetReader, err := file.NewParquetReader(bytes.NewReader(data.Bytes()))
	require.NoError(t, err)
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	require.NoError(t, err)
	recordReader, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
	require.NoError(t, err)
	defer recordReader.Release()
	require.True(t, recordReader.Next())
	got := recordReader.Record().Schema()
	require.Equal(t, "id", got.Field(0).Name)
	require.Equal(t, "date", got.Field(1).Name)
	require.Equal(t, "age", got.Field(2).Name)
}

func TestMongoDocsToRecord(t *testing.T) {
	docs := []map[string]any{
		{
			"name": "alice",
			"age":  30,
			"ok":   true,
		},
		{
			"name": "bob",
			"age":  40,
			"ok":   false,
		},
	}

	schema, err := InferMongoSchema(docs)
	require.NoError(t, err)

	record, err := MongoDocsToRecord(
		memory.NewGoAllocator(),
		schema,
		docs,
	)

	require.NoError(t, err)
	require.NotNil(t, record)

	defer record.Release()

	require.Equal(t, int64(2), record.NumRows())
	require.Equal(t, 3, int(record.NumCols()))
}

func TestMongoDocsToRecordEmpty(t *testing.T) {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{
				Name: "name",
				Type: arrow.BinaryTypes.String,
			},
		},
		nil,
	)

	record, err := MongoDocsToRecord(
		memory.NewGoAllocator(),
		schema,
		nil,
	)

	require.NoError(t, err)
	require.Nil(t, record)
}

func TestInferMongoSchemaSpecialTypes(t *testing.T) {
	docs := []map[string]any{
		{
			"id":      primitive.NewObjectID(),
			"time":    time.Now(),
			"decimal": primitive.NewDecimal128(1, 2),
			"bytes":   []byte{1, 2, 3},
		},
	}

	schema, err := InferMongoSchema(docs)

	require.NoError(t, err)
	require.NotNil(t, schema)

	require.Equal(t, 4, schema.NumFields())
}

func TestMongoSchemaPlanIsOrderIndependentAndLossless(t *testing.T) {
	decimal, err := primitive.ParseDecimal128("12345.6789")
	require.NoError(t, err)
	docs := []map[string]any{{"n": int32(1), "mixed": "one", "id": primitive.NewObjectID(), "amount": decimal}, {"n": int64(2), "mixed": int64(2), "stamp": primitive.Timestamp{T: 5, I: 7}}}
	reversed := []map[string]any{docs[1], docs[0]}
	left, err := InferMongoSchemaPlan(docs, nil)
	require.NoError(t, err)
	right, err := InferMongoSchemaPlan(reversed, nil)
	require.NoError(t, err)
	require.True(t, mongoArrowSchemaEqual(left.Schema, right.Schema))
	require.Equal(t, arrow.PrimitiveTypes.Int64, left.byName["n"].typ)
	require.Equal(t, mongoExtendedJSONCodec, left.byName["mixed"].policy.Fallback.Name)
	require.Equal(t, mongoObjectIDCodec, left.byName["id"].policy.Fallback.Name)
	require.Equal(t, mongoDecimalCodec, left.byName["amount"].policy.Fallback.Name)
	require.Equal(t, mongoTimestampCodec, left.byName["stamp"].policy.Fallback.Name)
	record, err := MongoDocsToRecordWithPlan(memory.NewGoAllocator(), left, docs)
	require.NoError(t, err)
	defer record.Release()
	amount := record.Column(0) // deterministic alphabetical order: amount
	require.Equal(t, "12345.6789", amount.(*array.String).Value(0))
}

func TestMongoSchemaPlanRejectsLateFieldAndMismatch(t *testing.T) {
	plan, err := InferMongoSchemaPlan([]map[string]any{{"value": int32(1)}}, nil)
	require.NoError(t, err)
	_, err = MongoDocsToRecordWithPlan(memory.NewGoAllocator(), plan, []map[string]any{{"value": "wrong"}})
	require.ErrorAs(t, err, new(*MongoSchemaViolationError))
	_, err = MongoDocsToRecordWithPlan(memory.NewGoAllocator(), plan, []map[string]any{{"value": int32(1), "late": true}})
	require.ErrorAs(t, err, new(*MongoSchemaViolationError))
}

func TestMongoBinarySubtypeAndMissingNullPolicy(t *testing.T) {
	plan, err := InferMongoSchemaPlan([]map[string]any{{"bin": primitive.Binary{Subtype: 4, Data: []byte{1}}}, {"bin": primitive.Binary{Subtype: 4, Data: []byte{2}}}, {}}, nil)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.Binary, plan.byName["bin"].typ)
	require.Equal(t, "true", plan.byName["bin"].policy.Metadata.Properties["mongodb.missing_and_null_collapsed"])
	mixed, err := InferMongoSchemaPlan([]map[string]any{{"bin": primitive.Binary{Subtype: 3, Data: []byte{1}}}, {"bin": primitive.Binary{Subtype: 4, Data: []byte{1}}}}, nil)
	require.NoError(t, err)
	require.Equal(t, mongoExtendedJSONCodec, mixed.byName["bin"].policy.Fallback.Name)
}

func TestMongoNumericWideningToFloat64(t *testing.T) {
	plan, err := InferMongoSchemaPlan([]map[string]any{{"n": int32(1)}, {"n": float64(2.5)}}, nil)
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Float64, plan.byName["n"].typ)
	record, err := MongoDocsToRecordWithPlan(memory.NewGoAllocator(), plan, []map[string]any{{"n": int32(1)}, {"n": float64(2.5)}})
	require.NoError(t, err)
	defer record.Release()
	values := record.Column(0).(*array.Float64)
	require.Equal(t, 1.0, values.Value(0))
	require.Equal(t, 2.5, values.Value(1))
}
