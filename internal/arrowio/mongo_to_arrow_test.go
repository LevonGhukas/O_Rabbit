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

func TestCurrentMongoUnexpectedIntegerValueBecomesZero(t *testing.T) {
	docs := []map[string]any{
		{"value": int64(7)},
		{"value": "not-an-integer"},
	}
	schema, err := InferMongoSchema(docs)
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Int64, schema.Field(0).Type)

	record, err := MongoDocsToRecord(memory.NewGoAllocator(), schema, docs)
	require.NoError(t, err)
	defer record.Release()
	values := record.Column(0).(*array.Int64)
	require.Equal(t, int64(7), values.Value(0))
	require.False(t, values.IsNull(1))
	require.Equal(t, int64(0), values.Value(1))
}

func TestCurrentMongoIncompatibleValuesBecomeNull(t *testing.T) {
	docs := []map[string]any{
		{"value": float64(1.5)},
		{"value": "not-a-float"},
	}
	schema, err := InferMongoSchema(docs)
	require.NoError(t, err)
	require.Equal(t, arrow.PrimitiveTypes.Float64, schema.Field(0).Type)

	record, err := MongoDocsToRecord(memory.NewGoAllocator(), schema, docs)
	require.NoError(t, err)
	defer record.Release()
	values := record.Column(0).(*array.Float64)
	require.Equal(t, float64(1.5), values.Value(0))
	require.True(t, values.IsNull(1))
}

func TestCurrentMongoInferenceFallsBackToString(t *testing.T) {
	schema, err := InferMongoSchema([]map[string]any{{
		"document": primitive.M{"nested": true},
		"empty":    nil,
		"id":       primitive.NewObjectID(),
	}})
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, schema.Field(0).Type)
	require.Equal(t, arrow.BinaryTypes.String, schema.Field(1).Type)
	require.Equal(t, arrow.BinaryTypes.String, schema.Field(2).Type)
}
