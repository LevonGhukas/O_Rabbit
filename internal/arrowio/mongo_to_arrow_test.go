package arrowio

import (
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
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
