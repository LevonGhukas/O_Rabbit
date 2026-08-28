package arrowio

import (
	"context"
	"database/sql"
	_ "modernc.org/sqlite"
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func requireBinaryConversionError(t *testing.T, err error, inputType, reason string) {
	t.Helper()
	var conversionErr *BinaryConversionError
	require.ErrorAs(t, err, &conversionErr)
	require.Equal(t, "Binary", conversionErr.Target)
	if inputType != "" {
		require.Equal(t, inputType, conversionErr.InputType)
	}
	require.Equal(t, reason, conversionErr.Reason)
}

func TestBinaryExactBytePreservation(t *testing.T) {
	plan := planBinary("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	values := [][]byte{
		{},
		{0x61, 0x62, 0x63},
		{0x00, 0x01, 0x00},
		{0xff, 0xfe, 0xfd},
		{0x00, 0x7f, 0x80, 0xff},
	}
	for _, value := range values {
		require.NoError(t, plan.Append(builder, value))
	}
	require.NoError(t, plan.Append(builder, nil))

	// BinaryBuilder copies its input into its internal values buffer.
	mutated := []byte{0x12, 0x34}
	require.NoError(t, plan.Append(builder, mutated))
	mutated[0] = 0xff

	result := builder.NewArray().(*array.Binary)
	defer result.Release()
	require.Equal(t, len(values)+2, result.Len())
	for i, want := range values {
		require.Equal(t, want, result.Value(i))
	}
	require.True(t, result.IsNull(len(values)))
	require.Equal(t, []byte{0x12, 0x34}, result.Value(len(values)+1))
}

func TestBinaryRejectsNonByteInputsWithoutAppending(t *testing.T) {
	type unsupported struct{ Value int }
	plan := planBinary("value")
	builder := plan.Builder(memory.DefaultAllocator)
	defer builder.Release()

	for _, value := range []any{"abc", 123, 1.25, unsupported{Value: 7}} {
		err := plan.Append(builder, value)
		requireBinaryConversionError(t, err, "", "non-byte source representation")
	}

	result := builder.NewArray().(*array.Binary)
	defer result.Release()
	require.Zero(t, result.Len(), "failed non-null values must not become binary values or nulls")
}

func TestSQLBinaryPlanPropagatesNonByteError(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE binary_values (payload BLOB)")
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO binary_values (payload) VALUES (123)")
	require.NoError(t, err)

	rows, err := db.QueryContext(context.Background(), "SELECT payload FROM binary_values")
	require.NoError(t, err)
	defer rows.Close()
	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)

	count, _, err := RowsToRecordBatchesEngineWithOverrides("mysql", rows, []string{"payload"}, columnTypes, nil, 1, memory.DefaultAllocator, -1, connectors.CursorDomainUnknown, func(_ *arrow.Schema, _ arrow.RecordBatch) error {
		return nil
	})
	require.Zero(t, count)
	requireBinaryConversionError(t, err, "int64", "non-byte source representation")
}
