package parquetio_test

import (
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
)

func TestPostgresMigratedPlansWriteParquet(t *testing.T) {
	columns := []struct {
		name, source     string
		precision, scale int64
		decimal          bool
		value            any
	}{
		{"id", "INT4", 0, 0, false, int32(7)}, {"amount", "NUMERIC", 18, 2, true, "12.34"},
		{"created", "TIMESTAMP", 0, 0, false, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{"uuid", "UUID", 0, 0, false, "a0b1c2d3-e4f5-6789-abcd-0123456789ab"}, {"json", "JSONB", 0, 0, false, map[string]any{"a": "one"}},
		{"items", "INTEGER[]", 0, 0, false, "{1,2,3}"},
	}
	fields := make([]arrow.Field, 0, len(columns))
	values := make([]arrow.Array, 0, len(columns))
	for _, column := range columns {
		plan := arrowio.PlanForSQLColumn("postgres", column.name, column.source, column.precision, column.scale, column.decimal)
		builder := plan.Builder(memory.DefaultAllocator)
		require.NoError(t, plan.Append(builder, column.value))
		values = append(values, builder.NewArray())
		builder.Release()
		fields = append(fields, arrow.Field{Name: plan.Name, Type: plan.DataType, Nullable: true})
	}
	defer func() {
		for _, value := range values {
			value.Release()
		}
	}()
	record := array.NewRecordBatch(arrow.NewSchema(fields, nil), values, 1)
	defer record.Release()
	writer, path, err := parquetio.NewTempFileWriter(record.Schema(), parquetio.Options{})
	require.NoError(t, err)
	defer os.Remove(path)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	meta, err := parquetio.ComputeFileMeta(path)
	require.NoError(t, err)
	require.Greater(t, meta.Bytes, int64(0))
}
