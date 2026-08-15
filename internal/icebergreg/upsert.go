package icebergreg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
)

func buildUpsertDeleteFilter(ctx context.Context, tbl *icetable.Table, files, keys []string) (iceberg.BooleanExpression, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}

	expressions := make([]iceberg.BooleanExpression, 0)
	seen := make(map[string]struct{})
	for _, path := range files {
		input, err := fs.Open(path)
		if err != nil {
			return nil, fmt.Errorf("open upsert parquet %s: %w", path, err)
		}
		err = func() error {
			defer input.Close()
			parquetReader, err := file.NewParquetReader(input)
			if err != nil {
				return err
			}
			arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 64 * 1024}, memory.DefaultAllocator)
			if err != nil {
				return err
			}
			records, err := arrowReader.GetRecordReader(ctx, nil, nil)
			if err != nil {
				return err
			}
			defer records.Release()

			for records.Next() {
				record := records.RecordBatch()
				rowExpressions, err := upsertExpressionsFromRecord(tbl.Schema(), record, keys, seen)
				if err != nil {
					return err
				}
				expressions = append(expressions, rowExpressions...)
			}
			return records.Err()
		}()
		if err != nil {
			return nil, fmt.Errorf("read upsert parquet %s: %w", path, err)
		}
	}
	if len(expressions) == 0 {
		return iceberg.AlwaysFalse{}, nil
	}
	return joinExpressions(expressions, false), nil
}

func upsertExpressionsFromRecord(schema *iceberg.Schema, record arrow.RecordBatch, keys []string, seen map[string]struct{}) ([]iceberg.BooleanExpression, error) {
	columnIndexes := make([]int, len(keys))
	fields := make([]iceberg.NestedField, len(keys))
	for i, key := range keys {
		indexes := record.Schema().FieldIndices(key)
		if len(indexes) != 1 {
			return nil, fmt.Errorf("upsert key column %q is missing or ambiguous in parquet schema", key)
		}
		field, ok := schema.FindFieldByName(key)
		if !ok {
			return nil, fmt.Errorf("upsert key column %q does not exist in Iceberg schema", key)
		}
		columnIndexes[i] = indexes[0]
		fields[i] = field
	}

	out := make([]iceberg.BooleanExpression, 0, record.NumRows())
	for row := 0; row < int(record.NumRows()); row++ {
		parts := make([]iceberg.BooleanExpression, 0, len(keys))
		var signature bytes.Buffer
		for i, key := range keys {
			column := record.Column(columnIndexes[i])
			if column.IsNull(row) {
				return nil, fmt.Errorf("upsert key column %q contains NULL", key)
			}
			literal, err := literalFromArrow(column, row, fields[i].Type)
			if err != nil {
				return nil, fmt.Errorf("upsert key column %q: %w", key, err)
			}
			raw, err := literal.MarshalBinary()
			if err != nil {
				return nil, err
			}
			var size [8]byte
			binary.BigEndian.PutUint64(size[:], uint64(len(raw)))
			signature.Write(size[:])
			signature.Write(raw)
			parts = append(parts, iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference(key), literal))
		}
		if _, exists := seen[signature.String()]; exists {
			return nil, fmt.Errorf("incoming upsert data contains a duplicate key")
		}
		seen[signature.String()] = struct{}{}
		out = append(out, joinExpressions(parts, true))
	}
	return out, nil
}

func joinExpressions(expressions []iceberg.BooleanExpression, and bool) iceberg.BooleanExpression {
	if len(expressions) == 1 {
		return expressions[0]
	}
	middle := len(expressions) / 2
	left := joinExpressions(expressions[:middle], and)
	right := joinExpressions(expressions[middle:], and)
	if and {
		return iceberg.NewAnd(left, right)
	}
	return iceberg.NewOr(left, right)
}

func literalFromArrow(values arrow.Array, row int, fieldType iceberg.Type) (iceberg.Literal, error) {
	switch values := values.(type) {
	case *array.Boolean:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.Int8:
		return iceberg.NewLiteral(int32(values.Value(row))), nil
	case *array.Int16:
		return iceberg.NewLiteral(int32(values.Value(row))), nil
	case *array.Int32:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.Int64:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.Float32:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.Float64:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.String:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.LargeString:
		return iceberg.NewLiteral(values.Value(row)), nil
	case *array.Binary:
		return iceberg.NewLiteral(append([]byte(nil), values.Value(row)...)), nil
	case *array.LargeBinary:
		return iceberg.NewLiteral(append([]byte(nil), values.Value(row)...)), nil
	case *array.FixedSizeBinary:
		raw := append([]byte(nil), values.Value(row)...)
		if _, ok := fieldType.(iceberg.UUIDType); ok {
			value, err := uuid.FromBytes(raw)
			if err != nil {
				return nil, err
			}
			return iceberg.NewLiteral(value), nil
		}
		return iceberg.NewLiteral(raw), nil
	case *array.Date32:
		return iceberg.NewLiteral(iceberg.Date(values.Value(row))), nil
	case *array.Date64:
		return iceberg.NewLiteral(iceberg.Date(int64(values.Value(row)) / (24 * 60 * 60 * 1000))), nil
	case *array.Time32:
		unit := values.DataType().(*arrow.Time32Type).Unit
		return iceberg.NewLiteral(iceberg.Time(timeToMicros(int64(values.Value(row)), unit))), nil
	case *array.Time64:
		unit := values.DataType().(*arrow.Time64Type).Unit
		return iceberg.NewLiteral(iceberg.Time(timeToMicros(int64(values.Value(row)), unit))), nil
	case *array.Timestamp:
		unit := values.DataType().(*arrow.TimestampType).Unit
		value := int64(values.Value(row))
		switch fieldType.(type) {
		case iceberg.TimestampNsType, iceberg.TimestampTzNsType:
			return iceberg.NewLiteral(iceberg.TimestampNano(timeToNanos(value, unit))), nil
		default:
			return iceberg.NewLiteral(iceberg.Timestamp(timeToMicros(value, unit))), nil
		}
	case *array.Decimal128:
		typeInfo, ok := fieldType.(iceberg.DecimalType)
		if !ok {
			return nil, fmt.Errorf("decimal Arrow value does not match Iceberg type %s", fieldType)
		}
		return iceberg.NewLiteral(iceberg.Decimal{Val: values.Value(row), Scale: typeInfo.Scale()}), nil
	default:
		return nil, fmt.Errorf("unsupported Arrow key type %s", values.DataType())
	}
}

func timeToMicros(value int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return value * 1_000_000
	case arrow.Millisecond:
		return value * 1_000
	case arrow.Nanosecond:
		return value / 1_000
	default:
		return value
	}
}

func timeToNanos(value int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return value * 1_000_000_000
	case arrow.Millisecond:
		return value * 1_000_000
	case arrow.Microsecond:
		return value * 1_000
	default:
		return value
	}
}
