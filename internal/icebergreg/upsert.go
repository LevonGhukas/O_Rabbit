package icebergreg

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

// applyUpsertDelete removes the existing rows that the run's files replace.
// A full refresh replaces every file anyway, and an empty table has nothing
// to replace, so only incremental runs into a table with data delete rows.
func applyUpsertDelete(ctx context.Context, tx *icetable.Transaction, tbl *icetable.Table, reg RunConfig, fullRefresh bool, newFiles []string, props iceberg.Properties) error {
	if !reg.Upsert.Enabled || fullRefresh || tbl.CurrentSnapshot() == nil {
		return nil
	}
	filter, err := buildUpsertDeleteFilter(ctx, tbl, newFiles, reg.Upsert.Keys, reg.Upsert.MaxKeys)
	if err != nil {
		return err
	}
	if filter.Equals(iceberg.AlwaysFalse{}) {
		return nil
	}
	return deleteUpsertMatches(ctx, tx, filter, reg.Upsert.Mode, props)
}

// deleteUpsertMatches deletes the rows an upsert replaces. It always deletes
// copy-on-write: iceberg-go v0.5's merge-on-read delete writes no position
// deletes for files added without Parquet field IDs, which is how O_Rabbit
// registers every worker file, so the old rows would silently stay and each
// key would end up with two current rows. The table keeps the configured
// write.delete.mode for other engines.
func deleteUpsertMatches(ctx context.Context, tx *icetable.Transaction, filter iceberg.BooleanExpression, mode string, props iceberg.Properties) error {
	if err := tx.SetProperties(iceberg.Properties{icetable.WriteDeleteModeKey: icetable.WriteModeCopyOnWrite}); err != nil {
		return err
	}
	if err := tx.Delete(ctx, filter, props); err != nil {
		return fmt.Errorf("iceberg upsert delete existing rows: %w", err)
	}
	if mode == "" {
		mode = icetable.WriteModeMergeOnRead
	}
	return tx.SetProperties(iceberg.Properties{icetable.WriteDeleteModeKey: mode})
}

// Default upsert.max_keys values: the most incoming rows (and so keys) one
// upsert run may carry. The key set, the delete filter and iceberg-go's
// delete planning grow with the key count, and iceberg-go v0.5 has no
// equality-delete writer that would avoid it, so larger runs are rejected
// before any row is read. A single key column is matched with one IN set;
// composite keys need one predicate per key, so every scanned row is tested
// against every key and their bound is much lower.
const (
	DefaultUpsertMaxKeys          = 500_000
	DefaultCompositeUpsertMaxKeys = 10_000
)

// upsertMaxKeys returns the effective key bound for keys.
func upsertMaxKeys(configured int64, keys []string) int64 {
	if configured > 0 {
		return configured
	}
	if len(keys) > 1 {
		return DefaultCompositeUpsertMaxKeys
	}
	return DefaultUpsertMaxKeys
}

// upsertInPruneLimit is iceberg-go's inPredicateLimit: above 200 values an IN
// predicate no longer prunes data files by column bounds.
const upsertInPruneLimit = 200

// upsertKeyRanges is how many sorted key ranges accompany a large IN set so
// iceberg-go can still skip data files whose bounds miss every range. It also
// bounds the per-row cost of the filter: one set lookup plus this many range
// checks, whatever the key count.
const upsertKeyRanges = 64

// buildUpsertDeleteFilter returns a filter matching the existing rows whose
// keys appear in files. It rejects runs above maxKeys from the Parquet
// footers alone, then reads only the key columns.
func buildUpsertDeleteFilter(ctx context.Context, tbl *icetable.Table, files, keys []string, maxKeys int64) (iceberg.BooleanExpression, error) {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, err
	}
	maxKeys = upsertMaxKeys(maxKeys, keys)

	var incoming int64
	for _, path := range files {
		rows, err := parquetRowCount(fs, path)
		if err != nil {
			return nil, err
		}
		incoming += rows
	}
	if incoming > maxKeys {
		return nil, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf(
			"upsert run has %d incoming rows, above upsert.max_keys=%d; split the export into smaller incremental runs, use a full refresh, or raise upsert.max_keys",
			incoming, maxKeys))
	}

	tuples := make([][]iceberg.Literal, 0, incoming)
	seen := make(map[string]struct{}, incoming)
	for _, path := range files {
		err := readUpsertKeyColumns(ctx, fs, path, keys, func(record arrow.RecordBatch) error {
			rowKeys, err := upsertKeysFromRecord(tbl.Schema(), record, keys, seen)
			if err != nil {
				return err
			}
			tuples = append(tuples, rowKeys...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("read upsert parquet %s: %w", path, err)
		}
	}
	return upsertDeleteExpression(keys, tuples), nil
}

func parquetRowCount(fs iceio.IO, path string) (int64, error) {
	input, err := fs.Open(path)
	if err != nil {
		return 0, fmt.Errorf("open upsert parquet %s: %w", path, err)
	}
	reader, err := file.NewParquetReader(input)
	if err != nil {
		_ = input.Close()
		return 0, fmt.Errorf("read upsert parquet footer %s: %w", path, err)
	}
	defer reader.Close()
	return reader.NumRows(), nil
}

// readUpsertKeyColumns streams only the key columns of one file.
func readUpsertKeyColumns(ctx context.Context, fs iceio.IO, path string, keys []string, fn func(arrow.RecordBatch) error) error {
	input, err := fs.Open(path)
	if err != nil {
		return err
	}
	parquetReader, err := file.NewParquetReader(input)
	if err != nil {
		_ = input.Close()
		return err
	}
	defer parquetReader.Close()
	columns := make([]int, len(keys))
	for i, key := range keys {
		columns[i] = parquetReader.MetaData().Schema.ColumnIndexByName(key)
		if columns[i] < 0 {
			return fmt.Errorf("upsert key column %q is missing in parquet schema", key)
		}
	}
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 64 * 1024}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	records, err := arrowReader.GetRecordReader(ctx, columns, nil)
	if err != nil {
		return err
	}
	defer records.Release()
	for records.Next() {
		if err := fn(records.RecordBatch()); err != nil {
			return err
		}
	}
	return records.Err()
}

// upsertKeysFromRecord returns each row's key literals in keys order. It
// rejects NULL keys and keys already in seen, so one run never carries two
// rows for the same key.
func upsertKeysFromRecord(schema *iceberg.Schema, record arrow.RecordBatch, keys []string, seen map[string]struct{}) ([][]iceberg.Literal, error) {
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

	out := make([][]iceberg.Literal, 0, record.NumRows())
	var signature bytes.Buffer
	for row := 0; row < int(record.NumRows()); row++ {
		tuple := make([]iceberg.Literal, len(keys))
		signature.Reset()
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
			tuple[i] = literal
		}
		if _, exists := seen[signature.String()]; exists {
			return nil, fmt.Errorf("incoming upsert data contains a duplicate key")
		}
		seen[signature.String()] = struct{}{}
		out = append(out, tuple)
	}
	return out, nil
}

// upsertDeleteExpression matches rows whose key equals one of tuples.
//
// A single key column becomes one IN set, so each row costs one set lookup.
// Above upsertInPruneLimit values it is AND-ed with at most upsertKeyRanges
// sorted [min, max] ranges, so a copy-on-write delete still rewrites only data
// files whose bounds overlap the incoming keys.
//
// Composite keys need exact tuple matches, so they stay an OR of per-row AND
// predicates; their cost grows with the key count, which max_keys bounds.
func upsertDeleteExpression(keys []string, tuples [][]iceberg.Literal) iceberg.BooleanExpression {
	if len(tuples) == 0 {
		return iceberg.AlwaysFalse{}
	}
	if len(keys) == 1 {
		ref := iceberg.Reference(keys[0])
		values := make([]iceberg.Literal, len(tuples))
		for i, tuple := range tuples {
			values[i] = tuple[0]
		}
		in := iceberg.SetPredicate(iceberg.OpIn, ref, values)
		if len(values) <= upsertInPruneLimit {
			return in
		}
		sort.Slice(values, func(i, j int) bool { return compareLiterals(values[i], values[j]) < 0 })
		size := (len(values) + upsertKeyRanges - 1) / upsertKeyRanges
		ranges := make([]iceberg.BooleanExpression, 0, upsertKeyRanges)
		for start := 0; start < len(values); start += size {
			lo, hi := values[start], values[min(start+size, len(values))-1]
			ranges = append(ranges, iceberg.NewAnd(
				iceberg.LiteralPredicate(iceberg.OpGTEQ, ref, lo),
				iceberg.LiteralPredicate(iceberg.OpLTEQ, ref, hi),
			))
		}
		return iceberg.NewAnd(in, joinExpressions(ranges, false))
	}
	expressions := make([]iceberg.BooleanExpression, 0, len(tuples))
	parts := make([]iceberg.BooleanExpression, len(keys))
	for _, tuple := range tuples {
		for i, key := range keys {
			parts[i] = iceberg.LiteralPredicate(iceberg.OpEQ, iceberg.Reference(key), tuple[i])
		}
		expressions = append(expressions, joinExpressions(parts, true))
	}
	return joinExpressions(expressions, false)
}

// compareLiterals orders two literals of the same Iceberg type.
func compareLiterals(a, b iceberg.Literal) int {
	switch x := a.(type) {
	case iceberg.TypedLiteral[bool]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[bool]).Value())
	case iceberg.TypedLiteral[int32]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[int32]).Value())
	case iceberg.TypedLiteral[int64]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[int64]).Value())
	case iceberg.TypedLiteral[float32]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[float32]).Value())
	case iceberg.TypedLiteral[float64]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[float64]).Value())
	case iceberg.TypedLiteral[iceberg.Date]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[iceberg.Date]).Value())
	case iceberg.TypedLiteral[iceberg.Time]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[iceberg.Time]).Value())
	case iceberg.TypedLiteral[iceberg.Timestamp]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[iceberg.Timestamp]).Value())
	case iceberg.TypedLiteral[iceberg.TimestampNano]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[iceberg.TimestampNano]).Value())
	case iceberg.TypedLiteral[string]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[string]).Value())
	case iceberg.TypedLiteral[[]byte]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[[]byte]).Value())
	case iceberg.TypedLiteral[uuid.UUID]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[uuid.UUID]).Value())
	case iceberg.TypedLiteral[iceberg.Decimal]:
		return x.Comparator()(x.Value(), b.(iceberg.TypedLiteral[iceberg.Decimal]).Value())
	}
	panic(fmt.Sprintf("upsert key literal %T is not comparable", a))
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
