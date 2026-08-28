package arrowio

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

type ColumnPlan struct {
	Name     string
	DataType arrow.DataType
	Builder  func(mem memory.Allocator) array.Builder
	Append   func(b array.Builder, v any) error
	// Policy describes the source type mapping internally. It is not emitted
	// into Arrow metadata and therefore cannot affect physical output in Phase 0.
	Policy *TypePolicy
}

// schemaFromPlans handles schema from plans behavior.
func schemaFromPlans(plans []ColumnPlan) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(plans))
	for _, p := range plans {
		fields = append(fields, arrow.Field{Name: p.Name, Type: p.DataType, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

// PlansFromSQL converts sql column types to ColumnPlans and an Arrow Schema.
func PlansFromSQL(cols []string, colTypes []*sql.ColumnType) ([]ColumnPlan, *arrow.Schema, error) {
	return PlansFromSQLEngine("", cols, colTypes)
}

// PlansFromSQLEngine converts sql column types to ColumnPlans and an Arrow Schema using the source database engine.
func PlansFromSQLEngine(engine string, cols []string, colTypes []*sql.ColumnType) ([]ColumnPlan, *arrow.Schema, error) {
	if colTypes == nil {
		colTypes = make([]*sql.ColumnType, len(cols))
	} else if len(cols) != len(colTypes) {
		return nil, nil, fmt.Errorf("cols/colTypes length mismatch")
	}

	plans := make([]ColumnPlan, 0, len(cols))
	fields := make([]arrow.Field, 0, len(cols))
	for i := range cols {
		n := cols[i]
		dbType := ""
		nullable := true
		nullableKnown := false
		var (
			precision  int64
			scale      int64
			hasDecimal bool
		)
		if colTypes[i] != nil {
			dbType = strings.ToUpper(strings.TrimSpace(colTypes[i].DatabaseTypeName()))
			if p, s, ok := colTypes[i].DecimalSize(); ok {
				precision = int64(p)
				scale = int64(s)
				hasDecimal = true
			}
			if isNullable, ok := colTypes[i].Nullable(); ok {
				nullable = isNullable
				nullableKnown = true
			}
		}

		plan := PlanForSQLColumn(engine, n, dbType, precision, scale, hasDecimal)
		if plan.Policy != nil {
			plan.Policy.Metadata.NullableKnown = nullableKnown
			plan.Policy.Metadata.Nullable = nullable
		}
		plans = append(plans, plan)
		fields = append(fields, arrow.Field{Name: plan.Name, Type: plan.DataType, Nullable: nullable})
	}

	return plans, arrow.NewSchema(fields, nil), nil
}

// PlansFromPostgresSQLMetadata overlays catalog-enriched PostgreSQL UDT
// identity onto the normal ColumnType plans. Other SQL engines are untouched.
func PlansFromPostgresSQLMetadata(cols []string, colTypes []*sql.ColumnType, metadata []connectors.PostgresTypeMetadata) ([]ColumnPlan, *arrow.Schema, error) {
	plans, schema, err := PlansFromSQLEngine("postgres", cols, colTypes)
	if err != nil || len(metadata) == 0 {
		return plans, schema, err
	}
	byType := make(map[string]connectors.PostgresTypeMetadata, len(metadata))
	for _, item := range metadata {
		if item.ReportedType != "" {
			byType[strings.ToUpper(item.ReportedType)] = item
		}
	}
	fields := schema.Fields()
	for i, columnType := range colTypes {
		if columnType == nil {
			continue
		}
		item, ok := byType[strings.ToUpper(strings.TrimSpace(columnType.DatabaseTypeName()))]
		if !ok {
			continue
		}
		precision, scale, hasDecimal := int64(0), int64(0), false
		if p, s, ok := columnType.DecimalSize(); ok {
			precision, scale, hasDecimal = int64(p), int64(s), true
		}
		plan := PlanForPostgresColumnWithMetadata(cols[i], columnType.DatabaseTypeName(), precision, scale, hasDecimal, &item)
		plan.Policy.Metadata.NullableKnown = plans[i].Policy.Metadata.NullableKnown
		plan.Policy.Metadata.Nullable = plans[i].Policy.Metadata.Nullable
		plans[i] = plan
		fields[i].Type = plan.DataType
	}
	return plans, arrow.NewSchema(fields, nil), nil
}

func parseTimestampValue(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t, true
	}
	layouts := []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func parseFloat64Text(raw string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func parseBooleanText(raw string) (bool, bool) {
	s := strings.TrimSpace(raw)
	switch {
	case s == "1" || strings.EqualFold(s, "true") || strings.EqualFold(s, "t") || strings.EqualFold(s, "yes") || strings.EqualFold(s, "y"):
		return true, true
	case s == "0" || strings.EqualFold(s, "false") || strings.EqualFold(s, "f") || strings.EqualFold(s, "no") || strings.EqualFold(s, "n"):
		return false, true
	default:
		return false, false
	}
}

func asFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int64:
		return float64(x), true
	case int32:
		return float64(x), true
	case int16:
		return float64(x), true
	case int8:
		return float64(x), true
	case int:
		return float64(x), true
	case uint64:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint:
		return float64(x), true
	case []byte:
		return parseFloat64Text(string(x))
	case string:
		return parseFloat64Text(x)
	default:
		return 0, false
	}
}

func asBool(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case int64:
		return booleanFromInt64(x)
	case int32:
		return booleanFromInt64(int64(x))
	case int16:
		return booleanFromInt64(int64(x))
	case int8:
		return booleanFromInt64(int64(x))
	case int:
		return booleanFromInt64(int64(x))
	case uint64:
		return booleanFromUint64(x)
	case uint32:
		return booleanFromUint64(uint64(x))
	case uint16:
		return booleanFromUint64(uint64(x))
	case uint8:
		return booleanFromUint64(uint64(x))
	case uint:
		return booleanFromUint64(uint64(x))
	case []byte:
		if len(x) == 1 {
			if x[0] == 0 {
				return false, true
			}
			if x[0] == 1 {
				return true, true
			}
		}
		return parseBooleanText(string(x))
	case string:
		return parseBooleanText(x)
	default:
		return false, false
	}
}

func booleanFromInt64(v int64) (bool, bool) {
	if v == 0 {
		return false, true
	}
	if v == 1 {
		return true, true
	}
	return false, false
}

func booleanFromUint64(v uint64) (bool, bool) {
	if v == 0 {
		return false, true
	}
	if v == 1 {
		return true, true
	}
	return false, false
}

func toInt64Checked(v any) (int64, string) {
	switch x := v.(type) {
	case int64:
		return x, ""
	case int32:
		return int64(x), ""
	case int16:
		return int64(x), ""
	case int8:
		return int64(x), ""
	case int:
		return int64(x), ""
	case uint64:
		if x > 9223372036854775807 {
			return 0, "overflow"
		}
		return int64(x), ""
	case uint32:
		return int64(x), ""
	case uint16:
		return int64(x), ""
	case uint8:
		return int64(x), ""
	case uint:
		if uint64(x) > 9223372036854775807 {
			return 0, "overflow"
		}
		return int64(x), ""
	case []byte:
		i, err := strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
		if err != nil {
			return 0, integerParseReason(err)
		}
		return i, ""
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, integerParseReason(err)
		}
		return i, ""
	default:
		return 0, "invalid integer representation"
	}
}

func toUint64Checked(v any) (uint64, string) {
	switch x := v.(type) {
	case uint64:
		return x, ""
	case uint32:
		return uint64(x), ""
	case uint16:
		return uint64(x), ""
	case uint8:
		return uint64(x), ""
	case uint:
		return uint64(x), ""
	case int64:
		if x < 0 {
			return 0, "negative value"
		}
		return uint64(x), ""
	case int32:
		if x < 0 {
			return 0, "negative value"
		}
		return uint64(x), ""
	case int16:
		if x < 0 {
			return 0, "negative value"
		}
		return uint64(x), ""
	case int8:
		if x < 0 {
			return 0, "negative value"
		}
		return uint64(x), ""
	case int:
		if x < 0 {
			return 0, "negative value"
		}
		return uint64(x), ""
	case []byte:
		i, err := strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64)
		if err != nil {
			if strings.HasPrefix(strings.TrimSpace(string(x)), "-") {
				return 0, "negative value"
			}
			return 0, integerParseReason(err)
		}
		return i, ""
	case string:
		i, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64)
		if err != nil {
			if strings.HasPrefix(strings.TrimSpace(x), "-") {
				return 0, "negative value"
			}
			return 0, integerParseReason(err)
		}
		return i, ""
	default:
		return 0, "invalid integer representation"
	}
}

func integerParseReason(err error) string {
	if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
		return "overflow"
	}
	return "invalid integer representation"
}

// RowsToRecordBatches consumes *sql.Rows and emits Arrow Records via onRecord.
func RowsToRecordBatches(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return RowsToRecordBatchesWithOverrides(rows, cols, colTypes, nil, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func RowsToRecordBatchesWithOverrides(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return RowsToRecordBatchesEngineWithOverrides("", rows, cols, colTypes, targetTypes, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func RowsToRecordBatchesEngineWithOverrides(engine string, rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return rowsToRecordBatchesEngineWithPostgresMetadata(engine, rows, cols, colTypes, targetTypes, nil, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

// RowsToRecordBatchesPostgresMetadata runs normal SQL conversion with the
// connector's optional PostgreSQL user-defined-type classification.
func RowsToRecordBatchesPostgresMetadata(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, metadata []connectors.PostgresTypeMetadata, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return rowsToRecordBatchesEngineWithPostgresMetadata("postgres", rows, cols, colTypes, targetTypes, metadata, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func rowsToRecordBatchesEngineWithPostgresMetadata(engine string, rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, postgresMetadata []connectors.PostgresTypeMetadata, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	var plans []ColumnPlan
	var schema *arrow.Schema
	var err error
	if strings.EqualFold(engine, "postgres") && len(postgresMetadata) > 0 && len(targetTypes) == 0 {
		plans, schema, err = PlansFromPostgresSQLMetadata(cols, colTypes, postgresMetadata)
	} else {
		plans, schema, err = PlansFromSQLEngineWithOverrides(engine, cols, colTypes, targetTypes)
	}
	if err != nil {
		return 0, "", err
	}

	maxBatchSize := batchSize
	if maxBatchSize <= 0 {
		maxBatchSize = 50_000
	}
	if alloc == nil {
		alloc = memory.NewGoAllocator()
	}

	const (
		minBatchSize     = 1000
		targetBatchBytes = uint64(64 * 1024 * 1024) // 64MiB
	)

	curBatchSize := maxBatchSize
	if curBatchSize > 10_000 {
		curBatchSize = 10_000
	}
	if curBatchSize < minBatchSize {
		curBatchSize = minBatchSize
	}

	builders := make([]array.Builder, 0, len(plans))
	resetBuilders := func() {
		for _, b := range builders {
			b.Release()
		}
		builders = builders[:0]
		for _, p := range plans {
			b := p.Builder(alloc)
			b.Reserve(curBatchSize)
			builders = append(builders, b)
		}
	}
	resetBuilders()
	defer func() {
		for _, b := range builders {
			b.Release()
		}
	}()

	scanDests := make([]any, len(cols))
	vals := make([]any, len(cols))
	for i := range scanDests {
		scanDests[i] = &vals[i]
	}

	var (
		rowsTotal int64
		maxCursor string
		inBatch   int
	)

	emit := func() error {
		arrays := make([]arrow.Array, 0, len(builders))
		for _, b := range builders {
			arrays = append(arrays, b.NewArray())
		}
		rec := array.NewRecordBatch(schema, arrays, int64(inBatch))
		for _, a := range arrays {
			a.Release()
		}
		defer rec.Release()

		if inBatch > 0 {
			var recBytes uint64
			ncols := int(rec.NumCols())
			for i := 0; i < ncols; i++ {
				arr := rec.Column(i)
				if arr == nil {
					continue
				}
				data := arr.Data()
				if data == nil {
					continue
				}
				recBytes += data.SizeInBytes()
			}
			if recBytes > 0 {
				bpr := recBytes / uint64(inBatch)
				if bpr > 0 {
					next := int(targetBatchBytes / bpr)
					if next < minBatchSize {
						next = minBatchSize
					}
					if next > maxBatchSize {
						next = maxBatchSize
					}
					if next > curBatchSize*2 {
						next = curBatchSize * 2
					}
					if next < curBatchSize/2 {
						next = curBatchSize / 2
					}
					if next < minBatchSize {
						next = minBatchSize
					}
					curBatchSize = next
				}
			}
		}

		if err := onRecord(schema, rec); err != nil {
			return err
		}
		inBatch = 0
		resetBuilders()
		return nil
	}

	for rows.Next() {
		if err := rows.Scan(scanDests...); err != nil {
			return rowsTotal, maxCursor, err
		}
		for i, p := range plans {
			if err := p.Append(builders[i], vals[i]); err != nil {
				return rowsTotal, maxCursor, err
			}
		}

		if cursorIdx >= 0 && cursorIdx < len(vals) && cursorDomain != connectors.CursorDomainUnknown {
			if v, ok := connectors.EncodeCursorValue(cursorDomain, vals[cursorIdx]); ok {
				if maxCursor == "" || connectors.CompareCursorValues(cursorDomain, maxCursor, v) < 0 {
					maxCursor = v
				}
			}
		}

		rowsTotal++
		inBatch++
		if inBatch >= curBatchSize {
			if err := emit(); err != nil {
				return rowsTotal, maxCursor, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return rowsTotal, maxCursor, err
	}
	if inBatch > 0 {
		if err := emit(); err != nil {
			return rowsTotal, maxCursor, err
		}
	}
	return rowsTotal, maxCursor, nil
}

// PlansFromSQLWithOverrides returns Arrow plans and schema updated with requested target types and nullability.
func PlansFromSQLWithOverrides(cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string) ([]ColumnPlan, *arrow.Schema, error) {
	return PlansFromSQLEngineWithOverrides("", cols, colTypes, targetTypes)
}

// PlansFromSQLEngineWithOverrides returns Arrow plans and schema updated with requested target types and nullability for a given engine.
func PlansFromSQLEngineWithOverrides(engine string, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string) ([]ColumnPlan, *arrow.Schema, error) {
	plans, schema, err := PlansFromSQLEngine(engine, cols, colTypes)
	if err != nil || len(targetTypes) == 0 {
		return plans, schema, err
	}

	fields := schema.Fields()
	newFields := make([]arrow.Field, len(fields))
	copy(newFields, fields)

	for i, f := range fields {
		if targetTypeStr, ok := targetTypes[f.Name]; ok && strings.TrimSpace(targetTypeStr) != "" {
			tStr := strings.TrimSpace(targetTypeStr)
			nullable := f.Nullable
			if strings.HasPrefix(strings.ToLower(tStr), "nullable(") {
				nullable = true
			}

			newPlan := PlanForTargetType(f.Name, tStr)
			// An explicit target type changes execution representation, not the
			// source policy or its source-nullability metadata.
			newPlan.Policy = plans[i].Policy
			plans[i] = newPlan
			newFields[i] = arrow.Field{Name: f.Name, Type: newPlan.DataType, Nullable: nullable}
		}
	}

	return plans, arrow.NewSchema(newFields, nil), nil
}
