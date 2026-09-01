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
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

type ColumnPlan struct {
	Name     string
	DataType arrow.DataType
	Builder  func(mem memory.Allocator) array.Builder
	Append   func(b array.Builder, v any) error
}

type SQLPlanResult struct {
	Plans    []ColumnPlan
	Schema   *arrow.Schema
	Warnings []typesystem.TypeWarning
}

func LogicalTypeForSQLColumn(engine, dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	switch strings.ToLower(strings.TrimSpace(engine)) {
	case "postgres", "postgresql", "pg":
		return LogicalTypeForPostgresColumn(dbType, precision, scale, hasDecimal)
	case "mysql", "mariadb":
		return LogicalTypeForMySQLColumn(dbType, precision, scale, hasDecimal)
	case "mssql", "sqlserver", "ms-sql", "ms_sql":
		return LogicalTypeForMSSQLColumn(dbType, precision, scale, hasDecimal)
	case "oracle", "ora":
		return LogicalTypeForOracleColumn(dbType, precision, scale, hasDecimal)
	case "clickhouse", "ch":
		return LogicalTypeForClickHouseColumn(dbType, precision, scale, hasDecimal)
	case "trino":
		return LogicalTypeForTrinoColumn(dbType, precision, scale, hasDecimal)
	case "cassandra", "cql":
		return LogicalTypeForCassandraColumn(dbType, precision, scale, hasDecimal)
	case "sqlite", "sqlite3":
		return LogicalTypeForSQLiteColumn(dbType, precision, scale, hasDecimal)
	default:
		return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: strings.ToUpper(strings.TrimSpace(dbType))}, nil
	}
}

func PlansFromSQLEngineResult(engine string, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string) (SQLPlanResult, error) {
	plans, schema, err := PlansFromSQLEngineWithOverrides(engine, cols, colTypes, targetTypes)
	if err != nil {
		return SQLPlanResult{}, err
	}
	warnings := make([]typesystem.TypeWarning, 0)
	for i, col := range cols {
		var dbType string
		var p, s int64
		var dec bool
		if colTypes != nil && i < len(colTypes) && colTypes[i] != nil {
			dbType = colTypes[i].DatabaseTypeName()
			if pp, ss, ok := colTypes[i].DecimalSize(); ok {
				p = int64(pp)
				s = int64(ss)
				dec = true
			}
		}
		logical, err := LogicalTypeForSQLColumn(engine, dbType, p, s, dec)
		if err != nil {
			continue
		}
		if raw, ok := targetTypes[col]; ok && strings.TrimSpace(raw) != "" {
			if parsed, parseErr := typesystem.ParseType(raw); parseErr == nil {
				logical = parsed
			}
		}
		_, mapping, mapErr := PlanForLogicalType(col, logical)
		if mapErr == nil {
			if warning, ok := typesystem.WarningForMapping(col, mapping); ok {
				warnings = append(warnings, warning)
			}
		}
	}
	return SQLPlanResult{Plans: plans, Schema: schema, Warnings: typesystem.DeduplicateTypeWarnings(warnings)}, nil
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
			}
		}

		plan := PlanForSQLColumn(engine, n, dbType, precision, scale, hasDecimal)
		plans = append(plans, plan)
		fields = append(fields, arrow.Field{Name: plan.Name, Type: plan.DataType, Nullable: nullable})
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

func parseTruthyText(raw string) bool {
	s := strings.TrimSpace(raw)
	return s == "1" || strings.EqualFold(s, "true") || strings.EqualFold(s, "t") || strings.EqualFold(s, "yes") || strings.EqualFold(s, "y")
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
		return x != 0, true
	case int32:
		return x != 0, true
	case int16:
		return x != 0, true
	case int8:
		return x != 0, true
	case int:
		return x != 0, true
	case uint64:
		return x != 0, true
	case uint32:
		return x != 0, true
	case uint16:
		return x != 0, true
	case uint8:
		return x != 0, true
	case uint:
		return x != 0, true
	case []byte:
		if len(x) == 1 {
			return x[0] != 0, true
		}
		return parseTruthyText(string(x)), true
	case string:
		return parseTruthyText(x), true
	default:
		return false, false
	}
}

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		if x > 9223372036854775807 {
			return 0, false
		}
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint:
		return int64(x), true
	case []byte:
		i, err := strconv.ParseInt(strings.TrimSpace(string(x)), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

func asUint64(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case uint:
		return uint64(x), true
	case int64:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int32:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int16:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int8:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case []byte:
		if len(x) == 8 {
			var u uint64
			for _, b := range x {
				u = (u << 8) | uint64(b)
			}
			return u, true
		}
		i, err := strconv.ParseUint(strings.TrimSpace(string(x)), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	case string:
		i, err := strconv.ParseUint(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, false
		}
		return i, true
	default:
		return 0, false
	}
}

// RowsToRecordBatches consumes *sql.Rows and emits Arrow Records via onRecord.
func RowsToRecordBatches(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return RowsToRecordBatchesWithOverrides(rows, cols, colTypes, nil, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func RowsToRecordBatchesWithOverrides(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return RowsToRecordBatchesEngineWithOverrides("", rows, cols, colTypes, targetTypes, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func RowsToRecordBatchesEngineWithOverrides(engine string, rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	plans, schema, err := PlansFromSQLEngineWithOverrides(engine, cols, colTypes, targetTypes)
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

	targetTypesLower := make(map[string]string, len(targetTypes))
	for k, v := range targetTypes {
		targetTypesLower[strings.ToLower(strings.TrimSpace(k))] = v
	}

	fields := schema.Fields()
	newFields := make([]arrow.Field, len(fields))
	copy(newFields, fields)

	for i, f := range fields {
		targetTypeStr, ok := targetTypes[f.Name]
		if !ok {
			targetTypeStr, ok = targetTypesLower[strings.ToLower(strings.TrimSpace(f.Name))]
		}
		if ok && strings.TrimSpace(targetTypeStr) != "" {
			logical, parseErr := typesystem.ParseType(strings.TrimSpace(targetTypeStr))
			if parseErr != nil {
				return nil, nil, fmt.Errorf("column %s target type: %w", f.Name, parseErr)
			}
			newPlan, _, planErr := PlanForLogicalType(f.Name, logical)
			if planErr != nil {
				return nil, nil, fmt.Errorf("column %s target plan: %w", f.Name, planErr)
			}
			plans[i] = newPlan
			newFields[i] = arrow.Field{Name: f.Name, Type: newPlan.DataType, Nullable: logical.Nullable}
		}
	}

	return plans, arrow.NewSchema(newFields, nil), nil
}
