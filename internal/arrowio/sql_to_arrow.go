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
}

// schemaFromPlans handles schema from plans behavior.
// It exists to keep this logic isolated and reusable.
func schemaFromPlans(plans []ColumnPlan) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(plans))
	for _, p := range plans {
		fields = append(fields, arrow.Field{Name: p.Name, Type: p.DataType, Nullable: true})
	}
	return arrow.NewSchema(fields, nil)
}

// PlansFromSQL handles plans from sql behavior.
// It exists to keep this logic isolated and reusable.
func PlansFromSQL(cols []string, colTypes []*sql.ColumnType) ([]ColumnPlan, *arrow.Schema, error) {
	if len(cols) != len(colTypes) {
		return nil, nil, fmt.Errorf("cols/colTypes length mismatch")
	}

	plans := make([]ColumnPlan, 0, len(cols))
	for i := range cols {
		n := cols[i]
		dbType := ""
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
		}

		plans = append(plans, planForSQLColumnType(n, dbType, precision, scale, hasDecimal))
	}

	schema := schemaFromPlans(plans)
	return plans, schema, nil
}

func planForSQLColumnType(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	if intType := connectors.ClassifySQLIntegerType(dbType); intType.Integer {
		switch {
		case intType.Unsigned && intType.Bits > 64:
			return planString(name)
		case intType.Unsigned:
			return planUint64(name)
		case intType.Bits == 0 || intType.Bits <= 64:
			return planInt64(name)
		default:
			return planString(name)
		}
	}

	// Conservative mapping: use a small set of Arrow types that round-trip reliably.
	// You can specialize further per engine later.
	switch {
	case dbType == "NUMBER":
		if hasDecimal && scale == 0 && precision > 0 && precision <= 18 {
			return planInt64(name)
		}
		return planString(name)
	case dbType == "BIT" || dbType == "BOOL" || dbType == "BOOLEAN":
		return planBool(name)
	case dbType == "FLOAT" || dbType == "REAL" || strings.Contains(dbType, "DOUBLE"):
		return planFloat64(name)
	case strings.Contains(dbType, "DECIMAL") || strings.Contains(dbType, "NUMERIC") || dbType == "MONEY" || dbType == "SMALLMONEY":
		// Keep exact decimal values without guessing scale/precision.
		return planString(name)
	case strings.Contains(dbType, "CHAR") || strings.Contains(dbType, "TEXT") || strings.Contains(dbType, "UUID") || dbType == "UNIQUEIDENTIFIER" || dbType == "CLOB" || dbType == "NCLOB":
		return planString(name)
	case strings.Contains(dbType, "BINARY") || dbType == "IMAGE" || dbType == "VARBINARY" || dbType == "RAW" || dbType == "BLOB":
		return planBinary(name)
	case strings.Contains(dbType, "DATE") || strings.Contains(dbType, "TIME"):
		return planTimestampMs(name)
	default:
		return planString(name)
	}
}

// planInt64 handles plan int 64 behavior.
// It exists to keep this logic isolated and reusable.
func planInt64(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.Int64Builder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		if i, ok := asInt64(v); ok {
			bb.Append(i)
			return nil
		}
		return fmt.Errorf("int64 append: unsupported %T", v)
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Int64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
		Append:   appendFn,
	}
}

func planUint64(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.Uint64Builder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		if i, ok := asUint64(v); ok {
			bb.Append(i)
			return nil
		}
		return fmt.Errorf("uint64 append: unsupported %T", v)
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Uint64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewUint64Builder(mem) },
		Append:   appendFn,
	}
}

// planFloat64 handles plan float 64 behavior.
// It exists to keep this logic isolated and reusable.
func planFloat64(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.Float64Builder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		if f, ok := asFloat64(v); ok {
			bb.Append(f)
			return nil
		}
		switch x := v.(type) {
		case []byte:
			f, ok := parseFloat64Text(string(x))
			if !ok {
				bb.AppendNull()
				return nil
			}
			bb.Append(f)
		case string:
			f, ok := parseFloat64Text(x)
			if !ok {
				bb.AppendNull()
				return nil
			}
			bb.Append(f)
		default:
			return fmt.Errorf("float64 append: unsupported %T", v)
		}
		return nil
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Float64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
		Append:   appendFn,
	}
}

// planBool handles plan bool behavior.
// It exists to keep this logic isolated and reusable.
func planBool(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.BooleanBuilder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		if parsed, ok := asBool(v); ok {
			bb.Append(parsed)
			return nil
		}
		return fmt.Errorf("bool append: unsupported %T", v)
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.FixedWidthTypes.Boolean,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
		Append:   appendFn,
	}
}

// planString handles plan string behavior.
// It exists to keep this logic isolated and reusable.
func planString(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.StringBuilder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		switch x := v.(type) {
		case string:
			bb.Append(x)
		case []byte:
			bb.Append(string(x))
		case time.Time:
			bb.Append(x.UTC().Format(time.RFC3339Nano))
		default:
			bb.Append(fmt.Sprint(v))
		}
		return nil
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.String,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
		Append:   appendFn,
	}
}

// planBinary handles plan binary behavior.
// It exists to keep this logic isolated and reusable.
func planBinary(name string) ColumnPlan {
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.BinaryBuilder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		switch x := v.(type) {
		case []byte:
			bb.Append(x)
		case string:
			bb.Append([]byte(x))
		default:
			return fmt.Errorf("binary append: unsupported %T", v)
		}
		return nil
	}
	return ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.Binary,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) },
		Append:   appendFn,
	}
}

// planTimestampMs handles plan timestamp ms behavior.
// It exists to keep this logic isolated and reusable.
func planTimestampMs(name string) ColumnPlan {
	tsType := &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}
	appendFn := func(b array.Builder, v any) error {
		bb := b.(*array.TimestampBuilder)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		switch x := v.(type) {
		case time.Time:
			bb.Append(arrow.Timestamp(x.UTC().UnixMilli()))
		case string:
			t, ok := parseTimestampValue(x)
			if !ok {
				bb.AppendNull()
				return nil
			}
			bb.Append(arrow.Timestamp(t.UTC().UnixMilli()))
		case []byte:
			t, ok := parseTimestampValue(string(x))
			if !ok {
				bb.AppendNull()
				return nil
			}
			bb.Append(arrow.Timestamp(t.UTC().UnixMilli()))
		default:
			bb.AppendNull()
			return nil
		}
		return nil
	}
	return ColumnPlan{
		Name:     name,
		DataType: tsType,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewTimestampBuilder(mem, tsType) },
		Append:   appendFn,
	}
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
	return s == "1" || strings.EqualFold(s, "true")
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
	case int:
		return float64(x), true
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
	case int:
		return x != 0, true
	case []byte:
		return parseTruthyText(string(x)), true
	case string:
		return parseTruthyText(x), true
	default:
		return false, false
	}
}

// asInt64 handles as int 64 behavior.
// It exists to keep this logic isolated and reusable.
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
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
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
//
// The caller owns the record passed to onRecord (must call rec.Retain() if it needs it
// after onRecord returns; the iterator will Release() it).
func RowsToRecordBatches(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	return RowsToRecordBatchesWithOverrides(rows, cols, colTypes, nil, batchSize, alloc, cursorIdx, cursorDomain, onRecord)
}

func RowsToRecordBatchesWithOverrides(rows *sql.Rows, cols []string, colTypes []*sql.ColumnType, targetTypes map[string]string, batchSize int, alloc memory.Allocator, cursorIdx int, cursorDomain connectors.CursorDomain, onRecord func(schema *arrow.Schema, rec arrow.RecordBatch) error) (int64, string, error) {
	plans, schema, err := PlansFromSQLWithOverrides(cols, colTypes, targetTypes)
	if err != nil {
		return 0, "", err
	}
	// batchSize is treated as the *maximum* desired record-batch size. The function
	// adapts the actual batch size by bytes to avoid OOM on wide rows while keeping
	// large batches for throughput.
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
	// Start conservatively; we can ramp up quickly after the first emitted record.
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

		// Estimate record size and adapt the *next* batch size.
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
					// Avoid wild swings.
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
	plans, schema, err := PlansFromSQL(cols, colTypes)
	if err != nil || len(targetTypes) == 0 {
		return plans, schema, err
	}

	fields := schema.Fields()
	newFields := make([]arrow.Field, len(fields))
	copy(newFields, fields)

	for i, f := range fields {
		if targetTypeStr, ok := targetTypes[f.Name]; ok && strings.TrimSpace(targetTypeStr) != "" {
			tStr := strings.TrimSpace(targetTypeStr)
			nullable := strings.HasPrefix(strings.ToLower(tStr), "nullable(")
			
			cleanType := tStr
			if nullable {
				cleanType = strings.TrimSuffix(strings.TrimPrefix(tStr, "Nullable("), ")")
				cleanType = strings.TrimSuffix(strings.TrimPrefix(cleanType, "nullable("), ")")
			}
			
			var arrowType arrow.DataType
			switch strings.ToUpper(cleanType) {
			case "INT", "INT32", "INTEGER":
				arrowType = arrow.PrimitiveTypes.Int32
			case "INT64", "BIGINT":
				arrowType = arrow.PrimitiveTypes.Int64
			case "FLOAT", "FLOAT32":
				arrowType = arrow.PrimitiveTypes.Float32
			case "FLOAT64", "DOUBLE":
				arrowType = arrow.PrimitiveTypes.Float64
			case "BOOLEAN", "BOOL":
				arrowType = arrow.FixedWidthTypes.Boolean
			case "DATE":
				arrowType = arrow.FixedWidthTypes.Date32
			case "DATETIME", "TIMESTAMP":
				arrowType = arrow.FixedWidthTypes.Timestamp_us
			default:
				arrowType = arrow.BinaryTypes.String
			}
			
			newFields[i] = arrow.Field{Name: f.Name, Type: arrowType, Nullable: nullable}
		}
	}

	return plans, arrow.NewSchema(newFields, nil), nil
}
