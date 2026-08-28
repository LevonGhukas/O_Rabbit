package arrowio

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	precScaleRe = regexp.MustCompile(`\((\d+)(?:,\s*(-?\d+))?\)`)
)

// dereferenceValue unpacks pointers and interfaces until a concrete value or nil is reached.
func dereferenceValue(v any) any {
	if v == nil {
		return nil
	}
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer || rv.Kind() == reflect.Interface {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

// PlanForSQLColumn routes type planning to dialect-specific handlers based on the source engine.
func PlanForSQLColumn(engine, name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	// Keep this normalization aligned with the pre-policy planner behavior.
	normEngine := strings.ToLower(strings.TrimSpace(engine))
	upperType := strings.ToUpper(strings.TrimSpace(dbType))

	// Extract precision/scale if present in dbType (e.g. DECIMAL(18, 4) or NUMERIC(10, 2))
	if m := precScaleRe.FindStringSubmatch(upperType); m != nil {
		if p, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			precision = p
			hasDecimal = true
		}
		if len(m) > 2 && m[2] != "" {
			if s, err := strconv.ParseInt(m[2], 10, 64); err == nil {
				scale = s
			}
		}
	}

	var plan ColumnPlan
	switch normEngine {
	case "mysql", "mariadb":
		plan = planMySQLColumn(name, upperType, precision, scale, hasDecimal)
	case "postgres", "postgresql", "pg":
		plan = planPostgresColumn(name, upperType, precision, scale, hasDecimal)
	case "mssql", "sqlserver", "ms-sql", "ms_sql":
		plan = planMSSQLColumn(name, upperType, precision, scale, hasDecimal)
	case "oracle", "ora":
		plan = planOracleColumn(name, upperType, precision, scale, hasDecimal)
	case "clickhouse", "ch":
		plan = planClickHouseColumn(name, upperType, precision, scale, hasDecimal)
	case "trino":
		plan = planTrinoColumn(name, upperType, precision, scale, hasDecimal)
	case "cassandra", "cql":
		plan = planCassandraColumn(name, upperType, precision, scale, hasDecimal)
	case "sqlite", "sqlite3":
		plan = planSQLiteColumn(name, upperType, precision, scale, hasDecimal)
	default:
		plan = planGenericSQLColumn(name, upperType, precision, scale, hasDecimal)
	}
	return withSQLTypePolicy(plan, normEngine, dbType, precision, scale, hasDecimal)
}

func withSQLTypePolicy(plan ColumnPlan, engine, sourceType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	if engine != "postgres" && engine != "mysql" && engine != "mariadb" {
		return plan
	}

	kind := MappingNative
	var fallback *FallbackCodec
	if isDecimalSQLType(engine, sourceType) && !decimal128DeclarationSupported(precision, scale, hasDecimal) {
		kind = MappingFallback
		fallback = &FallbackCodec{Name: canonicalDecimalTextFallbackCodec, Version: 1}
	} else if isStructuredSQLType(engine, sourceType) {
		kind = MappingStructured
	} else if !isNativeSQLType(engine, sourceType) {
		kind = MappingFallback
		fallback = &FallbackCodec{Name: genericTextFallbackCodec, Version: 1}
	}

	policy := &TypePolicy{
		Version:      MappingPolicyVersionV1,
		SourceEngine: engine,
		SourceType:   strings.TrimSpace(sourceType),
		MappingKind:  kind,
		Metadata: SourceTypeMetadata{
			PrecisionKnown: hasDecimal,
			Precision:      precision,
			ScaleKnown:     hasDecimal,
			Scale:          scale,
		},
		Fallback: fallback,
	}
	plan.Policy = policy
	return plan
}

func isDecimalSQLType(engine, sourceType string) bool {
	base := strings.ToUpper(strings.TrimSpace(sourceType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	if engine == "postgres" {
		return clean == "NUMERIC" || clean == "DECIMAL"
	}
	return clean == "DECIMAL" || clean == "NUMERIC" || clean == "DEC" || clean == "FIXED"
}

// decimal128DeclarationSupported is intentionally limited to Arrow's Decimal128
// precision and scale multiplier range. Negative scales are representable by
// Arrow Decimal128 when their absolute value is at most 38.
func decimal128DeclarationSupported(precision, scale int64, hasDecimal bool) bool {
	return hasDecimal && precision >= 1 && precision <= 38 && scale >= -38 && scale <= precision
}

func isStructuredSQLType(engine, sourceType string) bool {
	base := strings.ToUpper(strings.TrimSpace(sourceType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	if engine == "postgres" {
		return strings.HasSuffix(clean, "[]") || strings.HasPrefix(clean, "_")
	}
	return clean == "GEOMETRY" || clean == "POINT" || clean == "LINESTRING" || clean == "POLYGON" || clean == "MULTIPOINT" || clean == "MULTILINESTRING" || clean == "MULTIPOLYGON" || clean == "GEOMETRYCOLLECTION"
}

func isNativeSQLType(engine, sourceType string) bool {
	base := strings.ToUpper(strings.TrimSpace(sourceType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	if engine == "postgres" {
		switch clean {
		case "INT2", "SMALLINT", "SMALLSERIAL", "INT4", "INTEGER", "INT", "SERIAL", "INT8", "BIGINT", "BIGSERIAL", "FLOAT4", "REAL", "FLOAT8", "DOUBLE PRECISION", "FLOAT", "NUMERIC", "DECIMAL", "MONEY", "BOOL", "BOOLEAN", "DATE", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE", "TIME", "TIME WITHOUT TIME ZONE", "TIMETZ", "TIME WITH TIME ZONE", "BYTEA", "TEXT", "VARCHAR", "CHAR", "BPCHAR", "NAME", "CITEXT", "XML":
			return true
		}
		return false
	}
	clean = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(clean, "UNSIGNED", ""), "ZEROFILL", ""))
	switch clean {
	case "TINYINT", "BOOL", "BOOLEAN", "SMALLINT", "MEDIUMINT", "INT", "INTEGER", "BIGINT", "BIT", "YEAR", "FLOAT", "DOUBLE", "DOUBLE PRECISION", "REAL", "DECIMAL", "NUMERIC", "DEC", "FIXED", "DATE", "DATETIME", "TIMESTAMP", "TIME", "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "JSON", "ENUM", "SET":
		return true
	}
	return false
}

// PlanForTargetType converts an explicit target type name (e.g. "UInt64", "Decimal(38,10)", "DateTime64(6)")
// into a ColumnPlan.
func PlanForTargetType(name, targetType string) ColumnPlan {
	tStr := strings.TrimSpace(targetType)
	upper := strings.ToUpper(tStr)

	// Unwrap Nullable(...) or LowCardinality(...)
	for {
		if strings.HasPrefix(upper, "NULLABLE(") && strings.HasSuffix(upper, ")") {
			upper = strings.TrimSuffix(strings.TrimPrefix(upper, "NULLABLE("), ")")
			upper = strings.TrimSpace(upper)
			continue
		}
		if strings.HasPrefix(upper, "LOWCARDINALITY(") && strings.HasSuffix(upper, ")") {
			upper = strings.TrimSuffix(strings.TrimPrefix(upper, "LOWCARDINALITY("), ")")
			upper = strings.TrimSpace(upper)
			continue
		}
		break
	}

	// Extract precision/scale
	var p, s int64
	var hasDec bool
	if m := precScaleRe.FindStringSubmatch(upper); m != nil {
		if pv, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			p = pv
			hasDec = true
		}
		if len(m) > 2 && m[2] != "" {
			if sv, err := strconv.ParseInt(m[2], 10, 64); err == nil {
				s = sv
			}
		}
	}

	switch {
	case upper == "UINT8":
		return planUint8(name)
	case upper == "UINT16":
		return planUint16(name)
	case upper == "UINT32":
		return planUint32(name)
	case upper == "UINT64":
		return planUint64(name)
	case upper == "INT8":
		return planInt8(name)
	case upper == "INT16":
		return planInt16(name)
	case upper == "INT32":
		return planInt32(name)
	case upper == "INT64":
		return planInt64(name)
	case upper == "FLOAT32":
		return planFloat32(name)
	case upper == "FLOAT64":
		return planFloat64(name)
	case upper == "BOOL" || upper == "BOOLEAN":
		return planBool(name)
	case strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") || strings.HasPrefix(upper, "NUMBER"):
		prec := int32(p)
		scaleVal := int32(s)
		if prec <= 0 || prec > 38 {
			prec = 38
		}
		if scaleVal < 0 {
			scaleVal = 0
		}
		if scaleVal > prec {
			scaleVal = prec
		}
		return planDecimal128(name, prec, scaleVal)
	case upper == "MONEY":
		return planDecimal128(name, 19, 4)
	case upper == "SMALLMONEY":
		return planDecimal128(name, 10, 4)
	case upper == "DATE" || upper == "DATE32":
		return planDate32(name)
	case strings.HasPrefix(upper, "TIME64") || upper == "TIME":
		return planTime64(name)
	case strings.HasPrefix(upper, "DATETIME64") || strings.HasPrefix(upper, "TIMESTAMP") || upper == "DATETIME":
		if strings.Contains(upper, "UTC") || strings.Contains(upper, "'UTC'") {
			return planTimestampUs(name, "UTC")
		}
		return planTimestampUs(name, "")
	case strings.HasPrefix(upper, "ARRAY(") && strings.HasSuffix(upper, ")"):
		inner := strings.TrimSuffix(strings.TrimPrefix(upper, "ARRAY("), ")")
		innerPlan := PlanForTargetType("item", inner)
		return planList(name, innerPlan)
	case upper == "BINARY" || upper == "BYTEA" || upper == "BLOB":
		return planBinary(name)
	default:
		return planGenericSQLColumn(name, upper, p, s, hasDec)
	}
}

// ---------------------------------------------------------------------------
// Standard Column Plans & Appenders
// ---------------------------------------------------------------------------

func planInt8(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Int8,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewInt8Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Int8Builder)
			return appendSignedInteger(b, v, "Int8", -128, 127, func(i int64) {
				bb.Append(int8(i))
			})
		},
	}
}

func planInt16(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Int16,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewInt16Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Int16Builder)
			return appendSignedInteger(b, v, "Int16", -32768, 32767, func(i int64) {
				bb.Append(int16(i))
			})
		},
	}
}

func planInt32(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Int32,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewInt32Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Int32Builder)
			return appendSignedInteger(b, v, "Int32", -2147483648, 2147483647, func(i int64) {
				bb.Append(int32(i))
			})
		},
	}
}

func planInt64(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Int64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewInt64Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Int64Builder)
			return appendSignedInteger(b, v, "Int64", -1<<63, 1<<63-1, func(i int64) {
				bb.Append(i)
			})
		},
	}
}

func planUint8(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Uint8,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewUint8Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Uint8Builder)
			return appendUnsignedInteger(b, v, "UInt8", 255, func(u uint64) {
				bb.Append(uint8(u))
			})
		},
	}
}

func planUint16(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Uint16,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewUint16Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Uint16Builder)
			return appendUnsignedInteger(b, v, "UInt16", 65535, func(u uint64) {
				bb.Append(uint16(u))
			})
		},
	}
}

func planUint32(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Uint32,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewUint32Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Uint32Builder)
			return appendUnsignedInteger(b, v, "UInt32", 4294967295, func(u uint64) {
				bb.Append(uint32(u))
			})
		},
	}
}

func planUint64(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Uint64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewUint64Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Uint64Builder)
			return appendUnsignedInteger(b, v, "UInt64", ^uint64(0), func(u uint64) {
				bb.Append(u)
			})
		},
	}
}

// IntegerConversionError reports a non-null value that cannot be represented
// by the fixed Arrow integer type selected before row processing began.
type IntegerConversionError struct {
	Target    string
	InputType string
	Reason    string
}

func (e *IntegerConversionError) Error() string {
	return fmt.Sprintf("%s conversion from %s failed: %s", e.Target, e.InputType, e.Reason)
}

func appendSignedInteger(b array.Builder, v any, target string, min, max int64, appendValue func(int64)) error {
	v = dereferenceValue(v)
	if v == nil {
		b.AppendNull()
		return nil
	}
	i, reason := toInt64Checked(v)
	if reason != "" {
		return &IntegerConversionError{Target: target, InputType: fmt.Sprintf("%T", v), Reason: reason}
	}
	if i < min {
		return &IntegerConversionError{Target: target, InputType: fmt.Sprintf("%T", v), Reason: "underflow"}
	}
	if i > max {
		return &IntegerConversionError{Target: target, InputType: fmt.Sprintf("%T", v), Reason: "overflow"}
	}
	appendValue(i)
	return nil
}

func appendUnsignedInteger(b array.Builder, v any, target string, max uint64, appendValue func(uint64)) error {
	v = dereferenceValue(v)
	if v == nil {
		b.AppendNull()
		return nil
	}
	u, reason := toUint64Checked(v)
	if reason != "" {
		return &IntegerConversionError{Target: target, InputType: fmt.Sprintf("%T", v), Reason: reason}
	}
	if u > max {
		return &IntegerConversionError{Target: target, InputType: fmt.Sprintf("%T", v), Reason: "overflow"}
	}
	appendValue(u)
	return nil
}

func planFloat32(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Float32,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewFloat32Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Float32Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if f, ok := asFloat64(v); ok {
				if !math.IsInf(f, 0) && (f > math.MaxFloat32 || f < -math.MaxFloat32) {
					return &ScalarConversionError{Target: "Float32", InputType: fmt.Sprintf("%T", v), Reason: "overflow"}
				}
				bb.Append(float32(f))
				return nil
			}
			return &ScalarConversionError{Target: "Float32", InputType: fmt.Sprintf("%T", v), Reason: "invalid float representation"}
		},
	}
}

func planFloat64(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Float64,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewFloat64Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Float64Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if f, ok := asFloat64(v); ok {
				bb.Append(f)
				return nil
			}
			return &ScalarConversionError{Target: "Float64", InputType: fmt.Sprintf("%T", v), Reason: "invalid float representation"}
		},
	}
}

func planBool(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.FixedWidthTypes.Boolean,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewBooleanBuilder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.BooleanBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if parsed, ok := asBool(v); ok {
				bb.Append(parsed)
				return nil
			}
			return &ScalarConversionError{Target: "Boolean", InputType: fmt.Sprintf("%T", v), Reason: "invalid boolean representation"}
		},
	}
}

const (
	minArrowDate32Days = int64(-1 << 31)
	maxArrowDate32Days = int64(1<<31 - 1)
	dayMicroseconds    = int64(24 * 60 * 60 * 1_000_000)
)

func planDate32(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.PrimitiveTypes.Date32,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewDate32Builder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Date32Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			d, reason := asDate32(v)
			if reason != "" {
				return &TemporalConversionError{Target: "Date32", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(d)
			return nil
		},
	}
}

func planTime64(name string) ColumnPlan {
	timeType := arrow.FixedWidthTypes.Time64us.(*arrow.Time64Type)
	return ColumnPlan{
		Name:     name,
		DataType: timeType,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewTime64Builder(mem, timeType) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Time64Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			tMicros, reason := asTime64Microseconds(v)
			if reason != "" {
				return &TemporalConversionError{Target: "Time64[us]", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			if tMicros < 0 || tMicros >= dayMicroseconds {
				return &TemporalConversionError{Target: "Time64[us]", InputType: fmt.Sprintf("%T", v), Reason: "outside time-of-day range"}
			}
			bb.Append(arrow.Time64(tMicros))
			return nil
		},
	}
}

func planTimestampUs(name string, timeZone string) ColumnPlan {
	tsType := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: timeZone}
	return ColumnPlan{
		Name:     name,
		DataType: tsType,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewTimestampBuilder(mem, tsType) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.TimestampBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			t, ok := asTimestamp(v)
			if !ok {
				return &TemporalConversionError{Target: "Timestamp[us]", InputType: fmt.Sprintf("%T", v), Reason: "unsupported timestamp representation"}
			}
			if timeZone == "UTC" {
				t = t.UTC()
			}
			micros, reason := timestampMicroseconds(t)
			if reason != "" {
				return &TemporalConversionError{Target: "Timestamp[us]", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(arrow.Timestamp(micros))
			return nil
		},
	}
}

func planDecimal128(name string, precision, scale int32) ColumnPlan {
	if precision <= 0 || precision > 38 {
		precision = 38
	}
	if scale < 0 {
		scale = 0
	}
	if scale > precision {
		scale = precision
	}
	decType := &arrow.Decimal128Type{Precision: precision, Scale: scale}
	return ColumnPlan{
		Name:     name,
		DataType: decType,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewDecimal128Builder(mem, decType) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Decimal128Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			num, reason := legacyDecimal128(v, precision, scale)
			if reason != "" {
				return &DecimalConversionError{Target: fmt.Sprintf("Decimal128(%d,%d)", precision, scale), InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(num)
			return nil
		},
	}
}

func planDeclaredDecimal(name string, precision, scale int64, hasDecimal bool) ColumnPlan {
	if !decimal128DeclarationSupported(precision, scale, hasDecimal) {
		return planDecimalTextFallback(name)
	}
	return planExactDecimal128(name, int32(precision), int32(scale))
}

func planExactDecimal128(name string, precision, scale int32) ColumnPlan {
	decType := &arrow.Decimal128Type{Precision: precision, Scale: scale}
	return ColumnPlan{
		Name:     name,
		DataType: decType,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewDecimal128Builder(mem, decType) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.Decimal128Builder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			num, reason := exactDecimal128(v, precision, scale)
			if reason != "" {
				return &DecimalConversionError{Target: fmt.Sprintf("Decimal128(%d,%d)", precision, scale), InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(num)
			return nil
		},
	}
}

func planDecimalTextFallback(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.String,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.StringBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			text, reason := decimalText(v, 0, false)
			if reason != "" {
				return &DecimalConversionError{Target: "decimal text fallback", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			if _, _, _, reason := splitDecimalText(text); reason != "" {
				return &DecimalConversionError{Target: "decimal text fallback", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(text)
			return nil
		},
	}
}

// DecimalConversionError reports a non-null decimal value that cannot be
// represented by its already-selected Arrow decimal plan.
type DecimalConversionError struct {
	Target    string
	InputType string
	Reason    string
}

// TemporalConversionError reports a non-null temporal value that cannot be
// represented exactly by the selected Arrow temporal type.
type TemporalConversionError struct {
	Target    string
	InputType string
	Reason    string
}

// BinaryConversionError reports a non-null value that does not provide an
// exact byte representation for the selected Arrow binary plan.
type BinaryConversionError struct {
	Target    string
	InputType string
	Reason    string
}

// ScalarConversionError reports a non-null scalar value that cannot be
// represented by a selected shared Arrow scalar plan.
type ScalarConversionError struct {
	Target    string
	InputType string
	Reason    string
}

func (e *ScalarConversionError) Error() string {
	return fmt.Sprintf("%s conversion from %s failed: %s", e.Target, e.InputType, e.Reason)
}

func (e *BinaryConversionError) Error() string {
	return fmt.Sprintf("%s conversion from %s failed: %s", e.Target, e.InputType, e.Reason)
}

func (e *TemporalConversionError) Error() string {
	return fmt.Sprintf("%s conversion from %s failed: %s", e.Target, e.InputType, e.Reason)
}

func (e *DecimalConversionError) Error() string {
	return fmt.Sprintf("%s conversion from %s failed: %s", e.Target, e.InputType, e.Reason)
}

func planString(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.String,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.StringBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			s, reason := asSafeString(v)
			if reason != "" {
				return &ScalarConversionError{Target: "String", InputType: fmt.Sprintf("%T", v), Reason: reason}
			}
			bb.Append(s)
			return nil
		},
	}
}

func planBinary(name string) ColumnPlan {
	return ColumnPlan{
		Name:     name,
		DataType: arrow.BinaryTypes.Binary,
		Builder:  func(mem memory.Allocator) array.Builder { return array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.BinaryBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			data, ok := v.([]byte)
			if !ok {
				return &BinaryConversionError{Target: "Binary", InputType: fmt.Sprintf("%T", v), Reason: "non-byte source representation"}
			}
			bb.Append(data)
			return nil
		},
	}
}

func planList(name string, itemPlan ColumnPlan) ColumnPlan {
	listType := arrow.ListOf(itemPlan.DataType)
	return ColumnPlan{
		Name:     name,
		DataType: listType,
		Builder: func(mem memory.Allocator) array.Builder {
			return array.NewListBuilder(mem, itemPlan.DataType)
		},
		Append: func(b array.Builder, v any) error {
			lb := b.(*array.ListBuilder)
			v = dereferenceValue(v)
			if v == nil {
				lb.AppendNull()
				return nil
			}

			items, ok := extractSliceItems(v)
			if !ok {
				return &ScalarConversionError{Target: "List", InputType: fmt.Sprintf("%T", v), Reason: "unsupported list representation"}
			}

			lb.Append(true)
			valBuilder := lb.ValueBuilder()
			for _, item := range items {
				if item == nil {
					if err := itemPlan.Append(valBuilder, nil); err != nil {
						return err
					}
				} else {
					if err := itemPlan.Append(valBuilder, item); err != nil {
						return err
					}
				}
			}
			return nil
		},
	}
}

// ---------------------------------------------------------------------------
// Conversion Helpers
// ---------------------------------------------------------------------------

func asSafeString(v any) (string, string) {
	if v == nil {
		return "", ""
	}
	switch x := v.(type) {
	case string:
		return x, ""
	case []byte:
		// Check for 16-byte UUID / GUID format
		if len(x) == 16 {
			return formatCanonicalUUID(x), ""
		}
		return string(x), ""
	case [16]byte:
		return formatCanonicalUUID(x[:]), ""
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), ""
	case fmt.Stringer:
		return x.String(), ""
	case bool:
		return strconv.FormatBool(x), ""
	case int:
		return strconv.FormatInt(int64(x), 10), ""
	case int8:
		return strconv.FormatInt(int64(x), 10), ""
	case int16:
		return strconv.FormatInt(int64(x), 10), ""
	case int32:
		return strconv.FormatInt(int64(x), 10), ""
	case int64:
		return strconv.FormatInt(x, 10), ""
	case uint:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint8:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint16:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint32:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint64:
		return strconv.FormatUint(x, 10), ""
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32), ""
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64), ""
	}
	return "", "unsupported string representation"
}

func formatCanonicalUUID(b []byte) string {
	if len(b) != 16 {
		return hex.EncodeToString(b)
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3],
		b[4], b[5],
		b[6], b[7],
		b[8], b[9],
		b[10], b[11],
		b[12], b[13],
		b[14], b[15],
	)
}

func cleanDecimalString(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, "€", "")
	s = strings.ReplaceAll(s, "£", "")
	s = strings.ReplaceAll(s, "¥", "")
	s = strings.ReplaceAll(s, ",", "")
	return strings.TrimSpace(s)
}

// legacyDecimal128 keeps pre-Phase-1B plans (for example MONEY and explicit
// target overrides) exact without changing their selected Arrow type.
func legacyDecimal128(v any, precision, scale int32) (decimal128.Num, string) {
	switch x := v.(type) {
	case string:
		return decimal128FromExactText(cleanDecimalString(x), precision, scale)
	case []byte:
		return decimal128FromExactText(cleanDecimalString(string(x)), precision, scale)
	default:
		return exactDecimal128(v, precision, scale)
	}
}

func asDecimal128(v any, prec, scale int32) (decimal128.Num, bool) {
	switch x := v.(type) {
	case decimal128.Num:
		return x, true
	case string:
		cleaned := cleanDecimalString(x)
		if num, err := decimal128.FromString(cleaned, prec, scale); err == nil {
			return num, true
		}
	case []byte:
		cleaned := cleanDecimalString(string(x))
		if num, err := decimal128.FromString(cleaned, prec, scale); err == nil {
			return num, true
		}
	case float64:
		s := strconv.FormatFloat(x, 'f', int(scale), 64)
		if num, err := decimal128.FromString(s, prec, scale); err == nil {
			return num, true
		}
	case float32:
		s := strconv.FormatFloat(float64(x), 'f', int(scale), 32)
		if num, err := decimal128.FromString(s, prec, scale); err == nil {
			return num, true
		}
	case int64:
		s := strconv.FormatInt(x, 10)
		if num, err := decimal128.FromString(s, prec, scale); err == nil {
			return num, true
		}
	case int:
		s := strconv.Itoa(x)
		if num, err := decimal128.FromString(s, prec, scale); err == nil {
			return num, true
		}
	default:
		s := cleanDecimalString(fmt.Sprint(v))
		if num, err := decimal128.FromString(s, prec, scale); err == nil {
			return num, true
		}
	}
	return decimal128.Num{}, false
}

func exactDecimal128(v any, precision, scale int32) (decimal128.Num, string) {
	text, reason := decimalText(v, scale, true)
	if reason != "" {
		return decimal128.Num{}, reason
	}
	return decimal128FromExactText(text, precision, scale)
}

func decimalText(v any, scale int32, allowDecimal128 bool) (string, string) {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x), ""
	case []byte:
		return strings.TrimSpace(string(x)), ""
	case int:
		return strconv.Itoa(x), ""
	case int8:
		return strconv.FormatInt(int64(x), 10), ""
	case int16:
		return strconv.FormatInt(int64(x), 10), ""
	case int32:
		return strconv.FormatInt(int64(x), 10), ""
	case int64:
		return strconv.FormatInt(x, 10), ""
	case uint:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint8:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint16:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint32:
		return strconv.FormatUint(uint64(x), 10), ""
	case uint64:
		return strconv.FormatUint(x, 10), ""
	case decimal128.Num:
		if allowDecimal128 {
			return x.ToString(scale), ""
		}
	}
	return "", "unsupported decimal representation"
}

func decimal128FromExactText(text string, precision, scale int32) (decimal128.Num, string) {
	negative, whole, fractional, reason := splitDecimalText(text)
	if reason != "" {
		return decimal128.Num{}, reason
	}

	var unscaledText string
	if scale >= 0 {
		if int32(len(fractional)) > scale {
			extra := fractional[scale:]
			if strings.Trim(extra, "0") != "" {
				return decimal128.Num{}, "scale mismatch"
			}
			fractional = fractional[:scale]
		}
		unscaledText = whole + fractional + strings.Repeat("0", int(scale)-len(fractional))
	} else {
		if strings.Trim(fractional, "0") != "" {
			return decimal128.Num{}, "scale mismatch"
		}
		unscaledText = whole
	}

	if unscaledText == "" {
		unscaledText = "0"
	}
	unscaled := new(big.Int)
	if _, ok := unscaled.SetString(unscaledText, 10); !ok {
		return decimal128.Num{}, "invalid decimal representation"
	}
	if negative {
		unscaled.Neg(unscaled)
	}
	if scale < 0 {
		factor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-scale)), nil)
		remainder := new(big.Int)
		unscaled.QuoRem(unscaled, factor, remainder)
		if remainder.Sign() != 0 {
			return decimal128.Num{}, "scale mismatch"
		}
	}
	if unscaled.BitLen() > 127 {
		return decimal128.Num{}, "precision overflow"
	}
	num := decimal128.FromBigInt(unscaled)
	if !num.FitsInPrecision(precision) {
		return decimal128.Num{}, "precision overflow"
	}
	return num, ""
}

func splitDecimalText(text string) (negative bool, whole, fractional, reason string) {
	if text == "" {
		return false, "", "", "invalid decimal representation"
	}
	if text[0] == '+' || text[0] == '-' {
		negative = text[0] == '-'
		text = text[1:]
	}
	if text == "" {
		return false, "", "", "invalid decimal representation"
	}
	parts := strings.Split(text, ".")
	if len(parts) > 2 {
		return false, "", "", "invalid decimal representation"
	}
	whole = parts[0]
	if len(parts) == 2 {
		fractional = parts[1]
	}
	if whole == "" && fractional == "" {
		return false, "", "", "invalid decimal representation"
	}
	if whole == "" {
		whole = "0"
	}
	if !decimalDigits(whole) || !decimalDigits(fractional) {
		return false, "", "", "invalid decimal representation"
	}
	return negative, whole, fractional, ""
}

func decimalDigits(text string) bool {
	for _, r := range text {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func asDate32(v any) (arrow.Date32, string) {
	switch x := v.(type) {
	case time.Time:
		if x.IsZero() {
			return 0, "invalid date value"
		}
		return date32FromCalendarDate(x)
	case arrow.Date32:
		return x, ""
	case string:
		raw := strings.TrimSpace(x)
		if raw == "" || raw == "0000-00-00" || raw == "0000-00-00 00:00:00" {
			return 0, "invalid date value"
		}
		if t, err := time.Parse("2006-01-02", raw); err == nil {
			return date32FromCalendarDate(t)
		}
		if t, ok := parseTimestampValue(raw); ok {
			return date32FromCalendarDate(t)
		}
	case []byte:
		return asDate32(string(x))
	case int64:
		if x < minArrowDate32Days || x > maxArrowDate32Days {
			return 0, "outside Date32 range"
		}
		return arrow.Date32(x), ""
	case int32:
		return arrow.Date32(x), ""
	}
	return 0, "unsupported date representation"
}

func date32FromCalendarDate(t time.Time) (arrow.Date32, string) {
	year, month, day := t.Date()
	calendarDate := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	days := calendarDate.Unix() / (24 * 60 * 60)
	if days < minArrowDate32Days || days > maxArrowDate32Days {
		return 0, "outside Date32 range"
	}
	return arrow.Date32(days), ""
}

func asTime64Microseconds(v any) (int64, string) {
	switch x := v.(type) {
	case time.Time:
		hour, min, sec := x.Clock()
		if x.Nanosecond()%1_000 != 0 {
			return 0, "sub-microsecond precision"
		}
		usec := int64(x.Nanosecond() / 1000)
		total := int64(hour)*3600_000_000 + int64(min)*60_000_000 + int64(sec)*1_000_000 + usec
		return total, ""
	case time.Duration:
		if x%time.Microsecond != 0 {
			return 0, "sub-microsecond precision"
		}
		return x.Microseconds(), ""
	case int64:
		return x, ""
	case string:
		raw := strings.TrimSpace(x)
		neg := false
		if strings.HasPrefix(raw, "-") {
			neg = true
			raw = strings.TrimPrefix(raw, "-")
		}
		parts := strings.Split(raw, ":")
		if len(parts) == 3 {
			h, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return 0, "invalid time representation"
			}
			m, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return 0, "invalid time representation"
			}
			var s int64
			var usec int64
			secParts := strings.Split(parts[2], ".")
			if len(secParts) > 2 {
				return 0, "invalid time representation"
			}
			s, err = strconv.ParseInt(secParts[0], 10, 64)
			if err != nil {
				return 0, "invalid time representation"
			}
			if len(secParts) == 2 {
				fracStr := secParts[1]
				if !decimalDigits(fracStr) {
					return 0, "invalid time representation"
				}
				if len(fracStr) > 6 && strings.Trim(fracStr[6:], "0") != "" {
					return 0, "sub-microsecond precision"
				}
				fracStr = fracStr[:min(len(fracStr), 6)]
				for len(fracStr) < 6 {
					fracStr += "0"
				}
				usec, err = strconv.ParseInt(fracStr, 10, 64)
				if err != nil {
					return 0, "invalid time representation"
				}
			}
			if h < 0 || m < 0 || m >= 60 || s < 0 || s >= 60 {
				return 0, "invalid time representation"
			}
			if h >= 24 {
				return 0, "outside time-of-day range"
			}
			total := h*3600_000_000 + m*60_000_000 + s*1_000_000 + usec
			if neg {
				total = -total
			}
			return total, ""
		}
	case []byte:
		return asTime64Microseconds(string(x))
	}
	return 0, "unsupported time representation"
}

func timestampMicroseconds(t time.Time) (int64, string) {
	if t.Nanosecond()%1_000 != 0 {
		return 0, "sub-microsecond precision"
	}
	seconds := t.Unix()
	microseconds := int64(t.Nanosecond() / 1_000)
	const minTimestampMicroseconds = int64(-1 << 63)
	const maxTimestampMicroseconds = int64(1<<63 - 1)
	if seconds < minTimestampMicroseconds/1_000_000 || seconds > maxTimestampMicroseconds/1_000_000 {
		return 0, "outside Timestamp[us] range"
	}
	base := seconds * 1_000_000
	if base > maxTimestampMicroseconds-microseconds {
		return 0, "outside Timestamp[us] range"
	}
	return base + microseconds, ""
}

func asTimestamp(v any) (time.Time, bool) {
	switch x := v.(type) {
	case time.Time:
		if x.IsZero() {
			return time.Time{}, false
		}
		return x, true
	case string:
		raw := strings.TrimSpace(x)
		if raw == "" || raw == "0000-00-00" || raw == "0000-00-00 00:00:00" {
			return time.Time{}, false
		}
		return parseTimestampValue(raw)
	case []byte:
		return parseTimestampValue(string(x))
	case int64:
		if x > 1e14 {
			return time.UnixMicro(x).UTC(), true
		}
		return time.UnixMilli(x).UTC(), true
	default:
		return time.Time{}, false
	}
}

func extractSliceItems(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	switch x := v.(type) {
	case []any:
		return x, true
	case string:
		raw := strings.TrimSpace(x)
		if strings.HasPrefix(raw, "{") && strings.HasSuffix(raw, "}") {
			inner := raw[1 : len(raw)-1]
			if inner == "" {
				return []any{}, true
			}
			parts := splitPGArrayElements(inner)
			out := make([]any, len(parts))
			for i, p := range parts {
				if strings.EqualFold(p, "NULL") {
					out[i] = nil
				} else {
					out[i] = p
				}
			}
			return out, true
		}
		if strings.HasPrefix(raw, "[") && strings.HasSuffix(raw, "]") {
			var parsed []any
			if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
				return parsed, true
			}
		}
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		n := rv.Len()
		out := make([]any, n)
		for i := 0; i < n; i++ {
			elem := rv.Index(i).Interface()
			out[i] = dereferenceValue(elem)
		}
		return out, true
	}
	return nil, false
}

func splitPGArrayElements(s string) []string {
	var elements []string
	var cur strings.Builder
	inQuote := false
	escape := false

	for i := 0; i < len(s); i++ {
		c := s[i]
		if escape {
			cur.WriteByte(c)
			escape = false
			continue
		}
		if c == '\\' {
			escape = true
			continue
		}
		if c == '"' {
			inQuote = !inQuote
			continue
		}
		if c == ',' && !inQuote {
			elements = append(elements, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	elements = append(elements, cur.String())
	return elements
}
