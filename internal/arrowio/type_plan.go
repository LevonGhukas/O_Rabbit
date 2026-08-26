package arrowio

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

var (
	precScaleRe = regexp.MustCompile(`\((\d+)(?:,\s*(\d+))?\)`)
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

	switch normEngine {
	case "mysql", "mariadb":
		return planMySQLColumn(name, upperType, precision, scale, hasDecimal)
	case "postgres", "postgresql", "pg":
		return planPostgresColumn(name, upperType, precision, scale, hasDecimal)
	case "mssql", "sqlserver", "ms-sql", "ms_sql":
		return planMSSQLColumn(name, upperType, precision, scale, hasDecimal)
	case "oracle", "ora":
		return planOracleColumn(name, upperType, precision, scale, hasDecimal)
	case "clickhouse", "ch":
		return planClickHouseColumn(name, upperType, precision, scale, hasDecimal)
	case "trino":
		return planTrinoColumn(name, upperType, precision, scale, hasDecimal)
	case "cassandra", "cql":
		return planCassandraColumn(name, upperType, precision, scale, hasDecimal)
	case "sqlite", "sqlite3":
		return planSQLiteColumn(name, upperType, precision, scale, hasDecimal)
	default:
		return planGenericSQLColumn(name, upperType, precision, scale, hasDecimal)
	}
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if i, ok := asInt64(v); ok && i >= -128 && i <= 127 {
				bb.Append(int8(i))
				return nil
			}
			return conversionFailure(name, "int8", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if i, ok := asInt64(v); ok && i >= -32768 && i <= 32767 {
				bb.Append(int16(i))
				return nil
			}
			return conversionFailure(name, "int16", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if i, ok := asInt64(v); ok && i >= -2147483648 && i <= 2147483647 {
				bb.Append(int32(i))
				return nil
			}
			return conversionFailure(name, "int32", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if i, ok := asInt64(v); ok {
				bb.Append(i)
				return nil
			}
			return conversionFailure(name, "int64", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if u, ok := asUint64(v); ok && u <= 255 {
				bb.Append(uint8(u))
				return nil
			}
			return conversionFailure(name, "uint8", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if u, ok := asUint64(v); ok && u <= 65535 {
				bb.Append(uint16(u))
				return nil
			}
			return conversionFailure(name, "uint16", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			if u, ok := asUint64(v); ok && u <= 4294967295 {
				bb.Append(uint32(u))
				return nil
			}
			return conversionFailure(name, "uint32", v, "integer out of range or invalid")
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
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			// Iceberg has no uint64. Phase 1 deliberately supports only values
			// representable by Iceberg long; full-domain Decimal(20,0) is Phase 2.
			if u, ok := asUint64(v); ok && u <= uint64(^uint64(0)>>1) {
				bb.Append(u)
				return nil
			}
			return conversionFailure(name, "uint64", v, "uint64 above MaxInt64 is unsupported by Iceberg")
		},
	}
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
				bb.Append(float32(f))
				return nil
			}
			return conversionFailure(name, "float32", v, "invalid floating-point representation")
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
			return conversionFailure(name, "float64", v, "invalid floating-point representation")
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
			return conversionFailure(name, "bool", v, "invalid boolean representation")
		},
	}
}

const (
	MinClickHouseDate32 = arrow.Date32(-25567) // 1900-01-01
	MaxClickHouseDate32 = arrow.Date32(120530) // 2299-12-31
)

var (
	MinClickHouseTimestamp = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	MaxClickHouseTimestamp = time.Date(2299, 12, 31, 23, 59, 59, 999999000, time.UTC)
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
			d, ok := asDate32(v)
			if !ok {
				return conversionFailure(name, "date32", v, "invalid date representation")
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
			tMicros, ok := asTime64Microseconds(v)
			if !ok {
				return conversionFailure(name, "time64[us]", v, "invalid time representation or unsupported precision")
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
				return conversionFailure(name, tsType.String(), v, "invalid timestamp representation")
			}
			if timeZone == "UTC" {
				t = t.UTC()
			}
			if t.Nanosecond()%1000 != 0 {
				return conversionFailure(name, tsType.String(), v, "nanosecond precision cannot be represented by timestamp[us]")
			}
			bb.Append(arrow.Timestamp(t.UnixMicro()))
			return nil
		},
	}
}

func planDecimal128(name string, precision, scale int32) ColumnPlan {
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
			num, ok := asDecimal128(v, precision, scale)
			if !ok {
				return conversionFailure(name, decType.String(), v, "invalid decimal, precision overflow, or inexact scale conversion")
			}
			bb.Append(num)
			return nil
		},
	}
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
			s, ok := exactString(v)
			if !ok {
				return conversionFailure(name, "string", v, "implicit stringification is not lossless")
			}
			bb.Append(s)
			return nil
		},
	}
}

// planUUID is an explicit source-type policy. Generic string/binary conversion
// never guesses UUID solely from a 16-byte payload.
func planUUID(name string) ColumnPlan {
	return ColumnPlan{
		Name: name, DataType: arrow.BinaryTypes.String,
		Builder: func(mem memory.Allocator) array.Builder { return array.NewStringBuilder(mem) },
		Append: func(b array.Builder, v any) error {
			bb := b.(*array.StringBuilder)
			v = dereferenceValue(v)
			if v == nil {
				bb.AppendNull()
				return nil
			}
			switch x := v.(type) {
			case string:
				bb.Append(x)
				return nil
			case []byte:
				if len(x) != 16 {
					return conversionFailure(name, "uuid", v, "UUID bytes must be 16 bytes")
				}
				bb.Append(formatCanonicalUUID(x))
				return nil
			case [16]byte:
				bb.Append(formatCanonicalUUID(x[:]))
				return nil
			default:
				return conversionFailure(name, "uuid", v, "UUID requires string or 16-byte representation")
			}
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
			if x, ok := v.([]byte); ok {
				bb.Append(x)
				return nil
			}
			return conversionFailure(name, "binary", v, "binary requires []byte driver representation")
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
				return conversionFailure(name, listType.String(), v, "invalid list representation")
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

func exactString(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), utf8.Valid(x)
	default:
		return "", false
	}
}

func conversionFailure(column, target string, value any, reason string) error {
	return &ConversionError{Column: column, TargetType: target, ValueType: fmt.Sprintf("%T", value), Reason: reason}
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

func asDecimal128(v any, prec, scale int32) (decimal128.Num, bool) {
	if prec <= 0 || prec > 38 || scale < 0 || scale > prec {
		return decimal128.Num{}, false
	}
	switch x := v.(type) {
	case decimal128.Num:
		return parseExactDecimal128(x.ToString(scale), prec, scale)
	case string:
		return parseExactDecimal128(x, prec, scale)
	case []byte:
		return parseExactDecimal128(string(x), prec, scale)
	case int64:
		return parseExactDecimal128(strconv.FormatInt(x, 10), prec, scale)
	case int:
		return parseExactDecimal128(strconv.Itoa(x), prec, scale)
	case int8:
		return parseExactDecimal128(strconv.FormatInt(int64(x), 10), prec, scale)
	case int16:
		return parseExactDecimal128(strconv.FormatInt(int64(x), 10), prec, scale)
	case int32:
		return parseExactDecimal128(strconv.FormatInt(int64(x), 10), prec, scale)
	case uint:
		return parseExactDecimal128(strconv.FormatUint(uint64(x), 10), prec, scale)
	case uint8:
		return parseExactDecimal128(strconv.FormatUint(uint64(x), 10), prec, scale)
	case uint16:
		return parseExactDecimal128(strconv.FormatUint(uint64(x), 10), prec, scale)
	case uint32:
		return parseExactDecimal128(strconv.FormatUint(uint64(x), 10), prec, scale)
	case uint64:
		return parseExactDecimal128(strconv.FormatUint(x, 10), prec, scale)
	}
	return decimal128.Num{}, false
}

// parseExactDecimal128 accepts only canonical plain decimal notation. It permits
// zero-extension to target scale but never rounding or locale-specific cleanup.
func parseExactDecimal128(raw string, precision, scale int32) (decimal128.Num, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return decimal128.Num{}, false
	}
	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = s[1:]
	}
	parts := strings.Split(s, ".")
	if len(parts) > 2 || parts[0] == "" {
		return decimal128.Num{}, false
	}
	whole, frac := parts[0], ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if !allASCIIDigits(whole) || (frac != "" && !allASCIIDigits(frac)) {
		return decimal128.Num{}, false
	}
	if len(frac) > int(scale) {
		if strings.Trim(frac[scale:], "0") != "" {
			return decimal128.Num{}, false
		}
		frac = frac[:scale]
	}
	frac += strings.Repeat("0", int(scale)-len(frac))
	unscaledText := strings.TrimLeft(whole+frac, "0")
	if unscaledText == "" {
		unscaledText = "0"
	}
	if len(unscaledText) > int(precision) {
		return decimal128.Num{}, false
	}
	// big.Int validates arbitrary-width input before Arrow parses its 128-bit form.
	if _, ok := new(big.Int).SetString(unscaledText, 10); !ok {
		return decimal128.Num{}, false
	}
	canonical := whole
	if scale > 0 {
		canonical += "." + frac
	}
	if neg && unscaledText != "0" {
		canonical = "-" + canonical
	}
	n, err := decimal128.FromString(canonical, precision, scale)
	return n, err == nil
}

func allASCIIDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func asDate32(v any) (arrow.Date32, bool) {
	switch x := v.(type) {
	case time.Time:
		if x.IsZero() {
			return 0, false
		}
		return arrow.Date32FromTime(x.UTC()), true
	case arrow.Date32:
		return x, true
	case string:
		raw := strings.TrimSpace(x)
		if raw == "" || raw == "0000-00-00" || raw == "0000-00-00 00:00:00" {
			return 0, false
		}
		if t, err := time.Parse("2006-01-02", raw); err == nil {
			return arrow.Date32FromTime(t), true
		}
		if t, ok := parseTimestampValue(raw); ok {
			return arrow.Date32FromTime(t), true
		}
	case []byte:
		return asDate32(string(x))
	case int64:
		return arrow.Date32(x), true
	case int32:
		return arrow.Date32(x), true
	}
	return 0, false
}

func asTime64Microseconds(v any) (int64, bool) {
	switch x := v.(type) {
	case time.Time:
		hour, min, sec := x.Clock()
		usec := int64(x.Nanosecond() / 1000)
		total := int64(hour)*3600_000_000 + int64(min)*60_000_000 + int64(sec)*1_000_000 + usec
		return total, true
	case time.Duration:
		return x.Microseconds(), true
	case int64:
		return x, true
	case string:
		raw := strings.TrimSpace(x)
		neg := false
		if strings.HasPrefix(raw, "-") {
			neg = true
			raw = strings.TrimPrefix(raw, "-")
		}
		parts := strings.Split(raw, ":")
		if len(parts) >= 2 {
			h, _ := strconv.ParseInt(parts[0], 10, 64)
			m, _ := strconv.ParseInt(parts[1], 10, 64)
			var s int64
			var usec int64
			if len(parts) >= 3 {
				secParts := strings.Split(parts[2], ".")
				s, _ = strconv.ParseInt(secParts[0], 10, 64)
				if len(secParts) > 1 {
					fracStr := secParts[1]
					for len(fracStr) < 6 {
						fracStr += "0"
					}
					if len(fracStr) > 6 {
						if strings.Trim(fracStr[6:], "0") != "" {
							return 0, false
						}
						fracStr = fracStr[:6]
					}
					usec, _ = strconv.ParseInt(fracStr, 10, 64)
				}
			}
			total := h*3600_000_000 + m*60_000_000 + s*1_000_000 + usec
			if neg {
				total = -total
			}
			return total, true
		}
	case []byte:
		return asTime64Microseconds(string(x))
	}
	return 0, false
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
		if strings.EqualFold(raw, "infinity") || strings.EqualFold(raw, "-infinity") {
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
