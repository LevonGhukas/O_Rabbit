package arrowio

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	mysqlBitTextCodec           = "mysql-bit-text"
	mysqlTimeTextCodec          = "mysql-time-text"
	mysqlJSONTextCodec          = "mysql-json-text"
	mysqlEnumTextCodec          = "mysql-enum-text"
	mysqlSetTextCodec           = "mysql-set-text"
	mysqlGeometryBinaryCodec    = "mysql-geometry-binary"
	mysqlUnsignedIntegerCodec   = "mysql-unsigned-integer-text"
	mysqlExtensionTextCodec     = "mysql-extension-text"
	mariadbBitTextCodec         = "mariadb-bit-text"
	mariadbTimeTextCodec        = "mariadb-time-text"
	mariadbJSONTextCodec        = "mariadb-json-text"
	mariadbEnumTextCodec        = "mariadb-enum-text"
	mariadbSetTextCodec         = "mariadb-set-text"
	mariadbGeometryBinaryCodec  = "mariadb-geometry-binary"
	mariadbUnsignedIntegerCodec = "mariadb-unsigned-integer-text"
	mariadbUUIDTextCodec        = "mariadb-uuid-text"
	mariadbNetworkTextCodec     = "mariadb-network-text"
	mariadbExtensionTextCodec   = "mariadb-extension-text"
)

func planMySQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	return planMySQLFamilyColumn(name, dbType, precision, scale, hasDecimal, "mysql")
}

// planMariaDBColumn deliberately has its own entry point. The engines share
// most grammar and renderers, but their source identity and JSON/extensions
// remain independently expressible.
func planMariaDBColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	return planMySQLFamilyColumn(name, dbType, precision, scale, hasDecimal, "mariadb")
}

func planMySQLFamilyColumn(name, dbType string, precision, scale int64, hasDecimal bool, engine string) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := mysqlBaseType(base)

	// 1. Integer types & Unsigned variants
	isUnsigned := strings.Contains(base, "UNSIGNED") || strings.Contains(base, "ZEROFILL")

	switch {
	case clean == "TINYINT":
		if isUnsigned {
			return planUint8(name)
		}
		return planInt8(name)
	case clean == "BOOL" || clean == "BOOLEAN":
		return planBool(name)

	case clean == "SMALLINT":
		if isUnsigned {
			return planUint16(name)
		}
		return planInt16(name)

	case clean == "MEDIUMINT" || clean == "INT" || clean == "INTEGER":
		if isUnsigned {
			// INT/INTEGER exposes the full UInt32 domain. Iceberg has only a
			// signed 32-bit int, so retain the exact decimal representation rather
			// than allowing a later Arrow->Iceberg schema conversion to narrow it.
			if clean == "INT" || clean == "INTEGER" {
				return planMySQLUnsignedIntegerText(name, clean, engine, ^uint64(0)>>32)
			}
			return planUint32(name)
		}
		return planInt32(name)

	case clean == "BIGINT":
		if isUnsigned {
			// BIGINT UNSIGNED exceeds Iceberg's signed long domain. This fallback
			// is declaration-driven, never selected from sampled values.
			return planMySQLUnsignedIntegerText(name, clean, engine, ^uint64(0))
		}
		return planInt64(name)

	case clean == "BIT":
		return planMySQLBitText(name, clean, base, engine)

	case clean == "YEAR":
		return planInt16(name)

	// 2. Floating point
	case clean == "FLOAT":
		return planFloat32(name)
	case clean == "DOUBLE" || clean == "DOUBLE PRECISION" || clean == "REAL":
		return planFloat64(name)

	// 3. Exact Decimals
	case clean == "DECIMAL" || clean == "NUMERIC" || clean == "DEC" || clean == "FIXED":
		return planDeclaredDecimal(name, precision, scale, hasDecimal)

	// 4. Dates & Times
	case clean == "DATE":
		return planDate32(name)
	case clean == "DATETIME":
		if mysqlTemporalPrecision(base) > 6 {
			return planMySQLText(name, clean, engine, mysqlCodec(engine, "mysql-datetime-text", "mariadb-datetime-text"), "datetime", validMySQLDateTime)
		}
		return planTimestampUs(name, "")
	case clean == "TIMESTAMP":
		if mysqlTemporalPrecision(base) > 6 {
			return planMySQLText(name, clean, engine, mysqlCodec(engine, "mysql-timestamp-text", "mariadb-timestamp-text"), "timestamp", validMySQLDateTime)
		}
		return planTimestampUs(name, "")
	case clean == "TIME":
		return planMySQLText(name, clean, engine, mysqlCodec(engine, mysqlTimeTextCodec, mariadbTimeTextCodec), "duration", validMySQLTime)

	// 5. Strings, JSON & Binaries
	case clean == "BINARY" || clean == "VARBINARY" || clean == "BLOB" || clean == "TINYBLOB" || clean == "MEDIUMBLOB" || clean == "LONGBLOB":
		return planBinary(name)

	case clean == "GEOMETRY" || clean == "POINT" || clean == "LINESTRING" || clean == "POLYGON" || clean == "MULTIPOINT" || clean == "MULTILINESTRING" || clean == "MULTIPOLYGON" || clean == "GEOMETRYCOLLECTION":
		return planMySQLBinary(name, clean, engine, mysqlCodec(engine, mysqlGeometryBinaryCodec, mariadbGeometryBinaryCodec), "geometry")

	case clean == "JSON":
		return planMySQLText(name, clean, engine, mysqlCodec(engine, mysqlJSONTextCodec, mariadbJSONTextCodec), "json", func(text string) bool { return json.Valid([]byte(text)) })
	case clean == "ENUM":
		return planMySQLText(name, clean, engine, mysqlCodec(engine, mysqlEnumTextCodec, mariadbEnumTextCodec), "enum", func(string) bool { return true })
	case clean == "SET":
		return planMySQLText(name, clean, engine, mysqlCodec(engine, mysqlSetTextCodec, mariadbSetTextCodec), "set", func(string) bool { return true })
	case clean == "UUID" && engine == "mariadb":
		return planMySQLText(name, clean, engine, mariadbUUIDTextCodec, "uuid", validUUIDText)
	case (clean == "INET6" || clean == "INET4" || clean == "INET") && engine == "mariadb":
		return planMySQLText(name, clean, engine, mariadbNetworkTextCodec, "network", func(string) bool { return true })
	case clean == "VARCHAR" || clean == "CHAR" || clean == "TEXT" || clean == "TINYTEXT" || clean == "MEDIUMTEXT" || clean == "LONGTEXT":
		return planString(name)

	default:
		return planMySQLText(name, clean, engine, mysqlCodec(engine, mysqlExtensionTextCodec, mariadbExtensionTextCodec), "extension", func(string) bool { return true })
	}
}

func mysqlBaseType(base string) string {
	base = strings.TrimSpace(strings.Split(base, "(")[0])
	return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(base, "UNSIGNED", ""), "ZEROFILL", ""))
}

func mysqlTemporalPrecision(base string) int64 {
	start, end := strings.IndexByte(base, '('), strings.IndexByte(base, ')')
	if start < 0 || end <= start+1 {
		return 0
	}
	p, err := strconv.ParseInt(strings.TrimSpace(base[start+1:end]), 10, 64)
	if err != nil {
		return 0
	}
	return p
}

func mysqlCodec(engine, mysql, mariadb string) string {
	if engine == "mariadb" {
		return mariadb
	}
	return mysql
}

func planMySQLText(name, sourceType, engine, codec, semantic string, valid func(string) bool) ColumnPlan {
	plan := planString(name)
	plan.Append = func(b array.Builder, v any) error {
		bb := b.(*array.StringBuilder)
		v = dereferenceValue(v)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		text, ok := mysqlTextValue(v)
		if !ok || !valid(text) {
			return &ScalarConversionError{Target: fmt.Sprintf("%s %s text", engine, sourceType), InputType: fmt.Sprintf("%T", v), Reason: "invalid source textual representation"}
		}
		bb.Append(text)
		return nil
	}
	plan.Policy = &TypePolicy{Version: MappingPolicyVersionV1, MappingKind: MappingFallback, Fallback: &FallbackCodec{Name: codec, Version: 1}, Metadata: SourceTypeMetadata{Properties: map[string]string{engine + ".semantic_type": semantic, engine + ".type_name": sourceType}}}
	return plan
}

func planMySQLUnsignedIntegerText(name, sourceType, engine string, max uint64) ColumnPlan {
	plan := planString(name)
	plan.Append = func(b array.Builder, v any) error {
		bb := b.(*array.StringBuilder)
		v = dereferenceValue(v)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		u, reason := toUint64Checked(v)
		if reason != "" {
			return &IntegerConversionError{Target: fmt.Sprintf("%s %s unsigned integer text", engine, sourceType), InputType: fmt.Sprintf("%T", v), Reason: reason}
		}
		if u > max {
			return &IntegerConversionError{Target: fmt.Sprintf("%s %s unsigned integer text", engine, sourceType), InputType: fmt.Sprintf("%T", v), Reason: "overflow"}
		}
		bb.Append(strconv.FormatUint(u, 10))
		return nil
	}
	plan.Policy = &TypePolicy{
		Version:     MappingPolicyVersionV1,
		MappingKind: MappingFallback,
		Fallback:    &FallbackCodec{Name: mysqlCodec(engine, mysqlUnsignedIntegerCodec, mariadbUnsignedIntegerCodec), Version: 1},
		Metadata: SourceTypeMetadata{Properties: map[string]string{
			engine + ".semantic_type": "unsigned-integer",
			engine + ".type_name":     sourceType,
		}},
	}
	return plan
}

func planMySQLBinary(name, sourceType, engine, codec, semantic string) ColumnPlan {
	plan := planBinary(name)
	plan.Policy = &TypePolicy{Version: MappingPolicyVersionV1, MappingKind: MappingFallback, Fallback: &FallbackCodec{Name: codec, Version: 1}, Metadata: SourceTypeMetadata{Properties: map[string]string{engine + ".semantic_type": semantic, engine + ".type_name": sourceType}}}
	return plan
}

func planMySQLBitText(name, sourceType, base, engine string) ColumnPlan {
	width, known := mysqlTemporalPrecision(base), strings.Contains(base, "(")
	valid := func(text string) bool {
		if text == "" || (known && int64(len(text)) != width) {
			return false
		}
		for _, c := range text {
			if c != '0' && c != '1' {
				return false
			}
		}
		return true
	}
	plan := planMySQLText(name, sourceType, engine, mysqlCodec(engine, mysqlBitTextCodec, mariadbBitTextCodec), "bit-string", valid)
	plan.Append = func(b array.Builder, v any) error {
		bb := b.(*array.StringBuilder)
		v = dereferenceValue(v)
		if v == nil {
			bb.AppendNull()
			return nil
		}
		text, ok := mysqlBitTextValue(v, width, known)
		if !ok || !valid(text) {
			return &ScalarConversionError{Target: fmt.Sprintf("%s %s text", engine, sourceType), InputType: fmt.Sprintf("%T", v), Reason: "invalid source bit representation"}
		}
		bb.Append(text)
		return nil
	}
	plan.Policy.Metadata.BitWidthKnown, plan.Policy.Metadata.BitWidth = known, width
	return plan
}

func mysqlTextValue(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return x, true
	case []byte:
		return string(x), true
	default:
		return "", false
	}
}

// go-sql-driver/mysql returns BIT values as packed bytes. A textual test/mock
// representation is accepted too; packed bytes are expanded using the declared
// width, retaining leading zeroes and avoiding an integer intermediate.
func mysqlBitTextValue(v any, width int64, known bool) (string, bool) {
	if text, ok := v.(string); ok {
		return text, true
	}
	data, ok := v.([]byte)
	if !ok {
		return "", false
	}
	textual := len(data) > 0
	for _, b := range data {
		if b != '0' && b != '1' {
			textual = false
			break
		}
	}
	if textual {
		return string(data), true
	}
	if len(data) == 0 {
		return "", false
	}
	var out strings.Builder
	out.Grow(len(data) * 8)
	for _, b := range data {
		out.WriteString(fmt.Sprintf("%08b", b))
	}
	text := out.String()
	if known {
		if width <= 0 || width > int64(len(text)) {
			return "", false
		}
		text = text[len(text)-int(width):]
	}
	return text, true
}

func validMySQLTime(text string) bool {
	if strings.HasPrefix(text, "-") || strings.HasPrefix(text, "+") {
		text = text[1:]
	}
	parts := strings.SplitN(text, ".", 2)
	fields := strings.Split(parts[0], ":")
	if len(fields) != 3 || (len(parts) == 2 && (len(parts[1]) == 0 || len(parts[1]) > 6)) {
		return false
	}
	for _, field := range fields {
		if _, err := strconv.ParseUint(field, 10, 16); err != nil {
			return false
		}
	}
	mins, _ := strconv.ParseUint(fields[1], 10, 16)
	secs, _ := strconv.ParseUint(fields[2], 10, 16)
	return mins < 60 && secs < 60
}

func validMySQLDateTime(text string) bool { return text != "" }
func validUUIDText(text string) bool      { return postgresUUIDTextRe.MatchString(text) }
