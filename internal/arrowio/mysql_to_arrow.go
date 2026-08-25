package arrowio

import (
	"strings"
)

func planMySQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	// 1. Integer types & Unsigned variants
	isUnsigned := strings.Contains(base, "UNSIGNED") || strings.Contains(base, "ZEROFILL")

	switch {
	case clean == "TINYINT" || clean == "BOOL" || clean == "BOOLEAN":
		if clean == "BOOL" || clean == "BOOLEAN" || (base == "TINYINT(1)" && !isUnsigned) {
			return planBool(name)
		}
		if isUnsigned {
			return planUint8(name)
		}
		return planInt8(name)

	case clean == "SMALLINT":
		if isUnsigned {
			return planUint16(name)
		}
		return planInt16(name)

	case clean == "MEDIUMINT" || clean == "INT" || clean == "INTEGER":
		if isUnsigned {
			return planUint32(name)
		}
		return planInt32(name)

	case clean == "BIGINT":
		if isUnsigned {
			return planUint64(name)
		}
		return planInt64(name)

	case clean == "BIT":
		if base == "BIT(1)" {
			return planBool(name)
		}
		return planUint64(name)

	case clean == "YEAR":
		return planInt16(name)

	// 2. Floating point
	case clean == "FLOAT":
		return planFloat32(name)
	case clean == "DOUBLE" || clean == "DOUBLE PRECISION" || clean == "REAL":
		return planFloat64(name)

	// 3. Exact Decimals
	case clean == "DECIMAL" || clean == "NUMERIC" || clean == "DEC" || clean == "FIXED":
		prec := int32(precision)
		scaleVal := int32(scale)
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

	// 4. Dates & Times
	case clean == "DATE":
		return planDate32(name)
	case clean == "DATETIME":
		return planTimestampUs(name, "")
	case clean == "TIMESTAMP":
		return planTimestampUs(name, "UTC")
	case clean == "TIME":
		return planTime64(name)

	// 5. Strings, JSON & Binaries
	case clean == "BINARY" || clean == "VARBINARY" || clean == "BLOB" || clean == "TINYBLOB" || clean == "MEDIUMBLOB" || clean == "LONGBLOB":
		return planBinary(name)

	case clean == "GEOMETRY" || clean == "POINT" || clean == "LINESTRING" || clean == "POLYGON" || clean == "MULTIPOINT" || clean == "MULTILINESTRING" || clean == "MULTIPOLYGON" || clean == "GEOMETRYCOLLECTION":
		// Spatial types preserved as binary or string representation
		return planBinary(name)

	case clean == "JSON" || clean == "VARCHAR" || clean == "CHAR" || clean == "TEXT" || clean == "TINYTEXT" || clean == "MEDIUMTEXT" || clean == "LONGTEXT" || clean == "ENUM" || clean == "SET":
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}
