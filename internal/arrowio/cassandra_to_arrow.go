package arrowio

import (
	"strings"
)

func planCassandraColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToLower(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	clean = strings.TrimSpace(strings.Split(clean, "<")[0])

	switch clean {
	// 1. Integers
	case "tinyint":
		return planInt8(name)
	case "smallint":
		return planInt16(name)
	case "int", "integer":
		return planInt32(name)
	case "bigint", "varint", "counter":
		return planInt64(name)

	// 2. Floats
	case "float":
		return planFloat32(name)
	case "double":
		return planFloat64(name)

	// 3. Exact Decimals
	case "decimal":
		prec := int32(precision)
		scaleVal := int32(scale)
		if prec <= 0 || prec > 38 {
			prec = 38
		}
		if !hasDecimal {
			scaleVal = 10
		}
		if scaleVal < 0 {
			scaleVal = 0
		}
		if scaleVal > prec {
			scaleVal = prec
		}
		return planDecimal128(name, prec, scaleVal)

	// 4. Boolean
	case "boolean", "bool":
		return planBool(name)

	// 5. Dates & Times
	case "date":
		return planDate32(name)
	case "time":
		return planTime64(name)
	case "timestamp":
		return planTimestampUs(name, "UTC")

	// 6. Binary
	case "blob":
		return planBinary(name)

	// 7. Strings, UUID, Inet, Collections, UDTs
	case "uuid", "timeuuid", "text", "varchar", "ascii", "inet", "list", "set", "map", "tuple", "udt", "frozen":
		return planString(name)

	default:
		return planGenericSQLColumn(name, strings.ToUpper(base), precision, scale, hasDecimal)
	}
}
