package arrowio

import (
	"strings"
)

func planTrinoColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	switch {
	// 1. Boolean
	case clean == "BOOLEAN" || clean == "BOOL":
		return planBool(name)

	// 2. Integers
	case clean == "TINYINT":
		return planInt8(name)
	case clean == "SMALLINT":
		return planInt16(name)
	case clean == "INTEGER" || clean == "INT":
		return planInt32(name)
	case clean == "BIGINT":
		return planInt64(name)

	// 3. Floats
	case clean == "REAL":
		return planFloat32(name)
	case clean == "DOUBLE" || clean == "FLOAT":
		return planFloat64(name)

	// 4. Exact Decimals / Number
	case clean == "DECIMAL" || clean == "NUMERIC" || clean == "NUMBER":
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

	// 5. Dates & Times (Check TIMESTAMP before TIME)
	case clean == "DATE":
		return planDate32(name)
	case strings.HasPrefix(clean, "TIMESTAMP"):
		if strings.Contains(base, "WITH TIME ZONE") || strings.Contains(base, "TZ") {
			return planTimestampUs(name, "UTC")
		}
		return planTimestampUs(name, "")
	case strings.HasPrefix(clean, "TIME"):
		return planTime64(name)

	// 6. Arrays
	case strings.HasPrefix(base, "ARRAY(") && strings.HasSuffix(base, ")"):
		inner := strings.TrimSuffix(strings.TrimPrefix(base, "ARRAY("), ")")
		innerPlan := planTrinoColumn("item", inner, 0, 0, false)
		return planList(name, innerPlan)

	// 7. Binary
	case clean == "VARBINARY":
		return planBinary(name)

	// 8. Strings, UUID, IP, JSON, Row, Map
	case clean == "UUID" || clean == "VARCHAR" || clean == "CHAR" || clean == "JSON" || clean == "IPADDRESS" || clean == "ROW" || clean == "MAP" || strings.Contains(clean, "INTERVAL"):
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}
