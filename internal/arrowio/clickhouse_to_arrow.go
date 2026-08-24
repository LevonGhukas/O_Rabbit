package arrowio

import (
	"strings"
)

func planClickHouseColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.TrimSpace(dbType)
	upper := strings.ToUpper(base)

	// Unwrap Nullable(...) and LowCardinality(...)
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

	clean := strings.TrimSpace(strings.Split(upper, "(")[0])

	switch {
	// 1. Unsigned Integers
	case clean == "UINT8":
		return planUint8(name)
	case clean == "UINT16":
		return planUint16(name)
	case clean == "UINT32":
		return planUint32(name)
	case clean == "UINT64":
		return planUint64(name)

	// 2. Signed Integers
	case clean == "INT8":
		return planInt8(name)
	case clean == "INT16":
		return planInt16(name)
	case clean == "INT32":
		return planInt32(name)
	case clean == "INT64":
		return planInt64(name)

	// 3. Floats & BFloat
	case clean == "FLOAT32" || clean == "BFLOAT16":
		return planFloat32(name)
	case clean == "FLOAT64":
		return planFloat64(name)

	// 4. Boolean
	case clean == "BOOL" || clean == "BOOLEAN":
		return planBool(name)

	// 5. Decimals
	case clean == "DECIMAL":
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
	case clean == "DECIMAL32":
		scaleVal := int32(scale)
		if scaleVal < 0 {
			scaleVal = 0
		}
		return planDecimal128(name, 9, scaleVal)
	case clean == "DECIMAL64":
		scaleVal := int32(scale)
		if scaleVal < 0 {
			scaleVal = 0
		}
		return planDecimal128(name, 18, scaleVal)
	case clean == "DECIMAL128":
		scaleVal := int32(scale)
		if scaleVal < 0 {
			scaleVal = 0
		}
		return planDecimal128(name, 38, scaleVal)

	// 6. Dates & Times
	case clean == "DATE" || clean == "DATE32":
		return planDate32(name)
	case clean == "DATETIME" || clean == "DATETIME64":
		if strings.Contains(upper, "UTC") || strings.Contains(upper, "'UTC'") {
			return planTimestampUs(name, "UTC")
		}
		return planTimestampUs(name, "")
	case clean == "TIME" || clean == "TIME64":
		return planTime64(name)

	// 7. Arrays
	case strings.HasPrefix(upper, "ARRAY(") && strings.HasSuffix(upper, ")"):
		inner := strings.TrimSuffix(strings.TrimPrefix(upper, "ARRAY("), ")")
		innerPlan := planClickHouseColumn("item", inner, 0, 0, false)
		return planList(name, innerPlan)

	// 8. Strings, UUID, IP, JSON, Tuples, Maps, Enums
	case clean == "UUID" || clean == "IPV4" || clean == "IPV6" || clean == "STRING" || clean == "FIXEDSTRING" || clean == "ENUM8" || clean == "ENUM16" || clean == "JSON" || clean == "TUPLE" || clean == "MAP" || clean == "DYNAMIC" || clean == "VARIANT":
		return planString(name)

	default:
		return planGenericSQLColumn(name, upper, precision, scale, hasDecimal)
	}
}
