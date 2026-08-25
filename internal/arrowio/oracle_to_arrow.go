package arrowio

import (
	"strings"
)

func planOracleColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	clean = strings.TrimPrefix(clean, "DB_TYPE_")

	switch {
	// 1. Number types
	case clean == "NUMBER":
		if scale == 0 && precision > 0 && hasDecimal {
			switch {
			case precision <= 4:
				return planInt16(name)
			case precision <= 9:
				return planInt32(name)
			case precision <= 18:
				return planInt64(name)
			case precision <= 38:
				return planDecimal128(name, int32(precision), 0)
			default:
				return planString(name)
			}
		}
		if precision > 0 && scale >= 0 && hasDecimal {
			prec := int32(precision)
			scaleVal := int32(scale)
			if prec > 38 {
				prec = 38
			}
			if scaleVal > prec {
				scaleVal = prec
			}
			return planDecimal128(name, prec, scaleVal)
		}
		// Oracle NUMBER without precision or with float representation
		return planDecimal128(name, 38, 10)

	// 2. Floats
	case clean == "FLOAT" || clean == "BINARY_FLOAT":
		return planFloat32(name)
	case clean == "BINARY_DOUBLE" || clean == "DOUBLE" || clean == "DOUBLE PRECISION":
		return planFloat64(name)

	// 3. Dates & Timestamps (Note: Oracle DATE includes hours, minutes, seconds)
	case clean == "DATE":
		return planTimestampUs(name, "")
	case strings.Contains(base, "WITH TIME ZONE") || strings.Contains(base, "WITH LOCAL TIME ZONE") || strings.HasSuffix(clean, "TZ"):
		return planTimestampUs(name, "UTC")
	case strings.HasPrefix(clean, "TIMESTAMP"):
		return planTimestampUs(name, "")

	// 4. Binaries
	case clean == "RAW" || clean == "LONG RAW" || clean == "BLOB" || clean == "BFILE":
		return planBinary(name)

	// 5. Strings
	case clean == "VARCHAR" || clean == "VARCHAR2" || clean == "NVARCHAR2" || clean == "CHAR" || clean == "NCHAR" || clean == "CLOB" || clean == "NCLOB" || clean == "LONG" || clean == "ROWID" || clean == "UROWID" || clean == "XMLTYPE" || strings.Contains(clean, "INTERVAL"):
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}
