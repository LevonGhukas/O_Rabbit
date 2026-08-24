package arrowio

import (
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

func planSQLiteColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	return planGenericSQLColumn(name, dbType, precision, scale, hasDecimal)
}

func planGenericSQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	if intType := connectors.ClassifySQLIntegerType(base); intType.Integer {
		switch {
		case intType.Unsigned && intType.Bits > 64:
			return planString(name)
		case intType.Unsigned:
			switch {
			case intType.Bits <= 8:
				return planUint8(name)
			case intType.Bits <= 16:
				return planUint16(name)
			case intType.Bits <= 32:
				return planUint32(name)
			default:
				return planUint64(name)
			}
		default:
			switch {
			case intType.Bits > 0 && intType.Bits <= 8:
				return planInt8(name)
			case intType.Bits > 0 && intType.Bits <= 16:
				return planInt16(name)
			case intType.Bits > 0 && intType.Bits <= 32:
				return planInt32(name)
			default:
				return planInt64(name)
			}
		}
	}

	switch {
	case clean == "BIT" || clean == "BOOL" || clean == "BOOLEAN":
		return planBool(name)

	case clean == "FLOAT" || clean == "REAL":
		return planFloat32(name)
	case strings.Contains(clean, "DOUBLE") || clean == "FLOAT8":
		return planFloat64(name)

	case clean == "NUMBER" || clean == "NUMERIC" || strings.Contains(clean, "DECIMAL") || clean == "MONEY" || clean == "SMALLMONEY":
		if hasDecimal && scale == 0 && precision > 0 && precision <= 18 {
			return planInt64(name)
		}
		if hasDecimal && precision > 0 && precision <= 38 {
			scaleVal := int32(scale)
			if scaleVal < 0 {
				scaleVal = 0
			}
			return planDecimal128(name, int32(precision), scaleVal)
		}
		return planDecimal128(name, 38, 10)

	case clean == "DATE":
		return planDate32(name)
	case clean == "TIME":
		return planTime64(name)
	case strings.Contains(clean, "DATE") || strings.Contains(clean, "TIME") || strings.Contains(clean, "TIMESTAMP"):
		if strings.Contains(base, "UTC") || strings.Contains(base, "WITH TIME ZONE") {
			return planTimestampUs(name, "UTC")
		}
		return planTimestampUs(name, "")

	case strings.Contains(clean, "BINARY") || clean == "IMAGE" || clean == "VARBINARY" || clean == "RAW" || clean == "BLOB" || clean == "BYTEA":
		return planBinary(name)

	default:
		return planString(name)
	}
}
