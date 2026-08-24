package arrowio

import (
	"strings"
)

func planMSSQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])

	switch clean {
	// 1. Integers
	case "TINYINT":
		// MSSQL TINYINT is 8-bit unsigned (0 to 255)
		return planUint8(name)
	case "SMALLINT":
		return planInt16(name)
	case "INT", "INTEGER":
		return planInt32(name)
	case "BIGINT":
		return planInt64(name)

	// 2. Boolean
	case "BIT":
		return planBool(name)

	// 3. Floats
	case "FLOAT":
		return planFloat64(name)
	case "REAL":
		return planFloat32(name)

	// 4. Exact Decimals & Money
	case "DECIMAL", "NUMERIC":
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

	case "MONEY":
		return planDecimal128(name, 19, 4)
	case "SMALLMONEY":
		return planDecimal128(name, 10, 4)

	// 5. Dates & Times
	case "DATE":
		return planDate32(name)
	case "DATETIME", "DATETIME2", "SMALLDATETIME":
		return planTimestampUs(name, "")
	case "DATETIMEOFFSET":
		return planTimestampUs(name, "UTC")
	case "TIME":
		return planTime64(name)

	// 6. Binaries
	case "BINARY", "VARBINARY", "IMAGE", "ROWVERSION", "TIMESTAMP":
		return planBinary(name)

	// 7. Uniqueidentifier, Strings, UDT & Variants
	case "UNIQUEIDENTIFIER":
		return planString(name)
	case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT", "XML", "JSON", "SQL_VARIANT", "HIERARCHYID", "GEOMETRY", "GEOGRAPHY":
		return planString(name)

	default:
		return planGenericSQLColumn(name, base, precision, scale, hasDecimal)
	}
}
