package arrowio

import (
	"fmt"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func LogicalTypeForSQLiteColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.TrimSpace(dbType)
	base, args, hasArgs := clickHouseBaseAndArgs(raw)
	if raw == "" {
		return sqliteUnknown(""), nil
	}
	if base == "INTEGER" || base == "INT" || base == "BIGINT" {
		return sqliteKnown(typesystem.KindInt64)
	}
	if integer := connectors.ClassifySQLIntegerType(strings.ToUpper(raw)); integer.Integer {
		if integer.Unsigned {
			switch {
			case integer.Bits > 64:
				return sqliteUnknown(base), nil
			case integer.Bits <= 8:
				return sqliteKnown(typesystem.KindUInt8)
			case integer.Bits <= 16:
				return sqliteKnown(typesystem.KindUInt16)
			case integer.Bits <= 32:
				return sqliteKnown(typesystem.KindUInt32)
			default:
				return sqliteKnown(typesystem.KindUInt64)
			}
		}
		switch {
		case integer.Bits > 0 && integer.Bits <= 8:
			return sqliteKnown(typesystem.KindInt8)
		case integer.Bits > 0 && integer.Bits <= 16:
			return sqliteKnown(typesystem.KindInt16)
		case integer.Bits > 0 && integer.Bits <= 32:
			return sqliteKnown(typesystem.KindInt32)
		default:
			return sqliteKnown(typesystem.KindInt64)
		}
	}
	switch base {
	case "REAL", "DOUBLE", "FLOAT":
		return sqliteKnown(typesystem.KindFloat64)
	case "TEXT", "CHAR", "VARCHAR", "CLOB":
		return sqliteKnown(typesystem.KindString)
	case "BLOB", "BINARY", "VARBINARY":
		return sqliteKnown(typesystem.KindBinary)
	case "BOOL", "BOOLEAN":
		return sqliteKnown(typesystem.KindBool)
	case "DATE":
		return sqliteKnown(typesystem.KindDate)
	case "TIME":
		return sqliteKnown(typesystem.KindTime)
	case "DATETIME", "TIMESTAMP":
		return sqliteKnown(typesystem.KindTimestamp)
	case "DECIMAL", "NUMERIC", "NUMBER":
		if hasArgs {
			if len(args) != 2 {
				return typesystem.LogicalType{}, fmt.Errorf("SQLite %s requires precision and scale", base)
			}
			var err error
			precision, err = clickHouseIntArg(args[0])
			if err != nil {
				return typesystem.LogicalType{}, err
			}
			scale, err = clickHouseIntArg(args[1])
			if err != nil {
				return typesystem.LogicalType{}, err
			}
			hasDecimal = true
		}
		if !hasDecimal || precision <= 0 || scale < 0 || scale > precision || precision > math.MaxInt32 || scale > math.MaxInt32 {
			return sqliteUnknown(base), nil
		}
		return typesystem.Decimal(int32(precision), int32(scale)), nil
	default:
		return sqliteUnknown(base), nil
	}
}

func sqliteKnown(k typesystem.Kind) (typesystem.LogicalType, error) {
	return typesystem.LogicalType{Kind: k}, nil
}
func sqliteUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}
func planSQLiteColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForSQLiteColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		if p, _, planErr := PlanForLogicalType(name, t); planErr == nil {
			return p
		}
	}
	p, _, fallbackErr := PlanForLogicalType(name, sqliteUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("SQLite fallback plan: %v", fallbackErr))
	}
	return p
}

// planGenericSQLColumn remains the legacy planner for engines not yet using a
// LogicalType mapper.
func planGenericSQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	if intType := connectors.ClassifySQLIntegerType(base); intType.Integer {
		if intType.Unsigned {
			switch {
			case intType.Bits > 64:
				return planString(name)
			case intType.Bits <= 8:
				return planUint8(name)
			case intType.Bits <= 16:
				return planUint16(name)
			case intType.Bits <= 32:
				return planUint32(name)
			default:
				return planUint64(name)
			}
		}
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
	switch {
	case clean == "BIT" || clean == "BOOL" || clean == "BOOLEAN":
		return planBool(name)
	case clean == "REAL" || clean == "FLOAT4" || clean == "FLOAT24" || (clean == "FLOAT" && hasDecimal && precision > 0 && precision <= 24):
		return planFloat32(name)
	case clean == "FLOAT" || clean == "FLOAT53" || clean == "FLOAT8" || strings.Contains(clean, "DOUBLE"):
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
