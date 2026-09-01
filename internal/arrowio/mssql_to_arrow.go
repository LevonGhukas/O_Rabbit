package arrowio

import (
	"fmt"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// LogicalTypeForMSSQLColumn translates SQL Server metadata into ORabbit's
// source-independent type system. SQL Server TIMESTAMP is a ROWVERSION binary
// value; it is deliberately distinct from temporal timestamp types here.
func LogicalTypeForMSSQLColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(raw, "(")[0])
	known := func(kind typesystem.Kind) (typesystem.LogicalType, error) {
		return typesystem.LogicalType{Kind: kind}, nil
	}

	switch clean {
	case "TINYINT":
		return known(typesystem.KindUInt8)
	case "SMALLINT":
		return known(typesystem.KindInt16)
	case "INT", "INTEGER":
		return known(typesystem.KindInt32)
	case "BIGINT":
		return known(typesystem.KindInt64)
	case "BIT":
		return known(typesystem.KindBool)
	case "REAL", "FLOAT24":
		return known(typesystem.KindFloat32)
	case "FLOAT53", "DOUBLE", "DOUBLE PRECISION":
		return known(typesystem.KindFloat64)
	case "FLOAT":
		if hasDecimal && precision > 0 && precision <= 24 {
			return known(typesystem.KindFloat32)
		}
		return known(typesystem.KindFloat64)
	case "DECIMAL", "NUMERIC", "NUMBER":
		if !hasDecimal || precision > math.MaxInt32 || scale > math.MaxInt32 || precision < math.MinInt32 || scale < math.MinInt32 {
			return mssqlUnknown(clean), nil
		}
		t := typesystem.Decimal(int32(precision), int32(scale))
		if err := t.Validate(); err != nil {
			return mssqlUnknown(clean), nil
		}
		return t, nil
	case "MONEY":
		return typesystem.Decimal(19, 4), nil
	case "SMALLMONEY":
		return typesystem.Decimal(10, 4), nil
	case "DATE":
		return known(typesystem.KindDate)
	case "DATETIME", "DATETIME2", "SMALLDATETIME":
		return known(typesystem.KindTimestamp)
	case "DATETIMEOFFSET":
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
	case "TIME":
		return known(typesystem.KindTime)
	case "BINARY", "VARBINARY", "IMAGE", "ROWVERSION", "TIMESTAMP":
		return known(typesystem.KindBinary)
	case "UNIQUEIDENTIFIER":
		return known(typesystem.KindUUID)
	case "CHAR", "VARCHAR", "TEXT", "NCHAR", "NVARCHAR", "NTEXT":
		return known(typesystem.KindString)
	// SQL Server JSON is normally text. A JSON type reported by a driver is
	// interpreted semantically and stored through the explicit JSON fallback.
	case "JSON":
		return known(typesystem.KindJSON)
	case "XML", "SQL_VARIANT", "HIERARCHYID", "GEOMETRY", "GEOGRAPHY":
		return mssqlUnknown(clean), nil
	default:
		return mssqlUnknown(clean), nil
	}
}

func mssqlUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

// planMSSQLColumn retains the legacy planner signature while routing MSSQL
// through LogicalType, shared conversion, and canonical Arrow appenders.
func planMSSQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForMSSQLColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		plan, _, planErr := PlanForLogicalType(name, t)
		if planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, mssqlUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("mssql fallback plan: %v", fallbackErr))
	}
	return plan
}
