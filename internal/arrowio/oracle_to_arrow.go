package arrowio

import (
	"fmt"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// LogicalTypeForOracleColumn translates Oracle metadata into ORabbit's
// source-independent type system. Oracle DATE is a timestamp because it
// includes a time-of-day component.
func LogicalTypeForOracleColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(raw, "(")[0])
	clean = strings.TrimPrefix(clean, "DB_TYPE_")
	// godror-style DB_TYPE_ names use underscores for multi-word Oracle types.
	switch clean {
	case "LONG_RAW":
		clean = "LONG RAW"
	case "TIMESTAMP_WITH_TIME_ZONE":
		clean = "TIMESTAMP WITH TIME ZONE"
	case "TIMESTAMP_WITH_LOCAL_TIME_ZONE":
		clean = "TIMESTAMP WITH LOCAL TIME ZONE"
	}
	known := func(kind typesystem.Kind) (typesystem.LogicalType, error) {
		return typesystem.LogicalType{Kind: kind}, nil
	}

	switch clean {
	case "NUMBER":
		if scale == 0 && precision > 0 && hasDecimal {
			switch {
			case precision <= 4:
				return known(typesystem.KindInt16)
			case precision <= 9:
				return known(typesystem.KindInt32)
			case precision <= 18:
				return known(typesystem.KindInt64)
			}
		}
		if !hasDecimal || precision > math.MaxInt32 || scale > math.MaxInt32 || precision < math.MinInt32 || scale < math.MinInt32 {
			return oracleUnknown(clean), nil
		}
		t := typesystem.Decimal(int32(precision), int32(scale))
		if err := t.Validate(); err != nil {
			return oracleUnknown(clean), nil
		}
		return t, nil
	case "FLOAT", "BINARY_FLOAT":
		return known(typesystem.KindFloat32)
	case "BINARY_DOUBLE", "DOUBLE", "DOUBLE PRECISION":
		return known(typesystem.KindFloat64)
	case "DATE", "TIMESTAMP":
		return known(typesystem.KindTimestamp)
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP WITH LOCAL TIMEZONE":
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
	case "RAW", "LONG RAW", "BLOB":
		return known(typesystem.KindBinary)
	case "VARCHAR", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG":
		return known(typesystem.KindString)
	case "BFILE", "ROWID", "UROWID", "XMLTYPE":
		return oracleUnknown(clean), nil
	default:
		if strings.HasPrefix(clean, "INTERVAL ") {
			return oracleUnknown(clean), nil
		}
		return oracleUnknown(clean), nil
	}
}

func oracleUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

// planOracleColumn retains the legacy planner signature while routing Oracle
// through LogicalType, shared conversion, and canonical Arrow appenders.
func planOracleColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForOracleColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		plan, _, planErr := PlanForLogicalType(name, t)
		if planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, oracleUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("oracle fallback plan: %v", fallbackErr))
	}
	return plan
}
