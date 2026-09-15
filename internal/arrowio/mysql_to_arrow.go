package arrowio

import (
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func LogicalTypeForMySQLColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	base := strings.ToUpper(strings.TrimSpace(dbType))
	clean := strings.TrimSpace(strings.Split(base, "(")[0])
	if fields := strings.Fields(clean); len(fields) > 0 {
		clean = fields[0]
	}
	unsigned := strings.Contains(base, "UNSIGNED") || strings.Contains(base, "ZEROFILL")
	known := func(kind typesystem.Kind) (typesystem.LogicalType, error) {
		return typesystem.LogicalType{Kind: kind}, nil
	}
	switch clean {
	case "TINYINT":
		if strings.HasPrefix(base, "TINYINT(1)") && !unsigned {
			return known(typesystem.KindBool)
		}
		if unsigned {
			return known(typesystem.KindUInt8)
		}
		return known(typesystem.KindInt8)
	case "BOOL", "BOOLEAN":
		if !unsigned {
			return known(typesystem.KindBool)
		}
		return known(typesystem.KindUInt8)
	case "SMALLINT":
		if unsigned {
			return known(typesystem.KindUInt16)
		}
		return known(typesystem.KindInt16)
	case "MEDIUMINT", "INT", "INTEGER":
		if unsigned {
			return known(typesystem.KindUInt32)
		}
		return known(typesystem.KindInt32)
	case "BIGINT":
		if unsigned {
			return known(typesystem.KindUInt64)
		}
		return known(typesystem.KindInt64)
	case "YEAR":
		return known(typesystem.KindInt16)
	case "BIT":
		if strings.HasPrefix(base, "BIT(1)") {
			return known(typesystem.KindBool)
		}
		return mysqlUnknown(clean), nil
	case "FLOAT":
		return known(typesystem.KindFloat32)
	case "DOUBLE", "DOUBLE PRECISION", "REAL":
		return known(typesystem.KindFloat64)
	case "DECIMAL", "NUMERIC", "DEC", "FIXED":
		if !hasDecimal {
			return mysqlUnknown(clean), nil
		}
		t := typesystem.Decimal(int32(precision), int32(scale))
		if t.Validate() != nil {
			return mysqlUnknown(clean), nil
		}
		return t, nil
	case "DATE":
		return known(typesystem.KindDate)
	case "DATETIME":
		return known(typesystem.KindTimestamp)
	case "TIMESTAMP":
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
	// MySQL TIME permits signed and >24-hour durations; KindTime intentionally does not.
	case "TIME":
		return mysqlUnknown(clean), nil
	case "BINARY", "VARBINARY", "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return known(typesystem.KindBinary)
	case "JSON":
		return known(typesystem.KindJSON)
	case "VARCHAR", "CHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return known(typesystem.KindString)
	// Spatial values are driver-dependent, so avoid claiming canonical binary semantics.
	case "GEOMETRY", "POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION", "ENUM", "SET":
		return mysqlUnknown(clean), nil
	default:
		return mysqlUnknown(clean), nil
	}
}

func mysqlUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

func planMySQLColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, _ := LogicalTypeForMySQLColumn(dbType, precision, scale, hasDecimal)
	plan, _, err := PlanForLogicalType(name, t)
	if err != nil {
		panic(err)
	}
	return plan
}
