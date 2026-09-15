package arrowio

import (
	"fmt"
	"math"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func LogicalTypeForTrinoColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.TrimSpace(dbType)
	upper := strings.ToUpper(raw)
	if inner, ok := clickHouseOuter(raw, "ARRAY"); ok {
		e, err := LogicalTypeForTrinoColumn(inner, 0, 0, false)
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		return typesystem.ArrayOf(e), nil
	}
	known := func(k typesystem.Kind) (typesystem.LogicalType, error) { return typesystem.LogicalType{Kind: k}, nil }
	if strings.HasPrefix(upper, "TIMESTAMP") {
		if strings.HasSuffix(upper, "WITH TIME ZONE") {
			return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, nil
		}
		return known(typesystem.KindTimestamp)
	}
	if strings.HasPrefix(upper, "TIME") {
		if strings.HasSuffix(upper, "WITH TIME ZONE") {
			return trinoUnknown(upper), nil
		}
		return known(typesystem.KindTime)
	}
	base, args, hasArgs := clickHouseBaseAndArgs(raw)
	switch base {
	case "BOOLEAN", "BOOL":
		return known(typesystem.KindBool)
	case "TINYINT":
		return known(typesystem.KindInt8)
	case "SMALLINT":
		return known(typesystem.KindInt16)
	case "INTEGER", "INT":
		return known(typesystem.KindInt32)
	case "BIGINT":
		return known(typesystem.KindInt64)
	case "REAL":
		return known(typesystem.KindFloat32)
	case "DOUBLE", "FLOAT":
		return known(typesystem.KindFloat64)
	case "DATE":
		return known(typesystem.KindDate)
	case "VARBINARY":
		return known(typesystem.KindBinary)
	case "VARCHAR", "CHAR":
		return known(typesystem.KindString)
	case "UUID":
		return known(typesystem.KindUUID)
	case "JSON":
		return known(typesystem.KindJSON)
	case "DECIMAL", "NUMERIC", "NUMBER":
		if hasArgs {
			if len(args) != 2 {
				return typesystem.LogicalType{}, fmt.Errorf("Trino %s requires precision and scale", base)
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
			return trinoUnknown(base), nil
		}
		return typesystem.Decimal(int32(precision), int32(scale)), nil
	default:
		return trinoUnknown(upper), nil
	}
}

func trinoUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

func planTrinoColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForTrinoColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		if plan, _, planErr := PlanForLogicalType(name, t); planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, trinoUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("Trino fallback plan: %v", fallbackErr))
	}
	return plan
}
