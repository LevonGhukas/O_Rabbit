package arrowio

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// LogicalTypeForClickHouseColumn translates the ClickHouse source type
// grammar into ORabbit semantics. It deliberately keeps unsupported source
// semantics unknown rather than pretending their textual storage is native.
func LogicalTypeForClickHouseColumn(dbType string, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	raw := strings.TrimSpace(dbType)
	if raw == "" {
		return clickHouseUnknown(""), nil
	}
	if inner, ok := clickHouseOuter(raw, "Nullable"); ok {
		t, err := LogicalTypeForClickHouseColumn(inner, precision, scale, hasDecimal)
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		t.Nullable = true
		return t, nil
	}
	if inner, ok := clickHouseOuter(raw, "LowCardinality"); ok {
		return LogicalTypeForClickHouseColumn(inner, precision, scale, hasDecimal)
	}
	if inner, ok := clickHouseOuter(raw, "Array"); ok {
		element, err := LogicalTypeForClickHouseColumn(inner, 0, 0, false)
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		return typesystem.ArrayOf(element), nil
	}

	base, args, hasArgs := clickHouseBaseAndArgs(raw)
	known := func(kind typesystem.Kind) (typesystem.LogicalType, error) {
		return typesystem.LogicalType{Kind: kind}, nil
	}
	switch base {
	case "INT8":
		return known(typesystem.KindInt8)
	case "INT16":
		return known(typesystem.KindInt16)
	case "INT32":
		return known(typesystem.KindInt32)
	case "INT64":
		return known(typesystem.KindInt64)
	case "UINT8":
		return known(typesystem.KindUInt8)
	case "UINT16":
		return known(typesystem.KindUInt16)
	case "UINT32":
		return known(typesystem.KindUInt32)
	case "UINT64":
		return known(typesystem.KindUInt64)
	case "FLOAT32", "BFLOAT16":
		return known(typesystem.KindFloat32)
	case "FLOAT64":
		return known(typesystem.KindFloat64)
	case "BOOL", "BOOLEAN":
		return known(typesystem.KindBool)
	case "DATE", "DATE32":
		return known(typesystem.KindDate)
	case "DATETIME", "DATETIME64":
		zone, err := clickHouseTimezone(args, hasArgs, base == "DATETIME64")
		if err != nil {
			return typesystem.LogicalType{}, err
		}
		if zone != "" {
			return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: zone}, nil
		}
		return known(typesystem.KindTimestamp)
	case "TIME", "TIME64":
		// ClickHouse Time values are duration-like and need not fit a 24-hour
		// time-of-day. Preserve them through the explicit lossless fallback.
		return clickHouseUnknown(base), nil
	case "STRING", "FIXEDSTRING":
		return known(typesystem.KindString)
	case "UUID":
		return known(typesystem.KindUUID)
	case "JSON":
		return known(typesystem.KindJSON)
	case "DECIMAL", "DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256":
		return clickHouseDecimal(base, args, hasArgs, precision, scale, hasDecimal)
	default:
		return clickHouseUnknown(strings.ToUpper(raw)), nil
	}
}

func clickHouseDecimal(base string, args []string, hasArgs bool, precision, scale int64, hasDecimal bool) (typesystem.LogicalType, error) {
	var p, s int64
	if base == "DECIMAL" {
		if !hasArgs {
			if !hasDecimal {
				return typesystem.LogicalType{}, fmt.Errorf("ClickHouse Decimal requires precision and scale")
			}
			p, s = precision, scale
		} else if len(args) != 2 {
			return typesystem.LogicalType{}, fmt.Errorf("ClickHouse Decimal requires precision and scale")
		}
		if hasArgs {
			var err error
			p, err = clickHouseIntArg(args[0])
			if err != nil {
				return typesystem.LogicalType{}, fmt.Errorf("ClickHouse Decimal precision: %w", err)
			}
			s, err = clickHouseIntArg(args[1])
			if err != nil {
				return typesystem.LogicalType{}, fmt.Errorf("ClickHouse Decimal scale: %w", err)
			}
		}
	} else {
		if !hasArgs || len(args) != 1 {
			return typesystem.LogicalType{}, fmt.Errorf("ClickHouse %s requires scale", base)
		}
		var err error
		s, err = clickHouseIntArg(args[0])
		if err != nil {
			return typesystem.LogicalType{}, fmt.Errorf("ClickHouse %s scale: %w", base, err)
		}
		switch base {
		case "DECIMAL32":
			p = 9
		case "DECIMAL64":
			p = 18
		case "DECIMAL128":
			p = 38
		case "DECIMAL256":
			p = 76
		}
	}
	if p > math.MaxInt32 || s > math.MaxInt32 || p < math.MinInt32 || s < math.MinInt32 {
		return typesystem.LogicalType{}, fmt.Errorf("ClickHouse %s precision or scale out of range", base)
	}
	t := typesystem.Decimal(int32(p), int32(s))
	if err := t.Validate(); err != nil {
		return typesystem.LogicalType{}, fmt.Errorf("ClickHouse %s: %w", base, err)
	}
	return t, nil
}

func clickHouseTimezone(args []string, hasArgs, requiresPrecision bool) (string, error) {
	if !hasArgs {
		return "", nil
	}
	if requiresPrecision {
		if len(args) == 1 {
			if _, err := clickHouseIntArg(args[0]); err != nil {
				return "", fmt.Errorf("ClickHouse DateTime64 precision: %w", err)
			}
			return "", nil
		}
		if len(args) != 2 {
			return "", fmt.Errorf("ClickHouse DateTime64 expects precision and optional timezone")
		}
		if _, err := clickHouseIntArg(args[0]); err != nil {
			return "", fmt.Errorf("ClickHouse DateTime64 precision: %w", err)
		}
		return clickHouseZoneArg(args[1])
	}
	if len(args) != 1 {
		return "", fmt.Errorf("ClickHouse DateTime expects optional timezone")
	}
	return clickHouseZoneArg(args[0])
}

func clickHouseZoneArg(arg string) (string, error) {
	zone := strings.Trim(strings.TrimSpace(arg), "'\"")
	if zone == "" {
		return "", fmt.Errorf("ClickHouse timezone cannot be empty")
	}
	return zone, nil
}

func clickHouseIntArg(arg string) (int64, error) {
	return strconv.ParseInt(strings.TrimSpace(arg), 10, 64)
}

func clickHouseBaseAndArgs(raw string) (string, []string, bool) {
	if open := strings.IndexByte(raw, '('); open >= 0 && strings.HasSuffix(strings.TrimSpace(raw), ")") {
		return strings.ToUpper(strings.TrimSpace(raw[:open])), clickHouseSplitArgs(strings.TrimSpace(raw[open+1 : len(strings.TrimSpace(raw))-1])), true
	}
	return strings.ToUpper(strings.TrimSpace(raw)), nil, false
}

func clickHouseSplitArgs(input string) []string {
	var result []string
	start, depth := 0, 0
	quote := byte(0)
	for i := 0; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			if c == quote {
				quote = 0
			}
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		switch c {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				result = append(result, strings.TrimSpace(input[start:i]))
				start = i + 1
			}
		}
	}
	result = append(result, strings.TrimSpace(input[start:]))
	return result
}

func clickHouseOuter(raw, name string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	prefix := name + "("
	if len(trimmed) < len(prefix) || !strings.EqualFold(trimmed[:len(prefix)], prefix) {
		return "", false
	}
	depth := 0
	for i := len(name); i < len(trimmed); i++ {
		switch trimmed[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				if strings.TrimSpace(trimmed[i+1:]) != "" {
					return "", false
				}
				return trimmed[len(name)+1 : i], true
			}
		}
	}
	return "", false
}

func clickHouseUnknown(source string) typesystem.LogicalType {
	return typesystem.LogicalType{Kind: typesystem.KindUnknown, SourceTypeName: source}
}

func planClickHouseColumn(name, dbType string, precision, scale int64, hasDecimal bool) ColumnPlan {
	t, err := LogicalTypeForClickHouseColumn(dbType, precision, scale, hasDecimal)
	if err == nil {
		if plan, _, planErr := PlanForLogicalType(name, t); planErr == nil {
			return plan
		}
	}
	plan, _, fallbackErr := PlanForLogicalType(name, clickHouseUnknown(strings.ToUpper(strings.TrimSpace(dbType))))
	if fallbackErr != nil {
		panic(fmt.Sprintf("ClickHouse fallback plan: %v", fallbackErr))
	}
	return plan
}
