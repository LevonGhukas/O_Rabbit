package typesystem

import (
	"math"
	"strconv"
	"strings"
)

func convertBool(value any, target LogicalType) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int:
		return boolFromSigned(int64(v), value, target)
	case int8:
		return boolFromSigned(int64(v), value, target)
	case int16:
		return boolFromSigned(int64(v), value, target)
	case int32:
		return boolFromSigned(int64(v), value, target)
	case int64:
		return boolFromSigned(v, value, target)
	case uint:
		return boolFromUnsigned(uint64(v), value, target)
	case uint8:
		return boolFromUnsigned(uint64(v), value, target)
	case uint16:
		return boolFromUnsigned(uint64(v), value, target)
	case uint32:
		return boolFromUnsigned(uint64(v), value, target)
	case uint64:
		return boolFromUnsigned(v, value, target)
	case string:
		return boolFromText(v, value, target)
	case []byte:
		return boolFromText(string(v), value, target)
	default:
		return false, conversionError(target, value, "boolean source must be bool, 0/1 integer, string, or []byte")
	}
}

func boolFromSigned(value int64, original any, target LogicalType) (bool, error) {
	if value == 0 {
		return false, nil
	}
	if value == 1 {
		return true, nil
	}
	return false, conversionError(target, original, "integer boolean must be 0 or 1")
}

func boolFromUnsigned(value uint64, original any, target LogicalType) (bool, error) {
	if value == 0 {
		return false, nil
	}
	if value == 1 {
		return true, nil
	}
	return false, conversionError(target, original, "integer boolean must be 0 or 1")
}

func boolFromText(text string, original any, target LogicalType) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "true", "t", "yes", "y", "1", "on":
		return true, nil
	case "false", "f", "no", "n", "0", "off":
		return false, nil
	default:
		return false, conversionError(target, original, "invalid boolean %q", text)
	}
}

func convertFloat(value any, target LogicalType, bits int) (any, error) {
	var number float64
	switch v := value.(type) {
	case int:
		number = float64(v)
	case int8:
		number = float64(v)
	case int16:
		number = float64(v)
	case int32:
		number = float64(v)
	case int64:
		number = float64(v)
	case uint:
		number = float64(v)
	case uint8:
		number = float64(v)
	case uint16:
		number = float64(v)
	case uint32:
		number = float64(v)
	case uint64:
		number = float64(v)
	case float32:
		number = float64(v)
	case float64:
		number = v
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(v), 64)
		if err != nil {
			return nil, conversionError(target, value, "invalid float %q", v)
		}
		number = parsed
	case []byte:
		return convertFloat(string(v), target, bits)
	default:
		return nil, conversionError(target, value, "float source must be numeric, string, or []byte")
	}

	if bits == 32 {
		if !math.IsNaN(number) && !math.IsInf(number, 0) && math.Abs(number) > math.MaxFloat32 {
			return nil, conversionError(target, value, "value %g out of range", number)
		}
		return float32(number), nil
	}
	return number, nil
}
