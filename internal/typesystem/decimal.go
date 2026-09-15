package typesystem

import (
	"math/big"
	"strings"
)

// DecimalValue is an exact base-10 decimal. Its numeric value is Unscaled
// multiplied by 10 to the power of -Scale.
type DecimalValue struct {
	Unscaled *big.Int
	Scale    int32
}

func (d DecimalValue) String() string {
	if d.Unscaled == nil {
		return "0"
	}

	sign := ""
	digits := new(big.Int).Set(d.Unscaled)
	if digits.Sign() < 0 {
		sign = "-"
		digits.Abs(digits)
	}
	text := digits.String()
	if d.Scale == 0 {
		return sign + text
	}
	if int(d.Scale) >= len(text) {
		return sign + "0." + strings.Repeat("0", int(d.Scale)-len(text)) + text
	}
	cut := len(text) - int(d.Scale)
	return sign + text[:cut] + "." + text[cut:]
}

func convertDecimal(value any, target LogicalType) (DecimalValue, error) {
	if err := target.Validate(); err != nil {
		return DecimalValue{}, conversionError(target, value, "%s", err)
	}

	decimal, err := decimalFromValue(value, target)
	if err != nil {
		return DecimalValue{}, err
	}
	if decimal.Unscaled == nil {
		return DecimalValue{}, conversionError(target, value, "decimal has nil unscaled value")
	}

	result := DecimalValue{Unscaled: new(big.Int).Set(decimal.Unscaled), Scale: decimal.Scale}
	targetScale := *target.Scale
	if result.Scale < targetScale {
		result.Unscaled.Mul(result.Unscaled, pow10(targetScale-result.Scale))
		result.Scale = targetScale
	} else if result.Scale > targetScale {
		factor := pow10(result.Scale - targetScale)
		quotient, remainder := new(big.Int), new(big.Int)
		quotient.QuoRem(result.Unscaled, factor, remainder)
		if remainder.Sign() != 0 {
			return DecimalValue{}, conversionError(target, value, "value requires dropping fractional digits")
		}
		result.Unscaled = quotient
		result.Scale = targetScale
	}

	precision := decimalPrecision(result.Unscaled)
	if precision > int(*target.Precision) {
		return DecimalValue{}, conversionError(target, value, "precision %d exceeds %d", precision, *target.Precision)
	}
	return result, nil
}

func decimalFromValue(value any, target LogicalType) (DecimalValue, error) {
	switch v := value.(type) {
	case DecimalValue:
		return v, nil
	case string:
		unscaled, scale, ok := parseDecimal(strings.TrimSpace(v))
		if !ok {
			return DecimalValue{}, conversionError(target, value, "invalid decimal %q", v)
		}
		return DecimalValue{Unscaled: unscaled, Scale: scale}, nil
	case []byte:
		return decimalFromValue(string(v), target)
	case int:
		return DecimalValue{Unscaled: big.NewInt(int64(v))}, nil
	case int8:
		return DecimalValue{Unscaled: big.NewInt(int64(v))}, nil
	case int16:
		return DecimalValue{Unscaled: big.NewInt(int64(v))}, nil
	case int32:
		return DecimalValue{Unscaled: big.NewInt(int64(v))}, nil
	case int64:
		return DecimalValue{Unscaled: big.NewInt(v)}, nil
	case uint:
		return DecimalValue{Unscaled: new(big.Int).SetUint64(uint64(v))}, nil
	case uint8:
		return DecimalValue{Unscaled: new(big.Int).SetUint64(uint64(v))}, nil
	case uint16:
		return DecimalValue{Unscaled: new(big.Int).SetUint64(uint64(v))}, nil
	case uint32:
		return DecimalValue{Unscaled: new(big.Int).SetUint64(uint64(v))}, nil
	case uint64:
		return DecimalValue{Unscaled: new(big.Int).SetUint64(v)}, nil
	default:
		return DecimalValue{}, conversionError(target, value, "decimal source must be integer, string, []byte, or DecimalValue")
	}
}

func parseDecimal(text string) (*big.Int, int32, bool) {
	if text == "" {
		return nil, 0, false
	}
	// PostgreSQL money values are commonly exposed as "$12,345.67". This is a
	// driver text representation, not a float conversion; normalize it before
	// exact base-10 parsing.
	if strings.HasPrefix(text, "-$") {
		text = "-" + strings.ReplaceAll(strings.TrimPrefix(text, "-$"), ",", "")
	} else if strings.HasPrefix(text, "$") {
		text = strings.ReplaceAll(strings.TrimPrefix(text, "$"), ",", "")
	}
	sign := ""
	if text[0] == '+' || text[0] == '-' {
		sign = text[:1]
		text = text[1:]
	}
	parts := strings.Split(text, ".")
	if len(parts) > 2 || (len(parts) == 2 && (parts[0] == "" || parts[1] == "")) || len(parts[0]) == 0 {
		return nil, 0, false
	}
	for _, part := range parts {
		for _, r := range part {
			if r < '0' || r > '9' {
				return nil, 0, false
			}
		}
	}
	scale := int32(0)
	digits := parts[0]
	if len(parts) == 2 {
		scale = int32(len(parts[1]))
		digits += parts[1]
	}
	result, ok := new(big.Int).SetString(sign+digits, 10)
	return result, scale, ok
}

func pow10(exponent int32) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exponent)), nil)
}

func decimalPrecision(value *big.Int) int {
	if value == nil || value.Sign() == 0 {
		return 1
	}
	absolute := new(big.Int).Abs(value)
	return len(absolute.String())
}
