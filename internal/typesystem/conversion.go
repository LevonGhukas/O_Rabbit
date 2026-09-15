package typesystem

import (
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Dereference unwraps interfaces and pointers until it reaches a concrete
// value. A nil encountered at any level is represented as nil.
func Dereference(value any) any {
	if value == nil {
		return nil
	}

	rv := reflect.ValueOf(value)
	for rv.Kind() == reflect.Interface || rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	return rv.Interface()
}

// Convert transforms a source value into the canonical runtime representation
// for target. Source nil always remains nil; schema-level nullability is
// enforced by the caller that owns the row/schema boundary.
func Convert(value any, target LogicalType) (any, error) {
	value = Dereference(value)
	if value == nil {
		return nil, nil
	}

	switch target.Kind {
	case KindString:
		return convertString(value, target)
	case KindBool:
		return convertBool(value, target)
	case KindInt8:
		return convertSigned(value, target, 8)
	case KindInt16:
		return convertSigned(value, target, 16)
	case KindInt32:
		return convertSigned(value, target, 32)
	case KindInt64:
		return convertSigned(value, target, 64)
	case KindUInt8:
		return convertUnsigned(value, target, 8)
	case KindUInt16:
		return convertUnsigned(value, target, 16)
	case KindUInt32:
		return convertUnsigned(value, target, 32)
	case KindUInt64:
		return convertUnsigned(value, target, 64)
	case KindFloat32:
		return convertFloat(value, target, 32)
	case KindFloat64:
		return convertFloat(value, target, 64)
	case KindDecimal:
		return convertDecimal(value, target)
	case KindDate:
		return convertDate(value, target)
	case KindTime:
		return convertTime(value, target)
	case KindTimestamp, KindTimestampTZ:
		return convertTimestamp(value, target)
	case KindUUID:
		return convertUUID(value, target)
	case KindBinary:
		return convertBinary(value, target)
	case KindArray:
		return convertArray(value, target)
	case KindUnknown, KindStruct, KindMap, KindJSON:
		return ToLosslessString(value)
	default:
		return nil, conversionError(target, value, "unsupported logical type")
	}
}

func convertString(value any, target LogicalType) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case bool:
		return strconv.FormatBool(v), nil
	case int:
		return strconv.Itoa(v), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case DecimalValue:
		return v.String(), nil
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	default:
		return "", conversionError(target, value, "unsupported string source")
	}
}

func convertSigned(value any, target LogicalType, bits int) (any, error) {
	n, err := signedInteger(value, target, bits)
	if err != nil {
		return nil, err
	}
	switch bits {
	case 8:
		return int8(n), nil
	case 16:
		return int16(n), nil
	case 32:
		return int32(n), nil
	default:
		return n, nil
	}
}

func signedInteger(value any, target LogicalType, bits int) (int64, error) {
	var n int64
	switch v := value.(type) {
	case int:
		n = int64(v)
	case int8:
		n = int64(v)
	case int16:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	case uint:
		return signedFromUint(uint64(v), value, target, bits)
	case uint8:
		return signedFromUint(uint64(v), value, target, bits)
	case uint16:
		return signedFromUint(uint64(v), value, target, bits)
	case uint32:
		return signedFromUint(uint64(v), value, target, bits)
	case uint64:
		return signedFromUint(v, value, target, bits)
	case string:
		parsed, err := strconv.ParseInt(trimText(v), 10, 64)
		if err != nil {
			return 0, conversionError(target, value, "invalid integer %q", v)
		}
		n = parsed
	case []byte:
		return signedInteger(string(v), target, bits)
	default:
		return 0, conversionError(target, value, "integer source must be signed/unsigned integer, string, or []byte")
	}

	min, max := signedBounds(bits)
	if n < min || n > max {
		return 0, conversionError(target, value, "value %d out of range", n)
	}
	return n, nil
}

func signedFromUint(n uint64, original any, target LogicalType, bits int) (int64, error) {
	_, max := signedBounds(bits)
	if n > uint64(max) {
		return 0, conversionError(target, original, "value %d out of range", n)
	}
	return int64(n), nil
}

func signedBounds(bits int) (int64, int64) {
	if bits == 64 {
		return -1 << 63, 1<<63 - 1
	}
	max := int64(1<<(bits-1) - 1)
	return -max - 1, max
}

func convertUnsigned(value any, target LogicalType, bits int) (any, error) {
	n, err := unsignedInteger(value, target, bits)
	if err != nil {
		return nil, err
	}
	switch bits {
	case 8:
		return uint8(n), nil
	case 16:
		return uint16(n), nil
	case 32:
		return uint32(n), nil
	default:
		return n, nil
	}
}

func unsignedInteger(value any, target LogicalType, bits int) (uint64, error) {
	var n uint64
	switch v := value.(type) {
	case int:
		if v < 0 {
			return 0, conversionError(target, value, "negative value %d", v)
		}
		n = uint64(v)
	case int8:
		if v < 0 {
			return 0, conversionError(target, value, "negative value %d", v)
		}
		n = uint64(v)
	case int16:
		if v < 0 {
			return 0, conversionError(target, value, "negative value %d", v)
		}
		n = uint64(v)
	case int32:
		if v < 0 {
			return 0, conversionError(target, value, "negative value %d", v)
		}
		n = uint64(v)
	case int64:
		if v < 0 {
			return 0, conversionError(target, value, "negative value %d", v)
		}
		n = uint64(v)
	case uint:
		n = uint64(v)
	case uint8:
		n = uint64(v)
	case uint16:
		n = uint64(v)
	case uint32:
		n = uint64(v)
	case uint64:
		n = v
	case string:
		parsed, err := strconv.ParseUint(trimText(v), 10, 64)
		if err != nil {
			return 0, conversionError(target, value, "invalid unsigned integer %q", v)
		}
		n = parsed
	case []byte:
		return unsignedInteger(string(v), target, bits)
	default:
		return 0, conversionError(target, value, "integer source must be signed/unsigned integer, string, or []byte")
	}

	max := uint64(^uint64(0))
	if bits < 64 {
		max = 1<<bits - 1
	}
	if n > max {
		return 0, conversionError(target, value, "value %d out of range", n)
	}
	return n, nil
}

func trimText(value string) string {
	return strings.TrimSpace(value)
}
