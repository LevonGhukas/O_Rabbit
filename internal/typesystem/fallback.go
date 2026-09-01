package typesystem

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"time"
)

// ToLosslessString produces a tagged, stable representation for values that
// cannot yet be represented by a native logical type. It never uses fmt.Sprint
// as a catch-all because that representation is neither stable nor reversible.
func ToLosslessString(value any) (string, error) {
	value = Dereference(value)
	if value == nil {
		return "", conversionError(LogicalType{Kind: KindUnknown}, nil, "nil has no fallback string")
	}

	switch v := value.(type) {
	case string:
		return v, nil
	case []byte:
		return "base64:" + base64.StdEncoding.EncodeToString(v), nil
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
		return v.UTC().Format(time.RFC3339Nano), nil
	}
	reflected := reflect.ValueOf(value)
	if reflected.Kind() == reflect.Array && reflected.Type().Elem().Kind() == reflect.Uint8 {
		bytes := make([]byte, reflected.Len())
		for index := range bytes {
			bytes[index] = byte(reflected.Index(index).Uint())
		}
		return "base64:" + base64.StdEncoding.EncodeToString(bytes), nil
	}

	if !safeJSONValue(reflected) {
		return "", conversionError(LogicalType{Kind: KindUnknown}, value, "value cannot be represented safely as JSON")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", conversionError(LogicalType{Kind: KindUnknown}, value, "cannot serialize JSON: %s", err)
	}
	return "json:" + string(encoded), nil
}

func safeJSONValue(value reflect.Value) bool {
	if !value.IsValid() {
		return true
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return true
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Bool, reflect.String,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Float32, reflect.Float64:
		return !math.IsNaN(value.Float()) && !math.IsInf(value.Float(), 0)
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return false
		}
		for index := 0; index < value.Len(); index++ {
			if !safeJSONValue(value.Index(index)) {
				return false
			}
		}
		return true
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return false
		}
		for _, key := range value.MapKeys() {
			if !safeJSONValue(value.MapIndex(key)) {
				return false
			}
		}
		return true
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			field := value.Type().Field(index)
			if field.PkgPath != "" || !safeJSONValue(value.Field(index)) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
