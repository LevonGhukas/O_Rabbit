package typesystem

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
)

// MapEntry is one key/value pair of a source map.
type MapEntry struct {
	Key   any
	Value any
}

// MapValue is a source map whose keys keep their own type, in a stable order.
// Sources hand typed maps over in this form so a column can be stored either
// as a native map or, for columns that keep a text type, as JSON text.
type MapValue []MapEntry

// TupleValue is a positional source tuple, stored natively as a struct whose
// fields are matched by position.
type TupleValue []any

// NewMapValue builds a MapValue from a Go map, ordering entries by key so the
// same map always produces the same output.
func NewMapValue(m any) (MapValue, bool) {
	rv := reflect.ValueOf(m)
	if rv.Kind() != reflect.Map {
		return nil, false
	}
	out := make(MapValue, 0, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		out = append(out, MapEntry{Key: iter.Key().Interface(), Value: iter.Value().Interface()})
	}
	SortMapEntries(out)
	return out, true
}

// SortMapEntries orders entries by key: numbers numerically, everything else
// by its text form.
func SortMapEntries(entries MapValue) {
	sort.SliceStable(entries, func(i, j int) bool { return lessMapKey(entries[i].Key, entries[j].Key) })
}

func lessMapKey(a, b any) bool {
	if x, ok := mapKeyNumber(a); ok {
		if y, ok := mapKeyNumber(b); ok {
			return x < y
		}
	}
	return fmt.Sprint(a) < fmt.Sprint(b)
}

func mapKeyNumber(v any) (float64, bool) {
	switch x := Dereference(v).(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// JSONText renders the map as one JSON object, keys in their text form. It
// is the representation of map columns stored as text.
func (m MapValue) JSONText() string { return nestedJSONText(m) }

// JSONText renders the tuple as one JSON array. It is the representation of
// tuple columns stored as text.
func (t TupleValue) JSONText() string { return nestedJSONText(t) }

func nestedJSONText(v any) string {
	encoded, err := json.Marshal(plainJSONValue(v))
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(encoded)
}

// plainJSONValue turns nested map and tuple values into the plain Go values
// encoding/json writes as objects and arrays.
func plainJSONValue(v any) any {
	switch x := v.(type) {
	case MapValue:
		out := make(map[string]any, len(x))
		for _, e := range x {
			out[fmt.Sprint(e.Key)] = plainJSONValue(e.Value)
		}
		return out
	case TupleValue:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = plainJSONValue(item)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, item := range x {
			out[i] = plainJSONValue(item)
		}
		return out
	}
	return v
}

// convertMap converts a source map to []MapEntry whose keys and values have
// the target's key and value types. Map keys are never null.
func convertMap(value any, target LogicalType) ([]MapEntry, error) {
	if target.Key == nil || target.Value == nil {
		return nil, conversionError(target, value, "map requires key and value types")
	}
	entries, ok := value.(MapValue)
	if !ok {
		if entries, ok = NewMapValue(value); !ok {
			return nil, conversionError(target, value, "map source must be a map")
		}
	}
	out := make([]MapEntry, len(entries))
	for i, e := range entries {
		key := Dereference(e.Key)
		if key == nil {
			return nil, conversionError(target, value, "map key %d is null", i)
		}
		convertedKey, err := Convert(key, *target.Key)
		if err != nil {
			return nil, conversionError(target, value, "map key %d: %s", i, err)
		}
		item := Dereference(e.Value)
		if item == nil {
			if !target.Value.Nullable {
				return nil, conversionError(target, value, "map value for key %v is null but the value type is not nullable", key)
			}
			out[i] = MapEntry{Key: convertedKey}
			continue
		}
		convertedItem, err := Convert(item, *target.Value)
		if err != nil {
			return nil, conversionError(target, value, "map value for key %v: %s", key, err)
		}
		out[i] = MapEntry{Key: convertedKey, Value: convertedItem}
	}
	return out, nil
}

// convertStruct converts a positional tuple, or a map of field names, to one
// value per target field.
func convertStruct(value any, target LogicalType) ([]any, error) {
	var items []any
	switch v := value.(type) {
	case TupleValue:
		items = []any(v)
	case []any:
		items = v
	case MapValue:
		byName := make(map[string]any, len(v))
		for _, e := range v {
			byName[fmt.Sprint(e.Key)] = e.Value
		}
		items = make([]any, len(target.Fields))
		for i, field := range target.Fields {
			items[i] = byName[field.Name]
		}
	default:
		return nil, conversionError(target, value, "struct source must be a tuple or a map of field names")
	}
	if len(items) != len(target.Fields) {
		return nil, conversionError(target, value, "struct has %d fields but the value has %d", len(target.Fields), len(items))
	}
	out := make([]any, len(items))
	for i, field := range target.Fields {
		item := Dereference(items[i])
		if item == nil {
			if !field.Type.Nullable {
				return nil, conversionError(target, value, "struct field %q is null but its type is not nullable", field.Name)
			}
			continue
		}
		converted, err := Convert(item, field.Type)
		if err != nil {
			return nil, conversionError(target, value, "struct field %q: %s", field.Name, err)
		}
		out[i] = converted
	}
	return out, nil
}

// TupleFieldName is the struct field name of tuple element i (f1, f2, ...).
func TupleFieldName(i int) string { return "f" + strconv.Itoa(i+1) }
