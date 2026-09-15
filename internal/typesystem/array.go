package typesystem

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
)

func convertArray(value any, target LogicalType) ([]any, error) {
	if target.Element == nil {
		return nil, conversionError(target, value, "array requires element type")
	}
	items, err := arrayItems(value, target)
	if err != nil {
		return nil, err
	}

	converted := make([]any, len(items))
	for i, item := range items {
		item = Dereference(item)
		if item == nil {
			if !target.Element.Nullable {
				return nil, conversionError(target, value, "array element %d is null but element type is not nullable", i)
			}
			converted[i] = nil
			continue
		}
		convertedItem, err := Convert(item, *target.Element)
		if err != nil {
			return nil, conversionError(target, value, "array element %d: %s", i, err)
		}
		converted[i] = convertedItem
	}
	return converted, nil
}

func arrayItems(value any, target LogicalType) ([]any, error) {
	switch v := value.(type) {
	case string:
		return parseArrayText(v, target, value)
	case []byte:
		return parseArrayText(string(v), target, value)
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, conversionError(target, value, "array source must be a slice, array, JSON array, or PostgreSQL array")
	}
	items := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		items[i] = rv.Index(i).Interface()
	}
	return items, nil
}

func parseArrayText(text string, target LogicalType, original any) ([]any, error) {
	text = strings.TrimSpace(text)
	if strings.HasPrefix(text, "[") {
		decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
		decoder.UseNumber()
		var items []any
		if err := decoder.Decode(&items); err != nil {
			return nil, conversionError(target, original, "invalid JSON array")
		}
		return normalizeJSONNumbers(items).([]any), nil
	}
	if strings.HasPrefix(text, "{") && strings.HasSuffix(text, "}") {
		items, ok := parsePostgresArray(text[1 : len(text)-1])
		if !ok {
			return nil, conversionError(target, original, "invalid PostgreSQL array")
		}
		return items, nil
	}
	return nil, conversionError(target, original, "invalid array text")
}

func normalizeJSONNumbers(value any) any {
	switch value := value.(type) {
	case json.Number:
		return value.String()
	case []any:
		for index := range value {
			value[index] = normalizeJSONNumbers(value[index])
		}
	case map[string]any:
		for key := range value {
			value[key] = normalizeJSONNumbers(value[key])
		}
	}
	return value
}

// parsePostgresArray supports the one-dimensional text form used by database
// drivers: commas delimit unquoted elements, quoted elements may use a
// backslash escape, and unquoted NULL is a null element.
func parsePostgresArray(text string) ([]any, bool) {
	if text == "" {
		return []any{}, true
	}

	items := make([]any, 0)
	for index := 0; index < len(text); {
		quoted := text[index] == '"'
		var builder strings.Builder
		if quoted {
			index++
			closed := false
			for index < len(text) {
				if text[index] == '\\' {
					index++
					if index >= len(text) {
						return nil, false
					}
					builder.WriteByte(text[index])
					index++
					continue
				}
				if text[index] == '"' {
					index++
					closed = true
					break
				}
				builder.WriteByte(text[index])
				index++
			}
			if !closed {
				return nil, false
			}
		} else {
			start := index
			for index < len(text) && text[index] != ',' {
				index++
			}
			builder.WriteString(text[start:index])
		}

		if index < len(text) && text[index] != ',' {
			return nil, false
		}
		item := builder.String()
		if !quoted && strings.EqualFold(item, "NULL") {
			items = append(items, nil)
		} else {
			items = append(items, item)
		}
		if index == len(text) {
			break
		}
		index++
		if index == len(text) {
			return nil, false
		}
	}
	return items, true
}
