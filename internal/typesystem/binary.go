package typesystem

func convertBinary(value any, target LogicalType) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...), nil
	case string:
		return []byte(v), nil
	default:
		return nil, conversionError(target, value, "binary source must be []byte or string")
	}
}
