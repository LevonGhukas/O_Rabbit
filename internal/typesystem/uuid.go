package typesystem

import (
	"encoding/hex"
	"strings"
)

func convertUUID(value any, target LogicalType) (string, error) {
	switch v := value.(type) {
	case string:
		return parseUUIDText(v, target, value)
	case []byte:
		if len(v) == 16 {
			return formatUUID(v), nil
		}
		return parseUUIDText(string(v), target, value)
	case [16]byte:
		return formatUUID(v[:]), nil
	default:
		return "", conversionError(target, value, "UUID source must be canonical text, 16 raw bytes, or [16]byte")
	}
}

func parseUUIDText(text string, target LogicalType, original any) (string, error) {
	text = strings.TrimSpace(text)
	if len(text) != 36 || text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return "", conversionError(target, original, "invalid UUID %q", text)
	}
	compact := strings.ReplaceAll(text, "-", "")
	if len(compact) != 32 {
		return "", conversionError(target, original, "invalid UUID %q", text)
	}
	decoded, err := hex.DecodeString(compact)
	if err != nil {
		return "", conversionError(target, original, "invalid UUID %q", text)
	}
	return formatUUID(decoded), nil
}

func formatUUID(bytes []byte) string {
	encoded := hex.EncodeToString(bytes)
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
}
