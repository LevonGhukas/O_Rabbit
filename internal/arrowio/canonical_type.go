package arrowio

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
)

// CanonicalTypeForArrow renders a storage Arrow type in ORabbit's native type
// vocabulary (the same spelling column_types overrides accept), so clients see
// "string" rather than Arrow's internal "utf8".
func CanonicalTypeForArrow(dt arrow.DataType) string {
	if dt == nil {
		return ""
	}
	switch t := dt.(type) {
	case *arrow.Decimal128Type:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case *arrow.Decimal256Type:
		return fmt.Sprintf("decimal(%d,%d)", t.Precision, t.Scale)
	case *arrow.TimestampType:
		if t.TimeZone != "" {
			return "timestamp_tz[" + t.TimeZone + "]"
		}
		return "timestamp"
	case *arrow.ListType:
		return "array<" + CanonicalTypeForArrow(t.Elem()) + ">"
	case *arrow.LargeListType:
		return "array<" + CanonicalTypeForArrow(t.Elem()) + ">"
	case *arrow.FixedSizeListType:
		return "array<" + CanonicalTypeForArrow(t.Elem()) + ">"
	}
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "string"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY, arrow.BINARY_VIEW:
		return "binary"
	case arrow.BOOL:
		return "bool"
	case arrow.INT8:
		return "int8"
	case arrow.INT16:
		return "int16"
	case arrow.INT32:
		return "int32"
	case arrow.INT64:
		return "int64"
	case arrow.UINT8:
		return "uint8"
	case arrow.UINT16:
		return "uint16"
	case arrow.UINT32:
		return "uint32"
	case arrow.UINT64:
		return "uint64"
	case arrow.FLOAT32:
		return "float32"
	case arrow.FLOAT64:
		return "float64"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIME32, arrow.TIME64:
		return "time"
	}
	if ext, ok := dt.(arrow.ExtensionType); ok && ext.ExtensionName() == "arrow.uuid" {
		return "uuid"
	}
	return dt.String()
}
