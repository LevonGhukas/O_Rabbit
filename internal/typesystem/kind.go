package typesystem

// Kind is ORabbit's canonical, source-independent logical type vocabulary.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindString
	KindBool
	KindInt8
	KindInt16
	KindInt32
	KindInt64
	KindUInt8
	KindUInt16
	KindUInt32
	KindUInt64
	KindFloat32
	KindFloat64
	KindDecimal
	KindDate
	KindTime
	KindTimestamp
	KindTimestampTZ
	KindUUID
	KindBinary
	KindJSON
	KindArray
	KindStruct
	KindMap
)

// String returns the stable ORabbit-native spelling of a logical kind.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindBool:
		return "bool"
	case KindInt8:
		return "int8"
	case KindInt16:
		return "int16"
	case KindInt32:
		return "int32"
	case KindInt64:
		return "int64"
	case KindUInt8:
		return "uint8"
	case KindUInt16:
		return "uint16"
	case KindUInt32:
		return "uint32"
	case KindUInt64:
		return "uint64"
	case KindFloat32:
		return "float32"
	case KindFloat64:
		return "float64"
	case KindDecimal:
		return "decimal"
	case KindDate:
		return "date"
	case KindTime:
		return "time"
	case KindTimestamp:
		return "timestamp"
	case KindTimestampTZ:
		return "timestamp_tz"
	case KindUUID:
		return "uuid"
	case KindBinary:
		return "binary"
	case KindJSON:
		return "json"
	case KindArray:
		return "array"
	case KindStruct:
		return "struct"
	case KindMap:
		return "map"
	default:
		return "unknown"
	}
}
