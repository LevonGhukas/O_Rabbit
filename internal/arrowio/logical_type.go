package arrowio

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// ArrowTypeForLogicalType maps an ORabbit logical type to an Arrow physical
// type. The MappingResult makes fallback decisions explicit rather than
// hiding them behind an Arrow string type.
func ArrowTypeForLogicalType(t typesystem.LogicalType) (arrow.DataType, typesystem.MappingResult, error) {
	if err := t.Validate(); err != nil {
		return nil, typesystem.MappingResult{}, fmt.Errorf("invalid logical type %s: %w", t.String(), err)
	}

	exact := func(dataType arrow.DataType) (arrow.DataType, typesystem.MappingResult, error) {
		return dataType, typesystem.MappingFor(t, dataType.String(), typesystem.MappingExact, ""), nil
	}
	fallback := func(class typesystem.MappingClass, reason string) (arrow.DataType, typesystem.MappingResult, error) {
		return arrow.BinaryTypes.String, typesystem.MappingFor(t, arrow.BinaryTypes.String.String(), class, reason), nil
	}

	switch t.Kind {
	case typesystem.KindBool:
		return exact(arrow.FixedWidthTypes.Boolean)
	case typesystem.KindInt8:
		return exact(arrow.PrimitiveTypes.Int8)
	case typesystem.KindInt16:
		return exact(arrow.PrimitiveTypes.Int16)
	case typesystem.KindInt32:
		return exact(arrow.PrimitiveTypes.Int32)
	case typesystem.KindInt64:
		return exact(arrow.PrimitiveTypes.Int64)
	case typesystem.KindUInt8:
		return exact(arrow.PrimitiveTypes.Uint8)
	case typesystem.KindUInt16:
		return exact(arrow.PrimitiveTypes.Uint16)
	case typesystem.KindUInt32:
		return exact(arrow.PrimitiveTypes.Uint32)
	case typesystem.KindUInt64:
		return exact(arrow.PrimitiveTypes.Uint64)
	case typesystem.KindFloat32:
		return exact(arrow.PrimitiveTypes.Float32)
	case typesystem.KindFloat64:
		return exact(arrow.PrimitiveTypes.Float64)
	case typesystem.KindString:
		return exact(arrow.BinaryTypes.String)
	case typesystem.KindBinary:
		return exact(arrow.BinaryTypes.Binary)
	case typesystem.KindDecimal:
		if *t.Precision > 38 {
			return fallback(typesystem.MappingSemanticFallback, "Arrow-to-Iceberg bridge supports Decimal128 only (precision <= 38)")
		}
		return exact(&arrow.Decimal128Type{Precision: *t.Precision, Scale: *t.Scale})
	case typesystem.KindDate:
		return exact(arrow.FixedWidthTypes.Date32)
	case typesystem.KindTime:
		return exact(arrow.FixedWidthTypes.Time64us)
	case typesystem.KindTimestamp:
		return exact(&arrow.TimestampType{Unit: arrow.Microsecond})
	case typesystem.KindTimestampTZ:
		zone := t.Timezone
		if zone == "" {
			zone = "UTC"
		}
		return exact(&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: zone})
	case typesystem.KindUUID:
		return fallback(typesystem.MappingSemanticFallback, "runtime conversion emits canonical UUID text; current storage bridge requires Arrow UUID extension values for native Iceberg UUID")
	case typesystem.KindJSON:
		return fallback(typesystem.MappingSemanticFallback, "native JSON conversion is not implemented")
	case typesystem.KindStruct:
		return fallback(typesystem.MappingSemanticFallback, "native struct conversion is not implemented")
	case typesystem.KindMap:
		return fallback(typesystem.MappingSemanticFallback, "native map conversion is not implemented")
	case typesystem.KindUnknown:
		return fallback(typesystem.MappingUnsupportedFallback, "unknown logical type uses lossless string fallback")
	case typesystem.KindArray:
		elementType, elementMapping, err := ArrowTypeForLogicalType(*t.Element)
		if err != nil {
			return nil, typesystem.MappingResult{}, fmt.Errorf("array element: %w", err)
		}
		class := elementMapping.Class
		reason := elementMapping.Reason
		return arrow.ListOf(elementType), typesystem.MappingFor(t, arrow.ListOf(elementType).String(), class, reason), nil
	default:
		return nil, typesystem.MappingResult{}, fmt.Errorf("unsupported logical type %s", t.String())
	}
}
