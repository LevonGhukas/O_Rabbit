package icebergreg

import (
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// IcebergMapping describes the Iceberg type expected by the current storage
// pipeline. TypeName is used deliberately instead of constructing a schema:
// nested Iceberg types require field IDs, which remain the responsibility of
// ArrowSchemaToIcebergWithFreshIDs.
type IcebergMapping struct {
	LogicalType typesystem.LogicalType
	TypeName    string
	Class       typesystem.MappingClass
	Fallback    bool
	Reason      string
}

// StorageMapping is the resolved physical representation for the existing
// Arrow-to-Iceberg schema path. ArrowType is never allowed to disagree with
// ExpectedIceberg.
type StorageMapping struct {
	LogicalType     typesystem.LogicalType
	ArrowType       arrow.DataType
	Arrow           typesystem.MappingResult
	ExpectedIceberg IcebergMapping
	Class           typesystem.MappingClass
	Fallback        bool
	Reason          string
}

// IcebergMappingForLogicalType determines the Iceberg representation before a
// schema is constructed. It models the installed Iceberg bridge, including its
// UTC-only Arrow timestamp-with-timezone compatibility.
func IcebergMappingForLogicalType(t typesystem.LogicalType) (IcebergMapping, error) {
	if err := t.Validate(); err != nil {
		return IcebergMapping{}, fmt.Errorf("invalid logical type %s: %w", t.String(), err)
	}
	exact := func(typeName string) IcebergMapping {
		return icebergMapping(t, typeName, typesystem.MappingExact, "")
	}
	promoted := func(typeName string, reason string) IcebergMapping {
		return icebergMapping(t, typeName, typesystem.MappingSafePromotion, reason)
	}
	fallback := func(class typesystem.MappingClass, reason string) IcebergMapping {
		return icebergMapping(t, "string", class, reason)
	}

	switch t.Kind {
	case typesystem.KindBool:
		return exact("boolean"), nil
	case typesystem.KindInt8:
		return promoted("int", "Iceberg has no int8; int preserves the full range"), nil
	case typesystem.KindInt16:
		return promoted("int", "Iceberg has no int16; int preserves the full range"), nil
	case typesystem.KindInt32:
		return exact("int"), nil
	case typesystem.KindInt64:
		return exact("long"), nil
	case typesystem.KindUInt8:
		return promoted("int", "Iceberg int preserves the full uint8 range"), nil
	case typesystem.KindUInt16:
		return promoted("int", "Iceberg int preserves the full uint16 range"), nil
	case typesystem.KindUInt32:
		return promoted("long", "Iceberg long preserves the full uint32 range"), nil
	case typesystem.KindUInt64:
		return fallback(typesystem.MappingSemanticFallback, "Iceberg long cannot represent the full uint64 range"), nil
	case typesystem.KindFloat32:
		return exact("float"), nil
	case typesystem.KindFloat64:
		return exact("double"), nil
	case typesystem.KindString:
		return exact("string"), nil
	case typesystem.KindBinary:
		return exact("binary"), nil
	case typesystem.KindDecimal:
		if *t.Precision > 38 {
			return fallback(typesystem.MappingSemanticFallback, "Iceberg decimal precision is limited to 38"), nil
		}
		return exact(fmt.Sprintf("decimal(%d, %d)", *t.Precision, *t.Scale)), nil
	case typesystem.KindDate:
		return exact("date"), nil
	case typesystem.KindTime:
		return exact("time"), nil
	case typesystem.KindTimestamp:
		return exact("timestamp"), nil
	case typesystem.KindTimestampTZ:
		if t.Timezone != "" && !isUTCAlias(t.Timezone) {
			return fallback(typesystem.MappingSemanticFallback, "current Arrow-to-Iceberg bridge accepts timezone-aware timestamps only for UTC aliases"), nil
		}
		return exact("timestamptz"), nil
	case typesystem.KindUUID:
		return fallback(typesystem.MappingSemanticFallback, "Iceberg supports UUID, but current runtime conversion and Arrow storage path use canonical UUID text"), nil
	case typesystem.KindJSON:
		return fallback(typesystem.MappingSemanticFallback, "native JSON conversion is not implemented"), nil
	case typesystem.KindStruct:
		return fallback(typesystem.MappingSemanticFallback, "native struct conversion is not implemented"), nil
	case typesystem.KindMap:
		return fallback(typesystem.MappingSemanticFallback, "native map conversion is not implemented"), nil
	case typesystem.KindUnknown:
		return fallback(typesystem.MappingUnsupportedFallback, "unknown logical type uses lossless string fallback"), nil
	case typesystem.KindArray:
		element, err := IcebergMappingForLogicalType(*t.Element)
		if err != nil {
			return IcebergMapping{}, fmt.Errorf("array element: %w", err)
		}
		return icebergMapping(t, "list<"+element.TypeName+">", element.Class, element.Reason), nil
	default:
		return IcebergMapping{}, fmt.Errorf("unsupported logical type %s", t.String())
	}
}

func icebergMapping(t typesystem.LogicalType, typeName string, class typesystem.MappingClass, reason string) IcebergMapping {
	return IcebergMapping{
		LogicalType: t,
		TypeName:    typeName,
		Class:       class,
		Fallback:    class == typesystem.MappingSemanticFallback || class == typesystem.MappingUnsupportedFallback,
		Reason:      reason,
	}
}

// ResolveStorageMapping selects an Arrow physical type which the installed
// ArrowSchemaToIcebergWithFreshIDs bridge will produce as ExpectedIceberg.
func ResolveStorageMapping(t typesystem.LogicalType) (StorageMapping, error) {
	expected, err := IcebergMappingForLogicalType(t)
	if err != nil {
		return StorageMapping{}, err
	}

	if t.Kind == typesystem.KindArray {
		element, err := ResolveStorageMapping(*t.Element)
		if err != nil {
			return StorageMapping{}, fmt.Errorf("array element: %w", err)
		}
		dataType := arrow.ListOf(element.ArrowType)
		mapping := typesystem.MappingFor(t, dataType.String(), element.Class, element.Reason)
		resolved := StorageMapping{
			LogicalType:     t,
			ArrowType:       dataType,
			Arrow:           mapping,
			ExpectedIceberg: expected,
			Class:           element.Class,
			Fallback:        element.Fallback,
			Reason:          element.Reason,
		}
		return verifyStorageMapping(resolved)
	}

	dataType, arrowMapping, err := arrowio.ArrowTypeForLogicalType(t)
	if err != nil {
		return StorageMapping{}, err
	}
	if expected.Fallback {
		dataType = arrow.BinaryTypes.String
		arrowMapping = typesystem.MappingFor(t, dataType.String(), expected.Class, expected.Reason)
	}

	resolved := StorageMapping{
		LogicalType:     t,
		ArrowType:       dataType,
		Arrow:           arrowMapping,
		ExpectedIceberg: expected,
		Class:           combineMappingClass(arrowMapping.Class, expected.Class),
		Fallback:        arrowMapping.Fallback || expected.Fallback,
		Reason:          firstReason(expected.Reason, arrowMapping.Reason),
	}
	return verifyStorageMapping(resolved)
}

// ArrowTypeCompatibleWithIceberg verifies compatibility against the installed
// Arrow-to-Iceberg bridge rather than maintaining a second guessed type table.
func ArrowTypeCompatibleWithIceberg(dataType arrow.DataType, expected IcebergMapping) (bool, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "value", Type: dataType, Nullable: expected.LogicalType.Nullable}}, nil)
	converted, err := icetable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		return false, err
	}
	fields := converted.Fields()
	if len(fields) != 1 {
		return false, fmt.Errorf("unexpected Iceberg field count %d", len(fields))
	}
	return fields[0].Type.String() == expected.TypeName, nil
}

func verifyStorageMapping(mapping StorageMapping) (StorageMapping, error) {
	compatible, err := ArrowTypeCompatibleWithIceberg(mapping.ArrowType, mapping.ExpectedIceberg)
	if err != nil {
		return StorageMapping{}, fmt.Errorf("Arrow type %s is not accepted by the Arrow-to-Iceberg bridge: %w", mapping.ArrowType, err)
	}
	if !compatible {
		return StorageMapping{}, fmt.Errorf("Arrow type %s does not map to expected Iceberg type %s", mapping.ArrowType, mapping.ExpectedIceberg.TypeName)
	}
	return mapping, nil
}

func combineMappingClass(left, right typesystem.MappingClass) typesystem.MappingClass {
	for _, candidate := range []typesystem.MappingClass{
		typesystem.MappingUnsupportedFallback,
		typesystem.MappingSemanticFallback,
		typesystem.MappingSafePromotion,
	} {
		if left == candidate || right == candidate {
			return candidate
		}
	}
	return typesystem.MappingExact
}

func firstReason(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func isUTCAlias(timezone string) bool {
	return strings.EqualFold(timezone, "UTC") || timezone == "+00:00" || timezone == "Etc/UTC" || timezone == "Z"
}
