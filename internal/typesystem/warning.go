package typesystem

import "sort"

// TypeWarning describes a schema-level storage fallback. It is deliberately
// emitted during planning, never during per-row conversion.
type TypeWarning struct {
	Column      string       `json:"column"`
	SourceType  string       `json:"source_type,omitempty"`
	LogicalType string       `json:"logical_type"`
	StorageType string       `json:"storage_type"`
	Class       MappingClass `json:"class"`
	Reason      string       `json:"reason"`
}

func WarningForMapping(column string, mapping MappingResult) (TypeWarning, bool) {
	if mapping.Class != MappingSemanticFallback && mapping.Class != MappingUnsupportedFallback {
		return TypeWarning{}, false
	}
	return TypeWarning{Column: column, SourceType: mapping.LogicalType.SourceTypeName, LogicalType: mapping.LogicalType.String(), StorageType: mapping.Destination, Class: mapping.Class, Reason: mapping.Reason}, true
}

func DeduplicateTypeWarnings(input []TypeWarning) []TypeWarning {
	seen := map[string]TypeWarning{}
	for _, w := range input {
		key := w.Column + "\x00" + w.SourceType + "\x00" + w.LogicalType + "\x00" + w.StorageType + "\x00" + string(w.Class) + "\x00" + w.Reason
		seen[key] = w
	}
	out := make([]TypeWarning, 0, len(seen))
	for _, w := range seen {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.Column+a.SourceType+a.LogicalType+a.StorageType+string(a.Class)+a.Reason < b.Column+b.SourceType+b.LogicalType+b.StorageType+string(b.Class)+b.Reason
	})
	return out
}
