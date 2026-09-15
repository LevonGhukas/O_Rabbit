package typesystem

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWarningForMapping(t *testing.T) {
	logical := LogicalType{Kind: KindUUID}
	_, ok := WarningForMapping("id", MappingFor(logical, "string", MappingExact, ""))
	require.False(t, ok)
	w, ok := WarningForMapping("id", MappingFor(logical, "string", MappingSemanticFallback, "uuid text"))
	require.True(t, ok)
	require.Equal(t, "uuid", w.LogicalType)
	require.Equal(t, "string", w.StorageType)
	require.Equal(t, MappingSemanticFallback, w.Class)
}
func TestDeduplicateTypeWarnings(t *testing.T) {
	w := TypeWarning{Column: "x", LogicalType: "uuid", StorageType: "string", Class: MappingSemanticFallback, Reason: "r"}
	out := DeduplicateTypeWarnings([]TypeWarning{w, w, {Column: "a", LogicalType: "unknown", StorageType: "string", Class: MappingUnsupportedFallback, Reason: "u"}})
	require.Len(t, out, 2)
	require.Equal(t, "a", out[0].Column)
}
