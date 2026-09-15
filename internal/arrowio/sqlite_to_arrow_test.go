package arrowio

import (
	"testing"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
	"github.com/stretchr/testify/require"
)

func TestSQLiteDeclaredTypeFallbacks(t *testing.T) {
	for _, source := range []string{"CUSTOM_AFFINITY", "MONEYISH", "MYDATEVALUE", ""} {
		logical, err := LogicalTypeForSQLiteColumn(source, 0, 0, false)
		require.NoError(t, err)
		require.Equal(t, typesystem.KindUnknown, logical.Kind)
		_, mapping, err := PlanForLogicalType("value", logical)
		require.NoError(t, err)
		require.Equal(t, typesystem.MappingUnsupportedFallback, mapping.Class)
	}
}
