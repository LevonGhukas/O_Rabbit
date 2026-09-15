package icebergreg

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestParsedTypesResolveToCompatibleStorageMappings(t *testing.T) {
	tests := []struct {
		input        string
		wantArrow    arrow.DataType
		wantIceberg  string
		wantFallback bool
	}{
		{"uint64", arrow.BinaryTypes.String, "string", true},
		{"UUID", arrow.BinaryTypes.String, "string", true},
		{"decimal(18,2)", &arrow.Decimal128Type{Precision: 18, Scale: 2}, "decimal(18, 2)", false},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			logical, err := typesystem.ParseType(test.input)
			require.NoError(t, err)
			resolved, err := ResolveStorageMapping(logical)
			require.NoError(t, err)
			require.Equal(t, test.wantArrow, resolved.ArrowType)
			require.Equal(t, test.wantIceberg, resolved.ExpectedIceberg.TypeName)
			require.Equal(t, test.wantFallback, resolved.Fallback)
		})
	}
}
