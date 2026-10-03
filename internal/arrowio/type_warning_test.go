package arrowio

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSQLPlanWarnings(t *testing.T) {
	// An explicit uuid override is stored natively, so it needs no fallback warning.
	r, err := PlansFromSQLEngineResult("postgres", []string{"id"}, nil, map[string]string{"id": "uuid"})
	require.NoError(t, err)
	require.Empty(t, r.Warnings)
	r, err = PlansFromSQLEngineResult("sqlite", []string{"x"}, nil, nil)
	require.NoError(t, err)
	require.Len(t, r.Warnings, 1)
}
