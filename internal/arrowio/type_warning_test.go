package arrowio

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSQLPlanWarnings(t *testing.T) {
	r, err := PlansFromSQLEngineResult("postgres", []string{"id"}, nil, map[string]string{"id": "uuid"})
	require.NoError(t, err)
	require.Len(t, r.Warnings, 1)
	require.Equal(t, "id", r.Warnings[0].Column)
	require.Equal(t, "semantic_fallback", string(r.Warnings[0].Class))
	r, err = PlansFromSQLEngineResult("sqlite", []string{"x"}, nil, nil)
	require.NoError(t, err)
	require.Len(t, r.Warnings, 1)
}
