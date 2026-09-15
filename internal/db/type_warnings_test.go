package db

import (
	"context"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRunTypeWarningsRoundTrip(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	warnings := []typesystem.TypeWarning{{Column: "id", LogicalType: "uuid", StorageType: "string", Class: typesystem.MappingSemanticFallback, Reason: "text"}}
	require.NoError(t, st.CreateRun(ctx, Run{ID: "warnings", JobID: "job", DatasetKey: "dataset", Status: "SUCCEEDED", CorrelationID: "c", StartedAt: "2026-01-01T00:00:00Z", TypeWarnings: warnings}))
	got, err := st.GetRun(ctx, "warnings")
	require.NoError(t, err)
	require.Equal(t, warnings, got.TypeWarnings)
	runs, err := st.ListRuns(ctx)
	require.NoError(t, err)
	require.Equal(t, warnings, runs[0].TypeWarnings)
}
func TestRunTypeWarningsStrictJSON(t *testing.T) {
	st := openTestStore(t)
	ctx := context.Background()
	require.NoError(t, st.CreateRun(ctx, Run{ID: "empty", JobID: "j", DatasetKey: "d", Status: "SUCCEEDED", CorrelationID: "c", StartedAt: "x"}))
	got, err := st.GetRun(ctx, "empty")
	require.NoError(t, err)
	require.NotNil(t, got.TypeWarnings)
	require.Empty(t, got.TypeWarnings)
	_, err = st.db.Exec(`UPDATE runs SET type_warnings_json='bad' WHERE id='empty'`)
	require.NoError(t, err)
	_, err = st.GetRun(ctx, "empty")
	require.Error(t, err)
}
