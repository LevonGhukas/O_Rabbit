package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
)

// exportRunSnapshot exports the source snapshot every task of a
// STRONG_SNAPSHOT run reads from, and returns "" for any other consistency
// mode. A source that cannot honour the snapshot fails planning rather than
// silently reading live data.
func exportRunSnapshot(ctx context.Context, reader any, engine, sourceMode, consistencyMode string) (string, error) {
	if !strings.EqualFold(strings.TrimSpace(consistencyMode), jobopts.ConsistencyStrongSnapshot) {
		return "", nil
	}
	exporter, ok := reader.(connectors.SnapshotExporter)
	if !ok {
		return "", configurationInvalid(fmt.Errorf("STRONG_SNAPSHOT consistency mode is not supported for engine %q", engine))
	}
	// Oracle applies the snapshot as AS OF SCN on the table, which cannot be
	// attached to an arbitrary source query.
	if engine == "oracle" && sourceMode == "query" {
		return "", configurationInvalid(fmt.Errorf("STRONG_SNAPSHOT consistency mode is not supported for oracle query mode; use table mode"))
	}
	snapshot, err := exporter.ExportSnapshot(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to export snapshot for STRONG_SNAPSHOT consistency: %w", err)
	}
	snapshot = strings.TrimSpace(snapshot)
	if snapshot == "" {
		return "", fmt.Errorf("failed to export snapshot for STRONG_SNAPSHOT consistency: source returned an empty snapshot")
	}
	return snapshot, nil
}

func configurationInvalid(err error) error {
	return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, err)
}
