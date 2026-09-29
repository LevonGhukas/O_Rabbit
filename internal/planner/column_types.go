package planner

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// resolveEffectiveColumnTypes verifies the job's column overrides against the
// source and returns the column types every task must write, together with
// the type warnings for the run.
//
// User overrides win unless the source data shows they would lose values or
// violate NOT NULL; those columns fall back to a lossless type and get a
// warning. Columns without overrides keep source inference, plus catalog-proven
// NOT NULL for drivers that cannot report nullability themselves.
func resolveEffectiveColumnTypes(ctx context.Context, st *db.Store, k crypto.Key, job db.Job, srcEngine string, o jobopts.Options, runID string) (map[string]string, []typesystem.TypeWarning, error) {
	if connectors.SupportsDocumentReader(srcEngine) || !connectors.SupportsOrderedCursor(srcEngine) {
		return o.ColumnTypes, nil, nil
	}
	reader, err := openCursorReader(ctx, st, k, job.SourceConnectionID, srcEngine)
	if err != nil {
		// Connection problems are reported by the extraction itself with
		// source-specific context; do not fail planning here.
		slog.Warn("column type verification skipped: source unavailable", "run_id", runID, "source_engine", srcEngine, "error", err)
		return o.ColumnTypes, nil, nil
	}
	defer reader.Close()

	var (
		query    string
		cols     []string
		colTypes []*sql.ColumnType
	)
	if o.NormalizedSourceMode() == "query" {
		query = o.Query
		q, ok := reader.(connectors.SourceQueryReader)
		if !ok {
			return o.ColumnTypes, nil, nil
		}
		cols, colTypes, err = q.DescribeQuery(ctx, query)
	} else {
		query, _ = connectors.TableAsQuery(srcEngine, o.Table)
		d, ok := reader.(tableDescriber)
		if !ok {
			return o.ColumnTypes, nil, nil
		}
		cols, colTypes, err = d.DescribeTable(ctx, o.Table)
	}
	if err != nil || len(cols) == 0 {
		// Schema discovery failures surface later with source-specific errors.
		return o.ColumnTypes, nil, nil
	}

	knownNotNull := map[string]bool{}
	if d, ok := reader.(connectors.QueryNotNullDescriber); ok && query != "" {
		if m, derr := d.DescribeQueryNotNull(ctx, query); derr == nil {
			knownNotNull = m
		} else {
			slog.Warn("catalog nullability lookup failed; treating columns as nullable", "run_id", runID, "source_engine", srcEngine, "error", derr)
		}
	}

	resolutions, err := arrowio.PlanColumnResolutions(srcEngine, cols, colTypes, o.ColumnTypes, knownNotNull)
	if err != nil {
		return nil, nil, err
	}

	var probes []connectors.ColumnProbe
	for _, r := range resolutions {
		if r.NeedsProbe() {
			probes = append(probes, r.Probe)
		}
	}
	stats := map[string]connectors.ColumnProbeResult{}
	var probeErr error
	if len(probes) > 0 {
		emitPlanEvent(ctx, st, runID, "INFO", "verifying selected column types against source data", map[string]any{"columns": len(probes)})
		prober, ok := reader.(connectors.QueryColumnProber)
		switch {
		case !ok || query == "":
			probeErr = errUnsupportedProbe
		default:
			results, perr := prober.ProbeQueryColumns(ctx, query, probes)
			probeErr = perr
			for _, r := range results {
				stats[r.Column] = r
			}
		}
	}

	effective, overrideWarnings := arrowio.FinalizeColumnResolutions(resolutions, stats, probeErr)
	for key, raw := range o.ColumnTypes {
		if !resultHasColumn(cols, key) {
			effective[key] = raw // unmatched overrides keep their previous behavior
		}
	}

	warnings := overrideWarnings
	if result, e := arrowio.PlansFromSQLEngineResult(srcEngine, cols, colTypes, effective); e == nil {
		warnings = append(warnings, result.Warnings...)
	}
	return effective, warnings, nil
}

var errUnsupportedProbe = errors.New("this source does not support value checks")

func resultHasColumn(cols []string, name string) bool {
	want := strings.ToLower(strings.TrimSpace(name))
	for _, c := range cols {
		if strings.ToLower(strings.TrimSpace(c)) == want {
			return true
		}
	}
	return false
}
