// Package partitionspec holds the task partition spec shared by the worker
// (which extracts a task) and the master (which reconciles it against the
// source), so both derive identical bounds from the same JSON.
package partitionspec

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/hwmboundary"
)

// Spec is the decoded partition_spec_json of a task.
type Spec struct {
	Type           string            `json:"type"`
	SourceMode     string            `json:"source_mode"`
	QueryHash      string            `json:"query_hash"`
	Table          string            `json:"table"`
	CursorColumn   string            `json:"cursor_column"`
	CursorDomain   string            `json:"cursor_domain"`
	Lower          string            `json:"lower"`
	Upper          string            `json:"upper"`
	LowerExclusive bool              `json:"lower_exclusive"`
	UpperInclusive bool              `json:"upper_inclusive"`
	OutputPart     int64             `json:"output_part"`
	WhereClause    string            `json:"where_clause,omitempty"`
	SelectColumns  []string          `json:"select_columns,omitempty"`
	ColumnTypes    map[string]string `json:"column_types,omitempty"`
	RecordPath     string            `json:"record_path,omitempty"`
	FileFormat     string            `json:"format,omitempty"`
	IDColumn       string            `json:"id_column"` // legacy alias
	From           int64             `json:"from"`      // legacy alias
	To             int64             `json:"to"`        // legacy alias

	// FetchSize is rows per source round-trip; 0 keeps the engine default.
	FetchSize int `json:"fetch_size,omitempty"`
	// HWMBoundarySkip lists rows at the high-water mark already exported by
	// an earlier run; only the task whose range starts at the mark has it.
	HWMBoundarySkip *hwmboundary.Skip `json:"hwm_boundary_skip,omitempty"`
	// TrackHWMBoundary asks the task to report the rows at its largest cursor value.
	TrackHWMBoundary bool `json:"track_hwm_boundary,omitempty"`
	// SnapshotContext is the source snapshot a STRONG_SNAPSHOT run exported at
	// planning (Postgres snapshot ID, Oracle SCN); every task reads from it.
	SnapshotContext string `json:"snapshot_context,omitempty"`
}

// Normalize fills defaults and maps legacy int-range specs onto cursor bounds.
func Normalize(ps Spec) Spec {
	ps.SourceMode = strings.ToLower(strings.TrimSpace(ps.SourceMode))
	if ps.SourceMode == "" {
		ps.SourceMode = "table"
	}
	if strings.TrimSpace(ps.CursorColumn) == "" {
		ps.CursorColumn = strings.TrimSpace(ps.IDColumn)
	}
	if strings.TrimSpace(ps.CursorDomain) == "" {
		switch ps.Type {
		case "sql_int_range", "mssql_int_range":
			ps.CursorDomain = string(connectors.CursorDomainInt64)
		}
	}
	if strings.TrimSpace(ps.Lower) == "" && (ps.Type == "sql_int_range" || ps.Type == "mssql_int_range") {
		ps.Lower = fmt.Sprintf("%d", ps.From)
		ps.LowerExclusive = true
	}
	if strings.TrimSpace(ps.Upper) == "" && (ps.Type == "sql_int_range" || ps.Type == "mssql_int_range") {
		ps.Upper = fmt.Sprintf("%d", ps.To)
		ps.UpperInclusive = true
	}
	return ps
}

// Parse decodes and normalizes a partition_spec_json document.
func Parse(raw []byte) (Spec, error) {
	var ps Spec
	if strings.TrimSpace(string(raw)) != "" {
		if err := json.Unmarshal(raw, &ps); err != nil {
			return Spec{}, fmt.Errorf("parse partition_spec_json: %w", err)
		}
	}
	return Normalize(ps), nil
}

// CursorQuery builds the CursorQuery the worker issues for this spec. sourceQuery
// is the query-mode source SQL (empty in table mode). The planned lower and
// upper bounds and their inclusivity come only from the spec.
func (ps Spec) CursorQuery(sourceQuery string) connectors.CursorQuery {
	return connectors.CursorQuery{
		Table:           ps.Table,
		SourceQuery:     sourceQuery,
		CursorColumn:    ps.CursorColumn,
		CursorDomain:    connectors.NormalizeCursorDomain(ps.CursorDomain),
		LowerBound:      ps.Lower,
		UpperBound:      ps.Upper,
		LowerExclusive:  ps.LowerExclusive,
		UpperInclusive:  ps.UpperInclusive,
		SnapshotContext: strings.TrimSpace(ps.SnapshotContext),
		WhereClause:     ps.WhereClause,
		SelectColumns:   ps.SelectColumns,
		ColumnTypes:     ps.ColumnTypes,
	}
}
