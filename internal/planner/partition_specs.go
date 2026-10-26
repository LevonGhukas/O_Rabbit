package planner

import (
	"encoding/json"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
)

// PartitionSpecSingleWithRecordPath constructs a single-task partition
// specification with optional JSON record selection metadata.
func PartitionSpecSingleWithRecordPath(table, sourceMode, queryHash, recordPath string) json.RawMessage {
	return PartitionSpecSingleWithFileOptions(table, sourceMode, queryHash, recordPath, "", nil)
}

// PartitionSpecSingleWithFileOptions constructs a single-task partition
// specification with optional file-source metadata. columnTypes are the
// resolved types of every file column (see resolveFileColumnTypes).
func PartitionSpecSingleWithFileOptions(table, sourceMode, queryHash, recordPath, fileFormat string, columnTypes map[string]string) json.RawMessage {
	part := map[string]any{
		"type":        "single",
		"source_mode": sourceMode,
		"table":       table,
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(recordPath) != "" {
		part["record_path"] = strings.TrimSpace(recordPath)
	}
	if strings.TrimSpace(fileFormat) != "" {
		part["format"] = strings.TrimSpace(fileFormat)
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	b, _ := json.Marshal(part)
	return json.RawMessage(b)
}

func partitionSpecSQLCursorRange(table, sourceMode, queryHash, whereClause string, selectColumns []string, columnTypes map[string]string, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, upper string, upperInclusive bool, outputPart int, snapshotCtx string) json.RawMessage {
	part := map[string]any{
		"type":            "sql_cursor_range",
		"source_mode":     sourceMode,
		"table":           table,
		"cursor_column":   column,
		"cursor_domain":   domain,
		"output_part":     outputPart,
		"lower":           strings.TrimSpace(lower),
		"lower_exclusive": lowerExclusive,
		"upper":           strings.TrimSpace(upper),
		"upper_inclusive": upperInclusive,
	}
	if strings.TrimSpace(whereClause) != "" {
		part["where_clause"] = strings.TrimSpace(whereClause)
	}
	if len(selectColumns) > 0 {
		part["select_columns"] = selectColumns
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	if len(selectColumns) > 0 {
		part["select_columns"] = selectColumns
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(snapshotCtx) != "" {
		part["snapshot_context"] = strings.TrimSpace(snapshotCtx)
	}
	b, _ := json.Marshal(part)
	return b
}

func partitionSpecSQLCursorSingle(table, sourceMode, queryHash, whereClause string, selectColumns []string, columnTypes map[string]string, column string, domain connectors.CursorDomain, lower string, lowerExclusive bool, snapshotCtx string) json.RawMessage {
	part := map[string]any{
		"type":            "sql_cursor_single",
		"source_mode":     sourceMode,
		"table":           table,
		"cursor_column":   column,
		"cursor_domain":   domain,
		"lower":           strings.TrimSpace(lower),
		"lower_exclusive": lowerExclusive,
	}
	if strings.TrimSpace(whereClause) != "" {
		part["where_clause"] = strings.TrimSpace(whereClause)
	}
	if len(selectColumns) > 0 {
		part["select_columns"] = selectColumns
	}
	if len(columnTypes) > 0 {
		part["column_types"] = columnTypes
	}
	if strings.TrimSpace(queryHash) != "" {
		part["query_hash"] = strings.TrimSpace(queryHash)
	}
	if strings.TrimSpace(snapshotCtx) != "" {
		part["snapshot_context"] = strings.TrimSpace(snapshotCtx)
	}
	b, _ := json.Marshal(part)
	return b
}
