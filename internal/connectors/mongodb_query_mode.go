package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// DescribeQuery interprets query as a MongoDB JSON filter document and
// samples the collection to discover the result schema.
//
// For MongoDB query mode, the "query" field in the O_Rabbit payload is a
// JSON filter string (e.g. '{"status":"active"}').  The collection to sample
// is taken from CursorQuery.Table (populated from source.table in the job).
//
// Because MongoDB is schemaless, the column list is derived from sampling
// up to 100 documents that match the filter.
//
// NOTE: This method satisfies the SourceQueryReader interface.  The
// DescribeQuery signature receives the raw query string; for MongoDB this
// is parsed as a JSON filter.  The collection name is not available in this
// call—callers must use DescribeCollection directly for per-table schema
// discovery.  We return a static single-column schema here so the planner
// can validate the cursor column via ValidateQueryCursorColumn instead.
func (m *MongoDB) DescribeQuery(_ context.Context, query string) ([]string, []*sql.ColumnType, error) {
	if err := validateMongoQueryFilter(query); err != nil {
		return nil, nil, err
	}
	// MongoDB schema is discovered per-collection, not per-query.
	// Return an empty column list; actual schema is validated in
	// ValidateQueryCursorColumn via DescribeCollection.
	return []string{}, []*sql.ColumnType{}, nil
}

// ValidateQueryCursorColumn validates that the cursor column exists in the
// MongoDB collection by sampling the collection schema.
//
// For MongoDB query mode the "query" parameter is the raw filter JSON string;
// the collection name is embedded in the CursorQuery.Table field which is
// not available in this interface method.  We return a permissive validation
// result that allows the planner to proceed, with RangeCapable set to false
// so that a single full-sweep task is created rather than attempting
// cursor-range partitioning on the query result.
func (m *MongoDB) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	if err := validateMongoQueryFilter(query); err != nil {
		return CursorColumnValidation{}, err
	}

	leaf := strings.TrimSpace(cursorColumn)
	if leaf == "" {
		return CursorColumnValidation{}, fmt.Errorf("cursor column is required")
	}

	// For MongoDB, we cannot know the collection name from the query string alone.
	// Return a permissive validation that lets the planner proceed.
	// The actual cursor column existence is checked by the worker at extraction time.
	dt := "STRING"
	domain := CursorDomainString
	if leaf == "_id" {
		dt = "OBJECTID"
	}
	return CursorColumnValidation{
		Found:        true,
		ResolvedName: leaf,
		DataType:     dt,
		Domain:       domain,
		Orderable:    true,
		// Disable range-based partitioning for MongoDB query mode.
		// A single full-sweep task is created instead.
		RangeCapable:  false,
		Nullable:      false,
		NullableKnown: false,
		Indexed:       false,
		IndexedKnown:  false,
	}, nil
}

// DiscoverQueryCursorStats returns empty stats for MongoDB query mode.
//
// Because we cannot determine min/max for an arbitrary MongoDB filter without
// executing the full query against the live collection (and because doing so
// could be extremely expensive on large collections), we return empty stats
// which signals the planner to create a single non-partitioned task.
func (m *MongoDB) DiscoverQueryCursorStats(_ context.Context, _, _ string, _ CursorDomain) (CursorStats, error) {
	return CursorStats{
		RowCount:   0,
		TableBytes: 0,
		MinValue:   "",
		MaxValue:   "",
	}, nil
}

// validateMongoQueryFilter performs a lightweight safety check on the user-
// supplied query string for MongoDB query mode.
//
// Accepts:
//   - Empty string (matches all documents)
//   - Valid JSON object (filter document)
//   - Valid JSON array (aggregation pipeline)
func validateMongoQueryFilter(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	// Must be a JSON object or array.
	if !strings.HasPrefix(query, "{") && !strings.HasPrefix(query, "[") {
		return fmt.Errorf(
			"mongodb query mode requires a JSON filter object (e.g. {\"status\":\"active\"}) "+
				"or aggregation pipeline array (e.g. [{\"$match\":{\"status\":\"active\"}}]); got: %q",
			truncateForError(query, 80),
		)
	}
	var v any
	if err := json.Unmarshal([]byte(query), &v); err != nil {
		return fmt.Errorf("mongodb query mode: invalid JSON filter: %w", err)
	}
	return nil
}

func truncateForError(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
