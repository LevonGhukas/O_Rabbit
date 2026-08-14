package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

// cassandraQueryModeTimeout reuses the validate timeout for CQL query introspection.
const cassandraQueryModeTimeout = cassandraValidateTimeout

// DescribeQuery executes the user-supplied CQL SELECT with LIMIT 0 to introspect
// the result columns without fetching any data.
//
// The query must be a plain CQL SELECT (no semi-colon required). We add LIMIT 1
// and immediately close the iterator after reading column metadata.
func (c *Cassandra) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	query = strings.TrimSpace(query)
	query = strings.TrimSuffix(query, ";")
	if query == "" {
		return nil, nil, fmt.Errorf("cassandra query is required")
	}
	if err := validateCassandraCQLQuery(query); err != nil {
		return nil, nil, err
	}

	qctx, cancel := context.WithTimeout(ctx, cassandraQueryModeTimeout)
	defer cancel()

	// Use LIMIT 1 rather than LIMIT 0 because gocql returns no column metadata
	// from a zero-row result set.
	cql := fmt.Sprintf("%s LIMIT 1", query)
	iter := c.session.Query(cql).WithContext(qctx).Iter()
	cols := iter.Columns()
	_ = iter.Close()

	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("cassandra describe query: no columns returned (invalid CQL or empty table)")
	}

	names := make([]string, len(cols))
	cts := make([]*sql.ColumnType, len(cols))
	for i, col := range cols {
		names[i] = col.Name
		ct, err := cassandraColumnType(col.Name, col.TypeInfo.Type().String(), true)
		if err != nil {
			// Fall back to a string type rather than failing entirely.
			ct, _ = cassandraColumnType(col.Name, "text", true)
		}
		cts[i] = ct
	}
	return names, cts, nil
}

// ValidateQueryCursorColumn checks whether cursorColumn appears in the result
// set of the user-supplied CQL query.
func (c *Cassandra) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	cols, cts, err := c.DescribeQuery(ctx, query)
	if err != nil {
		return CursorColumnValidation{}, fmt.Errorf("cassandra validate query cursor: %w", err)
	}

	leaf := identLeaf(cursorColumn)
	for i, col := range cols {
		if !cursorColumnMatches(col, leaf) {
			continue
		}
		out := CursorColumnValidation{
			Found:        true,
			ResolvedName: col,
		}
		if i < len(cts) && cts[i] != nil {
			out.DataType = strings.TrimSpace(cts[i].DatabaseTypeName())
			class := classifyCassandraCursorType(out.DataType)
			out.Domain = class.Domain
			out.Orderable = class.Orderable
			out.RangeCapable = class.RangeCapable
		}
		// For query mode, we cannot reliably determine nullability from the
		// synthetic SQL driver without parsing system_schema.columns (which
		// is unreliable due to aliases/expressions in CQL).
		// We flag it as unknown so the planner permits the incremental job,
		// putting the onus on the user to select a valid non-null cursor.
		out.NullableKnown = false
		out.Nullable = false
		return out, nil
	}
	return CursorColumnValidation{}, nil
}

// DiscoverQueryCursorStats returns statistics for the query result set with
// respect to the cursor column.  Because Cassandra does not support
// MIN/MAX aggregations across partitions in CQL query mode, we return
// a conservative full-range stats object that forces a single-task scan.
func (c *Cassandra) DiscoverQueryCursorStats(_ context.Context, _, _ string, domain CursorDomain) (CursorStats, error) {
	// Cassandra CQL query mode does not support incremental cursor statistics.
	// Return a zero-range stats object to force a non-partitioned single sweep.
	return CursorStats{
		SourceIsLocal: c.sourceIsLocal,
		RowCount:      0,
		TableBytes:    0,
		// Empty min/max signals the planner to do a full single-partition extraction.
		MinValue: "",
		MaxValue: "",
	}, nil
}

// validateCassandraCQLQuery performs a lightweight safety check on a user-
// supplied CQL query before executing it.
func validateCassandraCQLQuery(query string) error {
	upper := strings.ToUpper(strings.TrimSpace(query))
	if !strings.HasPrefix(upper, "SELECT") {
		return fmt.Errorf("cassandra query mode only supports SELECT statements")
	}
	// Reject obviously destructive keywords using regex word boundaries
	// to avoid false positives (e.g. 'created_at') and handle tabs/newlines.
	for _, kw := range []string{"INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE"} {
		matched, _ := regexp.MatchString(`(?i)\b`+kw+`\b`, upper)
		if matched {
			return fmt.Errorf("cassandra query mode rejects unsafe CQL keyword %q", kw)
		}
	}
	return nil
}
