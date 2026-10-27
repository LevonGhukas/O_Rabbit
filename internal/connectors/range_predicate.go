package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// RangeCounter counts the source rows a CursorQuery selects. It evaluates the
// same predicate (where clause, bounds and bound inclusivity) as QueryCursor,
// so a count can be compared with the rows an extraction produced.
type RangeCounter interface {
	CountRange(ctx context.Context, q CursorQuery) (int64, error)
}

// supportsRangeCount lists the engines whose readers implement RangeCounter.
var supportsRangeCount = map[string]bool{
	"postgres":   true,
	"mysql":      true,
	"mariadb":    true,
	"mssql":      true,
	"oracle":     true,
	"clickhouse": true,
	"trino":      true,
}

// SupportsRangeCount reports whether the engine can count a cursor range.
func SupportsRangeCount(engine string) bool {
	return supportsRangeCount[NormalizeSourceEngine(engine)]
}

// rangeSQL is the resolved SQL building blocks shared by a driver's row query
// and its count query: the quoted source, the quoted cursor column and the
// WHERE predicate (without the WHERE keyword) with its positional arguments.
type rangeSQL struct {
	Table  string
	Cursor string
	Where  string
	Args   []any
}

// whereSQL returns " WHERE <predicate>", or "" when there is no predicate.
func (r rangeSQL) whereSQL() string {
	if r.Where == "" {
		return ""
	}
	return " WHERE " + r.Where
}

// newRangeSQL builds the predicate for q: the optional where clause first
// (when honorWhere is set), then the lower bound, then the upper bound.
func newRangeSQL(q CursorQuery, table, cursor string, honorWhere bool, placeholder func(pos int) string) (rangeSQL, error) {
	if NormalizeCursorDomain(string(q.CursorDomain)) == CursorDomainUnknown {
		return rangeSQL{}, fmt.Errorf("cursor domain is required")
	}
	clauses := make([]string, 0, 3)
	if honorWhere && strings.TrimSpace(q.WhereClause) != "" {
		clauses = append(clauses, fmt.Sprintf("(%s)", q.WhereClause))
	}
	args := make([]any, 0, 2)
	if strings.TrimSpace(q.LowerBound) != "" {
		arg, err := ParseCursorArgument(q.CursorDomain, q.LowerBound)
		if err != nil {
			return rangeSQL{}, err
		}
		op := ">="
		if q.LowerExclusive {
			op = ">"
		}
		args = append(args, arg)
		clauses = append(clauses, fmt.Sprintf("%s %s %s", cursor, op, placeholder(len(args))))
	}
	if strings.TrimSpace(q.UpperBound) != "" {
		arg, err := ParseCursorArgument(q.CursorDomain, q.UpperBound)
		if err != nil {
			return rangeSQL{}, err
		}
		op := "<"
		if q.UpperInclusive {
			op = "<="
		}
		args = append(args, arg)
		clauses = append(clauses, fmt.Sprintf("%s %s %s", cursor, op, placeholder(len(args))))
	}
	return rangeSQL{Table: table, Cursor: cursor, Where: strings.Join(clauses, " AND "), Args: args}, nil
}

func questionPlaceholder(int) string { return "?" }

func dollarPlaceholder(pos int) string { return fmt.Sprintf("$%d", pos) }

func mssqlPlaceholder(pos int) string { return fmt.Sprintf("@p%d", pos) }

func oraclePlaceholder(pos int) string { return fmt.Sprintf(":%d", pos) }

// queryRangeCount runs a COUNT(*) statement and returns its single value.
func queryRangeCount(ctx context.Context, db *sql.DB, query string, args []any) (int64, error) {
	var n int64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

const (
	countExpr = "COUNT(*)"
	// mssqlCountExpr avoids the INT overflow COUNT(*) has past 2^31 rows.
	mssqlCountExpr = "COUNT_BIG(*)"
	// clickHouseCountExpr casts the UInt64 count to a signed type so it scans
	// into int64.
	clickHouseCountExpr = "toInt64(count())"
)

// countFrom builds "SELECT <count> FROM <source><where>".
func countFrom(expr, source string, r rangeSQL) string {
	return "SELECT " + expr + " FROM " + source + r.whereSQL()
}
