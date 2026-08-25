package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

type MariaDB struct {
	MySQL
}

const (
	mariadbStatsTimeout    = 2 * time.Minute
	mariadbValidateTimeout = 20 * time.Second
	envMariaDBOrderedReads = "ORABBIT_MARIADB_ORDERED_RANGE_READS"
)

var (
	mariadbOrderedReadsOnce sync.Once
	mariadbOrderedReads     = true
)

func OpenMariaDB(ctx context.Context, dsn string) (*MariaDB, error) {
	m, err := OpenMySQL(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &MariaDB{MySQL: *m}, nil
}

func (m *MariaDB) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, mariadbValidateTimeout)
	defer cancel()
	return describeQuery(qctx, m.db, "mariadb", query)
}

func (m *MariaDB) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, m.db, "mariadb", q)
	}

	qt, err := quoteMySQLMultipartIdent(q.Table)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	qc, err := quoteMySQLMultipartIdent(q.CursorColumn)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	if NormalizeCursorDomain(string(q.CursorDomain)) == CursorDomainUnknown {
		return nil, nil, nil, -1, fmt.Errorf("cursor domain is required")
	}

	clauses := make([]string, 0, 3)
	if strings.TrimSpace(q.WhereClause) != "" {
		clauses = append(clauses, fmt.Sprintf("(%s)", q.WhereClause))
	}
	args := make([]any, 0, 2)
	if strings.TrimSpace(q.LowerBound) != "" {
		lowerArg, err := ParseCursorArgument(q.CursorDomain, q.LowerBound)
		if err != nil {
			return nil, nil, nil, -1, err
		}
		op := ">="
		if q.LowerExclusive {
			op = ">"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s ?", qc, op))
		args = append(args, lowerArg)
	}
	if strings.TrimSpace(q.UpperBound) != "" {
		upperArg, err := ParseCursorArgument(q.CursorDomain, q.UpperBound)
		if err != nil {
			return nil, nil, nil, -1, err
		}
		op := "<"
		if q.UpperInclusive {
			op = "<="
		}
		clauses = append(clauses, fmt.Sprintf("%s %s ?", qc, op))
		args = append(args, upperArg)
	}

	selectClause, err := renderSelectColumns(mysqlIdentifierRenderer, q.SelectColumns)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	query := fmt.Sprintf("SELECT %s FROM %s", selectClause, qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if mariadbOrderedRangeReadsEnabled() {
		query += fmt.Sprintf(" ORDER BY %s ASC", qc)
	}
	query += ";"

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, -1, err
	}

	cols, err := rows.Columns()
	if err != nil {
		rows.Close()
		return nil, nil, nil, -1, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, nil, nil, -1, err
	}
	cursorIdx := -1
	for i, c := range cols {
		if cursorColumnMatches(c, q.CursorColumn) {
			cursorIdx = i
			break
		}
	}
	return rows, cols, ct, cursorIdx, nil
}

func (m *MariaDB) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, mariadbStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, m.db, "mariadb", query, cursorColumn, domain, m.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (m *MariaDB) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, mariadbValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, m.db, "mariadb", query, cursorColumn)
}

func mariadbOrderedRangeReadsEnabled() bool {
	mariadbOrderedReadsOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envMariaDBOrderedReads))
		if raw == "" {
			mariadbOrderedReads = true
			return
		}
		if parsed, ok := envutil.ParseBoolEnv(raw); ok {
			mariadbOrderedReads = parsed
			return
		}
		mariadbOrderedReads = true
	})
	return mariadbOrderedReads
}
