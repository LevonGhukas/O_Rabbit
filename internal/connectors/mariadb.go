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
	m, err := openMySQL(ctx, dsn, "tx_read_only")
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

	rng, err := mariadbRange(q)
	if err != nil {
		return nil, nil, nil, -1, err
	}

	query := fmt.Sprintf("SELECT %s FROM %s", buildMariaDBSelectClause(q.SelectColumns), rng.Table)
	query += rng.whereSQL()
	if mariadbOrderedRangeReadsEnabled() {
		query += fmt.Sprintf(" ORDER BY %s ASC", rng.Cursor)
	}
	query += ";"

	rows, err := m.db.QueryContext(ctx, query, rng.Args...)
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

func buildMariaDBSelectClause(cols []string) string {
	if len(cols) == 0 {
		return "*"
	}
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c != "" {
			if q, err := quoteMySQLMultipartIdent(c); err == nil {
				quoted = append(quoted, q)
			} else {
				quoted = append(quoted, c)
			}
		}
	}
	if len(quoted) == 0 {
		return "*"
	}
	return strings.Join(quoted, ", ")
}
