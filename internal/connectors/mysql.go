package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

type MySQL struct {
	db            *sql.DB
	sourceIsLocal bool
}

const (
	mysqlPingTimeout     = 10 * time.Second
	mysqlStatsTimeout    = 2 * time.Minute
	mysqlValidateTimeout = 20 * time.Second
	envMySQLOrderedReads = "ORABBIT_MYSQL_ORDERED_RANGE_READS"
)

var (
	mysqlOrderedReadsOnce sync.Once
	mysqlOrderedReads     = true
)

func OpenMySQL(ctx context.Context, dsn string) (*MySQL, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, mysqlPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MySQL{db: db, sourceIsLocal: mysqlDSNIsLocal(dsn)}, nil
}

func (m *MySQL) Close() error { return m.db.Close() }

func (m *MySQL) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, mysqlValidateTimeout)
	defer cancel()
	return describeQuery(qctx, m.db, "mysql", query)
}

func (m *MySQL) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	qt, err := quoteMySQLMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0;", qt))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	ct, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	return cols, ct, nil
}

var mysqlIdentPartRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func quoteMySQLMultipartIdent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	parts := strings.Split(s, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "`")
		if !mysqlIdentPartRe.MatchString(p) {
			return "", fmt.Errorf("unsafe identifier %q", s)
		}
		out = append(out, "`"+p+"`")
	}
	return strings.Join(out, "."), nil
}

func (m *MySQL) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, m.db, "mysql", q)
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

	query := fmt.Sprintf("SELECT %s FROM %s", buildMySQLSelectClause(q.SelectColumns), qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if mysqlOrderedRangeReadsEnabled() {
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

func (m *MySQL) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, mysqlStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, m.db, "mysql", query, cursorColumn, domain, m.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (m *MySQL) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quoteMySQLMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quoteMySQLMultipartIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, mysqlStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: m.sourceIsLocal}

	qBounds := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s;", qc, qc, qt)
	var minv, maxv any
	if err := m.db.QueryRowContext(qctx, qBounds).Scan(&minv, &maxv); err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	dbName, tableName := splitMySQLTableIdent(table)
	if dbName != "" && tableName != "" {
		var rc, sz sql.NullInt64
		err := m.db.QueryRowContext(qctx, `
			SELECT
				COALESCE(table_rows, 0),
				COALESCE(data_length + index_length, 0)
			FROM information_schema.tables
			WHERE table_schema = ? AND table_name = ?;`,
			dbName, tableName,
		).Scan(&rc, &sz)
		if err == nil {
			if rc.Valid {
				out.RowCount = rc.Int64
			}
			if sz.Valid {
				out.TableBytes = sz.Int64
			}
		}
	}

	return out, nil
}

func (m *MySQL) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, mysqlValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, m.db, "mysql", query, cursorColumn)
}

func (m *MySQL) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, mysqlValidateTimeout)
	defer cancel()

	cols, ct, err := m.DescribeTable(vctx, table)
	if err != nil {
		return out, fmt.Errorf("describe table for ordered-cursor validation (%s): %w", table, err)
	}

	leaf := identLeaf(cursorColumn)
	for i, c := range cols {
		if !cursorColumnMatches(c, leaf) {
			continue
		}
		out.Found = true
		out.ResolvedName = c
		if i < len(ct) && ct[i] != nil {
			out.DataType = strings.ToUpper(strings.TrimSpace(ct[i].DatabaseTypeName()))
			class := ClassifySQLCursorType(out.DataType)
			out.Domain = class.Domain
			out.Orderable = class.Orderable
			out.RangeCapable = class.RangeCapable
			if n, ok := ct[i].Nullable(); ok {
				out.NullableKnown = true
				out.Nullable = n
			}
		}
		break
	}
	if !out.Found {
		return out, nil
	}

	dbName, tableName := splitMySQLTableIdent(table)
	if dbName != "" && tableName != "" && leaf != "" {
		var hasIdx bool
		err := m.db.QueryRowContext(vctx, `
			SELECT EXISTS (
				SELECT 1
				FROM information_schema.statistics
				WHERE table_schema = ?
				  AND table_name = ?
				  AND column_name = ?
				  AND index_name != 'PRIMARY'
			);`,
			dbName, tableName, leaf,
		).Scan(&hasIdx)
		if err == nil {
			out.IndexedKnown = true
			out.Indexed = hasIdx
		}
	}

	return out, nil
}

func splitMySQLTableIdent(s string) (dbName, tableName string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ".")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "`")
		if p == "" {
			continue
		}
		clean = append(clean, p)
	}
	switch len(clean) {
	case 0:
		return "", ""
	case 1:
		return "", clean[0]
	default:
		return clean[len(clean)-2], clean[len(clean)-1]
	}
}

func mysqlOrderedRangeReadsEnabled() bool {
	mysqlOrderedReadsOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envMySQLOrderedReads))
		if raw == "" {
			mysqlOrderedReads = true
			return
		}
		if parsed, ok := envutil.ParseBoolEnv(raw); ok {
			mysqlOrderedReads = parsed
			return
		}
		mysqlOrderedReads = true
	})
	return mysqlOrderedReads
}

func mysqlDSNIsLocal(dsn string) bool {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return false
	}
	if host, ok := mysqlHostFromDSN(raw); ok {
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}
	return false
}

func mysqlHostFromDSN(dsn string) (string, bool) {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return "", false
	}
	// MySQL DSN: user:password@protocol(host:port)/dbname?params
	// Find the @ sign
	atIdx := strings.LastIndex(raw, "@")
	if atIdx < 0 {
		return "", false
	}
	afterAt := raw[atIdx+1:]
	// Check for protocol(host) format (e.g., tcp(localhost:3306))
	if openParen := strings.Index(afterAt, "("); openParen >= 0 {
		closeParen := strings.Index(afterAt, ")")
		if closeParen > openParen {
			hostPort := afterAt[openParen+1 : closeParen]
			if h, _, err := net.SplitHostPort(hostPort); err == nil {
				return strings.ToLower(strings.TrimSpace(h)), true
			}
			return strings.ToLower(strings.TrimSpace(hostPort)), true
		}
	}
	// Simple host:port or host format without protocol
	if h, _, err := net.SplitHostPort(afterAt); err == nil {
		return strings.ToLower(strings.TrimSpace(h)), true
	}
	h := strings.TrimSpace(afterAt)
	if h != "" {
		return strings.ToLower(h), true
	}
	return "", false
}

func buildMySQLSelectClause(cols []string) string {
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
