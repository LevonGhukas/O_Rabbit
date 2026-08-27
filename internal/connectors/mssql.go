package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

// MSSQL connector using go-mssqldb driver.
type MSSQL struct {
	db            *sql.DB
	sourceIsLocal bool
}

const (
	mssqlPingTimeout     = 10 * time.Second
	mssqlStatsTimeout    = 2 * time.Minute
	mssqlValidateTimeout = 20 * time.Second
	envMSSQLOrderedReads = "ORABBIT_MSSQL_ORDERED_RANGE_READS"
)

var (
	mssqlOrderedReadsOnce sync.Once
	mssqlOrderedReads     = true
)

// OpenMSSQL opens a connection to MSSQL using the provided DSN and verifies it by pinging the database.
func OpenMSSQL(ctx context.Context, dsn string) (*MSSQL, error) {
	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	// Keep pool bounded per worker process.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, mssqlPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &MSSQL{db: db, sourceIsLocal: mssqlDSNIsLocal(dsn)}, nil
}

// Close closes the MSSQL database connection.
func (m *MSSQL) Close() error { return m.db.Close() }

func (m *MSSQL) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, mssqlValidateTimeout)
	defer cancel()
	return describeQuery(qctx, m.db, "mssql", query)
}

// DescribeTable returns column names and SQL column types for the given table without scanning data.
func (m *MSSQL) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	qt, err := quoteMSSQLMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}
	query := fmt.Sprintf("SELECT TOP (0) * FROM %s;", qt)
	rows, err := m.db.QueryContext(ctx, query)
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

var identPartRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// quoteMSSQLMultipartIdent safely quotes a multipart MSSQL identifier (e.g., database.schema.table).
func quoteMSSQLMultipartIdent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	parts := strings.Split(s, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		if !identPartRe.MatchString(p) {
			return "", fmt.Errorf("unsafe identifier %q", s)
		}
		out = append(out, "["+p+"]")
	}
	return strings.Join(out, "."), nil
}

// QueryCursor executes a query to fetch rows using ordered cursor bounds.
func (m *MSSQL) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, m.db, "mssql", q)
	}

	qt, err := quoteMSSQLMultipartIdent(q.Table)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	qc, err := quoteMSSQLMultipartIdent(q.CursorColumn)
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
	argPos := 1
	if strings.TrimSpace(q.LowerBound) != "" {
		lowerArg, err := ParseCursorArgument(q.CursorDomain, q.LowerBound)
		if err != nil {
			return nil, nil, nil, -1, err
		}
		op := ">="
		if q.LowerExclusive {
			op = ">"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s @p%d", qc, op, argPos))
		args = append(args, lowerArg)
		argPos++
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
		clauses = append(clauses, fmt.Sprintf("%s %s @p%d", qc, op, argPos))
		args = append(args, upperArg)
	}

	query := fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK)", buildMSSQLProjectedSelectClause(q.SelectColumns, q.ColumnTypes, q.FallbackProjections), qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if mssqlOrderedRangeReadsEnabled() {
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

func (m *MSSQL) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, mssqlStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, m.db, "mssql", query, cursorColumn, domain, m.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (m *MSSQL) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quoteMSSQLMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quoteMSSQLMultipartIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, mssqlStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: m.sourceIsLocal}

	minv, maxv, err := m.discoverBounds(qctx, qt, qc)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	dbName, schemaName, tableName := splitMSSQLTableIdent(table)
	if schemaName != "" && tableName != "" {
		sysPrefix := mssqlSystemPrefix(dbName)
		qRowCount := fmt.Sprintf(`
			SELECT COALESCE(SUM(ps.row_count), 0)
			FROM %sdm_db_partition_stats ps
			JOIN %sobjects o ON o.object_id = ps.object_id
			JOIN %sschemas s ON s.schema_id = o.schema_id
			WHERE s.name = @p1 AND o.name = @p2 AND ps.index_id IN (0,1);`, sysPrefix, sysPrefix, sysPrefix)

		var rc int64
		if err := m.db.QueryRowContext(qctx, qRowCount, schemaName, tableName).Scan(&rc); err == nil && rc > 0 {
			out.RowCount = rc
		} else {
			qCountFallback := fmt.Sprintf("SELECT COUNT_BIG(1) FROM %s WITH (NOLOCK);", qt)
			_ = m.db.QueryRowContext(qctx, qCountFallback).Scan(&out.RowCount)
		}

		qBytes := fmt.Sprintf(`
			SELECT COALESCE(SUM(a.total_pages), 0) * 8192
			FROM %stables t
			JOIN %sindexes i          ON t.object_id = i.object_id
			JOIN %spartitions p       ON i.object_id = p.object_id AND i.index_id = p.index_id
			JOIN %sallocation_units a ON p.partition_id = a.container_id
			JOIN %sschemas s          ON t.schema_id = s.schema_id
			WHERE s.name = @p1 AND t.name = @p2 AND i.index_id IN (0,1);`, sysPrefix, sysPrefix, sysPrefix, sysPrefix, sysPrefix)

		var tb int64
		if err := m.db.QueryRowContext(qctx, qBytes, schemaName, tableName).Scan(&tb); err == nil {
			out.TableBytes = tb
		}
	}

	return out, nil
}

func (m *MSSQL) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, mssqlValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, m.db, "mssql", query, cursorColumn)
}

func (m *MSSQL) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, mssqlValidateTimeout)
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

	dbName, schemaName, tableName := splitMSSQLTableIdent(table)
	if schemaName != "" && tableName != "" && leaf != "" {
		sysPrefix := mssqlSystemPrefix(dbName)
		qHasIndex := fmt.Sprintf(`
			SELECT CASE WHEN EXISTS (
				SELECT 1
				FROM %sindexes i
				JOIN %sindex_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
				JOIN %scolumns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
				JOIN %stables t ON t.object_id = i.object_id
				JOIN %sschemas s ON s.schema_id = t.schema_id
				WHERE s.name = @p1 AND t.name = @p2 AND c.name = @p3 AND i.is_disabled = 0
			) THEN 1 ELSE 0 END;`, sysPrefix, sysPrefix, sysPrefix, sysPrefix, sysPrefix)
		var idx int
		if err := m.db.QueryRowContext(vctx, qHasIndex, schemaName, tableName, leaf).Scan(&idx); err == nil {
			out.IndexedKnown = true
			out.Indexed = idx == 1
		}
	}
	return out, nil
}

func (m *MSSQL) discoverBounds(ctx context.Context, quotedTable, quotedCol string) (any, any, error) {
	qBounds := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s WITH (NOLOCK);", quotedCol, quotedCol, quotedTable)
	var minv, maxv any
	if err := m.db.QueryRowContext(ctx, qBounds).Scan(&minv, &maxv); err != nil {
		return nil, nil, err
	}
	return minv, maxv, nil
}

func mssqlSystemPrefix(dbName string) string {
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return "sys."
	}
	dbq, err := quoteMSSQLMultipartIdent(dbName)
	if err != nil || dbq == "" {
		return "sys."
	}
	return dbq + ".sys."
}

func splitMSSQLTableIdent(s string) (dbName, schemaName, tableName string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", "", ""
	}
	parts := strings.Split(s, ".")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, "[")
		p = strings.TrimSuffix(p, "]")
		if p == "" {
			continue
		}
		clean = append(clean, p)
	}
	if len(clean) == 3 {
		return clean[0], clean[1], clean[2]
	}
	if len(clean) == 2 {
		return "", clean[0], clean[1]
	}
	if len(clean) == 1 {
		return "", "dbo", clean[0]
	}
	return "", "", ""
}

func mssqlOrderedRangeReadsEnabled() bool {
	mssqlOrderedReadsOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envMSSQLOrderedReads))
		if raw == "" {
			mssqlOrderedReads = true
			return
		}
		if parsed, ok := envutil.ParseBoolEnv(raw); ok {
			mssqlOrderedReads = parsed
			return
		}
		mssqlOrderedReads = true
	})
	return mssqlOrderedReads
}

func mssqlDSNIsLocal(dsn string) bool {
	dsn = strings.TrimSpace(dsn)
	if strings.HasPrefix(strings.ToLower(dsn), "sqlserver://") {
		if u, err := url.Parse(dsn); err == nil {
			h := strings.ToLower(strings.TrimSpace(u.Hostname()))
			return h == "localhost" || h == "127.0.0.1" || h == "::1"
		}
	}
	if host, ok := mssqlHostFromKVDSN(dsn); ok {
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}
	return false
}

func mssqlHostFromKVDSN(dsn string) (string, bool) {
	parts := strings.Split(dsn, ";")
	for _, part := range parts {
		k, v, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(k))
		switch key {
		case "server", "addr", "address", "network address", "data source":
		default:
			continue
		}
		host := strings.TrimSpace(v)
		host = strings.TrimPrefix(strings.ToLower(host), "tcp:")
		host = strings.TrimPrefix(host, "np:")
		host = strings.TrimPrefix(host, "lpc:")
		if i := strings.Index(host, ","); i >= 0 {
			host = host[:i]
		}
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		host = strings.TrimSpace(host)
		if host != "" {
			return host, true
		}
	}
	return "", false
}

// buildMSSQLSelectClause applies only projections that SQL Server guarantees
// preserve a fallback value. XML is converted to unbounded NVARCHAR(MAX), not
// VARCHAR(8000), then aliased back to its original column name.
func buildMSSQLSelectClause(cols []string, typeHints ...map[string]string) string {
	var hints map[string]string
	if len(typeHints) > 0 {
		hints = typeHints[0]
	}
	return buildMSSQLProjectedSelectClause(cols, hints, nil)
}

func buildMSSQLProjectedSelectClause(cols []string, columnTypes map[string]string, projections []FallbackProjection) string {
	if len(cols) == 0 {
		return "*"
	}
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c != "" {
			if q, err := quoteMSSQLMultipartIdent(c); err == nil {
				if p, ok := fallbackProjectionForName(projections, c); ok {
					if expr, exact := FallbackProjectionSQL("mssql", q, p); exact {
						quoted = append(quoted, fmt.Sprintf("%s AS %s", expr, q))
						continue
					}
				}
				if strings.EqualFold(strings.TrimSpace(columnTypes[c]), "XML") {
					quoted = append(quoted, fmt.Sprintf("CONVERT(NVARCHAR(MAX), %s) AS %s", q, q))
				} else {
					quoted = append(quoted, q)
				}
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
