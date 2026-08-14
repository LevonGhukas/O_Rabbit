package connectors

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouse connector using the clickhouse-go database/sql driver.
type ClickHouse struct {
	db            *sql.DB
	sourceIsLocal bool
}

const (
	clickHousePingTimeout     = 10 * time.Second
	clickHouseStatsTimeout    = 2 * time.Minute
	clickHouseValidateTimeout = 20 * time.Second
)

var clickHouseIdentPartRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func OpenClickHouse(ctx context.Context, dsn string) (*ClickHouse, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, clickHousePingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &ClickHouse{db: db, sourceIsLocal: clickhouseDSNIsLocal(dsn)}, nil
}

func (c *ClickHouse) Close() error { return c.db.Close() }

func (c *ClickHouse) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, clickHouseValidateTimeout)
	defer cancel()
	return describeQuery(qctx, c.db, "clickhouse", query)
}

func (c *ClickHouse) DescribeTable(
	ctx context.Context,
	table string,
) ([]string, []*sql.ColumnType, error) {
	qt, err := quoteClickHouseMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}

	qctx, cancel := context.WithTimeout(
		ctx,
		clickHouseValidateTimeout,
	)
	defer cancel()

	rows, err := c.db.QueryContext(
		qctx,
		fmt.Sprintf("SELECT * FROM %s LIMIT 1;", qt),
	)
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

func (c *ClickHouse) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, c.db, "clickhouse", q)
	}

	qt, err := quoteClickHouseMultipartIdent(q.Table)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	qc, err := quoteClickHouseMultipartIdent(q.CursorColumn)
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

	query := fmt.Sprintf("SELECT * FROM %s", qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY %s ASC;", qc)

	rows, err := c.db.QueryContext(ctx, query, args...)
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
	for i, col := range cols {
		if cursorColumnMatches(col, q.CursorColumn) {
			cursorIdx = i
			break
		}
	}
	return rows, cols, ct, cursorIdx, nil
}

func (c *ClickHouse) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, clickHouseStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, c.db, "clickhouse", query, cursorColumn, domain, c.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (c *ClickHouse) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quoteClickHouseMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quoteClickHouseMultipartIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, clickHouseStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: c.sourceIsLocal}

	qBounds := fmt.Sprintf("SELECT min(%s), max(%s) FROM %s;", qc, qc, qt)
	var minv, maxv any
	if err := c.db.QueryRowContext(qctx, qBounds).Scan(&minv, &maxv); err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	dbName, tableName := splitClickHouseTableIdent(table)
	if dbName == "" {
		dbName, _ = c.currentDatabase(qctx)
	}
	if dbName != "" && tableName != "" {
		var rowsV, bytesV sql.NullInt64
		err := c.db.QueryRowContext(qctx, `
			SELECT
				COALESCE(sum(rows), 0),
				COALESCE(sum(data_compressed_bytes), 0)
			FROM system.parts
			WHERE active = 1 AND database = ? AND table = ?;`,
			dbName, tableName,
		).Scan(&rowsV, &bytesV)
		if err == nil {
			if rowsV.Valid {
				out.RowCount = rowsV.Int64
			}
			if bytesV.Valid {
				out.TableBytes = bytesV.Int64
			}
		}
	}
	if out.RowCount == 0 {
		qCount := fmt.Sprintf("SELECT count() FROM %s;", qt)
		_ = c.db.QueryRowContext(qctx, qCount).Scan(&out.RowCount)
	}

	return out, nil
}

func (c *ClickHouse) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, clickHouseValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, c.db, "clickhouse", query, cursorColumn)
}
func (c *ClickHouse) ValidateCursorColumn(
	ctx context.Context,
	table, cursorColumn string,
) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, clickHouseValidateTimeout)
	defer cancel()

	dbName, tableName := splitClickHouseTableIdent(table)
	if tableName == "" {
		return out, fmt.Errorf("invalid ClickHouse table name %q", table)
	}

	if dbName == "" {
		var err error
		dbName, err = c.currentDatabase(vctx)
		if err != nil {
			return out, fmt.Errorf(
				"resolve current ClickHouse database: %w",
				err,
			)
		}
	}

	leaf := identLeaf(cursorColumn)
	if leaf == "" {
		return out, fmt.Errorf("cursor column is required")
	}

	var resolvedName string
	var dataType string

	err := c.db.QueryRowContext(
		vctx,
		`
			SELECT name, type
			FROM system.columns
			WHERE database = ?
			  AND table = ?
			  AND lower(name) = lower(?)
			LIMIT 1;
		`,
		dbName,
		tableName,
		leaf,
	).Scan(&resolvedName, &dataType)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return out, nil
		}
		return out, fmt.Errorf(
			"look up cursor column %q in ClickHouse table %q: %w",
			cursorColumn,
			table,
			err,
		)
	}

	out.Found = true
	out.ResolvedName = resolvedName
	out.DataType = strings.TrimSpace(dataType)

	class := ClassifySQLCursorType(out.DataType)
	out.Domain = class.Domain
	out.Orderable = class.Orderable
	out.RangeCapable = class.RangeCapable

	out.NullableKnown = true
	out.Nullable = strings.HasPrefix(
		strings.ToUpper(out.DataType),
		"NULLABLE(",
	)

	var sortingKey sql.NullString
	err = c.db.QueryRowContext(
		vctx,
		`
			SELECT sorting_key
			FROM system.tables
			WHERE database = ?
			  AND name = ?
			LIMIT 1;
		`,
		dbName,
		tableName,
	).Scan(&sortingKey)

	if err == nil && sortingKey.Valid {
		out.IndexedKnown = true
		out.Indexed = clickhouseSortingKeyMentionsColumn(
			sortingKey.String,
			leaf,
		)
	}

	return out, nil
}

func (c *ClickHouse) currentDatabase(ctx context.Context) (string, error) {
	var dbName string
	if err := c.db.QueryRowContext(ctx, "SELECT currentDatabase();").Scan(&dbName); err != nil {
		return "", err
	}
	return strings.TrimSpace(dbName), nil
}

func quoteClickHouseMultipartIdent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	parts := strings.Split(s, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "`\"")
		if !clickHouseIdentPartRe.MatchString(p) {
			return "", fmt.Errorf("unsafe identifier %q", s)
		}
		out = append(out, "`"+p+"`")
	}
	return strings.Join(out, "."), nil
}

func splitClickHouseTableIdent(s string) (dbName, tableName string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ".")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "`\"")
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

func clickhouseSortingKeyMentionsColumn(sortingKey, column string) bool {
	column = strings.ToLower(strings.TrimSpace(column))
	if column == "" {
		return false
	}
	for _, tok := range strings.FieldsFunc(strings.TrimSpace(sortingKey), func(r rune) bool {
		return !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_')
	}) {
		if strings.EqualFold(tok, column) {
			return true
		}
	}
	return false
}

func clickhouseDSNIsLocal(dsn string) bool {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return false
	}
	if !strings.Contains(raw, "://") {
		raw = "clickhouse://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	hostList := strings.TrimSpace(u.Host)
	if hostList == "" {
		return false
	}
	hosts := strings.Split(hostList, ",")
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if !clickhouseHostIsLocal(host) {
			return false
		}
	}
	return true
}

func clickhouseHostIsLocal(host string) bool {
	u, err := url.Parse("tcp://" + host)
	if err != nil {
		return false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}
