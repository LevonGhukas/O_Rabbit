package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/trinodb/trino-go-client/trino"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

type Trino struct {
	db            *sql.DB
	sourceIsLocal bool
}

const (
	trinoPingTimeout     = 10 * time.Second
	trinoStatsTimeout    = 2 * time.Minute
	trinoValidateTimeout = 20 * time.Second
	envTrinoOrderedReads = "ORABBIT_TRINO_ORDERED_RANGE_READS"
)

var (
	trinoOrderedReadsOnce sync.Once
	trinoOrderedReads     = true
)

func OpenTrino(ctx context.Context, dsn string) (*Trino, error) {
	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, trinoPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Trino{db: db, sourceIsLocal: trinoDSNIsLocal(dsn)}, nil
}

func (t *Trino) Close() error { return t.db.Close() }

func (t *Trino) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, trinoValidateTimeout)
	defer cancel()
	return describeQuery(qctx, t.db, "trino", query)
}

func (t *Trino) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	qt, err := quoteTrinoMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := t.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0", qt))
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

func quoteTrinoMultipartIdent(s string) (string, error) {
	return trinoIdentifierRenderer.legacyQualified(s)
}

func (t *Trino) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, t.db, "trino", q)
	}

	qt, err := quoteTrinoMultipartIdent(q.Table)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	qc, err := quoteTrinoMultipartIdent(q.CursorColumn)
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

	selectClause, err := renderSelectColumns(trinoIdentifierRenderer, q.SelectColumns)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	query := fmt.Sprintf("SELECT %s FROM %s", selectClause, qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if trinoOrderedRangeReadsEnabled() {
		query += fmt.Sprintf(" ORDER BY %s ASC", qc)
	}

	rows, err := t.db.QueryContext(ctx, query, args...)
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

func (t *Trino) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, trinoStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, t.db, "trino", query, cursorColumn, domain, t.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (t *Trino) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quoteTrinoMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quoteTrinoMultipartIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, trinoStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: t.sourceIsLocal}

	qBounds := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, qt)
	var minv, maxv any
	if err := t.db.QueryRowContext(qctx, qBounds).Scan(&minv, &maxv); err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	if out.RowCount == 0 {
		qCount := fmt.Sprintf("SELECT count(*) FROM %s", qt)
		_ = t.db.QueryRowContext(qctx, qCount).Scan(&out.RowCount)
	}

	return out, nil
}

func (t *Trino) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, trinoValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, t.db, "trino", query, cursorColumn)
}

func (t *Trino) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, trinoValidateTimeout)
	defer cancel()

	cols, ct, err := t.DescribeTable(vctx, table)
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

	return out, nil
}

func trinoOrderedRangeReadsEnabled() bool {
	trinoOrderedReadsOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envTrinoOrderedReads))
		if raw == "" {
			trinoOrderedReads = true
			return
		}
		if parsed, ok := envutil.ParseBoolEnv(raw); ok {
			trinoOrderedReads = parsed
			return
		}
		trinoOrderedReads = true
	})
	return trinoOrderedReads
}

func trinoDSNIsLocal(dsn string) bool {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return false
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}
