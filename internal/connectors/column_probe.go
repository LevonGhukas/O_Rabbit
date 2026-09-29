package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/stdlib"
)

// columnProbeTimeout bounds the data scan used to verify user type overrides.
const columnProbeTimeout = 5 * time.Minute

// ColumnProbe asks the source for the facts needed to verify that a requested
// column type/nullability can hold every value without loss.
type ColumnProbe struct {
	Column   string
	Range    bool // MIN and MAX
	Nulls    bool // number of NULL values
	Fraction bool // number of values with more than Scale decimal places
	Scale    int32
}

// ColumnProbeResult carries the observed facts for one ColumnProbe. Min and
// Max are exact decimal values (nil when the column has no non-NULL values).
type ColumnProbeResult struct {
	Column        string
	Min, Max      *big.Rat
	NullCount     int64
	FractionCount int64
}

// QueryColumnProber is implemented by SQL sources that can aggregate over a
// query result to verify column overrides.
type QueryColumnProber interface {
	ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error)
}

// QueryNotNullDescriber is implemented by sources whose drivers do not report
// result-column nullability but whose catalog can. It returns the set of
// result columns that are guaranteed NOT NULL.
type QueryNotNullDescriber interface {
	DescribeQueryNotNull(ctx context.Context, query string) (map[string]bool, error)
}

// TableAsQuery renders a read-everything query for a table so table-mode
// sources can reuse the query-mode probes.
func TableAsQuery(engine, table string) (string, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	qt, err := d.quoteIdent(table)
	if err != nil {
		return "", err
	}
	return "SELECT * FROM " + qt, nil
}

func buildColumnProbeSQL(engine, query string, probes []ColumnProbe) (string, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(query)
	if err != nil {
		return "", err
	}
	exprs := make([]string, 0, len(probes)*4)
	for _, p := range probes {
		qc, err := d.quoteCursorColumn(p.Column)
		if err != nil {
			return "", err
		}
		scaled := qc
		if p.Scale > 0 {
			// An exact integer literal keeps the multiplication in the column's
			// numeric type instead of promoting it to floating point.
			scaled = fmt.Sprintf("(%s * 1%s)", qc, strings.Repeat("0", int(p.Scale)))
		}
		exprs = append(exprs,
			fmt.Sprintf("MIN(%s)", qc),
			fmt.Sprintf("MAX(%s)", qc),
			fmt.Sprintf("COUNT(*) - COUNT(%s)", qc),
			fmt.Sprintf("SUM(CASE WHEN %s <> FLOOR(%s) THEN 1 ELSE 0 END)", scaled, scaled),
		)
	}
	return fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(exprs, ", "), d.sourceExpr(sourceQuery), d.terminator), nil
}

func probeQueryColumns(ctx context.Context, db *sql.DB, engine, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	if len(probes) == 0 {
		return nil, nil
	}
	// MIN/MAX/FLOOR are only meaningful for numeric columns; a combined query
	// keeps it to one scan, so non-numeric probes only ask for NULL counts.
	var numeric, nullsOnly []ColumnProbe
	for _, p := range probes {
		if p.Range || p.Fraction {
			numeric = append(numeric, p)
		} else if p.Nulls {
			nullsOnly = append(nullsOnly, p)
		}
	}
	qctx, cancel := context.WithTimeout(ctx, columnProbeTimeout)
	defer cancel()

	out := make([]ColumnProbeResult, 0, len(probes))
	if len(numeric) > 0 {
		res, err := runColumnProbe(qctx, db, engine, query, numeric, true)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	if len(nullsOnly) > 0 {
		res, err := runColumnProbe(qctx, db, engine, query, nullsOnly, false)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}

func runColumnProbe(ctx context.Context, db *sql.DB, engine, query string, probes []ColumnProbe, numeric bool) ([]ColumnProbeResult, error) {
	var sqlText string
	var err error
	perColumn := 4
	if numeric {
		sqlText, err = buildColumnProbeSQL(engine, query, probes)
	} else {
		perColumn = 1
		sqlText, err = buildNullProbeSQL(engine, query, probes)
	}
	if err != nil {
		return nil, err
	}
	vals := make([]any, len(probes)*perColumn)
	dest := make([]any, len(vals))
	for i := range vals {
		dest[i] = &vals[i]
	}
	if err := db.QueryRowContext(ctx, sqlText).Scan(dest...); err != nil {
		return nil, fmt.Errorf("probe column values: %w", err)
	}
	out := make([]ColumnProbeResult, len(probes))
	for i, p := range probes {
		base := i * perColumn
		r := ColumnProbeResult{Column: p.Column}
		if numeric {
			r.Min = probeRat(vals[base])
			r.Max = probeRat(vals[base+1])
			r.NullCount = probeInt(vals[base+2])
			r.FractionCount = probeInt(vals[base+3])
		} else {
			r.NullCount = probeInt(vals[base])
		}
		out[i] = r
	}
	return out, nil
}

func buildNullProbeSQL(engine, query string, probes []ColumnProbe) (string, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(query)
	if err != nil {
		return "", err
	}
	exprs := make([]string, 0, len(probes))
	for _, p := range probes {
		qc, err := d.quoteCursorColumn(p.Column)
		if err != nil {
			return "", err
		}
		exprs = append(exprs, fmt.Sprintf("COUNT(*) - COUNT(%s)", qc))
	}
	return fmt.Sprintf("SELECT %s FROM %s%s", strings.Join(exprs, ", "), d.sourceExpr(sourceQuery), d.terminator), nil
}

func probeRat(v any) *big.Rat {
	switch x := v.(type) {
	case nil:
		return nil
	case *any:
		if x == nil {
			return nil
		}
		return probeRat(*x)
	case []byte:
		return probeRat(string(x))
	case float32:
		return probeRat(float64(x))
	case float64:
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return nil
		}
		return new(big.Rat).SetFloat64(x)
	}
	r, ok := new(big.Rat).SetString(strings.TrimSpace(fmt.Sprint(v)))
	if !ok {
		return nil
	}
	return r
}

func probeInt(v any) int64 {
	r := probeRat(v)
	if r == nil || !r.IsInt() || !r.Num().IsInt64() {
		return 0
	}
	return r.Num().Int64()
}

func (p *Postgres) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, p.db, "postgres", query, probes)
}

func (m *MySQL) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, m.db, "mysql", query, probes)
}

func (m *MSSQL) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, m.db, "mssql", query, probes)
}

func (o *Oracle) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, o.db, "oracle", query, probes)
}

func (c *ClickHouse) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, c.db, "clickhouse", query, probes)
}

func (t *Trino) ProbeQueryColumns(ctx context.Context, query string, probes []ColumnProbe) ([]ColumnProbeResult, error) {
	return probeQueryColumns(ctx, t.db, "trino", query, probes)
}

// Outer joins and grouping extensions can produce NULLs in columns that are
// NOT NULL in their base table, so catalog nullability is not trusted there.
var postgresNullIntroducingSQL = regexp.MustCompile(`(?i)\b(LEFT|RIGHT|FULL|OUTER|ROLLUP|CUBE|GROUPING)\b`)

// DescribeQueryNotNull resolves result columns that come straight from a
// NOT NULL table column. pgx does not expose nullability through
// database/sql, so this reads the column origin (table OID + attnum) from the
// wire protocol and checks pg_attribute.
func (p *Postgres) DescribeQueryNotNull(ctx context.Context, query string) (map[string]bool, error) {
	if postgresNullIntroducingSQL.MatchString(query) {
		return map[string]bool{}, nil
	}
	describeSQL, err := buildQueryModeDescribeSQL("postgres", query)
	if err != nil {
		return nil, err
	}
	qctx, cancel := context.WithTimeout(ctx, postgresValidateTimeout)
	defer cancel()
	conn, err := p.db.Conn(qctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	type origin struct {
		name   string
		table  uint32
		attnum uint16
	}
	var origins []origin
	err = conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("unexpected postgres driver connection %T", driverConn)
		}
		rows, err := sc.Conn().Query(qctx, describeSQL)
		if err != nil {
			return err
		}
		defer rows.Close()
		for _, fd := range rows.FieldDescriptions() {
			origins = append(origins, origin{name: fd.Name, table: fd.TableOID, attnum: fd.TableAttributeNumber})
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}

	out := map[string]bool{}
	for _, o := range origins {
		if o.table == 0 || o.attnum == 0 {
			continue
		}
		var notNull bool
		if err := conn.QueryRowContext(qctx, `SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid = $1 AND attnum = $2`, o.table, int(o.attnum)).Scan(&notNull); err != nil {
			if err == sql.ErrNoRows {
				continue
			}
			return nil, err
		}
		if notNull {
			out[o.name] = true
		}
	}
	return out, nil
}
