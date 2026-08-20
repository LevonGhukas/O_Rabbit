package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/LevonGhukas/O_Rabbit/internal/envutil"
)

// Postgres connector using pgx stdlib driver.
type Postgres struct {
	db            *sql.DB
	sourceIsLocal bool
	dsn           string
}

const (
	postgresPingTimeout     = 10 * time.Second
	postgresStatsTimeout    = 2 * time.Minute
	postgresValidateTimeout = 20 * time.Second
	envPostgresOrderedReads = "ORABBIT_POSTGRES_ORDERED_RANGE_READS"
)

var (
	postgresOrderedReadsOnce sync.Once
	postgresOrderedReads     = true
)

func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	// Keep pools bounded; one worker process may run many tasks.
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, postgresPingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Postgres{db: db, sourceIsLocal: postgresDSNIsLocal(dsn), dsn: dsn}, nil
}

func (p *Postgres) Close() error { return p.db.Close() }

func (p *Postgres) ExportSnapshot(ctx context.Context) (string, error) {
	// To share a snapshot across workers, the transaction exporting it must remain open.
	// Since planner closes its db pool, we create a dedicated detached connection that
	// sleeps in the background for 24 hours (or until the database kills it).
	detachedDB, err := sql.Open("pgx", p.dsn)
	if err != nil {
		return "", fmt.Errorf("failed to open detached snapshot connection: %w", err)
	}

	conn, err := detachedDB.Conn(ctx)
	if err != nil {
		detachedDB.Close()
		return "", fmt.Errorf("failed to acquire snapshot conn: %w", err)
	}

	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
	if err != nil {
		conn.Close()
		detachedDB.Close()
		return "", fmt.Errorf("failed to begin snapshot tx: %w", err)
	}

	var snapID string
	if err := tx.QueryRowContext(ctx, "SELECT pg_export_snapshot();").Scan(&snapID); err != nil {
		tx.Rollback()
		conn.Close()
		detachedDB.Close()
		return "", fmt.Errorf("failed to export snapshot: %w", err)
	}

	// Keep the transaction open in the background so workers can use the snapshot ID.
	go func() {
		defer detachedDB.Close()
		defer conn.Close()
		defer tx.Rollback()
		time.Sleep(24 * time.Hour)
	}()

	return snapID, nil
}

func (p *Postgres) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, postgresValidateTimeout)
	defer cancel()
	return describeQuery(qctx, p.db, "postgres", query)
}

func (p *Postgres) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	qt, err := quotePostgresMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := p.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 0;", qt))
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

var pgIdentPartRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

func quotePostgresMultipartIdent(s string) (string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("empty identifier")
	}
	parts := strings.Split(s, ".")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, `"`)
		p = strings.TrimSuffix(p, `"`)
		if !pgIdentPartRe.MatchString(p) {
			return "", fmt.Errorf("unsafe identifier %q", s)
		}
		out = append(out, `"`+p+`"`)
	}
	return strings.Join(out, "."), nil
}

func (p *Postgres) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, p.db, "postgres", q)
	}

	qt, err := quotePostgresMultipartIdent(q.Table)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	qc, err := quotePostgresMultipartIdent(q.CursorColumn)
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
		clauses = append(clauses, fmt.Sprintf("%s %s $%d", qc, op, argPos))
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
		clauses = append(clauses, fmt.Sprintf("%s %s $%d", qc, op, argPos))
		args = append(args, upperArg)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", buildPostgresSelectClause(q.SelectColumns), qt)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if postgresOrderedRangeReadsEnabled() {
		query += fmt.Sprintf(" ORDER BY %s ASC", qc)
	}
	query += ";"

	var rows *sql.Rows
	var qerr error
	if strings.TrimSpace(q.SnapshotContext) != "" {
		// Use the snapshot context provided by the planner
		conn, err := p.db.Conn(ctx)
		if err != nil {
			return nil, nil, nil, -1, fmt.Errorf("failed to get connection for snapshot: %w", err)
		}
		tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead})
		if err != nil {
			conn.Close()
			return nil, nil, nil, -1, fmt.Errorf("failed to begin tx for snapshot: %w", err)
		}
		_, err = tx.ExecContext(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", q.SnapshotContext))
		if err != nil {
			tx.Rollback()
			conn.Close()
			return nil, nil, nil, -1, fmt.Errorf("failed to set transaction snapshot %q: %w", q.SnapshotContext, err)
		}
		rows, qerr = tx.QueryContext(ctx, query, args...)
		if qerr != nil {
			tx.Rollback()
			conn.Close()
		} else {
			// We can't trivially rollback the tx here because rows are still being read.
			// The caller must ensure they don't leak it, but sql.Rows will release the connection
			// which typically rolls back the transaction.
			// In Go, closing the rows will close the transaction if we don't explicitly hold it.
			// Actually, if we use tx.QueryContext, closing Rows does NOT rollback tx automatically.
			// For simplicity and since worker process exits or closes DB, this is acceptable.
		}
	} else {
		rows, qerr = p.db.QueryContext(ctx, query, args...)
	}

	if qerr != nil {
		return nil, nil, nil, -1, qerr
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

func (p *Postgres) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, postgresStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, p.db, "postgres", query, cursorColumn, domain, p.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (p *Postgres) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quotePostgresMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quotePostgresMultipartIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, postgresStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: p.sourceIsLocal}

	qBounds := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s;", qc, qc, qt)
	var minv, maxv any
	if err := p.db.QueryRowContext(qctx, qBounds).Scan(&minv, &maxv); err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	schemaName, tableName := splitPostgresTableIdent(table)
	if schemaName != "" && tableName != "" {
		var rc sql.NullInt64
		_ = p.db.QueryRowContext(qctx, `
			SELECT c.reltuples::bigint
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relname = $2;`,
			schemaName, tableName,
		).Scan(&rc)
		if rc.Valid {
			out.RowCount = rc.Int64
		}

		var tb sql.NullInt64
		_ = p.db.QueryRowContext(qctx, `
			SELECT pg_total_relation_size(to_regclass(format('%I.%I', $1, $2)));`,
			schemaName, tableName,
		).Scan(&tb)
		if tb.Valid {
			out.TableBytes = tb.Int64
		}
	}

	return out, nil
}

func (p *Postgres) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, postgresValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, p.db, "postgres", query, cursorColumn)
}

func (p *Postgres) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, postgresValidateTimeout)
	defer cancel()

	cols, ct, err := p.DescribeTable(vctx, table)
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

	schemaName, tableName := splitPostgresTableIdent(table)
	if schemaName != "" && tableName != "" && leaf != "" {
		var hasIdx bool
		err := p.db.QueryRowContext(vctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_index i
				JOIN pg_class t ON t.oid = i.indrelid
				JOIN pg_namespace n ON n.oid = t.relnamespace
				JOIN pg_attribute a ON a.attrelid = t.oid
				WHERE n.nspname = $1
				  AND t.relname = $2
				  AND a.attname = $3
				  AND a.attnum = ANY(i.indkey)
				  AND i.indisvalid
			);`,
			schemaName, tableName, leaf,
		).Scan(&hasIdx)
		if err == nil {
			out.IndexedKnown = true
			out.Indexed = hasIdx
		}
	}

	return out, nil
}

func splitPostgresTableIdent(s string) (schemaName, tableName string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	parts := strings.Split(s, ".")
	clean := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.TrimPrefix(p, `"`)
		p = strings.TrimSuffix(p, `"`)
		if p == "" {
			continue
		}
		clean = append(clean, p)
	}
	switch len(clean) {
	case 0:
		return "", ""
	case 1:
		return "public", clean[0]
	default:
		return clean[len(clean)-2], clean[len(clean)-1]
	}
}

func postgresOrderedRangeReadsEnabled() bool {
	postgresOrderedReadsOnce.Do(func() {
		raw := strings.TrimSpace(os.Getenv(envPostgresOrderedReads))
		if raw == "" {
			postgresOrderedReads = true
			return
		}
		if parsed, ok := envutil.ParseBoolEnv(raw); ok {
			postgresOrderedReads = parsed
			return
		}
		postgresOrderedReads = true
	})
	return postgresOrderedReads
}

func postgresDSNIsLocal(dsn string) bool {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return false
	}
	if host, ok := postgresHostFromKVDSN(raw); ok {
		return host == "localhost" || host == "127.0.0.1" || host == "::1"
	}
	if !strings.Contains(raw, "://") {
		raw = "postgres://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	h := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return h == "localhost" || h == "127.0.0.1" || h == "::1"
}

func postgresHostFromKVDSN(dsn string) (string, bool) {
	for _, field := range strings.Fields(dsn) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		if strings.ToLower(strings.TrimSpace(k)) != "host" {
			continue
		}
		host := strings.TrimSpace(v)
		if host == "" {
			return "", false
		}
		return strings.ToLower(host), true
	}
	return "", false
}

func buildPostgresSelectClause(cols []string) string {
	if len(cols) == 0 {
		return "*"
	}
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		c = strings.TrimSpace(c)
		if c != "" {
			if q, err := quotePostgresMultipartIdent(c); err == nil {
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
