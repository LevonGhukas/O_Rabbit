package connectors

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

const queryModeAlias = "orabbit_query"

type queryModeDialect struct {
	engine         string
	aliasKeyword   string
	terminator     string
	quoteIdent     func(string) (string, error)
	placeholder    func(int) string
	orderedReadsOn func() bool
}

func queryModeDialectForEngine(engine string) (queryModeDialect, bool) {
	switch NormalizeSourceEngine(engine) {
	case "postgres":
		return queryModeDialect{
			engine:         "postgres",
			aliasKeyword:   "AS",
			terminator:     ";",
			quoteIdent:     quotePostgresMultipartIdent,
			placeholder:    func(i int) string { return fmt.Sprintf("$%d", i) },
			orderedReadsOn: postgresOrderedRangeReadsEnabled,
		}, true
	case "mysql":
		return queryModeDialect{
			engine:         "mysql",
			aliasKeyword:   "AS",
			terminator:     ";",
			quoteIdent:     quoteMySQLMultipartIdent,
			placeholder:    func(int) string { return "?" },
			orderedReadsOn: mysqlOrderedRangeReadsEnabled,
		}, true
	case "mariadb":
		return queryModeDialect{
			engine:         "mariadb",
			aliasKeyword:   "AS",
			terminator:     ";",
			quoteIdent:     quoteMySQLMultipartIdent,
			placeholder:    func(int) string { return "?" },
			orderedReadsOn: mariadbOrderedRangeReadsEnabled,
		}, true
	case "mssql":
		return queryModeDialect{
			engine:         "mssql",
			aliasKeyword:   "AS",
			terminator:     ";",
			quoteIdent:     quoteMSSQLMultipartIdent,
			placeholder:    func(i int) string { return fmt.Sprintf("@p%d", i) },
			orderedReadsOn: mssqlOrderedRangeReadsEnabled,
		}, true
	case "oracle":
		return queryModeDialect{
			engine:       "oracle",
			aliasKeyword: "",
			terminator:   "",
			quoteIdent:   quoteOracleCursorIdent,
			placeholder:  func(i int) string { return fmt.Sprintf(":%d", i) },
			orderedReadsOn: func() bool {
				return true
			},
		}, true
	case "clickhouse":
		return queryModeDialect{
			engine:       "clickhouse",
			aliasKeyword: "AS",
			terminator:   ";",
			quoteIdent:   quoteClickHouseMultipartIdent,
			placeholder:  func(int) string { return "?" },
			orderedReadsOn: func() bool {
				return true
			},
		}, true
	case "trino":
		return queryModeDialect{
			engine:         "trino",
			aliasKeyword:   "AS",
			terminator:     "",
			quoteIdent:     quoteTrinoMultipartIdent,
			placeholder:    func(int) string { return "?" },
			orderedReadsOn: trinoOrderedRangeReadsEnabled,
		}, true
	case "cassandra":
		// Cassandra CQL passthrough: the user query is a plain CQL SELECT.
		// No SQL subquery wrapping is applied — CQL does not support derived tables.
		// Cursor filtering is applied externally by the Cassandra connector's
		// token()-based scan. orderedReadsOn is false: CQL ORDER BY only works
		// within a single partition.
		return queryModeDialect{
			engine:       "cassandra",
			aliasKeyword: "",
			terminator:   "",
			quoteIdent:   quoteCassandraIdent,
			placeholder:  func(int) string { return "?" },
			orderedReadsOn: func() bool {
				return false
			},
		}, true
	case "mongodb":
		// MongoDB MQL passthrough: the user query is a JSON filter document.
		// No SQL subquery wrapping is applied — MongoDB does not speak SQL.
		// Cursor filtering is applied externally by the MongoDB connector via
		// BuildCursorFilter(). orderedReadsOn is false.
		return queryModeDialect{
			engine:       "mongodb",
			aliasKeyword: "",
			terminator:   "",
			quoteIdent:   func(s string) (string, error) { return s, nil },
			placeholder:  func(int) string { return "?" },
			orderedReadsOn: func() bool {
				return false
			},
		}, true
	default:
		return queryModeDialect{}, false
	}
}

func (d queryModeDialect) sourceExpr(query string) string {
	if sqlEndsInLineComment(query) {
		// Keep the closing parenthesis out of a trailing line comment.
		query += "\n"
	}
	if strings.TrimSpace(d.aliasKeyword) == "" {
		return fmt.Sprintf("(%s) %s", query, queryModeAlias)
	}
	return fmt.Sprintf("(%s) %s %s", query, d.aliasKeyword, queryModeAlias)
}

func (d queryModeDialect) quoteCursorColumn(cursorColumn string) (string, error) {
	leaf := queryResultColumnName(cursorColumn)
	if leaf == "" {
		return "", fmt.Errorf("cursor column is required")
	}
	switch d.engine {
	case "oracle":
		qc, err := quoteOracleQueryResultIdentifier(leaf)
		if err != nil {
			return "", err
		}
		return queryModeAlias + "." + qc, nil
	case "mysql", "mariadb", "clickhouse":
		return fmt.Sprintf("`%s`.`%s`", queryModeAlias, strings.ReplaceAll(leaf, "`", "``")), nil
	case "mssql":
		return fmt.Sprintf("[%s].[%s]", queryModeAlias, strings.ReplaceAll(leaf, "]", "]]")), nil
	case "postgres", "trino":
		return fmt.Sprintf(`"%s"."%s"`, queryModeAlias, strings.ReplaceAll(leaf, `"`, `""`)), nil
	default:
		return d.quoteIdent(queryModeAlias + "." + leaf)
	}
}

// QueryHash returns a short stable hash of the normalized query for state,
// events, and logs. It never includes the full query text.
func QueryHash(query string) string {
	normalized, err := NormalizeReadOnlySQLQuery(query)
	if err != nil {
		normalized = strings.TrimSpace(query)
	}
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])[:16]
}

// queryModeRange resolves the dialect, the wrapped source expression and the
// bound predicate shared by query-mode row and count statements. Query mode
// does not apply where_clause (tracked separately), so neither statement does.
func queryModeRange(engine string, q CursorQuery) (queryModeDialect, rangeSQL, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return d, rangeSQL{}, fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(q.SourceQuery)
	if err != nil {
		return d, rangeSQL{}, err
	}
	qc, err := d.quoteCursorColumn(q.CursorColumn)
	if err != nil {
		return d, rangeSQL{}, err
	}
	rng, err := newRangeSQL(q, d.sourceExpr(sourceQuery), qc, false, d.placeholder)
	return d, rng, err
}

func buildQueryModeCursorSQL(engine string, q CursorQuery) (string, []any, error) {
	d, rng, err := queryModeRange(engine, q)
	if err != nil {
		return "", nil, err
	}
	query := "SELECT * FROM " + rng.Table + rng.whereSQL()
	if d.orderedReadsOn != nil && d.orderedReadsOn() {
		query += fmt.Sprintf(" ORDER BY %s ASC", rng.Cursor)
	}
	query += d.terminator
	return query, rng.Args, nil
}

// buildQueryModeCountSQL counts the rows buildQueryModeCursorSQL selects.
func buildQueryModeCountSQL(engine string, q CursorQuery) (string, []any, error) {
	d, rng, err := queryModeRange(engine, q)
	if err != nil {
		return "", nil, err
	}
	expr := countExpr
	switch d.engine {
	case "mssql":
		expr = mssqlCountExpr
	case "clickhouse":
		expr = clickHouseCountExpr
	}
	return countFrom(expr, rng.Table, rng) + d.terminator, rng.Args, nil
}

// queryModeCount runs the count for a query-mode CursorQuery.
func queryModeCount(ctx context.Context, db *sql.DB, engine string, q CursorQuery) (int64, error) {
	query, args, err := buildQueryModeCountSQL(engine, q)
	if err != nil {
		return 0, err
	}
	return queryRangeCount(ctx, db, query, args)
}

func buildQueryModeStatsSQL(engine, query, cursorColumn string) (string, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(query)
	if err != nil {
		return "", err
	}
	qc, err := d.quoteCursorColumn(cursorColumn)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT MIN(%s), MAX(%s), COUNT(*) FROM %s%s", qc, qc, d.sourceExpr(sourceQuery), d.terminator), nil
}

func buildQueryModeDescribeSQL(engine, query string) (string, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(query)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0%s", d.sourceExpr(sourceQuery), d.terminator), nil
}

func queryModeCursor(ctx context.Context, db *sql.DB, engine string, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	query, args, err := buildQueryModeCursorSQL(engine, q)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	rows, err := db.QueryContext(ctx, query, args...)
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
	cursorIdx := queryResultColumnIndex(cols, q.CursorColumn)
	return rows, cols, ct, cursorIdx, nil
}

func describeQuery(ctx context.Context, db *sql.DB, engine, query string) ([]string, []*sql.ColumnType, error) {
	sqlText, err := buildQueryModeDescribeSQL(engine, query)
	if err != nil {
		return nil, nil, err
	}
	rows, err := db.QueryContext(ctx, sqlText)
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

func discoverQueryCursorStats(ctx context.Context, db *sql.DB, engine, query, cursorColumn string, domain CursorDomain, sourceIsLocal bool) (CursorStats, error) {
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}
	sqlText, err := buildQueryModeStatsSQL(engine, query, cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	out := CursorStats{SourceIsLocal: sourceIsLocal}
	var minv, maxv any
	var rowCount sql.NullInt64
	if err := db.QueryRowContext(ctx, sqlText).Scan(&minv, &maxv, &rowCount); err != nil {
		return CursorStats{}, err
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}
	if rowCount.Valid {
		out.RowCount = rowCount.Int64
	}
	return out, nil
}

func validateQueryCursorColumn(ctx context.Context, db *sql.DB, engine, query, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}
	cols, ct, err := describeQuery(ctx, db, engine, query)
	if err != nil {
		return out, err
	}

	columnIdx := queryResultColumnIndex(cols, cursorColumn)
	if columnIdx >= 0 {
		i := columnIdx
		c := cols[i]
		out.Found = true
		out.ResolvedName = c
		if i < len(ct) && ct[i] != nil {
			out.DataType = strings.TrimSpace(ct[i].DatabaseTypeName())
			class := classifyQueryCursorType(engine, out.DataType, ct[i])
			out.Domain = class.Domain
			out.Orderable = class.Orderable
			out.RangeCapable = class.RangeCapable
			if n, ok := ct[i].Nullable(); ok {
				out.NullableKnown = true
				out.Nullable = n
			} else if strings.HasPrefix(strings.ToUpper(out.DataType), "NULLABLE(") {
				out.NullableKnown = true
				out.Nullable = true
			}
		}
	}
	return out, nil
}

// Query-mode cursors address a column in the derived query result, not a
// schema-qualified source column. Preserve dots, spaces, Unicode and case in
// that result name; splitting on '.' corrupts valid quoted Oracle aliases.
func queryResultColumnName(raw string) string {
	name := strings.TrimSpace(raw)
	if len(name) >= 2 {
		switch {
		case strings.HasPrefix(name, `"`) && strings.HasSuffix(name, `"`):
			return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
		case strings.HasPrefix(name, "`") && strings.HasSuffix(name, "`"):
			return strings.ReplaceAll(name[1:len(name)-1], "``", "`")
		case strings.HasPrefix(name, "[") && strings.HasSuffix(name, "]"):
			return strings.ReplaceAll(name[1:len(name)-1], "]]", "]")
		}
	}
	return name
}

func queryResultColumnIndex(columns []string, cursorColumn string) int {
	requested := queryResultColumnName(cursorColumn)
	for i, column := range columns {
		if strings.TrimSpace(column) == requested {
			return i
		}
	}
	for i, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), requested) {
			return i
		}
	}
	// Backwards compatibility for old clients that sent a qualified cursor.
	leaf := identLeaf(cursorColumn)
	for i, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), leaf) {
			return i
		}
	}
	return -1
}

func quoteOracleQueryResultIdentifier(name string) (string, error) {
	name = queryResultColumnName(name)
	if name == "" {
		return "", fmt.Errorf("empty identifier")
	}
	if strings.ContainsRune(name, '\x00') {
		return "", fmt.Errorf("unsafe identifier")
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`, nil
}

func classifyQueryCursorType(engine, dataType string, ct *sql.ColumnType) SQLCursorTypeClass {
	if NormalizeSourceEngine(engine) == "oracle" {
		var (
			precision    int64
			scale        int64
			hasPrecision bool
			hasScale     bool
		)
		if ct != nil {
			if p, s, ok := ct.DecimalSize(); ok {
				precision = p
				scale = s
				hasPrecision = true
				hasScale = true
			}
		}
		class := classifyOracleCursorType(strings.ToUpper(strings.TrimSpace(dataType)), precision, scale, hasPrecision, hasScale)
		return SQLCursorTypeClass{Domain: class.Domain, Orderable: class.Orderable, RangeCapable: class.RangeCapable}
	}
	return ClassifySQLCursorType(dataType)
}

// NormalizeReadOnlySQLQuery checks that raw is one read-only SELECT or WITH
// statement under every SQL dialect that can lex it and returns it without
// surrounding whitespace or a final ';'. Like the where-clause check this is a
// guard against mistakes, not a sandbox: source connections are read-only.
func NormalizeReadOnlySQLQuery(raw string) (string, error) {
	query := strings.TrimSpace(raw)
	if query == "" {
		return "", fmt.Errorf("query must be non-empty")
	}
	query, err := cutReadOnlySQLStatement(query)
	if err != nil {
		return "", err
	}
	// Cutting at ';' can turn text a dialect could not lex into text it can,
	// so the cut statement is checked again and must be stable.
	again, err := cutReadOnlySQLStatement(query)
	if err != nil {
		return "", err
	}
	if again != query {
		return "", fmt.Errorf("query mode rejects SQL that dialects split into statements differently")
	}
	return query, nil
}

func cutReadOnlySQLStatement(query string) (string, error) {
	end, lexed := -1, false
	var lexErr error
	for _, dialect := range sqlDialects {
		res, err := lexSQL(query, dialect)
		if verr := checkReadOnlySQLShape(res, err); verr != nil {
			return "", verr
		}
		if err != nil {
			if lexErr == nil {
				lexErr = fmt.Errorf("query contains an %w", err)
			}
			continue
		}
		if lexed && res.statementEnd != end {
			return "", fmt.Errorf("query mode rejects SQL that dialects split into statements differently")
		}
		end, lexed = res.statementEnd, true
	}
	if !lexed {
		return "", lexErr
	}
	if end >= 0 {
		query = strings.TrimSpace(query[:end])
	}
	return query, nil
}

// checkReadOnlySQLShape rejects what one dialect would run as more than a
// single read-only statement. A dialect that cannot lex the text would fail
// to parse it, so only a statement separator before the error matters there.
func checkReadOnlySQLShape(res sqlLexResult, lexErr error) error {
	switch {
	case res.forbidden != "":
		return fmt.Errorf("query mode rejects %s", res.forbidden)
	case res.statementEnd >= 0 && (res.trailing || lexErr != nil):
		return fmt.Errorf("query mode rejects multiple statements")
	case lexErr != nil && !sqlLexErrorIsSkippable(lexErr):
		return fmt.Errorf("query contains an %w", lexErr)
	case lexErr != nil:
		return nil
	case res.firstWord == "":
		return fmt.Errorf("query must contain a SELECT statement")
	case !strings.EqualFold(res.firstWord, "SELECT") && !strings.EqualFold(res.firstWord, "WITH"):
		return fmt.Errorf("query mode only supports read-only SELECT or WITH SELECT queries")
	case res.unbalanced:
		return fmt.Errorf("query has unbalanced parentheses or brackets")
	case res.keyword != "":
		return fmt.Errorf("query mode rejects unsafe SQL keyword %q", res.keyword)
	}
	return nil
}

// ValidateWhereClause checks a user-supplied row filter before it is placed
// inside "WHERE (<clause>) AND <cursor range>". It must be a single boolean
// expression under every SQL dialect that can lex it: no statement
// separators, no comments (which could remove the cursor range that follows),
// balanced parentheses (so it cannot close the surrounding group), and none of
// the keywords rejected in query mode. Like query mode this is a guard against
// mistakes, not a sandbox: source connections must use a read-only database
// user.
func ValidateWhereClause(clause string) error {
	lexed := false
	var lexErr error
	for _, dialect := range sqlDialects {
		res, err := lexSQL(clause, dialect)
		switch {
		case res.forbidden != "":
			return fmt.Errorf("where_clause rejects %s", res.forbidden)
		case res.statementEnd >= 0:
			return fmt.Errorf("where_clause must be a single expression without ';'")
		case res.comments:
			return fmt.Errorf("where_clause must not contain comments")
		case err != nil && !sqlLexErrorIsSkippable(err):
			return fmt.Errorf("where_clause contains an %w", err)
		case err != nil:
			if lexErr == nil {
				lexErr = fmt.Errorf("where_clause contains an %w", err)
			}
			continue
		case res.unbalanced:
			return fmt.Errorf("where_clause has unbalanced parentheses or brackets")
		case res.keyword != "":
			return fmt.Errorf("where_clause rejects SQL keyword %q", res.keyword)
		}
		lexed = true
	}
	if !lexed {
		return lexErr
	}
	return nil
}

var destructiveSQLKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
	"GRANT", "REVOKE", "MERGE", "CALL", "EXEC", "EXECUTE", "COPY", "LOAD",
	"UNLOAD", "REPLACE", "INTO",
}

// destructiveSQLKeyword compares without allocating: it runs for every word
// of every dialect's reading of a query.
func destructiveSQLKeyword(token string) bool {
	token = strings.TrimSpace(token)
	for _, keyword := range destructiveSQLKeywords {
		if len(token) == len(keyword) && strings.EqualFold(token, keyword) {
			return true
		}
	}
	return false
}

func isSQLSpace(c byte) bool {
	return c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == '\f'
}

func isSQLIdentStart(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
}

func isSQLIdentPart(c byte) bool {
	return isSQLIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}
