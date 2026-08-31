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

func buildQueryModeCursorSQL(engine string, q CursorQuery) (string, []any, error) {
	d, ok := queryModeDialectForEngine(engine)
	if !ok {
		return "", nil, fmt.Errorf("query mode is not supported for %s", NormalizeSourceEngine(engine))
	}
	sourceQuery, err := NormalizeReadOnlySQLQuery(q.SourceQuery)
	if err != nil {
		return "", nil, err
	}
	qc, err := d.quoteCursorColumn(q.CursorColumn)
	if err != nil {
		return "", nil, err
	}
	if NormalizeCursorDomain(string(q.CursorDomain)) == CursorDomainUnknown {
		return "", nil, fmt.Errorf("cursor domain is required")
	}

	clauses := make([]string, 0, 2)
	args := make([]any, 0, 2)
	argPos := 1
	if strings.TrimSpace(q.LowerBound) != "" {
		lowerArg, err := ParseCursorArgument(q.CursorDomain, q.LowerBound)
		if err != nil {
			return "", nil, err
		}
		op := ">="
		if q.LowerExclusive {
			op = ">"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s %s", qc, op, d.placeholder(argPos)))
		args = append(args, lowerArg)
		argPos++
	}
	if strings.TrimSpace(q.UpperBound) != "" {
		upperArg, err := ParseCursorArgument(q.CursorDomain, q.UpperBound)
		if err != nil {
			return "", nil, err
		}
		op := "<"
		if q.UpperInclusive {
			op = "<="
		}
		clauses = append(clauses, fmt.Sprintf("%s %s %s", qc, op, d.placeholder(argPos)))
		args = append(args, upperArg)
	}

	query := "SELECT * FROM " + d.sourceExpr(sourceQuery)
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	if d.orderedReadsOn != nil && d.orderedReadsOn() {
		query += fmt.Sprintf(" ORDER BY %s ASC", qc)
	}
	query += d.terminator
	return query, args, nil
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
	cursorIdx := -1
	cursorIdx = queryResultColumnIndex(cols, q.CursorColumn)
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

func NormalizeReadOnlySQLQuery(raw string) (string, error) {
	query := strings.TrimSpace(raw)
	if query == "" {
		return "", fmt.Errorf("query must be non-empty")
	}

	tokens, finalSemi, err := scanSQLQuery(query)
	if err != nil {
		return "", err
	}
	if len(tokens) == 0 {
		return "", fmt.Errorf("query must contain a SELECT statement")
	}
	first := strings.ToUpper(tokens[0])
	if first != "SELECT" && first != "WITH" {
		return "", fmt.Errorf("query mode only supports read-only SELECT or WITH SELECT queries")
	}
	for _, tok := range tokens {
		if destructiveSQLKeyword(tok) {
			return "", fmt.Errorf("query mode rejects unsafe SQL keyword %q", tok)
		}
	}
	if finalSemi >= 0 {
		query = strings.TrimSpace(query[:finalSemi])
	}
	return query, nil
}

func scanSQLQuery(s string) ([]string, int, error) {
	tokens := make([]string, 0, 16)
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case isSQLSpace(c):
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			i += 2
			for i < len(s) && s[i] != '\n' && s[i] != '\r' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			next, ok := skipSQLBlockComment(s, i+2)
			if !ok {
				return nil, -1, fmt.Errorf("query contains an unterminated block comment")
			}
			i = next
		case c == '\'':
			next, ok := skipSQLQuoted(s, i+1, '\'')
			if !ok {
				return nil, -1, fmt.Errorf("query contains an unterminated string literal")
			}
			i = next
		case c == '"':
			next, ok := skipSQLQuoted(s, i+1, '"')
			if !ok {
				return nil, -1, fmt.Errorf("query contains an unterminated quoted identifier")
			}
			i = next
		case c == '`':
			next, ok := skipSQLQuoted(s, i+1, '`')
			if !ok {
				return nil, -1, fmt.Errorf("query contains an unterminated quoted identifier")
			}
			i = next
		case c == '[':
			next := strings.IndexByte(s[i+1:], ']')
			if next < 0 {
				return nil, -1, fmt.Errorf("query contains an unterminated bracket identifier")
			}
			i += next + 2
		case c == ';':
			if !sqlTrailingOnly(s[i+1:]) {
				return nil, -1, fmt.Errorf("query mode rejects multiple statements")
			}
			return tokens, i, nil
		case isSQLIdentStart(c):
			j := i + 1
			for j < len(s) && isSQLIdentPart(s[j]) {
				j++
			}
			tokens = append(tokens, strings.ToUpper(s[i:j]))
			i = j
		default:
			i++
		}
	}
	return tokens, -1, nil
}

func destructiveSQLKeyword(token string) bool {
	switch strings.ToUpper(strings.TrimSpace(token)) {
	case "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
		"GRANT", "REVOKE", "MERGE", "CALL", "EXEC", "EXECUTE", "COPY", "LOAD",
		"UNLOAD", "REPLACE", "INTO":
		return true
	default:
		return false
	}
}

func sqlTrailingOnly(s string) bool {
	for i := 0; i < len(s); {
		switch {
		case isSQLSpace(s[i]):
			i++
		case s[i] == '-' && i+1 < len(s) && s[i+1] == '-':
			i += 2
			for i < len(s) && s[i] != '\n' && s[i] != '\r' {
				i++
			}
		case s[i] == '/' && i+1 < len(s) && s[i+1] == '*':
			next, ok := skipSQLBlockComment(s, i+2)
			if !ok {
				return false
			}
			i = next
		default:
			return false
		}
	}
	return true
}

func skipSQLBlockComment(s string, i int) (int, bool) {
	for i+1 < len(s) {
		if s[i] == '*' && s[i+1] == '/' {
			return i + 2, true
		}
		i++
	}
	return len(s), false
}

func skipSQLQuoted(s string, i int, quote byte) (int, bool) {
	for i < len(s) {
		switch s[i] {
		case '\\':
			if i+1 < len(s) {
				i += 2
			} else {
				i++
			}
		case quote:
			if i+1 < len(s) && s[i+1] == quote {
				i += 2
				continue
			}
			return i + 1, true
		default:
			i++
		}
	}
	return len(s), false
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
