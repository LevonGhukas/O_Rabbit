package connectors

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// Cassandra connector using the gocql driver.
//
// Cassandra does not support global ORDER BY on arbitrary columns, so the
// ordered-cursor domain is always token(partition_key), which maps to
// CursorDomainInt64 (range: [math.MinInt64, math.MaxInt64]).
//
// The id_column in the job spec must be the partition key column name.  The
// connector wraps it as token(col) internally.
//
// DSN format:
//
//	cassandra://[user:pass@]host1,host2,.../keyspace[?consistency=quorum&timeout=30s&connect_timeout=5s]
type Cassandra struct {
	session       *gocql.Session
	keyspace      string
	sourceIsLocal bool
}

const (
	cassandraPingTimeout     = 10 * time.Second
	cassandraStatsTimeout    = 2 * time.Minute
	cassandraValidateTimeout = 20 * time.Second
)

// cassandraDSN holds the parsed fields from a cassandra:// DSN.
type cassandraDSN struct {
	hosts          []string
	keyspace       string
	username       string
	password       string
	consistency    gocql.Consistency
	timeout        time.Duration
	connectTimeout time.Duration
}

// parseCassandraDSN parses a cassandra:// DSN into its components.
func parseCassandraDSN(dsn string) (cassandraDSN, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return cassandraDSN{}, fmt.Errorf("empty cassandra DSN")
	}

	if !strings.HasPrefix(dsn, "cassandra://") {
		return cassandraDSN{}, fmt.Errorf("cassandra DSN must start with cassandra:// (got %q)", dsn)
	}

	// Since url.Parse complains about multiple hosts with ports, we manually split
	// the DSN into the scheme+auth part, the hosts part, and the path+query part.
	withoutScheme := strings.TrimPrefix(dsn, "cassandra://")

	authPart := ""
	hostsPathQuery := withoutScheme
	atIdx := strings.LastIndex(withoutScheme, "@")
	if atIdx >= 0 {
		authPart = withoutScheme[:atIdx]
		hostsPathQuery = withoutScheme[atIdx+1:]
	}

	slashIdx := strings.Index(hostsPathQuery, "/")
	if slashIdx < 0 {
		return cassandraDSN{}, fmt.Errorf("cassandra DSN: keyspace is required in the path (e.g. cassandra://host/keyspace)")
	}
	rawHosts := hostsPathQuery[:slashIdx]
	pathQuery := hostsPathQuery[slashIdx:]

	// Now we can use url.Parse safely by injecting a dummy host.
	dummyDSN := "cassandra://"
	if authPart != "" {
		dummyDSN += authPart + "@"
	}
	dummyDSN += "dummyhost" + pathQuery

	u, err := url.Parse(dummyDSN)
	if err != nil {
		return cassandraDSN{}, fmt.Errorf("parse cassandra DSN: %w", err)
	}

	hosts := make([]string, 0)
	for _, h := range strings.Split(rawHosts, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			hosts = append(hosts, h)
		}
	}
	if len(hosts) == 0 {
		return cassandraDSN{}, fmt.Errorf("cassandra DSN: no hosts specified")
	}

	keyspace := strings.TrimPrefix(u.Path, "/")
	if keyspace == "" {
		return cassandraDSN{}, fmt.Errorf("cassandra DSN: keyspace is required in the path (e.g. cassandra://host/keyspace)")
	}

	out := cassandraDSN{
		hosts:          hosts,
		keyspace:       keyspace,
		timeout:        30 * time.Second,
		connectTimeout: 5 * time.Second,
		consistency:    gocql.LocalQuorum,
	}

	if u.User != nil {
		out.username = u.User.Username()
		out.password, _ = u.User.Password()
	}

	q := u.Query()
	if c := q.Get("consistency"); c != "" {
		cons, err := parseCassandraConsistency(c)
		if err != nil {
			return cassandraDSN{}, fmt.Errorf("cassandra DSN consistency: %w", err)
		}
		out.consistency = cons
	}
	if t := q.Get("timeout"); t != "" {
		d, err := time.ParseDuration(t)
		if err != nil {
			return cassandraDSN{}, fmt.Errorf("cassandra DSN timeout: %w", err)
		}
		out.timeout = d
	}
	if t := q.Get("connect_timeout"); t != "" {
		d, err := time.ParseDuration(t)
		if err != nil {
			return cassandraDSN{}, fmt.Errorf("cassandra DSN connect_timeout: %w", err)
		}
		out.connectTimeout = d
	}

	return out, nil
}

func parseCassandraConsistency(s string) (gocql.Consistency, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "any":
		return gocql.Any, nil
	case "one":
		return gocql.One, nil
	case "two":
		return gocql.Two, nil
	case "three":
		return gocql.Three, nil
	case "quorum":
		return gocql.Quorum, nil
	case "all":
		return gocql.All, nil
	case "localquorum", "local_quorum":
		return gocql.LocalQuorum, nil
	case "eachquorum", "each_quorum":
		return gocql.EachQuorum, nil
	case "localOne", "local_one":
		return gocql.LocalOne, nil
	default:
		return 0, fmt.Errorf("unknown consistency level %q", s)
	}
}

// OpenCassandra opens a Cassandra connection from a cassandra:// DSN.
func OpenCassandra(ctx context.Context, dsn string) (*Cassandra, error) {
	parsed, err := parseCassandraDSN(dsn)
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(parsed.hosts...)
	cluster.Keyspace = parsed.keyspace
	cluster.Consistency = parsed.consistency
	cluster.Timeout = parsed.timeout
	cluster.ConnectTimeout = parsed.connectTimeout

	if parsed.username != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: parsed.username,
			Password: parsed.password,
		}
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("cassandra connect: %w", err)
	}

	// Lightweight connectivity check: query the local system table.
	pingCtx, cancel := context.WithTimeout(ctx, cassandraPingTimeout)
	defer cancel()
	if err := session.Query(
		"SELECT key FROM system.local WHERE key = 'local'",
	).WithContext(pingCtx).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("cassandra ping: %w", err)
	}

	return &Cassandra{
		session:       session,
		keyspace:      parsed.keyspace,
		sourceIsLocal: cassandraDSNIsLocal(parsed.hosts),
	}, nil
}

func (c *Cassandra) Close() error {
	c.session.Close()
	return nil
}

// DescribeTable queries system_schema.columns for the table and returns column
// names and synthesized *sql.ColumnType values.
//
// Since gocql does not use database/sql, we use a zero-row gocql query to
// obtain the real column type info from the driver, and then construct a
// thin sql.Rows bridge from which we extract the *sql.ColumnType objects.
func (c *Cassandra) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	keyspace, tableName, err := splitCassandraTableIdent(c.keyspace, table)
	if err != nil {
		return nil, nil, err
	}

	type colMeta struct {
		name     string
		cqlType  string
		nullable bool
	}

	// Query system_schema for column metadata.
	iter := c.session.Query(
		`SELECT column_name, type, kind FROM system_schema.columns
		 WHERE keyspace_name = ? AND table_name = ?`,
		keyspace, tableName,
	).WithContext(ctx).Iter()

	var colName, colType, colKind string
	cols := make([]colMeta, 0, 16)
	for iter.Scan(&colName, &colType, &colKind) {
		cols = append(cols, colMeta{
			name:     colName,
			cqlType:  colType,
			nullable: colKind == "regular" || colKind == "static",
		})
	}
	if err := iter.Close(); err != nil {
		return nil, nil, fmt.Errorf("cassandra describe %s.%s: %w", keyspace, tableName, err)
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("cassandra describe %s.%s: table not found or no columns", keyspace, tableName)
	}

	names := make([]string, len(cols))
	cts := make([]*sql.ColumnType, len(cols))
	for i, col := range cols {
		names[i] = col.name
		ct, err := cassandraColumnType(col.name, col.cqlType, col.nullable)
		if err != nil {
			return nil, nil, fmt.Errorf("cassandra describe column %s: %w", col.name, err)
		}
		cts[i] = ct
	}
	return names, cts, nil
}

// QueryCursor executes a token-range scan.
//
// The CursorColumn must be the partition key column.  Lower/upper bounds are
// applied as token(pk) comparisons.  The result is wrapped in a
// cassandraRows adapter that satisfies the *sql.Rows contract expected by
// the worker's Parquet writer.
func (c *Cassandra) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if NormalizeCursorDomain(string(q.CursorDomain)) == CursorDomainUnknown {
		return nil, nil, nil, -1, fmt.Errorf("cursor domain is required")
	}

	var cql string
	var args []any
	var cols []string
	var cts []*sql.ColumnType
	var err error

	if strings.TrimSpace(q.SourceQuery) != "" {
		if err := validateCassandraCQLQuery(q.SourceQuery); err != nil {
			return nil, nil, nil, -1, err
		}
		cql = q.SourceQuery
		cols, cts, err = c.DescribeQuery(ctx, cql)
		if err != nil {
			return nil, nil, nil, -1, err
		}
	} else {
		keyspace, _, err := splitCassandraTableIdent(c.keyspace, q.Table)
		if err != nil {
			return nil, nil, nil, -1, err
		}

		qtPK, err := quoteCassandraIdent(q.CursorColumn)
		if err != nil {
			return nil, nil, nil, -1, err
		}

		// Build the WHERE clause using token() comparisons.
		clauses := make([]string, 0, 2)
		args = make([]any, 0, 2)

		if strings.TrimSpace(q.LowerBound) != "" {
			lowerArg, err := ParseCursorArgument(CursorDomainInt64, q.LowerBound)
			if err != nil {
				return nil, nil, nil, -1, err
			}
			op := ">="
			if q.LowerExclusive {
				op = ">"
			}
			clauses = append(clauses, fmt.Sprintf("token(%s) %s ?", qtPK, op))
			args = append(args, lowerArg)
		}
		if strings.TrimSpace(q.UpperBound) != "" {
			upperArg, err := ParseCursorArgument(CursorDomainInt64, q.UpperBound)
			if err != nil {
				return nil, nil, nil, -1, err
			}
			op := "<"
			if q.UpperInclusive {
				op = "<="
			}
			clauses = append(clauses, fmt.Sprintf("token(%s) %s ?", qtPK, op))
			args = append(args, upperArg)
		}

		qt, err := quoteCassandraMultipartIdent(q.Table, keyspace)
		if err != nil {
			return nil, nil, nil, -1, err
		}

		cql = fmt.Sprintf("SELECT * FROM %s", qt)
		if len(clauses) > 0 {
			cql += " WHERE " + strings.Join(clauses, " AND ")
		}

		// Describe the table to get column metadata.
		cols, cts, err = c.DescribeTable(ctx, q.Table)
		if err != nil {
			return nil, nil, nil, -1, err
		}
	}

	iter := c.session.Query(cql, args...).WithContext(ctx).Iter()

	// Find cursor column index for the last-value checkpoint.
	cursorIdx := -1
	for i, col := range cols {
		if cursorColumnMatches(col, q.CursorColumn) {
			cursorIdx = i
			break
		}
	}

	// Wrap the gocql iterator in a sql.Rows-compatible bridge.
	rows, err := newCassandraRows(iter, cols)
	if err != nil {
		_ = iter.Close()
		return nil, nil, nil, -1, err
	}
	return rows, cols, cts, cursorIdx, nil
}

// DiscoverCursorStats returns min/max token values and estimated row/byte
// counts for the table.  Token min/max span the full int64 range; a real
// sample is used for the actual data extent.
func (c *Cassandra) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	keyspace, tableName, err := splitCassandraTableIdent(c.keyspace, table)
	if err != nil {
		return CursorStats{}, err
	}

	qtPK, err := quoteCassandraIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}

	qctx, cancel := context.WithTimeout(ctx, cassandraStatsTimeout)
	defer cancel()

	out := CursorStats{
		SourceIsLocal: c.sourceIsLocal,
		// Cassandra tokens span the full murmur3 int64 range.
		MinValue: "-9223372036854775808",
		MaxValue: "9223372036854775807",
	}

	// Estimated row count from system.size_estimates (available since C* 2.1.5).
	var estRows int64
	rowIter := c.session.Query(
		`SELECT mean_partition_size, partitions_count
		 FROM system.size_estimates
		 WHERE keyspace_name = ? AND table_name = ?`,
		keyspace, tableName,
	).WithContext(qctx).Iter()
	var meanSize, partCount int64
	for rowIter.Scan(&meanSize, &partCount) {
		estRows += partCount
		out.TableBytes += meanSize * partCount
	}
	_ = rowIter.Close()
	out.RowCount = estRows

	// Sample the actual min and max token to narrow the scan range.
	qt, err := quoteCassandraMultipartIdent(table, keyspace)
	if err != nil {
		return CursorStats{}, err
	}
	var minTok, maxTok int64
	minQ := fmt.Sprintf("SELECT token(%s) FROM %s LIMIT 1", qtPK, qt)
	if err := c.session.Query(minQ).WithContext(qctx).Scan(&minTok); err == nil {
		out.MinValue = fmt.Sprintf("%d", minTok)
	}
	maxQ := fmt.Sprintf("SELECT token(%s) FROM %s ORDER BY token(%s) DESC LIMIT 1", qtPK, qt, qtPK)
	if err := c.session.Query(maxQ).WithContext(qctx).Scan(&maxTok); err == nil {
		out.MaxValue = fmt.Sprintf("%d", maxTok)
	}

	return out, nil
}

// ValidateCursorColumn checks that cursorColumn is the partition key of the
// table by querying system_schema.columns.
func (c *Cassandra) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, cassandraValidateTimeout)
	defer cancel()

	keyspace, tableName, err := splitCassandraTableIdent(c.keyspace, table)
	if err != nil {
		return out, fmt.Errorf("validate cursor column (%s): %w", table, err)
	}

	leaf := identLeaf(cursorColumn)

	iter := c.session.Query(
		`SELECT column_name, type, kind FROM system_schema.columns
		 WHERE keyspace_name = ? AND table_name = ?`,
		keyspace, tableName,
	).WithContext(vctx).Iter()

	var colName, colType, colKind string
	for iter.Scan(&colName, &colType, &colKind) {
		if !strings.EqualFold(colName, leaf) {
			continue
		}
		out.Found = true
		out.ResolvedName = colName
		out.DataType = strings.ToUpper(colType)
		class := classifyCassandraCursorType(colType)
		if colKind == "partition_key" {
			out.Domain = CursorDomainInt64
			out.Orderable = true
			out.RangeCapable = true
		} else {
			out.Domain = class.Domain
			out.Orderable = class.Orderable
			out.RangeCapable = class.RangeCapable
		}
		out.NullableKnown = true
		out.Nullable = colKind != "partition_key"
		// Partition key columns are implicitly indexed (primary key).
		out.IndexedKnown = true
		out.Indexed = colKind == "partition_key"
		break
	}
	if err := iter.Close(); err != nil {
		return out, fmt.Errorf("cassandra validate cursor column: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Identifier quoting helpers
// ---------------------------------------------------------------------------

func quoteCassandraIdent(s string) (string, error) {
	parts, err := parseQualifiedIdentifier(s, doubleQuoteDialect())
	if err != nil {
		return "", err
	}
	if len(parts) != 1 {
		return "", fmt.Errorf("cassandra identifier must be a single identifier")
	}
	return quoteIdentifierPart(parts[0].name, doubleQuoteDialect())
}

func quoteCassandraMultipartIdent(table, defaultKeyspace string) (string, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "", fmt.Errorf("empty cassandra table identifier")
	}
	parts, err := parseQualifiedIdentifier(table, doubleQuoteDialect())
	if err != nil {
		return "", err
	}
	switch len(parts) {
	case 1:
		tq, err := quoteCassandraIdent(parts[0].name)
		if err != nil {
			return "", err
		}
		kq, err := quoteCassandraIdent(defaultKeyspace)
		if err != nil {
			return "", err
		}
		return kq + "." + tq, nil
	case 2:
		kq, err := quoteCassandraIdent(parts[0].name)
		if err != nil {
			return "", err
		}
		tq, err := quoteCassandraIdent(parts[1].name)
		if err != nil {
			return "", err
		}
		return kq + "." + tq, nil
	default:
		return "", fmt.Errorf("cassandra table identifier has too many parts: %q", table)
	}
}

// splitCassandraTableIdent returns (keyspace, tableName) from a possibly
// qualified "keyspace.table" or bare "table" identifier.
func splitCassandraTableIdent(defaultKeyspace, table string) (string, string, error) {
	table = strings.TrimSpace(table)
	parts, err := parseQualifiedIdentifier(table, doubleQuoteDialect())
	if err != nil {
		return "", "", err
	}
	switch len(parts) {
	case 1:
		return defaultKeyspace, parts[0].name, nil
	case 2:
		return parts[0].name, parts[1].name, nil
	default:
		return "", "", fmt.Errorf("cassandra table identifier has too many parts: %q", table)
	}
}

// cassandraDSNIsLocal checks whether all hosts in the DSN resolve to localhost.
func cassandraDSNIsLocal(hosts []string) bool {
	if len(hosts) == 0 {
		return false
	}
	for _, h := range hosts {
		host := h
		if idx := strings.LastIndex(h, ":"); idx > 0 {
			host = h[:idx]
		}
		host = strings.ToLower(strings.TrimSpace(host))
		if host != "localhost" && host != "127.0.0.1" && host != "::1" {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// CQL type classification
// ---------------------------------------------------------------------------

type cassandraCursorTypeClass struct {
	Domain       CursorDomain
	Orderable    bool
	RangeCapable bool
}

// classifyCassandraCursorType maps CQL type names to cursor domains.
// The token() function always returns int64, so token-based cursors are
// always CursorDomainInt64 regardless of the actual column type.
// This function classifies the underlying column type for informational
// purposes (e.g. ValidateCursorColumn), not the token itself.
func classifyCassandraCursorType(cqlType string) cassandraCursorTypeClass {
	t := strings.ToLower(strings.TrimSpace(cqlType))
	// Strip frozen<>, list<>, etc.
	if idx := strings.Index(t, "<"); idx >= 0 {
		t = t[:idx]
	}
	switch t {
	case "int", "smallint", "tinyint":
		return cassandraCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true}
	case "bigint", "counter", "varint":
		return cassandraCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true}
	case "timestamp":
		return cassandraCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
	case "date":
		return cassandraCursorTypeClass{Domain: CursorDomainDate, Orderable: true, RangeCapable: true}
	case "timeuuid", "uuid":
		return cassandraCursorTypeClass{Domain: CursorDomainUUID, Orderable: true, RangeCapable: false}
	case "text", "varchar", "ascii":
		return cassandraCursorTypeClass{Domain: CursorDomainString, Orderable: true, RangeCapable: false}
	case "decimal", "float", "double":
		return cassandraCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false}
	default:
		return cassandraCursorTypeClass{}
	}
}

// ---------------------------------------------------------------------------
// Synthetic *sql.ColumnType construction
//
// gocql does not use database/sql so we cannot call rows.ColumnTypes().
// We build a lightweight *sql.DB with an in-memory driver that returns a
// zero-row result set carrying exactly the column types we want.
// ---------------------------------------------------------------------------

// cassandraTypeDriver is a minimal database/sql driver that returns a
// predefined set of columns from a single zero-row result set.
type cassandraTypeDriver struct {
	cols []cassandraColDef
}

type cassandraColDef struct {
	name     string
	scanType string // Go reflect type name used to pick the driver.Value type
	nullable bool
}

type cassandraTypeConn struct {
	cols []cassandraColDef
}
type cassandraTypeStmt struct {
	cols []cassandraColDef
}
type cassandraTypeRows struct {
	cols   []cassandraColDef
	closed bool
}

func (d *cassandraTypeDriver) Open(_ string) (driver.Conn, error) {
	return &cassandraTypeConn{cols: d.cols}, nil
}
func (c *cassandraTypeConn) Prepare(query string) (driver.Stmt, error) {
	return &cassandraTypeStmt{cols: c.cols}, nil
}
func (c *cassandraTypeConn) Close() error { return nil }
func (c *cassandraTypeConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("cassandraTypeConn: transactions not supported")
}
func (s *cassandraTypeStmt) Close() error  { return nil }
func (s *cassandraTypeStmt) NumInput() int { return 0 }
func (s *cassandraTypeStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("cassandraTypeStmt: Exec not supported")
}
func (s *cassandraTypeStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &cassandraTypeRows{cols: s.cols}, nil
}
func (r *cassandraTypeRows) Columns() []string {
	out := make([]string, len(r.cols))
	for i, c := range r.cols {
		out[i] = c.name
	}
	return out
}
func (r *cassandraTypeRows) Close() error {
	r.closed = true
	return nil
}
func (r *cassandraTypeRows) Next(_ []driver.Value) error {
	// Zero-row result set: return EOF immediately.
	return io.EOF
}

func (r *cassandraTypeRows) ColumnTypeDatabaseTypeName(index int) string {
	if index >= 0 && index < len(r.cols) {
		return strings.ToUpper(r.cols[index].scanType)
	}
	return ""
}

// cassandraColumnType synthesizes a *sql.ColumnType for a CQL column.
func cassandraColumnType(name, cqlType string, nullable bool) (*sql.ColumnType, error) {
	def := cassandraColDef{
		name:     name,
		scanType: cqlType,
		nullable: nullable,
	}
	driverName := fmt.Sprintf("cassandra-type-synth-%p", &def)
	sql.Register(driverName, &cassandraTypeDriver{cols: []cassandraColDef{def}})

	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, fmt.Errorf("cassandraColumnType: open synthetic db: %w", err)
	}
	defer db.Close()

	rows, err := db.QueryContext(context.Background(), "")
	if err != nil {
		return nil, fmt.Errorf("cassandraColumnType: query synthetic db: %w", err)
	}
	defer rows.Close()

	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("cassandraColumnType: column types: %w", err)
	}
	if len(cts) != 1 {
		return nil, fmt.Errorf("cassandraColumnType: expected 1 column type, got %d", len(cts))
	}
	return cts[0], nil
}

// ---------------------------------------------------------------------------
// cassandraRows: bridge between gocql.Iter and *sql.Rows
//
// The worker's Parquet writer calls rows.Next(), rows.Scan(), etc.
// We implement this by registering another synthetic sql driver that
// streams data from the gocql iterator.
// ---------------------------------------------------------------------------

// cassandraRowsDriver feeds rows from a []map[string]any into sql.Rows.
type cassandraRowsDriver struct {
	cols []string
	data [][]driver.Value
}

type cassandraRowsConn struct{ d *cassandraRowsDriver }
type cassandraRowsStmt struct{ d *cassandraRowsDriver }
type cassandraRowsIter struct {
	d   *cassandraRowsDriver
	idx int
}

func (d *cassandraRowsDriver) Open(_ string) (driver.Conn, error) {
	return &cassandraRowsConn{d: d}, nil
}
func (c *cassandraRowsConn) Prepare(_ string) (driver.Stmt, error) {
	return &cassandraRowsStmt{d: c.d}, nil
}
func (c *cassandraRowsConn) Close() error { return nil }
func (c *cassandraRowsConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("cassandraRowsConn: transactions not supported")
}
func (s *cassandraRowsStmt) Close() error  { return nil }
func (s *cassandraRowsStmt) NumInput() int { return 0 }
func (s *cassandraRowsStmt) Exec(_ []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("cassandraRowsStmt: Exec not supported")
}
func (s *cassandraRowsStmt) Query(_ []driver.Value) (driver.Rows, error) {
	return &cassandraRowsIter{d: s.d, idx: 0}, nil
}
func (r *cassandraRowsIter) Columns() []string { return r.d.cols }
func (r *cassandraRowsIter) Close() error      { return nil }
func (r *cassandraRowsIter) Next(dest []driver.Value) error {
	if r.idx >= len(r.d.data) {
		return io.EOF
	}
	row := r.d.data[r.idx]
	r.idx++
	copy(dest, row)
	return nil
}

// newCassandraRows eagerly fetches all rows from the gocql iterator and wraps
// them in a *sql.Rows.  This is memory-resident but acceptable for the task
// sizes O_Rabbit uses (target_rows_per_task is typically 100k-500k rows with
// bounded column widths).
func newCassandraRows(iter *gocql.Iter, cols []string) (*sql.Rows, error) {
	data := make([][]driver.Value, 0, 1024)
	for {
		row := make(map[string]any)
		if !iter.MapScan(row) {
			break
		}
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			v := row[col]
			vals[i] = cassandraToDriverValue(v)
		}
		data = append(data, vals)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("cassandra rows iterator: %w", err)
	}

	d := &cassandraRowsDriver{cols: cols, data: data}
	driverName := fmt.Sprintf("cassandra-rows-%p", d)
	sql.Register(driverName, d)

	db, err := sql.Open(driverName, "")
	if err != nil {
		return nil, fmt.Errorf("cassandra rows: open synthetic db: %w", err)
	}

	rows, err := db.QueryContext(context.Background(), "")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("cassandra rows: query: %w", err)
	}
	return rows, nil
}

// cassandraToDriverValue converts a gocql scan value to a driver.Value.
func cassandraToDriverValue(v any) driver.Value {
	if v == nil {
		return nil
	}
	switch x := v.(type) {
	case int8:
		return int64(x)
	case int16:
		return int64(x)
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case uint8:
		return int64(x)
	case uint16:
		return int64(x)
	case uint32:
		return int64(x)
	case uint64:
		return int64(x)
	case float32:
		return float64(x)
	case []byte:
		// Return as string so it's compatible with driver.Value.
		return string(x)
	case gocql.UUID:
		return x.String()
	case time.Time:
		return x
	default:
		// For int64, float64, string, bool, time.Time: pass through.
		if dv, ok := v.(driver.Value); ok {
			return dv
		}
		return fmt.Sprintf("%v", v)
	}
}
