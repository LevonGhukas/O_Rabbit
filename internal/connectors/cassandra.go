package connectors

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	inf "gopkg.in/inf.v0"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
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

var cassandraIdentPartRe = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

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

		// Describe the table to get column metadata.
		cols, cts, err = c.DescribeTable(ctx, q.Table)
		if err != nil {
			return nil, nil, nil, -1, err
		}
		selectList := "*"
		if len(q.SelectColumns) > 0 {
			cols, cts, selectList, err = selectCassandraColumns(cols, cts, q.SelectColumns)
			if err != nil {
				return nil, nil, nil, -1, err
			}
		}

		where, err := cassandraWherePredicate(q.WhereClause)
		if err != nil {
			return nil, nil, nil, -1, err
		}
		if where != "" {
			clauses = append(clauses, where)
		}

		cql = fmt.Sprintf("SELECT %s FROM %s", selectList, qt)
		if len(clauses) > 0 {
			cql += " WHERE " + strings.Join(clauses, " AND ")
		}
		if where != "" {
			// User filters usually target non-key columns; Cassandra evaluates
			// them per token range, which each task already bounds.
			cql += " ALLOW FILTERING"
		}
	}

	// SELECT * on a prepared statement: request result metadata on every
	// execution so a stale/empty cached column list cannot make MapScan fail
	// with "not enough columns to scan into: have 0 want N".
	iter := c.session.Query(cql, args...).WithContext(ctx).NoSkipMetadata().Iter()

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
	s = strings.TrimSpace(s)
	s = strings.Trim(s, `"`)
	if s == "" {
		return "", fmt.Errorf("empty cassandra identifier")
	}
	if !cassandraIdentPartRe.MatchString(s) {
		return "", fmt.Errorf("unsafe cassandra identifier %q", s)
	}
	return `"` + s + `"`, nil
}

func quoteCassandraMultipartIdent(table, defaultKeyspace string) (string, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "", fmt.Errorf("empty cassandra table identifier")
	}
	parts := strings.SplitN(table, ".", 2)
	switch len(parts) {
	case 1:
		tq, err := quoteCassandraIdent(parts[0])
		if err != nil {
			return "", err
		}
		kq, err := quoteCassandraIdent(defaultKeyspace)
		if err != nil {
			return "", err
		}
		return kq + "." + tq, nil
	case 2:
		kq, err := quoteCassandraIdent(parts[0])
		if err != nil {
			return "", err
		}
		tq, err := quoteCassandraIdent(parts[1])
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
	parts := strings.SplitN(table, ".", 2)
	switch len(parts) {
	case 1:
		t := strings.Trim(parts[0], `"`)
		if !cassandraIdentPartRe.MatchString(t) {
			return "", "", fmt.Errorf("unsafe cassandra table name %q", parts[0])
		}
		return defaultKeyspace, t, nil
	case 2:
		k := strings.Trim(parts[0], `"`)
		t := strings.Trim(parts[1], `"`)
		if !cassandraIdentPartRe.MatchString(k) {
			return "", "", fmt.Errorf("unsafe cassandra keyspace name %q", parts[0])
		}
		if !cassandraIdentPartRe.MatchString(t) {
			return "", "", fmt.Errorf("unsafe cassandra table name %q", parts[1])
		}
		return k, t, nil
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
	scanner := newCassandraRowScanner(iter.Columns())
	data := make([][]driver.Value, 0, 1024)
	for {
		row, ok := scanner.scan(iter)
		if !ok {
			break
		}
		vals := make([]driver.Value, len(cols))
		for i, col := range cols {
			v := row[col]
			value, err := cassandraToDriverValue(v)
			if err != nil {
				return nil, fmt.Errorf("cassandra row column %s: %w", col, err)
			}
			vals[i] = value
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

// cassandraRawValue receives the serialized bytes of a CQL type gocql cannot
// map to a Go type (custom types such as Cassandra 5 vector).
type cassandraRawValue struct {
	custom string
	data   []byte
}

func (r *cassandraRawValue) UnmarshalCQL(_ gocql.TypeInfo, data []byte) error {
	if data == nil {
		r.data = nil
		return nil
	}
	r.data = append([]byte(nil), data...)
	return nil
}

// value decodes fixed-width Cassandra 5 vectors (float, double, int, bigint)
// into a list; any other custom type is returned as its raw bytes.
func (r *cassandraRawValue) value() any {
	if r.data == nil {
		return nil
	}
	if elems, ok := decodeCassandraVector(r.custom, r.data); ok {
		return elems
	}
	return r.data
}

var cassandraVectorTypeRe = regexp.MustCompile(`^org\.apache\.cassandra\.db\.marshal\.VectorType\(org\.apache\.cassandra\.db\.marshal\.(\w+)\s*,\s*(\d+)\)$`)

func decodeCassandraVector(custom string, data []byte) ([]any, bool) {
	m := cassandraVectorTypeRe.FindStringSubmatch(strings.TrimSpace(custom))
	if m == nil {
		return nil, false
	}
	dim, err := strconv.Atoi(m[2])
	if err != nil || dim < 0 {
		return nil, false
	}
	var width int
	switch m[1] {
	case "FloatType", "Int32Type":
		width = 4
	case "DoubleType", "LongType":
		width = 8
	default:
		return nil, false
	}
	if len(data) != dim*width {
		return nil, false
	}
	out := make([]any, dim)
	for i := 0; i < dim; i++ {
		chunk := data[i*width : (i+1)*width]
		switch m[1] {
		case "FloatType":
			out[i] = math.Float32frombits(binary.BigEndian.Uint32(chunk))
		case "Int32Type":
			out[i] = int32(binary.BigEndian.Uint32(chunk))
		case "DoubleType":
			out[i] = math.Float64frombits(binary.BigEndian.Uint64(chunk))
		case "LongType":
			out[i] = int64(binary.BigEndian.Uint64(chunk))
		}
	}
	return out, true
}

// cassandraTuple marks a scanned tuple so it is serialized as one JSON value.
type cassandraTuple []any

// cassandraRowScanner scans rows with explicit per-column destinations.
// gocql's MapScan discards RowData errors, so a single unsupported column type
// turns into "not enough columns to scan into: have 0 want N".
type cassandraRowScanner struct {
	names []string
	// tupleLen[i] is 0 for plain columns, otherwise the number of tuple elements
	// occupying consecutive scan slots.
	tupleLen []int
	newDests func() []any
}

func newCassandraRowScanner(columns []gocql.ColumnInfo) *cassandraRowScanner {
	s := &cassandraRowScanner{
		names:    make([]string, len(columns)),
		tupleLen: make([]int, len(columns)),
	}
	var factories []func() any
	for i, col := range columns {
		s.names[i] = col.Name
		if tuple, ok := col.TypeInfo.(gocql.TupleTypeInfo); ok {
			s.tupleLen[i] = len(tuple.Elems)
			for _, elem := range tuple.Elems {
				factories = append(factories, cassandraDestFactory(elem))
			}
			continue
		}
		factories = append(factories, cassandraDestFactory(col.TypeInfo))
	}
	s.newDests = func() []any {
		dests := make([]any, len(factories))
		for i, f := range factories {
			dests[i] = f()
		}
		return dests
	}
	return s
}

func cassandraDestFactory(info gocql.TypeInfo) func() any {
	if _, err := info.NewWithError(); err != nil {
		custom := info.Custom()
		return func() any { return &cassandraRawValue{custom: custom} }
	}
	return func() any { v, _ := info.NewWithError(); return v }
}

func (s *cassandraRowScanner) scan(iter *gocql.Iter) (map[string]any, bool) {
	dests := s.newDests()
	if !iter.Scan(dests...) {
		return nil, false
	}
	row := make(map[string]any, len(s.names))
	pos := 0
	for i, name := range s.names {
		if n := s.tupleLen[i]; n > 0 {
			elems := make([]any, n)
			for j := 0; j < n; j++ {
				elems[j] = cassandraDereference(dests[pos+j])
			}
			row[name] = cassandraTuple(elems)
			pos += n
			continue
		}
		row[name] = cassandraDereference(dests[pos])
		pos++
	}
	return row, true
}

func cassandraDereference(dest any) any {
	if raw, ok := dest.(*cassandraRawValue); ok {
		return raw.value()
	}
	return reflect.Indirect(reflect.ValueOf(dest)).Interface()
}

// cassandraToDriverValue converts a gocql scan value to a driver.Value.
func cassandraToDriverValue(v any) (driver.Value, error) {
	v = normalizeCassandraValue(v)
	if v == nil {
		return nil, nil
	}
	switch x := v.(type) {
	case int8:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case float32:
		return float64(x), nil
	case int64, float64, string, bool:
		return x, nil
	case []byte:
		return x, nil
	case gocql.UUID:
		return x.String(), nil
	case time.Time, time.Duration:
		return x, nil
	case []any:
		// list/set/vector values stay structured for the array converter.
		return x, nil
	default:
		text, err := typesystem.ToLosslessString(v)
		if err != nil {
			return nil, fmt.Errorf("lossless Cassandra driver conversion: %w", err)
		}
		return text, nil
	}
}

// normalizeCassandraValue rewrites gocql-specific Go values into values the
// type system converts natively: decimal/varint become exact strings, duration
// becomes ISO-8601 text, list/set/vector become []any, and map/UDT/tuple values
// become one JSON document.
func normalizeCassandraValue(v any) any {
	return normalizeCassandraNested(v, false)
}

// normalizeCassandraNested keeps containers as native Go structures when they
// are nested inside a JSON document, so they are encoded exactly once.
func normalizeCassandraNested(v any, inJSON bool) any {
	switch x := v.(type) {
	case nil:
		return nil
	case *inf.Dec:
		if x == nil {
			return nil
		}
		return x.String()
	case *big.Int:
		if x == nil {
			return nil
		}
		return x.String()
	case gocql.Duration:
		return cassandraISODuration(x)
	case gocql.UUID:
		return x.String()
	case net.IP:
		return x.String()
	case []byte, string, time.Time, time.Duration:
		return x
	case cassandraTuple:
		out := make([]any, len(x))
		for i, elem := range x {
			out[i] = normalizeCassandraNested(elem, true)
		}
		return cassandraJSONDocument(out, inJSON)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := range out {
			out[i] = normalizeCassandraNested(rv.Index(i).Interface(), inJSON)
		}
		return out
	case reflect.Map:
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			key := normalizeCassandraNested(iter.Key().Interface(), true)
			out[fmt.Sprint(key)] = normalizeCassandraNested(iter.Value().Interface(), true)
		}
		return cassandraJSONDocument(out, inJSON)
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return normalizeCassandraNested(rv.Elem().Interface(), inJSON)
	}
	return v
}

func cassandraJSONDocument(v any, inJSON bool) any {
	if inJSON {
		return v
	}
	encoded, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(encoded)
}

// cassandraISODuration renders a CQL duration as ISO-8601, e.g. P1M2DT3.5S.
func cassandraISODuration(d gocql.Duration) string {
	var b strings.Builder
	b.WriteString("P")
	if d.Months != 0 {
		fmt.Fprintf(&b, "%dM", d.Months)
	}
	if d.Days != 0 {
		fmt.Fprintf(&b, "%dD", d.Days)
	}
	if d.Nanoseconds != 0 {
		secs := strconv.FormatFloat(time.Duration(d.Nanoseconds).Seconds(), 'f', -1, 64)
		fmt.Fprintf(&b, "T%sS", secs)
	}
	if b.Len() == 1 {
		b.WriteString("T0S")
	}
	return b.String()
}

// selectCassandraColumns narrows described table columns to the requested
// selection (in request order) and returns the quoted CQL select list.
func selectCassandraColumns(cols []string, cts []*sql.ColumnType, selected []string) ([]string, []*sql.ColumnType, string, error) {
	outCols := make([]string, 0, len(selected))
	outTypes := make([]*sql.ColumnType, 0, len(selected))
	quoted := make([]string, 0, len(selected))
	seen := make(map[int]bool, len(selected))
	for _, want := range selected {
		want = strings.TrimSpace(want)
		if want == "" {
			continue
		}
		idx := -1
		for i, col := range cols {
			if cursorColumnMatches(col, want) {
				idx = i
				break
			}
		}
		if idx < 0 {
			return nil, nil, "", fmt.Errorf("cassandra select column %q not found in table", want)
		}
		if seen[idx] {
			continue
		}
		seen[idx] = true
		q, err := quoteCassandraIdent(cols[idx])
		if err != nil {
			return nil, nil, "", err
		}
		outCols = append(outCols, cols[idx])
		outTypes = append(outTypes, cts[idx])
		quoted = append(quoted, q)
	}
	if len(outCols) == 0 {
		return cols, cts, "*", nil
	}
	return outCols, outTypes, strings.Join(quoted, ", "), nil
}

var cassandraWhereOrRe = regexp.MustCompile(`(?i)\bOR\b`)

// cassandraWherePredicate validates a job filter for CQL: relations joined by
// AND only. CQL has no OR and no parenthesized boolean groups, so those are
// rejected up front instead of failing later with a driver syntax error.
func cassandraWherePredicate(where string) (string, error) {
	where = strings.TrimSpace(where)
	for strings.HasPrefix(where, "(") && strings.HasSuffix(where, ")") && balancedOuterParens(where) {
		where = strings.TrimSpace(where[1 : len(where)-1])
	}
	if where == "" {
		return "", nil
	}
	if strings.Contains(where, ";") {
		return "", fmt.Errorf("cassandra filter must be a single predicate")
	}
	if cassandraWhereOrRe.MatchString(stripCQLStringLiterals(where)) {
		return "", fmt.Errorf("cassandra filters support AND-joined conditions only (CQL has no OR)")
	}
	return where, nil
}

// balancedOuterParens reports whether the first "(" closes at the last rune.
func balancedOuterParens(s string) bool {
	depth := 0
	inString := false
	for i, r := range s {
		if r == '\'' {
			inString = !inString
		}
		if inString {
			continue
		}
		switch r {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 && i != len(s)-1 {
				return false
			}
		}
	}
	return depth == 0
}

func stripCQLStringLiterals(s string) string {
	var b strings.Builder
	inString := false
	for _, r := range s {
		if r == '\'' {
			inString = !inString
			continue
		}
		if !inString {
			b.WriteRune(r)
		}
	}
	return b.String()
}
