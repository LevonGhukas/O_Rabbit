package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type CursorDomain string

const (
	CursorDomainUnknown   CursorDomain = ""
	CursorDomainInt64     CursorDomain = "int64"
	CursorDomainUInt64    CursorDomain = "uint64"
	CursorDomainDecimal   CursorDomain = "decimal"
	CursorDomainDate      CursorDomain = "date"
	CursorDomainTimestamp CursorDomain = "timestamp"
	CursorDomainString    CursorDomain = "string"
	CursorDomainUUID      CursorDomain = "uuid"
)

type CursorQuery struct {
	Table           string
	SourceQuery     string
	CursorColumn    string
	CursorDomain    CursorDomain
	LowerBound      string
	UpperBound      string
	LowerExclusive  bool
	UpperInclusive  bool
	SnapshotContext string
	WhereClause     string
	SelectColumns   []string
	ColumnTypes     map[string]string
}

// SnapshotExporter allows exporting a consistent read snapshot (e.g. PG snapshot ID, Oracle SCN).
type SnapshotExporter interface {
	ExportSnapshot(ctx context.Context) (string, error)
}

type CursorStats struct {
	MinValue      string
	MaxValue      string
	RowCount      int64
	TableBytes    int64
	SourceIsLocal bool
}

type CursorColumnValidation struct {
	Found         bool
	ResolvedName  string
	DataType      string
	Domain        CursorDomain
	Orderable     bool
	RangeCapable  bool
	Nullable      bool
	NullableKnown bool
	Indexed       bool
	IndexedKnown  bool
}

// TableReader is the database-agnostic contract used by planner/worker for SQL ordered-cursor extraction.
type TableReader interface {
	Close() error
	DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error)
	QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error)
	DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error)
	ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error)
}

// SourceQueryReader is an optional extension for SQL engines that can safely
// wrap a read-only user query as a derived table for ordered-cursor extraction.
type SourceQueryReader interface {
	DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error)
	DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error)
	ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error)
}

// PostgresTypeMetadata is the small, source-owned description needed to plan
// PostgreSQL user-defined types. It deliberately does not model ranges or
// extensions.
type PostgresTypeMetadata struct {
	ReportedType    string
	TypeName        string
	Schema          string
	Kind            string // enum, domain, composite
	BaseType        string
	DomainChain     []string
	DomainNotNull   bool
	EnumLabels      []string
	CompositeFields []string
	// PostGISBinary is set only when a source-specific caller already knows the
	// driver returned exact geometry bytes; Arrow planning never inspects payloads.
	PostGISBinary bool
}

// PostgresTypeMetadataReader is optional so non-PostgreSQL connectors keep
// their existing planning contract.
type PostgresTypeMetadataReader interface {
	PostgresTypeMetadata(ctx context.Context, columnTypes []*sql.ColumnType) ([]PostgresTypeMetadata, error)
}

type sourceEngineSpec struct {
	Canonical             string
	Aliases               []string
	SupportsOrderedCursor bool
	QueryCapabilities     QueryCapabilities
	OpenCursor            func(context.Context, string) (TableReader, error)
	OpenDocument          func(context.Context, string) (DocumentReader, error)
}

var (
	sourceEnginesByCanonical = map[string]sourceEngineSpec{}
	sourceEngineAliases      = map[string]string{}
	sqlTypeTokenRe           = regexp.MustCompile(`[A-Z0-9]+`)
)

func init() {
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "mssql",
		Aliases:               []string{"sqlserver", "ms-sql"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageTSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenMSSQL(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "postgres",
		Aliases:               []string{"postgresql", "pg"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguagePostgresSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenPostgres(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "clickhouse",
		Aliases:               []string{"click-house", "ch"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageClickHouseSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenClickHouse(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "oracle",
		Aliases:               []string{"ora"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageOracleSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenOracle(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical: "flightsql",
		Aliases:   []string{"flight-sql", "flight_sql", "adbc", "adbc_flightsql"},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "mysql",
		Aliases:               []string{},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageMySQLSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenMySQL(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "mariadb",
		Aliases:               []string{"mariadb-server"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageMariaDBSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenMariaDB(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "trino",
		Aliases:               []string{},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageTrinoSQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenTrino(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "mongodb",
		Aliases:               []string{"mongo"},
		SupportsOrderedCursor: true,
		OpenDocument: func(ctx context.Context, dsn string) (DocumentReader, error) {
			return OpenMongoDB(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "cassandra",
		Aliases:               []string{"cql", "cassandra-db"},
		SupportsOrderedCursor: true,
		QueryCapabilities:     supportedSQLQueryCapabilities(QueryLanguageCQL),
		OpenCursor: func(ctx context.Context, dsn string) (TableReader, error) {
			return OpenCassandra(ctx, dsn)
		},
	})
	mustRegisterSourceEngine(sourceEngineSpec{
		Canonical:             "s3",
		Aliases:               []string{"file", "minio"},
		SupportsOrderedCursor: false,
		OpenDocument: func(ctx context.Context, dsn string) (DocumentReader, error) {
			return OpenS3(ctx, dsn)
		},
	})
}

func mustRegisterSourceEngine(spec sourceEngineSpec) {
	canon := strings.ToLower(strings.TrimSpace(spec.Canonical))
	if canon == "" {
		panic("source engine canonical name is empty")
	}
	queryCapabilities := spec.QueryCapabilities
	if queryCapabilities.Supported {
		if len(queryCapabilities.Languages) == 0 {
			panic("supported query engine has no query language: " + canon)
		}
		if !queryCapabilities.SchemaInferenceSupported {
			panic("supported query engine has no query schema inference: " + canon)
		}
	} else if len(queryCapabilities.Languages) > 0 || queryCapabilities.IncrementalSupported || queryCapabilities.SchemaInferenceSupported {
		panic("unsupported query engine advertises query features: " + canon)
	}
	spec.Canonical = canon
	if _, exists := sourceEnginesByCanonical[canon]; exists {
		panic("duplicate source engine registration: " + canon)
	}
	sourceEnginesByCanonical[canon] = spec
	sourceEngineAliases[canon] = canon
	for _, alias := range spec.Aliases {
		a := strings.ToLower(strings.TrimSpace(alias))
		if a == "" {
			continue
		}
		if existing, exists := sourceEngineAliases[a]; exists && existing != canon {
			panic("duplicate source engine alias: " + a)
		}
		sourceEngineAliases[a] = canon
	}
}

// KnownSourceEngines returns canonical engine names known by this build.
func KnownSourceEngines() []string {
	out := make([]string, 0, len(sourceEnginesByCanonical))
	for k := range sourceEnginesByCanonical {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// IsKnownSourceEngine reports whether the given engine is registered.
func IsKnownSourceEngine(engine string) bool {
	_, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	return ok
}

// NormalizeSourceEngine converts aliases into canonical engine names.
func NormalizeSourceEngine(raw string) string {
	engine := strings.ToLower(strings.TrimSpace(raw))
	if engine == "" {
		return ""
	}
	if canon, ok := sourceEngineAliases[engine]; ok {
		return canon
	}
	return engine
}

func SupportsDocumentReader(engine string) bool {
	spec, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	return ok && spec.OpenDocument != nil
}

// SupportsOrderedCursor reports whether the source engine supports ordered-cursor planning/extraction.
func SupportsOrderedCursor(engine string) bool {
	spec, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	return ok && spec.SupportsOrderedCursor
}

// SupportsQueryMode is the backwards-compatible query capability check used by
// the current API, planner, worker, and Iceberg registration paths.
func SupportsQueryMode(engine string) bool {
	return QueryCapabilitiesForEngine(engine).Supported
}

// SupportsIntRange is a backwards-compatible alias for older call sites/tests.
func SupportsIntRange(engine string) bool {
	return SupportsOrderedCursor(engine)
}

// OpenCursorReader opens a SQL ordered-cursor reader for the requested source engine.
func OpenCursorReader(ctx context.Context, engine, dsn string) (TableReader, error) {
	spec, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	if !ok {
		return nil, ClassifyConnectorError(fmt.Errorf("unsupported source engine: %s", engine))
	}
	if !spec.SupportsOrderedCursor || spec.OpenCursor == nil {
		return nil, ClassifyConnectorError(fmt.Errorf("engine %s does not support ordered cursor extraction", spec.Canonical))
	}
	r, err := spec.OpenCursor(ctx, dsn)
	if err != nil {
		return nil, ClassifyConnectorError(err)
	}
	if sqr, ok := r.(SourceQueryReader); ok {
		return &errorWrappedSourceQueryReader{
			errorWrappedTableReader: errorWrappedTableReader{inner: r},
			innerQueryReader:        sqr,
		}, nil
	}
	return &errorWrappedTableReader{inner: r}, nil
}

// OpenIntRangeReader is a backwards-compatible alias for the old int-range reader API.
func OpenDocumentReader(ctx context.Context, engine, dsn string) (DocumentReader, error) {
	spec, ok := sourceEnginesByCanonical[NormalizeSourceEngine(engine)]
	if !ok {
		return nil, ClassifyConnectorError(fmt.Errorf("unsupported source engine: %s", engine))
	}
	if spec.OpenDocument == nil {
		return nil, ClassifyConnectorError(fmt.Errorf("engine %s does not support document extraction", spec.Canonical))
	}
	r, err := spec.OpenDocument(ctx, dsn)
	if err != nil {
		return nil, ClassifyConnectorError(err)
	}
	return &errorWrappedDocumentReader{inner: r}, nil
}

type errorWrappedTableReader struct {
	inner TableReader
}

func (w *errorWrappedTableReader) Close() error {
	return ClassifyConnectorError(w.inner.Close())
}

func (w *errorWrappedTableReader) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	c, t, err := w.inner.DescribeTable(ctx, table)
	return c, t, ClassifyConnectorError(err)
}

func (w *errorWrappedTableReader) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	r, c, t, i, err := w.inner.QueryCursor(ctx, q)
	return r, c, t, i, ClassifyConnectorError(err)
}

func (w *errorWrappedTableReader) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	s, err := w.inner.DiscoverCursorStats(ctx, table, cursorColumn, domain)
	return s, ClassifyConnectorError(err)
}

func (w *errorWrappedTableReader) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	v, err := w.inner.ValidateCursorColumn(ctx, table, cursorColumn)
	return v, ClassifyConnectorError(err)
}

func (w *errorWrappedTableReader) PostgresTypeMetadata(ctx context.Context, columnTypes []*sql.ColumnType) ([]PostgresTypeMetadata, error) {
	reader, ok := w.inner.(PostgresTypeMetadataReader)
	if !ok {
		return nil, nil
	}
	metadata, err := reader.PostgresTypeMetadata(ctx, columnTypes)
	return metadata, ClassifyConnectorError(err)
}

type errorWrappedSourceQueryReader struct {
	errorWrappedTableReader
	innerQueryReader SourceQueryReader
}

func (w *errorWrappedSourceQueryReader) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	c, t, err := w.innerQueryReader.DescribeQuery(ctx, query)
	return c, t, ClassifyConnectorError(err)
}

func (w *errorWrappedSourceQueryReader) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	s, err := w.innerQueryReader.DiscoverQueryCursorStats(ctx, query, cursorColumn, domain)
	return s, ClassifyConnectorError(err)
}

func (w *errorWrappedSourceQueryReader) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	v, err := w.innerQueryReader.ValidateQueryCursorColumn(ctx, query, cursorColumn)
	return v, ClassifyConnectorError(err)
}

type errorWrappedDocumentReader struct {
	inner DocumentReader
}

func (w *errorWrappedDocumentReader) Close() error {
	return ClassifyConnectorError(w.inner.Close())
}

func (w *errorWrappedDocumentReader) DescribeCollection(ctx context.Context, collection string) ([]DocumentField, error) {
	c, err := w.inner.DescribeCollection(ctx, collection)
	return c, ClassifyConnectorError(err)
}

func (w *errorWrappedDocumentReader) StreamDocuments(ctx context.Context, collection string, filter map[string]any, batchSize int) (DocumentIterator, error) {
	it, err := w.inner.StreamDocuments(ctx, collection, filter, batchSize)
	if err != nil {
		return nil, ClassifyConnectorError(err)
	}
	return &errorWrappedDocumentIterator{inner: it}, nil
}

func (w *errorWrappedDocumentReader) DiscoverCollectionStats(ctx context.Context, collection string) (CollectionStats, error) {
	s, err := w.inner.DiscoverCollectionStats(ctx, collection)
	return s, ClassifyConnectorError(err)
}

func (w *errorWrappedDocumentReader) DiscoverCursorStats(ctx context.Context, collection, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	s, err := w.inner.DiscoverCursorStats(ctx, collection, cursorColumn, domain)
	return s, ClassifyConnectorError(err)
}

func (w *errorWrappedDocumentReader) ValidateCursorColumn(ctx context.Context, collection, cursorColumn string) (CursorColumnValidation, error) {
	v, err := w.inner.ValidateCursorColumn(ctx, collection, cursorColumn)
	return v, ClassifyConnectorError(err)
}

func (w *errorWrappedDocumentReader) BuildCursorFilter(q CursorQuery) (map[string]any, error) {
	f, err := w.inner.BuildCursorFilter(q)
	return f, ClassifyConnectorError(err)
}

type errorWrappedDocumentIterator struct {
	inner DocumentIterator
}

func (w *errorWrappedDocumentIterator) Next(ctx context.Context) bool {
	return w.inner.Next(ctx)
}

func (w *errorWrappedDocumentIterator) Decode() (map[string]any, error) {
	doc, err := w.inner.Decode()
	return doc, ClassifyConnectorError(err)
}

func (w *errorWrappedDocumentIterator) Err() error {
	return ClassifyConnectorError(w.inner.Err())
}

func (w *errorWrappedDocumentIterator) Close() error {
	return ClassifyConnectorError(w.inner.Close())
}

func (w *errorWrappedDocumentIterator) FieldOrder() []string {
	if it, ok := w.inner.(OrderedDocumentIterator); ok {
		return it.FieldOrder()
	}
	return nil
}
func OpenIntRangeReader(ctx context.Context, engine, dsn string) (TableReader, error) {
	return OpenCursorReader(ctx, engine, dsn)
}

func cursorColumnMatches(resultColumn, cursorColumn string) bool {
	return strings.EqualFold(strings.TrimSpace(resultColumn), identLeaf(cursorColumn))
}

func idColumnMatches(resultColumn, idColumn string) bool {
	return cursorColumnMatches(resultColumn, idColumn)
}

func NormalizeCursorDomain(raw string) CursorDomain {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case string(CursorDomainInt64):
		return CursorDomainInt64
	case string(CursorDomainUInt64):
		return CursorDomainUInt64
	case string(CursorDomainDecimal):
		return CursorDomainDecimal
	case string(CursorDomainDate):
		return CursorDomainDate
	case string(CursorDomainTimestamp), "datetime", "datetime2", "timestamptz":
		return CursorDomainTimestamp
	case string(CursorDomainString):
		return CursorDomainString
	case string(CursorDomainUUID):
		return CursorDomainUUID
	default:
		return CursorDomainUnknown
	}
}

type SQLCursorTypeClass struct {
	Domain       CursorDomain
	Orderable    bool
	RangeCapable bool
	Unsigned     bool
	Bits         int
}

func ClassifySQLCursorType(typeName string) SQLCursorTypeClass {
	raw := strings.ToUpper(strings.TrimSpace(typeName))
	if raw == "" {
		return SQLCursorTypeClass{}
	}

	tokens := sqlTypeTokenRe.FindAllString(raw, -1)
	if len(tokens) == 0 {
		return SQLCursorTypeClass{}
	}

	unsigned := false
	filtered := make([]string, 0, len(tokens))
	for _, tok := range tokens {
		switch tok {
		case "NULLABLE", "LOWCARDINALITY":
			continue
		case "UNSIGNED":
			unsigned = true
			continue
		default:
			filtered = append(filtered, tok)
		}
	}
	if len(filtered) == 0 {
		return SQLCursorTypeClass{}
	}

	for i := 0; i < len(filtered); i++ {
		tok := filtered[i]
		switch tok {
		case "BIGINT", "BIGSERIAL":
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 64), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 64}
		case "SMALLINT", "SMALLSERIAL":
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 16), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 16}
		case "TINYINT":
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 8), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 8}
		case "MEDIUMINT":
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 24), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 24}
		case "INTEGER", "INT", "SERIAL":
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 32), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 32}
		case "BIG":
			if i+1 < len(filtered) && filtered[i+1] == "INT" {
				return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 64), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 64}
			}
		case "SMALL":
			if i+1 < len(filtered) && filtered[i+1] == "INT" {
				return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 16), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 16}
			}
		case "TINY":
			if i+1 < len(filtered) && filtered[i+1] == "INT" {
				return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 8), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 8}
			}
		case "MEDIUM":
			if i+1 < len(filtered) && filtered[i+1] == "INT" {
				return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned, 24), Orderable: true, RangeCapable: true, Unsigned: unsigned, Bits: 24}
			}
		case "DATE":
			if containsToken(filtered, "TIME") || containsToken(filtered, "TIMESTAMP") || containsToken(filtered, "TIMESTAMPTZ") || containsToken(filtered, "DATETIME") || containsToken(filtered, "DATETIME2") {
				return SQLCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
			}
			return SQLCursorTypeClass{Domain: CursorDomainDate, Orderable: true, RangeCapable: true}
		case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "DATETIME2":
			return SQLCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
		case "TIME":
			return SQLCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: false}
		case "DECIMAL", "NUMERIC", "MONEY", "SMALLMONEY":
			return SQLCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false}
		case "UUID", "UNIQUEIDENTIFIER":
			return SQLCursorTypeClass{Domain: CursorDomainUUID, Orderable: true, RangeCapable: false}
		case "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "STRING", "FIXEDSTRING", "ENUM":
			return SQLCursorTypeClass{Domain: CursorDomainString, Orderable: true, RangeCapable: false}
		case "BOOL", "BOOLEAN", "BIT", "BLOB", "BINARY", "VARBINARY", "IMAGE", "JSON", "JSONB", "XML", "INTERVAL", "ARRAY", "MAP", "STRUCT", "TUPLE", "OBJECT", "SET":
			return SQLCursorTypeClass{}
		}
		switch {
		case strings.HasPrefix(tok, "DATETIME"), strings.HasPrefix(tok, "TIMESTAMP"):
			return SQLCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
		case strings.HasPrefix(tok, "DATE"):
			return SQLCursorTypeClass{Domain: CursorDomainDate, Orderable: true, RangeCapable: true}
		}
		if bits, unsignedType, ok := intBitsFromCompactTypeToken(tok); ok {
			if bits > 64 {
				return SQLCursorTypeClass{}
			}
			return SQLCursorTypeClass{Domain: pickIntegerDomain(unsigned || unsignedType, bits), Orderable: true, RangeCapable: bits > 0 && bits <= 64, Unsigned: unsigned || unsignedType, Bits: bits}
		}
	}
	return SQLCursorTypeClass{}
}

func pickIntegerDomain(unsigned bool, _ int) CursorDomain {
	if unsigned {
		return CursorDomainUInt64
	}
	return CursorDomainInt64
}

func containsToken(tokens []string, target string) bool {
	for _, tok := range tokens {
		if tok == target {
			return true
		}
	}
	return false
}

type SQLIntegerTypeClass struct {
	Integer  bool
	Unsigned bool
	Bits     int
}

func ClassifySQLIntegerType(typeName string) SQLIntegerTypeClass {
	class := ClassifySQLCursorType(typeName)
	if class.Domain != CursorDomainInt64 && class.Domain != CursorDomainUInt64 {
		return SQLIntegerTypeClass{}
	}
	return SQLIntegerTypeClass{Integer: true, Unsigned: class.Unsigned, Bits: class.Bits}
}

func IsSupportedCursorSQLType(typeName string) bool {
	return ClassifySQLCursorType(typeName).Orderable
}

func IsSupportedIntRangeSQLType(typeName string) bool {
	return ClassifySQLCursorType(typeName).RangeCapable
}

func SupportsCursorRangeSplit(domain CursorDomain) bool {
	switch NormalizeCursorDomain(string(domain)) {
	case CursorDomainInt64, CursorDomainUInt64, CursorDomainDate, CursorDomainTimestamp:
		return true
	default:
		return false
	}
}

func CompareCursorValues(domain CursorDomain, a, b string) int {
	domain = NormalizeCursorDomain(string(domain))
	a = strings.TrimSpace(a)
	b = strings.TrimSpace(b)
	switch domain {
	case CursorDomainInt64:
		ai, aok := parseInt64(a)
		bi, bok := parseInt64(b)
		if aok && bok {
			switch {
			case ai < bi:
				return -1
			case ai > bi:
				return 1
			default:
				return 0
			}
		}
	case CursorDomainUInt64:
		au, aok := parseUint64(a)
		bu, bok := parseUint64(b)
		if aok && bok {
			switch {
			case au < bu:
				return -1
			case au > bu:
				return 1
			default:
				return 0
			}
		}
	case CursorDomainDate:
		at, aok := parseDateValue(a)
		bt, bok := parseDateValue(b)
		if aok && bok {
			switch {
			case at.Before(bt):
				return -1
			case at.After(bt):
				return 1
			default:
				return 0
			}
		}
	case CursorDomainTimestamp:
		at, aok := parseTimestampValue(a)
		bt, bok := parseTimestampValue(b)
		if aok && bok {
			switch {
			case at.Before(bt):
				return -1
			case at.After(bt):
				return 1
			default:
				return 0
			}
		}
	case CursorDomainDecimal:
		ar, aok := parseDecimalValue(a)
		br, bok := parseDecimalValue(b)
		if aok && bok {
			return ar.Cmp(br)
		}
	case CursorDomainString, CursorDomainUUID:
		return strings.Compare(a, b)
	}
	return strings.Compare(a, b)
}

func EncodeCursorValue(domain CursorDomain, v any) (string, bool) {
	domain = NormalizeCursorDomain(string(domain))
	switch domain {
	case CursorDomainInt64:
		iv, ok := asInt64Value(v)
		if !ok {
			return "", false
		}
		return strconv.FormatInt(iv, 10), true
	case CursorDomainUInt64:
		uv, ok := asUint64Value(v)
		if !ok {
			return "", false
		}
		return strconv.FormatUint(uv, 10), true
	case CursorDomainDate:
		return encodeDateValue(v)
	case CursorDomainTimestamp:
		return encodeTimestampValue(v)
	case CursorDomainDecimal:
		return encodeDecimalValue(v)
	case CursorDomainString, CursorDomainUUID:
		s, ok := asStringValue(v)
		if !ok {
			return "", false
		}
		if domain == CursorDomainUUID {
			s = strings.ToLower(s)
		}
		return s, true
	default:
		return "", false
	}
}

func ParseCursorArgument(domain CursorDomain, text string) (any, error) {
	domain = NormalizeCursorDomain(string(domain))
	text = strings.TrimSpace(text)
	switch domain {
	case CursorDomainInt64:
		iv, ok := parseInt64(text)
		if !ok {
			return nil, fmt.Errorf("parse int64 cursor %q", text)
		}
		return iv, nil
	case CursorDomainUInt64:
		uv, ok := parseUint64(text)
		if !ok {
			return nil, fmt.Errorf("parse uint64 cursor %q", text)
		}
		return uv, nil
	case CursorDomainDate:
		t, ok := parseDateValue(text)
		if !ok {
			return nil, fmt.Errorf("parse date cursor %q", text)
		}
		return t, nil
	case CursorDomainTimestamp:
		t, ok := parseTimestampValue(text)
		if !ok {
			return nil, fmt.Errorf("parse timestamp cursor %q", text)
		}
		return t, nil
	case CursorDomainDecimal, CursorDomainString, CursorDomainUUID:
		return text, nil
	default:
		return nil, fmt.Errorf("unsupported cursor domain %q", domain)
	}
}

func CursorSuccessor(domain CursorDomain, text string) (string, bool) {
	domain = NormalizeCursorDomain(string(domain))
	switch domain {
	case CursorDomainInt64:
		v, ok := parseInt64(text)
		if !ok || v == math.MaxInt64 {
			return "", false
		}
		return strconv.FormatInt(v+1, 10), true
	case CursorDomainUInt64:
		v, ok := parseUint64(text)
		if !ok || v == math.MaxUint64 {
			return "", false
		}
		return strconv.FormatUint(v+1, 10), true
	case CursorDomainDate:
		t, ok := parseDateValue(text)
		if !ok {
			return "", false
		}
		return t.AddDate(0, 0, 1).UTC().Format("2006-01-02"), true
	case CursorDomainTimestamp:
		t, ok := parseTimestampValue(text)
		if !ok {
			return "", false
		}
		return t.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano), true
	case CursorDomainDecimal, CursorDomainString, CursorDomainUUID:
		return text, true
	default:
		return "", false
	}
}

func SplitCursorRange(domain CursorDomain, startInclusive, endInclusive string, parts int) ([]string, error) {
	domain = NormalizeCursorDomain(string(domain))
	if !SupportsCursorRangeSplit(domain) {
		return []string{strings.TrimSpace(endInclusive)}, nil
	}
	start, err := parseDiscreteCursor(domain, startInclusive)
	if err != nil {
		return nil, err
	}
	end, err := parseDiscreteCursor(domain, endInclusive)
	if err != nil {
		return nil, err
	}
	if start.Cmp(end) > 0 {
		return nil, fmt.Errorf("range start %q is after end %q", startInclusive, endInclusive)
	}
	if parts <= 1 {
		return []string{strings.TrimSpace(endInclusive)}, nil
	}

	width := new(big.Int).Sub(end, start)
	width.Add(width, big.NewInt(1))
	if width.Sign() <= 0 {
		return []string{strings.TrimSpace(endInclusive)}, nil
	}
	partCount := int64(parts)
	divisor := big.NewInt(partCount)
	baseSize := new(big.Int).Div(new(big.Int).Set(width), divisor)
	remainder := new(big.Int).Mod(new(big.Int).Set(width), divisor).Int64()
	if baseSize.Sign() <= 0 {
		baseSize = big.NewInt(1)
	}

	out := make([]string, 0, parts)
	cur := new(big.Int).Set(start)
	for idx := 0; idx < parts-1; idx++ {
		size := new(big.Int).Set(baseSize)
		if int64(idx) < remainder {
			size.Add(size, big.NewInt(1))
		}
		upper := new(big.Int).Add(cur, size)
		upper.Sub(upper, big.NewInt(1))
		if upper.Cmp(end) >= 0 {
			break
		}
		enc, err := formatDiscreteCursor(domain, upper)
		if err != nil {
			return nil, err
		}
		out = append(out, enc)
		cur.Add(upper, big.NewInt(1))
		if cur.Cmp(end) > 0 {
			break
		}
	}
	out = append(out, strings.TrimSpace(endInclusive))
	return out, nil
}

func CursorSpanUnits(domain CursorDomain, lowerExclusive, upperInclusive string) (int64, bool) {
	domain = NormalizeCursorDomain(string(domain))
	if !SupportsCursorRangeSplit(domain) {
		return 0, false
	}
	upper, err := parseDiscreteCursor(domain, upperInclusive)
	if err != nil {
		return 0, false
	}
	var lower *big.Int
	if strings.TrimSpace(lowerExclusive) != "" {
		lower, err = parseDiscreteCursor(domain, lowerExclusive)
		if err != nil {
			return 0, false
		}
	} else {
		lower = new(big.Int).Sub(upper, big.NewInt(1))
	}
	span := new(big.Int).Sub(upper, lower)
	if !span.IsInt64() {
		return 0, false
	}
	return span.Int64(), true
}

func ClosedCursorSpanUnits(domain CursorDomain, startInclusive, endInclusive string) (int64, bool) {
	domain = NormalizeCursorDomain(string(domain))
	if !SupportsCursorRangeSplit(domain) {
		return 0, false
	}
	start, err := parseDiscreteCursor(domain, startInclusive)
	if err != nil {
		return 0, false
	}
	end, err := parseDiscreteCursor(domain, endInclusive)
	if err != nil || start.Cmp(end) > 0 {
		return 0, false
	}
	span := new(big.Int).Sub(end, start)
	span.Add(span, big.NewInt(1))
	if !span.IsInt64() {
		return 0, false
	}
	return span.Int64(), true
}

func parseDiscreteCursor(domain CursorDomain, text string) (*big.Int, error) {
	domain = NormalizeCursorDomain(string(domain))
	text = strings.TrimSpace(text)
	switch domain {
	case CursorDomainInt64:
		iv, ok := parseInt64(text)
		if !ok {
			return nil, fmt.Errorf("parse int64 cursor %q", text)
		}
		return big.NewInt(iv), nil
	case CursorDomainUInt64:
		uv, ok := parseUint64(text)
		if !ok {
			return nil, fmt.Errorf("parse uint64 cursor %q", text)
		}
		return new(big.Int).SetUint64(uv), nil
	case CursorDomainDate:
		t, ok := parseDateValue(text)
		if !ok {
			return nil, fmt.Errorf("parse date cursor %q", text)
		}
		return big.NewInt(t.UTC().Unix() / 86400), nil
	case CursorDomainTimestamp:
		t, ok := parseTimestampValue(text)
		if !ok {
			return nil, fmt.Errorf("parse timestamp cursor %q", text)
		}
		return big.NewInt(t.UTC().UnixNano()), nil
	default:
		return nil, fmt.Errorf("cursor domain %q is not discrete", domain)
	}
}

func formatDiscreteCursor(domain CursorDomain, v *big.Int) (string, error) {
	domain = NormalizeCursorDomain(string(domain))
	switch domain {
	case CursorDomainInt64:
		if v == nil || !v.IsInt64() {
			return "", fmt.Errorf("int64 cursor overflow")
		}
		return strconv.FormatInt(v.Int64(), 10), nil
	case CursorDomainUInt64:
		if v == nil || v.Sign() < 0 || v.BitLen() > 64 {
			return "", fmt.Errorf("uint64 cursor overflow")
		}
		return strconv.FormatUint(v.Uint64(), 10), nil
	case CursorDomainDate:
		if v == nil || !v.IsInt64() {
			return "", fmt.Errorf("date cursor overflow")
		}
		base := time.Unix(v.Int64()*86400, 0).UTC()
		return base.Format("2006-01-02"), nil
	case CursorDomainTimestamp:
		if v == nil || !v.IsInt64() {
			return "", fmt.Errorf("timestamp cursor overflow")
		}
		return time.Unix(0, v.Int64()).UTC().Format(time.RFC3339Nano), nil
	default:
		return "", fmt.Errorf("cursor domain %q is not discrete", domain)
	}
}

func parseInt64(v string) (int64, bool) {
	iv, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, false
	}
	return iv, true
}

func parseUint64(v string) (uint64, bool) {
	uv, err := strconv.ParseUint(strings.TrimSpace(v), 10, 64)
	if err != nil {
		return 0, false
	}
	return uv, true
}

func asInt64Value(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int16:
		return int64(x), true
	case int8:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		if x > math.MaxInt64 {
			return 0, false
		}
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint8:
		return int64(x), true
	case []byte:
		return parseInt64(string(x))
	case string:
		return parseInt64(x)
	default:
		return 0, false
	}
}

func asUint64Value(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint64:
		return x, true
	case uint32:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint8:
		return uint64(x), true
	case int64:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int32:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int16:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int8:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case int:
		if x < 0 {
			return 0, false
		}
		return uint64(x), true
	case []byte:
		return parseUint64(string(x))
	case string:
		return parseUint64(x)
	default:
		return 0, false
	}
}

func asStringValue(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x), true
	case []byte:
		return strings.TrimSpace(string(x)), true
	case fmt.Stringer:
		return strings.TrimSpace(x.String()), true
	default:
		return strings.TrimSpace(fmt.Sprint(v)), v != nil
	}
}

func encodeDateValue(v any) (string, bool) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format("2006-01-02"), true
	case string:
		if t, ok := parseDateValue(x); ok {
			return t.UTC().Format("2006-01-02"), true
		}
	case []byte:
		if t, ok := parseDateValue(string(x)); ok {
			return t.UTC().Format("2006-01-02"), true
		}
	}
	return "", false
}

func encodeTimestampValue(v any) (string, bool) {
	switch x := v.(type) {
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), true
	case string:
		if t, ok := parseTimestampValue(x); ok {
			return t.UTC().Format(time.RFC3339Nano), true
		}
	case []byte:
		if t, ok := parseTimestampValue(string(x)); ok {
			return t.UTC().Format(time.RFC3339Nano), true
		}
	}
	return "", false
}

func encodeDecimalValue(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		if _, ok := parseDecimalValue(x); ok {
			return strings.TrimSpace(x), true
		}
	case []byte:
		s := strings.TrimSpace(string(x))
		if _, ok := parseDecimalValue(s); ok {
			return s, true
		}
	case int64, int32, int16, int8, int, uint64, uint32, uint16, uint8, float64, float32:
		s := strings.TrimSpace(fmt.Sprint(v))
		if _, ok := parseDecimalValue(s); ok {
			return s, true
		}
	}
	return "", false
}

func parseDateValue(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	layouts := []string{
		"2006-01-02",
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			y, m, d := t.UTC().Date()
			return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), true
		}
	}
	return time.Time{}, false
}

func parseTimestampValue(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, v); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func parseDecimalValue(v string) (*big.Rat, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil, false
	}
	r, ok := new(big.Rat).SetString(v)
	return r, ok
}

func intBitsFromCompactTypeToken(tok string) (bits int, unsigned bool, ok bool) {
	if strings.HasPrefix(tok, "UINT") && len(tok) > len("UINT") {
		n, err := strconv.Atoi(tok[len("UINT"):])
		if err == nil && n > 0 {
			return n, true, true
		}
	}
	if strings.HasPrefix(tok, "INT") && len(tok) > len("INT") {
		n, err := strconv.Atoi(tok[len("INT"):])
		if err == nil && n > 0 {
			return n, false, true
		}
	}
	return 0, false, false
}

func identLeaf(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	parts := strings.Split(raw, ".")
	leaf := strings.TrimSpace(parts[len(parts)-1])
	leaf = strings.Trim(leaf, "[]\"`")
	return leaf
}
