package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/sijms/go-ora/v2"
)

type Oracle struct {
	db            *sql.DB
	sourceIsLocal bool
}

const (
	oraclePingTimeout     = 10 * time.Second
	oracleStatsTimeout    = 2 * time.Minute
	oracleValidateTimeout = 20 * time.Second
)

type oracleObjectIdent struct {
	Owner  string
	Name   string
	Quoted string
}

type oracleIdentPart struct {
	Lookup string
	Quoted string
}

type oracleColumnMeta struct {
	Found       bool
	DataType    string
	Nullable    bool
	HasNullable bool
	Precision   int64
	Scale       int64
	HasDecimal  bool
}

type oracleCursorTypeClass struct {
	Domain       CursorDomain
	Orderable    bool
	RangeCapable bool
}

func oracleInt64CursorTypeClass() oracleCursorTypeClass {
	return oracleCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true}
}

func OpenOracle(ctx context.Context, dsn string) (*Oracle, error) {
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, oraclePingTimeout)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Oracle{db: db, sourceIsLocal: oracleDSNIsLocal(dsn)}, nil
}

func (o *Oracle) Close() error { return o.db.Close() }

func (o *Oracle) ExportSnapshot(ctx context.Context) (string, error) {
	var scn string
	// Try DBMS_FLASHBACK first, which doesn't require V$DATABASE select privileges in many environments.
	err := o.db.QueryRowContext(ctx, "SELECT TO_CHAR(DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER) FROM DUAL").Scan(&scn)
	if err != nil {
		err2 := o.db.QueryRowContext(ctx, "SELECT TO_CHAR(CURRENT_SCN) FROM V$DATABASE").Scan(&scn)
		if err2 != nil {
			return "", fmt.Errorf("failed to get Oracle SCN (dbms_flashback: %v, v$database: %v)", err, err2)
		}
	}
	return scn, nil
}

func (o *Oracle) DescribeQuery(ctx context.Context, query string) ([]string, []*sql.ColumnType, error) {
	qctx, cancel := context.WithTimeout(ctx, oracleValidateTimeout)
	defer cancel()
	return describeQuery(qctx, o.db, "oracle", query)
}

func (o *Oracle) DescribeTable(ctx context.Context, table string) ([]string, []*sql.ColumnType, error) {
	qt, err := quoteOracleMultipartIdent(table)
	if err != nil {
		return nil, nil, err
	}
	rows, err := o.db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s WHERE 1 = 0", qt))
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

func (o *Oracle) QueryCursor(ctx context.Context, q CursorQuery) (*sql.Rows, []string, []*sql.ColumnType, int, error) {
	if strings.TrimSpace(q.SourceQuery) != "" {
		return queryModeCursor(ctx, o.db, "oracle", q)
	}

	query, args, err := buildOracleCursorQuery(q)
	if err != nil {
		return nil, nil, nil, -1, err
	}
	rows, err := o.db.QueryContext(ctx, query, args...)
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

func (o *Oracle) DiscoverQueryCursorStats(ctx context.Context, query, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qctx, cancel := context.WithTimeout(ctx, oracleStatsTimeout)
	defer cancel()
	stats, err := discoverQueryCursorStats(qctx, o.db, "oracle", query, cursorColumn, domain, o.sourceIsLocal)
	if err != nil {
		return CursorStats{}, fmt.Errorf("discover query stats bounds for cursor %s: %w", cursorColumn, err)
	}
	return stats, nil
}

func (o *Oracle) DiscoverCursorStats(ctx context.Context, table, cursorColumn string, domain CursorDomain) (CursorStats, error) {
	qt, err := quoteOracleMultipartIdent(table)
	if err != nil {
		return CursorStats{}, err
	}
	qc, err := quoteOracleCursorIdent(cursorColumn)
	if err != nil {
		return CursorStats{}, err
	}
	if NormalizeCursorDomain(string(domain)) == CursorDomainUnknown {
		return CursorStats{}, fmt.Errorf("cursor domain is required")
	}

	qctx, cancel := context.WithTimeout(ctx, oracleStatsTimeout)
	defer cancel()

	out := CursorStats{SourceIsLocal: o.sourceIsLocal}

	var minv, maxv any
	qBounds := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, qt)
	if err := o.db.QueryRowContext(qctx, qBounds).Scan(&minv, &maxv); err != nil {
		return CursorStats{}, fmt.Errorf("discover stats bounds for %s.%s: %w", table, cursorColumn, err)
	}
	if v, ok := EncodeCursorValue(domain, minv); ok {
		out.MinValue = v
	}
	if v, ok := EncodeCursorValue(domain, maxv); ok {
		out.MaxValue = v
	}

	if rowCount, avgRowLen, ok := o.lookupTableStats(qctx, table); ok {
		out.RowCount = rowCount
		if rowCount > 0 && avgRowLen > 0 && rowCount <= math.MaxInt64/avgRowLen {
			out.TableBytes = rowCount * avgRowLen
		}
	}

	return out, nil
}

func (o *Oracle) ValidateQueryCursorColumn(ctx context.Context, query, cursorColumn string) (CursorColumnValidation, error) {
	vctx, cancel := context.WithTimeout(ctx, oracleValidateTimeout)
	defer cancel()
	return validateQueryCursorColumn(vctx, o.db, "oracle", query, cursorColumn)
}

func (o *Oracle) ValidateCursorColumn(ctx context.Context, table, cursorColumn string) (CursorColumnValidation, error) {
	out := CursorColumnValidation{}

	vctx, cancel := context.WithTimeout(ctx, oracleValidateTimeout)
	defer cancel()

	cols, ct, err := o.DescribeTable(vctx, table)
	if err != nil {
		return out, fmt.Errorf("describe table for ordered-cursor validation (%s): %w", table, err)
	}

	colIdent, err := parseOracleCursorIdent(cursorColumn)
	if err != nil {
		return out, err
	}

	colIdx := -1
	for i, c := range cols {
		if !cursorColumnMatches(c, colIdent.Lookup) {
			continue
		}
		out.Found = true
		out.ResolvedName = c
		colIdx = i
		break
	}
	if !out.Found {
		return out, nil
	}

	meta, metaErr := o.lookupColumnMeta(vctx, table, colIdent.Lookup)
	if metaErr == nil && meta.Found {
		out.DataType = meta.DataType
		out.NullableKnown = meta.HasNullable
		out.Nullable = meta.Nullable
	}

	var (
		precision    int64
		scale        int64
		hasPrecision bool
		hasScale     bool
	)
	if meta.Found && meta.HasDecimal {
		precision = meta.Precision
		scale = meta.Scale
		hasPrecision = true
		hasScale = true
	}

	if colIdx >= 0 && colIdx < len(ct) && ct[colIdx] != nil {
		if out.DataType == "" {
			out.DataType = strings.ToUpper(strings.TrimSpace(ct[colIdx].DatabaseTypeName()))
		}
		if !out.NullableKnown {
			if n, ok := ct[colIdx].Nullable(); ok {
				out.NullableKnown = true
				out.Nullable = n
			}
		}
		if p, s, ok := ct[colIdx].DecimalSize(); ok && !hasPrecision && !hasScale {
			precision = int64(p)
			scale = int64(s)
			hasPrecision = true
			hasScale = true
		}
	}

	class := classifyOracleCursorType(out.DataType, precision, scale, hasPrecision, hasScale)
	if class.Domain == CursorDomainUnknown && shouldProbeOracleAmbiguousNumber(meta, out.DataType, hasPrecision, hasScale) {
		probedClass, err := o.probeOracleAmbiguousNumberCursor(vctx, table, out.ResolvedName)
		if err != nil {
			return out, err
		}
		class = probedClass
	}
	out.Domain = class.Domain
	out.Orderable = class.Orderable
	out.RangeCapable = class.RangeCapable

	if indexed, known := o.lookupCursorIndexedBestEffort(vctx, table, colIdent.Lookup); known {
		out.IndexedKnown = true
		out.Indexed = indexed
	}

	return out, nil
}

func shouldProbeOracleAmbiguousNumber(meta oracleColumnMeta, dataType string, hasPrecision, hasScale bool) bool {
	typeName := strings.ToUpper(strings.TrimSpace(dataType))
	if typeName != "NUMBER" {
		return false
	}
	if meta.Found {
		return !meta.HasDecimal
	}
	return !hasPrecision && !hasScale
}

func (o *Oracle) probeOracleAmbiguousNumberCursor(ctx context.Context, table, cursorColumn string) (oracleCursorTypeClass, error) {
	fractionalSQL, boundsSQL, err := buildOracleAmbiguousNumberProbeQueries(table, cursorColumn)
	if err != nil {
		return oracleCursorTypeClass{}, err
	}

	var fractionalMarker int
	err = o.db.QueryRowContext(ctx, fractionalSQL).Scan(&fractionalMarker)
	switch {
	case err == nil:
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and contains fractional values; use an integer-only cursor or define NUMBER(18,0)", cursorColumn)
	case err != sql.ErrNoRows:
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and safety probe failed: fractional check: %w", cursorColumn, err)
	}

	var minv, maxv any
	if err := o.db.QueryRowContext(ctx, boundsSQL).Scan(&minv, &maxv); err != nil {
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and safety probe failed: min/max check: %w", cursorColumn, err)
	}

	return validateOracleAmbiguousNumberProbeResult(cursorColumn, false, minv, maxv)
}

func buildOracleCursorQuery(q CursorQuery) (string, []any, error) {
	qt, err := quoteOracleMultipartIdent(q.Table)
	if err != nil {
		return "", nil, err
	}
	qc, err := quoteOracleCursorIdent(q.CursorColumn)
	if err != nil {
		return "", nil, err
	}
	if NormalizeCursorDomain(string(q.CursorDomain)) == CursorDomainUnknown {
		return "", nil, fmt.Errorf("cursor domain is required")
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
			return "", nil, err
		}
		op := ">="
		if q.LowerExclusive {
			op = ">"
		}
		clauses = append(clauses, fmt.Sprintf("%s %s :%d", qc, op, argPos))
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
		clauses = append(clauses, fmt.Sprintf("%s %s :%d", qc, op, argPos))
		args = append(args, upperArg)
	}

	selectClause, err := renderSelectColumns(oracleIdentifierRenderer, q.SelectColumns)
	if err != nil {
		return "", nil, err
	}
	query := fmt.Sprintf("SELECT %s FROM %s", selectClause, qt)
	if scn := strings.TrimSpace(q.SnapshotContext); scn != "" {
		// Optional: Oracle requires AS OF SCN directly after the table identifier.
		query += fmt.Sprintf(" AS OF SCN %s", scn)
	}
	if len(clauses) > 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}
	query += fmt.Sprintf(" ORDER BY %s ASC", qc)
	return query, args, nil
}

func buildOracleAmbiguousNumberProbeQueries(table, cursorColumn string) (string, string, error) {
	qt, err := quoteOracleMultipartIdent(table)
	if err != nil {
		return "", "", err
	}
	qc, err := quoteOracleCursorIdent(cursorColumn)
	if err != nil {
		return "", "", err
	}
	fractionalSQL := fmt.Sprintf("SELECT 1 FROM %s WHERE %s != TRUNC(%s) AND ROWNUM = 1", qt, qc, qc)
	boundsSQL := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", qc, qc, qt)
	return fractionalSQL, boundsSQL, nil
}

func quoteOracleMultipartIdent(raw string) (string, error) {
	parts, err := splitLegacyIdentifier(raw, `"`, `"`)
	if err != nil {
		return "", err
	}
	// Preserve the case in legacy quoted input and in raw names that contain a
	// quote. Oracle folds only legacy unquoted identifiers to uppercase.
	if !strings.Contains(raw, `"`) {
		for i := range parts {
			parts[i] = strings.ToUpper(parts[i])
		}
	}
	return oracleIdentifierRenderer.qualified(parts...)
}

func quoteOracleCursorIdent(raw string) (string, error) {
	parts, err := splitLegacyIdentifier(raw, `"`, `"`)
	if err != nil {
		return "", err
	}
	part := parts[len(parts)-1]
	if !strings.Contains(raw, `"`) {
		part = strings.ToUpper(part)
	}
	return oracleIdentifierRenderer.part(part)
}

func parseOracleObjectIdent(raw string) (oracleObjectIdent, error) {
	parts, err := parseOracleMultipartIdent(raw, 2)
	if err != nil {
		return oracleObjectIdent{}, err
	}
	switch len(parts) {
	case 1:
		return oracleObjectIdent{
			Name:   parts[0].Lookup,
			Quoted: parts[0].Quoted,
		}, nil
	case 2:
		return oracleObjectIdent{
			Owner:  parts[0].Lookup,
			Name:   parts[1].Lookup,
			Quoted: parts[0].Quoted + "." + parts[1].Quoted,
		}, nil
	default:
		return oracleObjectIdent{}, fmt.Errorf("oracle identifiers support TABLE or SCHEMA.TABLE only")
	}
}

func parseOracleCursorIdent(raw string) (oracleIdentPart, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return oracleIdentPart{}, fmt.Errorf("empty identifier")
	}
	parts, err := splitLegacyIdentifier(raw, `"`, `"`)
	if err != nil {
		return oracleIdentPart{}, err
	}
	return parseOracleIdentPart(parts[len(parts)-1])
}

func parseOracleMultipartIdent(raw string, maxParts int) ([]oracleIdentPart, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty identifier")
	}
	parts, err := splitLegacyIdentifier(raw, `"`, `"`)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 || len(parts) > maxParts {
		return nil, fmt.Errorf("oracle identifiers support at most %d parts", maxParts)
	}
	out := make([]oracleIdentPart, 0, len(parts))
	for _, part := range parts {
		parsed, err := parseOracleIdentPart(part)
		if err != nil {
			return nil, err
		}
		out = append(out, parsed)
	}
	return out, nil
}

func parseOracleIdentPart(raw string) (oracleIdentPart, error) {
	part := strings.TrimSpace(raw)
	if part == "" {
		return oracleIdentPart{}, fmt.Errorf("empty identifier")
	}
	lookup := strings.ToUpper(part)
	quoted, err := oracleIdentifierRenderer.part(part)
	if err != nil {
		return oracleIdentPart{}, err
	}
	return oracleIdentPart{
		Lookup: lookup,
		Quoted: quoted,
	}, nil
}

func (o *Oracle) lookupColumnMeta(ctx context.Context, table, cursorColumn string) (oracleColumnMeta, error) {
	obj, err := parseOracleObjectIdent(table)
	if err != nil {
		return oracleColumnMeta{}, err
	}

	var (
		query string
		args  []any
	)
	if obj.Owner != "" {
		query = `
SELECT DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
FROM ALL_TAB_COLUMNS
WHERE OWNER = :1 AND TABLE_NAME = :2 AND COLUMN_NAME = :3`
		args = []any{obj.Owner, obj.Name, cursorColumn}
	} else {
		query = `
SELECT DATA_TYPE, NULLABLE, DATA_PRECISION, DATA_SCALE
FROM USER_TAB_COLUMNS
WHERE TABLE_NAME = :1 AND COLUMN_NAME = :2`
		args = []any{obj.Name, cursorColumn}
	}

	var (
		dataType  sql.NullString
		nullable  sql.NullString
		precision sql.NullInt64
		scale     sql.NullInt64
	)
	err = o.db.QueryRowContext(ctx, query, args...).Scan(&dataType, &nullable, &precision, &scale)
	if err != nil {
		if err == sql.ErrNoRows {
			return oracleColumnMeta{}, nil
		}
		return oracleColumnMeta{}, err
	}

	meta := oracleColumnMeta{
		Found:      dataType.Valid,
		DataType:   strings.ToUpper(strings.TrimSpace(dataType.String)),
		HasDecimal: precision.Valid && scale.Valid,
	}
	if nullable.Valid {
		meta.HasNullable = true
		meta.Nullable = strings.EqualFold(strings.TrimSpace(nullable.String), "Y")
	}
	if precision.Valid && scale.Valid {
		meta.Precision = precision.Int64
		meta.Scale = scale.Int64
	}
	return meta, nil
}

func (o *Oracle) lookupCursorIndexedBestEffort(ctx context.Context, table, cursorColumn string) (bool, bool) {
	obj, err := parseOracleObjectIdent(table)
	if err != nil {
		return false, false
	}

	var (
		query string
		args  []any
	)
	if obj.Owner != "" {
		query = `
SELECT COUNT(1)
FROM ALL_IND_COLUMNS c
JOIN ALL_INDEXES i
  ON i.OWNER = c.INDEX_OWNER
 AND i.INDEX_NAME = c.INDEX_NAME
 AND i.TABLE_OWNER = c.TABLE_OWNER
 AND i.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_OWNER = :1
  AND c.TABLE_NAME = :2
  AND c.COLUMN_NAME = :3
  AND i.STATUS = 'VALID'`
		args = []any{obj.Owner, obj.Name, cursorColumn}
	} else {
		query = `
SELECT COUNT(1)
FROM USER_IND_COLUMNS c
JOIN USER_INDEXES i
  ON i.INDEX_NAME = c.INDEX_NAME
 AND i.TABLE_NAME = c.TABLE_NAME
WHERE c.TABLE_NAME = :1
  AND c.COLUMN_NAME = :2
  AND i.STATUS = 'VALID'`
		args = []any{obj.Name, cursorColumn}
	}

	var count int64
	if err := o.db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
		return false, false
	}
	return count > 0, true
}

func (o *Oracle) lookupTableStats(ctx context.Context, table string) (rowCount int64, avgRowLen int64, ok bool) {
	obj, err := parseOracleObjectIdent(table)
	if err != nil {
		return 0, 0, false
	}

	var (
		query string
		args  []any
	)
	if obj.Owner != "" {
		query = `
SELECT NUM_ROWS, AVG_ROW_LEN
FROM ALL_TABLES
WHERE OWNER = :1 AND TABLE_NAME = :2`
		args = []any{obj.Owner, obj.Name}
	} else {
		query = `
SELECT NUM_ROWS, AVG_ROW_LEN
FROM USER_TABLES
WHERE TABLE_NAME = :1`
		args = []any{obj.Name}
	}

	var (
		rc sql.NullInt64
		al sql.NullInt64
	)
	if err := o.db.QueryRowContext(ctx, query, args...).Scan(&rc, &al); err != nil {
		return 0, 0, false
	}
	if rc.Valid {
		rowCount = rc.Int64
	}
	if al.Valid {
		avgRowLen = al.Int64
	}
	return rowCount, avgRowLen, true
}

func classifyOracleCursorType(typeName string, precision, scale int64, hasPrecision, hasScale bool) oracleCursorTypeClass {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	switch {
	case t == "DATE":
		return oracleCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
	case strings.HasPrefix(t, "TIMESTAMP"):
		if strings.Contains(t, "WITH TIME ZONE") || strings.Contains(t, "WITH LOCAL TIME ZONE") {
			return oracleCursorTypeClass{}
		}
		return oracleCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true}
	case t == "NUMBER" || strings.HasPrefix(t, "NUMBER("):
		if hasScale && scale == 0 && hasPrecision && precision > 0 && precision <= 18 {
			return oracleCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true}
		}
		return oracleCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false}
	case t == "INTEGER" || t == "INT" || t == "SMALLINT":
		return oracleCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true}
	case strings.HasPrefix(t, "VARCHAR2"), strings.HasPrefix(t, "NVARCHAR2"), t == "CHAR", t == "NCHAR":
		return oracleCursorTypeClass{Domain: CursorDomainString, Orderable: true, RangeCapable: false}
	case t == "RAW", t == "ROWID", t == "UROWID", t == "BLOB", t == "CLOB", t == "NCLOB",
		t == "FLOAT", t == "BINARY_FLOAT", t == "BINARY_DOUBLE":
		return oracleCursorTypeClass{}
	default:
		return oracleCursorTypeClass{}
	}
}

func validateOracleAmbiguousNumberProbeResult(cursorColumn string, fractionalFound bool, minv, maxv any) (oracleCursorTypeClass, error) {
	if fractionalFound {
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and contains fractional values; use an integer-only cursor or define NUMBER(18,0)", cursorColumn)
	}
	if minv == nil && maxv == nil {
		return oracleInt64CursorTypeClass(), nil
	}
	if minv == nil || maxv == nil {
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and safety probe returned incomplete min/max bounds", cursorColumn)
	}
	if _, err := parseOracleNumberAsInt64Strict(minv); err != nil {
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and min/max are not safely representable as int64: %w", cursorColumn, err)
	}
	if _, err := parseOracleNumberAsInt64Strict(maxv); err != nil {
		return oracleCursorTypeClass{}, fmt.Errorf("Oracle NUMBER cursor column %q has missing precision/scale and min/max are not safely representable as int64: %w", cursorColumn, err)
	}
	return oracleInt64CursorTypeClass(), nil
}

func parseOracleNumberAsInt64Strict(v any) (int64, error) {
	switch x := v.(type) {
	case nil:
		return 0, fmt.Errorf("value is NULL")
	case int64:
		return x, nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case int:
		return int64(x), nil
	case uint64:
		if x > math.MaxInt64 {
			return 0, fmt.Errorf("value %d exceeds int64", x)
		}
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return 0, fmt.Errorf("value %d exceeds int64", x)
		}
		return int64(x), nil
	case []byte:
		return parseOracleNumberIntTextStrict(string(x))
	case string:
		return parseOracleNumberIntTextStrict(x)
	case fmt.Stringer:
		return parseOracleNumberIntTextStrict(x.String())
	default:
		return 0, fmt.Errorf("unsupported Oracle NUMBER runtime type %T", v)
	}
}

func parseOracleNumberIntTextStrict(raw string) (int64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty numeric text")
	}

	start := 0
	if s[0] == '+' || s[0] == '-' {
		start = 1
	}
	if start >= len(s) {
		return 0, fmt.Errorf("missing digits in %q", raw)
	}
	for _, r := range s[start:] {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("non-integer numeric text %q", raw)
		}
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse int64 %q: %w", raw, err)
	}
	return v, nil
}

func oracleDSNIsLocal(dsn string) bool {
	raw := strings.TrimSpace(dsn)
	if raw == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
