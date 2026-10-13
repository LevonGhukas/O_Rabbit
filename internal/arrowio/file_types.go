package arrowio

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// File sources (CSV, XML, Excel, JSON, Parquet) have no source schema, so the
// planner reads the whole file once to decide every column's type before any
// worker starts. Workers and Iceberg registration then build the same schema
// from those types instead of guessing from a sample.

var (
	// Leading zeros ("007") and explicit signs ("+5") mark codes, not numbers.
	textIntPattern   = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)$`)
	textFloatPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)\.[0-9]+$`)
	textDatePattern  = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}$`)
	// ISO 8601 date-time: "T" or space, minutes required, seconds and
	// fractions optional, then an optional "Z" or "±HH:MM" offset (group 1).
	textTimestampPattern = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]{1,9})?)?(Z|[+-][0-9]{2}:[0-9]{2})?$`)
)

// maxExactFloatInt is the largest integer magnitude float64 holds exactly.
const maxExactFloatInt = 1 << 53

// DetectTextType classifies one non-empty text cell from a CSV, XML or Excel
// file. Only unambiguous ISO forms become dates and timestamps; anything else
// that is not clearly a number or boolean stays a string.
func DetectTextType(text string) typesystem.LogicalType {
	switch {
	case textIntPattern.MatchString(text):
		if _, err := strconv.ParseInt(text, 10, 64); err == nil {
			return typesystem.LogicalType{Kind: typesystem.KindInt64}
		}
		// Beyond int64: keep every digit rather than round through float64.
	case textFloatPattern.MatchString(text):
		if f, err := strconv.ParseFloat(text, 64); err == nil && !math.IsInf(f, 0) {
			return typesystem.LogicalType{Kind: typesystem.KindFloat64}
		}
	case strings.EqualFold(text, "true"), strings.EqualFold(text, "false"):
		return typesystem.LogicalType{Kind: typesystem.KindBool}
	case textDatePattern.MatchString(text):
		if _, err := time.Parse("2006-01-02", text); err == nil {
			return typesystem.LogicalType{Kind: typesystem.KindDate}
		}
	default:
		if m := textTimestampPattern.FindStringSubmatch(text); m != nil {
			if m[1] == "" {
				t := typesystem.LogicalType{Kind: typesystem.KindTimestamp}
				if _, err := typesystem.Convert(text, t); err == nil {
					return t
				}
			} else {
				t := typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}
				if _, err := typesystem.Convert(text, t); err == nil {
					return t
				}
			}
		}
	}
	return typesystem.LogicalType{Kind: typesystem.KindString}
}

// FileColumnType is one resolved file column.
type FileColumnType struct {
	Name string
	Type typesystem.LogicalType
}

// FileTypeResult holds the types every task of a file run writes.
type FileTypeResult struct {
	Columns  []FileColumnType
	Warnings []typesystem.TypeWarning
}

// Types returns the resolved types keyed by column, in the canonical form
// carried in task partition specs (see NewFileSchema).
func (r *FileTypeResult) Types() map[string]string {
	out := make(map[string]string, len(r.Columns))
	for _, c := range r.Columns {
		out[c.Name] = c.Type.String()
	}
	return out
}

// FileTypeScanner observes file rows and resolves column types, applying the
// user's overrides where the data allows and falling back with a warning
// where it does not.
type FileTypeScanner struct {
	textCells bool
	overrides map[string]string
	columns   map[string]*fileColumn
	seen      []string
	rows      int64
}

type fileColumn struct {
	name    string
	present int64
	nulls   int64

	detected      typesystem.LogicalType
	hasValue      bool
	mixed         bool
	mixedReason   string
	maxAbsInt     uint64
	fallbackNotes string

	requestedRaw string
	requested    *typesystem.LogicalType // nil for no override or "source"
	sourceType   bool
	sourceNull   bool
	overrideErr  string
}

// NewFileTypeScanner starts a scan. textCells is true when every value is an
// untyped text cell whose type must be detected (CSV, XML, Excel); typed
// formats (JSON, Parquet) keep the Go types their decoder produced.
func NewFileTypeScanner(textCells bool, overrides map[string]string) (*FileTypeScanner, error) {
	for col, raw := range overrides {
		if err := typesystem.ValidateOverride(raw); err != nil {
			return nil, fmt.Errorf("column %s: invalid type %q: %w", col, raw, err)
		}
	}
	return &FileTypeScanner{textCells: textCells, overrides: overrides, columns: map[string]*fileColumn{}}, nil
}

// Observe adds one decoded row to the scan.
func (s *FileTypeScanner) Observe(doc map[string]any) {
	s.rows++
	for name, value := range doc {
		col := s.column(name)
		col.present++
		if value == nil {
			col.nulls++
			continue
		}
		s.observeValue(col, value)
	}
}

func (s *FileTypeScanner) column(name string) *fileColumn {
	if col, ok := s.columns[name]; ok {
		return col
	}
	col := &fileColumn{name: name}
	if raw, ok := lookupColumnOverride(s.overrides, name); ok {
		col.requestedRaw = strings.TrimSpace(raw)
		if isSource, nullable := typesystem.IsSourceOverride(raw); isSource {
			col.sourceType, col.sourceNull = true, nullable
		} else if t, err := typesystem.ParseType(col.requestedRaw); err == nil {
			col.requested = &t // validated in NewFileTypeScanner
		}
	}
	s.columns[name] = col
	s.seen = append(s.seen, name)
	return col
}

func (s *FileTypeScanner) observeValue(col *fileColumn, value any) {
	if col.requested != nil && col.overrideErr == "" {
		if _, err := convertFileValue(value, *col.requested); err != nil {
			col.overrideErr = fmt.Sprintf("value %s in row %d is not a valid %s", quoteExample(value), s.rows, col.requested.Kind.String())
		}
	}
	if col.mixed {
		return
	}
	candidate, note := s.candidateType(value)
	if note != "" && col.fallbackNotes == "" {
		col.fallbackNotes = note
	}
	if candidate.Kind == typesystem.KindInt64 {
		col.maxAbsInt = max(col.maxAbsInt, absInt(value))
	}
	if !col.hasValue {
		col.detected, col.hasValue = candidate, true
		return
	}
	merged, ok := mergeFileTypes(col.detected, candidate, col.maxAbsInt)
	if !ok {
		col.mixed = true
		col.mixedReason = fmt.Sprintf("values mix %s and %s (for example %s in row %d)", col.detected.String(), candidate.String(), quoteExample(value), s.rows)
		return
	}
	col.detected = merged
}

// candidateType returns the type of one value and, for values that can only
// be kept as text, why.
func (s *FileTypeScanner) candidateType(value any) (typesystem.LogicalType, string) {
	if text, ok := value.(string); ok {
		if s.textCells {
			return DetectTextType(text), ""
		}
		return typesystem.LogicalType{Kind: typesystem.KindString}, ""
	}
	t, reason := mongoCandidate(value)
	if t.Kind == typesystem.KindUnknown {
		if reason == "" {
			reason = "the values have no direct column type"
		}
		return typesystem.LogicalType{Kind: typesystem.KindString}, "nested or special values are stored as text (" + reason + ")"
	}
	return t, ""
}

// mergeFileTypes widens two value types to one column type. ok is false when
// no type holds both without loss; the column is then stored as string.
func mergeFileTypes(a, b typesystem.LogicalType, maxAbsInt uint64) (typesystem.LogicalType, bool) {
	if a.Equal(b) {
		return a, true
	}
	pair := func(x, y typesystem.Kind) bool {
		return a.Kind == x && b.Kind == y || a.Kind == y && b.Kind == x
	}
	switch {
	case pair(typesystem.KindInt64, typesystem.KindFloat64):
		if maxAbsInt > maxExactFloatInt {
			return typesystem.LogicalType{}, false
		}
		return typesystem.LogicalType{Kind: typesystem.KindFloat64}, true
	case pair(typesystem.KindDate, typesystem.KindTimestamp):
		return typesystem.LogicalType{Kind: typesystem.KindTimestamp}, true
	case pair(typesystem.KindDecimal, typesystem.KindDecimal), pair(typesystem.KindArray, typesystem.KindArray):
		merged, ok := mergeMongoTypes(a, b, nil)
		return merged, ok
	}
	return typesystem.LogicalType{}, false
}

// Result resolves the final column types. Columns follow fieldOrder (the
// file's header, when it has one) and then first appearance.
func (s *FileTypeScanner) Result(fieldOrder []string) *FileTypeResult {
	result := &FileTypeResult{Columns: []FileColumnType{}, Warnings: []typesystem.TypeWarning{}}
	for _, name := range fileColumnOrder(fieldOrder, s.seen) {
		col := s.columns[name]
		nullRows := col.nulls + (s.rows - col.present)
		final, warning := col.resolve(nullRows)
		result.Columns = append(result.Columns, FileColumnType{Name: name, Type: final})
		if warning != nil {
			result.Warnings = append(result.Warnings, *warning)
		}
	}
	result.Warnings = typesystem.DeduplicateTypeWarnings(result.Warnings)
	return result
}

func (c *fileColumn) resolve(nullRows int64) (typesystem.LogicalType, *typesystem.TypeWarning) {
	stringType := typesystem.LogicalType{Kind: typesystem.KindString}
	detected, detectReason := c.detected, ""
	switch {
	case !c.hasValue:
		detected, detectReason = stringType, "every value is empty, so the column is stored as string."
	case c.mixed:
		detected, detectReason = stringType, c.mixedReason+", so the column is stored as string."
	case c.fallbackNotes != "":
		detectReason = c.fallbackNotes + "."
	}
	// Without an override, file columns are nullable: a later append may
	// contain empty values even when this file has none.
	detected.Nullable = true

	if c.requestedRaw == "" {
		if detectReason == "" {
			return detected, nil
		}
		return detected, &typesystem.TypeWarning{
			Column:      c.name,
			LogicalType: detected.String(),
			StorageType: detected.String(),
			Class:       typesystem.MappingSemanticFallback,
			Reason:      fmt.Sprintf("Column %q: %s", c.name, detectReason),
		}
	}

	final := detected
	var reasons []string
	nullable := c.sourceNull
	if c.requested != nil {
		final, nullable = *c.requested, c.requested.Nullable
		if c.overrideErr != "" {
			final = stringType
			reasons = append(reasons, fmt.Sprintf("you selected %s, but %s, so the column is stored as string.", c.requested.Kind.String(), c.overrideErr))
		}
	} else if detectReason != "" {
		reasons = append(reasons, detectReason)
	}
	if !nullable && nullRows > 0 {
		nullable = true
		reasons = append(reasons, fmt.Sprintf("you marked the column as not nullable, but %d row(s) have no value, so it is stored as nullable.", nullRows))
	}
	final.Nullable = nullable
	if len(reasons) == 0 {
		return final, nil
	}
	return final, &typesystem.TypeWarning{
		Column:      c.name,
		SourceType:  detected.String(),
		LogicalType: c.requestedRaw,
		StorageType: final.String(),
		Class:       typesystem.MappingOverrideFallback,
		Reason:      fmt.Sprintf("Column %q: %s", c.name, strings.Join(reasons, " ")),
	}
}

// InferFileTypesFromReader streams a file source and resolves its column
// types. limit bounds the rows read (0 reads the whole file).
func InferFileTypesFromReader(ctx context.Context, reader connectors.DocumentReader, collection string, filter map[string]any, overrides map[string]string, limit int64) (*FileTypeResult, error) {
	it, err := reader.StreamDocuments(ctx, collection, filter, 1000)
	if err != nil {
		return nil, fmt.Errorf("stream file rows: %w", err)
	}
	defer it.Close()
	scanner, err := NewFileTypeScanner(connectors.IsTextDocumentIterator(it), overrides)
	if err != nil {
		return nil, err
	}
	var fieldOrder []string
	if ordered, ok := it.(connectors.OrderedDocumentIterator); ok {
		fieldOrder = ordered.FieldOrder()
	}
	for (limit <= 0 || scanner.rows < limit) && it.Next(ctx) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		doc, err := it.Decode()
		if err != nil {
			return nil, fmt.Errorf("decode file row %d: %w", scanner.rows+1, err)
		}
		scanner.Observe(doc)
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("read file rows: %w", err)
	}
	return scanner.Result(fieldOrder), nil
}

// FileField is one column of a FileSchema.
type FileField struct {
	Name    string
	Type    typesystem.LogicalType
	Plan    ColumnPlan
	Mapping typesystem.MappingResult
}

// FileSchema is the Arrow schema of a file source built from resolved types.
type FileSchema struct {
	Schema *arrow.Schema
	Fields []FileField
}

// NewFileSchema builds the schema from the column types the planner resolved
// (FileTypeResult.Types). fieldOrder is the file's own column order; columns
// it does not list follow in name order, so every caller gets the same schema.
func NewFileSchema(fieldOrder []string, types map[string]string) (*FileSchema, error) {
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	fs := &FileSchema{}
	arrowFields := make([]arrow.Field, 0, len(types))
	for _, name := range fileColumnOrder(fieldOrder, names) {
		raw, ok := types[name]
		if !ok {
			continue
		}
		t, err := typesystem.ParseType(raw)
		if err != nil {
			return nil, fmt.Errorf("column %s: invalid type %q: %w", name, raw, err)
		}
		plan, mapping, err := PlanForLogicalType(name, t)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", name, err)
		}
		fs.Fields = append(fs.Fields, FileField{Name: name, Type: t, Plan: plan, Mapping: mapping})
		arrowFields = append(arrowFields, arrow.Field{Name: name, Type: plan.DataType, Nullable: t.Nullable})
	}
	fs.Schema = arrow.NewSchema(arrowFields, nil)
	return fs, nil
}

// FileDocsToRecord converts decoded file rows into one record batch of fs.
func FileDocsToRecord(alloc memory.Allocator, fs *FileSchema, docs []map[string]any) (arrow.RecordBatch, error) {
	if len(docs) == 0 {
		return nil, nil
	}
	builders := make([]array.Builder, len(fs.Fields))
	defer func() {
		for _, b := range builders {
			if b != nil {
				b.Release()
			}
		}
	}()
	for i, field := range fs.Fields {
		builders[i] = field.Plan.Builder(alloc)
		builders[i].Reserve(len(docs))
		for _, doc := range docs {
			value := doc[field.Name]
			if value == nil {
				if !field.Type.Nullable {
					return nil, fmt.Errorf("column %s: empty value in a NOT NULL column", field.Name)
				}
				builders[i].AppendNull()
				continue
			}
			normalized, err := normalizeFileValue(value, field.Type)
			if err != nil {
				return nil, fmt.Errorf("column %s: %w", field.Name, err)
			}
			if err := field.Plan.Append(builders[i], normalized); err != nil {
				return nil, fmt.Errorf("column %s: %w", field.Name, err)
			}
		}
	}
	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
	}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	return array.NewRecordBatch(fs.Schema, arrays, int64(len(docs))), nil
}

// normalizeFileValue prepares a decoded value for the column plan. String
// columns keep text exactly as written; other values go through the same
// normalization as document sources.
func normalizeFileValue(value any, t typesystem.LogicalType) (any, error) {
	if t.Kind == typesystem.KindString {
		if text, ok := value.(string); ok {
			return text, nil
		}
		return mongoLosslessString(value)
	}
	return mongoNormalize(value, t)
}

// convertFileValue reports whether value can be stored as t.
func convertFileValue(value any, t typesystem.LogicalType) (any, error) {
	normalized, err := normalizeFileValue(value, t)
	if err != nil {
		return nil, err
	}
	return typesystem.Convert(normalized, storageConversionTarget(t))
}

func fileColumnOrder(preferred, rest []string) []string {
	out := make([]string, 0, len(rest))
	seen := map[string]bool{}
	present := map[string]bool{}
	for _, name := range rest {
		present[name] = true
	}
	for _, name := range preferred {
		if present[name] && !seen[name] {
			out = append(out, name)
			seen[name] = true
		}
	}
	for _, name := range rest {
		if !seen[name] {
			out = append(out, name)
			seen[name] = true
		}
	}
	return out
}

func absInt(value any) uint64 {
	var n int64
	switch v := value.(type) {
	case string:
		n, _ = strconv.ParseInt(v, 10, 64)
	case int:
		n = int64(v)
	case int32:
		n = int64(v)
	case int64:
		n = v
	}
	if n < 0 {
		return uint64(-(n + 1)) + 1
	}
	return uint64(n)
}

// quoteExample renders a sample value for a warning, shortened so a warning
// never carries a whole cell.
func quoteExample(value any) string {
	text, err := typesystem.ToLosslessString(value)
	if err != nil {
		text = fmt.Sprintf("%v", value)
	}
	const maxRunes = 40
	if utf8.RuneCountInString(text) > maxRunes {
		text = string([]rune(text)[:maxRunes]) + "…"
	}
	return strconv.Quote(text)
}
