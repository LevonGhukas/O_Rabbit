package arrowio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

func TestDetectTextType(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"42", "int64"},
		{"-7", "int64"},
		{"0", "int64"},
		{"9223372036854775807", "int64"},
		{"12345678901234567890", "string"}, // beyond int64: keep every digit
		{"007", "string"},                  // leading zero: a code
		{"-007", "string"},
		{"00", "string"},
		{"007.5", "string"},
		{"+5", "string"},
		{"1e5", "float64"},
		{"-1.5E-3", "float64"},
		{"1e", "string"},
		{"NaN", "string"}, // a "no value" marker, handled by the scanner
		{"3.14", "float64"},
		{"-0.5", "float64"},
		{"0.5", "float64"},
		{".5", "string"},
		{"5.", "string"},
		{"1,234.50", "string"},
		{"0.1234567890123456", "string"}, // 16 significant digits: a float would round it
		{"true", "bool"},
		{"FALSE", "bool"},
		{"yes", "string"},
		{"2024-01-15", "date"},
		{"2024-13-45", "string"},
		{"15/01/2024", "string"},
		{"01/15/2024", "string"},
		{"2024-01-15 10:30", "timestamp"},
		{"2024-01-15 10:30:00", "timestamp"},
		{"2024-01-15T10:30:00.123456", "timestamp"},
		{"2024-01-15T10:30:00Z", "timestamp_tz[UTC]"},
		{"2024-01-15 10:30:00+04:00", "timestamp_tz[UTC]"},
		{"2024-01-15T10:30+04:00", "timestamp_tz[UTC]"},
		{"2024-01-15 25:00:00", "string"},
		{"ACC001", "string"},
		{" 42", "int64"}, // surrounding whitespace is ignored
		{" 2024-01-15 ", "date"},
	}
	for _, tt := range tests {
		if got := DetectTextType(tt.in).String(); got != tt.want {
			t.Errorf("DetectTextType(%q)=%s, want %s", tt.in, got, tt.want)
		}
	}
}

func scanText(t *testing.T, overrides map[string]string, header []string, rows ...[]string) *FileTypeResult {
	t.Helper()
	s, err := NewFileTypeScanner(true, overrides)
	if err != nil {
		t.Fatalf("scanner: %v", err)
	}
	for _, row := range rows {
		doc := map[string]any{}
		for i, h := range header {
			if row[i] == "" {
				doc[h] = nil
			} else {
				doc[h] = row[i]
			}
		}
		s.Observe(doc)
	}
	return s.Result(header)
}

func typeOf(r *FileTypeResult, column string) string {
	for _, c := range r.Columns {
		if c.Name == column {
			return c.Type.String()
		}
	}
	return "<missing>"
}

func warningFor(r *FileTypeResult, column string) *typesystem.TypeWarning {
	for i := range r.Warnings {
		if r.Warnings[i].Column == column {
			return &r.Warnings[i]
		}
	}
	return nil
}

func TestFileTypeScannerDetectsAndMerges(t *testing.T) {
	header := []string{"id", "day", "at", "amount", "code", "flag", "empty", "mixed", "big"}
	r := scanText(t, nil, header,
		[]string{"1", "2024-01-15", "2024-01-15", "10", "007", "true", "", "2024-01-15", "9007199254740993"},
		[]string{"2", "2024-01-16", "2024-01-16 10:30:00", "10.5", "ACC1", "false", "", "soon", "0.5"},
	)
	want := map[string]string{
		"id":     "nullable<int64>",
		"day":    "nullable<date>",
		"at":     "nullable<timestamp>", // date + timestamp widen to timestamp
		"amount": "nullable<float64>",   // int + float widen to float
		"code":   "nullable<string>",
		"flag":   "nullable<bool>",
		"empty":  "nullable<string>",
		"mixed":  "nullable<string>",
		"big":    "nullable<string>", // beyond 2^53 cannot widen to float exactly
	}
	for col, typ := range want {
		if got := typeOf(r, col); got != typ {
			t.Errorf("%s=%s, want %s", col, got, typ)
		}
	}
	for _, col := range []string{"empty", "mixed", "big"} {
		w := warningFor(r, col)
		if w == nil || w.Class != typesystem.MappingSemanticFallback {
			t.Errorf("%s warning=%+v, want a semantic fallback", col, w)
		}
	}
	if w := warningFor(r, "mixed"); w == nil || !strings.Contains(w.Reason, `"soon" in row 2`) {
		t.Errorf("mixed warning=%+v, want the example value and row", w)
	}
	if w := warningFor(r, "id"); w != nil {
		t.Errorf("id has warning %+v, want none", w)
	}
	// Columns keep the file's header order.
	for i, c := range r.Columns {
		if c.Name != header[i] {
			t.Fatalf("column %d=%s, want %s", i, c.Name, header[i])
		}
	}
}

func TestFileTypeScannerOverrides(t *testing.T) {
	header := []string{"fits", "bad", "notnull", "srcnull", "srcnotnull", "allempty"}
	overrides := map[string]string{
		"fits":       "int32",
		"BAD":        "date", // matched case-insensitively
		"notnull":    "int64",
		"srcnull":    "nullable<source>",
		"srcnotnull": "source",
		"allempty":   "nullable<int64>",
		"absent":     "date", // not in the file: ignored
	}
	r := scanText(t, overrides, header,
		[]string{"1", "2024-01-15", "5", "2024-01-15", "x", ""},
		[]string{"2", "soon", "", "", "y", ""},
	)
	want := map[string]string{
		"fits":       "int32",
		"bad":        "string",
		"notnull":    "nullable<int64>",
		"srcnull":    "nullable<date>",
		"srcnotnull": "string",
		"allempty":   "nullable<int64>",
	}
	for col, typ := range want {
		if got := typeOf(r, col); got != typ {
			t.Errorf("%s=%s, want %s", col, got, typ)
		}
	}
	if w := warningFor(r, "bad"); w == nil || w.Class != typesystem.MappingOverrideFallback || w.LogicalType != "date" || !strings.Contains(w.Reason, `"soon" in row 2 is not a valid date`) {
		t.Errorf("bad warning=%+v", w)
	}
	if w := warningFor(r, "notnull"); w == nil || !strings.Contains(w.Reason, "1 row(s) have no value") {
		t.Errorf("notnull warning=%+v", w)
	}
	for _, col := range []string{"fits", "srcnull", "srcnotnull", "allempty"} {
		if w := warningFor(r, col); w != nil {
			t.Errorf("%s has warning %+v, want none", col, w)
		}
	}
	if typeOf(r, "absent") != "<missing>" {
		t.Errorf("override for a column not in the file must not add it")
	}
}

func TestFileTypeScannerRejectsInvalidOverride(t *testing.T) {
	if _, err := NewFileTypeScanner(true, map[string]string{"a": "not a type<"}); err == nil {
		t.Fatal("want an error for an invalid override")
	}
}

func TestFileTypeScannerTypedValues(t *testing.T) {
	// JSON keeps decoded types: strings are not detected, missing keys are NULL.
	s, err := NewFileTypeScanner(false, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.Observe(map[string]any{"n": float64(1), "d": "2024-01-15", "obj": map[string]any{"a": 1.0}})
	s.Observe(map[string]any{"n": float64(2)})
	r := s.Result(nil)
	if typeOf(r, "n") != "nullable<float64>" || typeOf(r, "d") != "nullable<string>" || typeOf(r, "obj") != "nullable<string>" {
		t.Fatalf("types=%+v", r.Columns)
	}
	if warningFor(r, "obj") == nil {
		t.Fatal("nested object stored as text should warn")
	}
}

func TestFileSchemaConvertsCSVRows(t *testing.T) {
	header := []string{"row_id", "date", "posting_at", "account_code", "x_balance", "transaction_count", "status"}
	rows := [][]string{
		{"1", "2024-01-15", "2024-01-15 09:00:00", "0012", "100.25", "3", "active"},
		{"2", "2024-02-01", "2024-02-01T10:30:00", "ACC-7", "-5.5", "", "closed"},
	}
	r := scanText(t, map[string]string{"row_id": "int64"}, header, rows...)
	fs, err := NewFileSchema(header, r.Types())
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	wantIDs := []arrow.Type{arrow.INT64, arrow.DATE32, arrow.TIMESTAMP, arrow.STRING, arrow.FLOAT64, arrow.INT64, arrow.STRING}
	for i, f := range fs.Schema.Fields() {
		if f.Name != header[i] || f.Type.ID() != wantIDs[i] {
			t.Fatalf("field %d=%s %s, want %s %s", i, f.Name, f.Type, header[i], wantIDs[i])
		}
	}
	if fs.Schema.Field(0).Nullable {
		t.Fatal("row_id was overridden as NOT NULL")
	}

	docs := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		doc := map[string]any{}
		for i, h := range header {
			if row[i] != "" {
				doc[h] = row[i]
			}
		}
		docs = append(docs, doc)
	}
	rec, err := FileDocsToRecord(memory.NewGoAllocator(), fs, docs)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	defer rec.Release()

	if got := rec.Column(1).(*array.Date32).Value(1).ToTime(); !got.Equal(time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("date=%v", got)
	}
	ts := rec.Column(2).(*array.Timestamp)
	unit := fs.Schema.Field(2).Type.(*arrow.TimestampType).Unit
	if got := ts.Value(1).ToTime(unit); !got.Equal(time.Date(2024, 2, 1, 10, 30, 0, 0, time.UTC)) {
		t.Errorf("timestamp=%v", got)
	}
	if got := rec.Column(3).(*array.String).Value(0); got != "0012" {
		t.Errorf("account_code=%q, want leading zeros kept", got)
	}
	if got := rec.Column(4).(*array.Float64).Value(1); got != -5.5 {
		t.Errorf("x_balance=%v", got)
	}
	if !rec.Column(5).IsNull(1) {
		t.Error("empty transaction_count should be NULL")
	}
}

func TestFileDocsToRecordRejectsNullInNotNullColumn(t *testing.T) {
	fs, err := NewFileSchema([]string{"id"}, map[string]string{"id": "int64"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FileDocsToRecord(memory.NewGoAllocator(), fs, []map[string]any{{"id": nil}}); err == nil {
		t.Fatal("want an error for NULL in a NOT NULL column")
	}
}

func TestNewFileSchemaOrdersUnlistedColumnsByName(t *testing.T) {
	fs, err := NewFileSchema([]string{"b"}, map[string]string{"c": "string", "a": "string", "b": "string"})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, f := range fs.Fields {
		names = append(names, f.Name)
	}
	if strings.Join(names, ",") != "b,a,c" {
		t.Fatalf("order=%v, want b,a,c", names)
	}
}

func TestFileTypeScannerNullTokens(t *testing.T) {
	header := []string{"amount", "day", "label", "only_tokens", "strict"}
	r := scanText(t, map[string]string{"strict": "int64"}, header,
		[]string{"10.5", "2024-01-15", "N/A", "null", "1"},
		[]string{"N/A", "NULL", "ok", "NA", "nan"},
		[]string{"3", " 2024-01-17", "none", "", "3"},
	)
	want := map[string]string{
		"amount":      "nullable<float64>", // "N/A" is NULL in a number column
		"day":         "nullable<date>",
		"label":       "nullable<string>", // a string column keeps the markers as text
		"only_tokens": "nullable<string>",
		"strict":      "nullable<int64>", // NOT NULL requested, but "nan" is no value
	}
	for col, typ := range want {
		if got := typeOf(r, col); got != typ {
			t.Errorf("%s=%s, want %s", col, got, typ)
		}
	}
	if w := warningFor(r, "amount"); w == nil || !strings.Contains(w.Reason, `1 value(s) such as "N/A" mean "no value"`) {
		t.Errorf("amount warning=%+v", w)
	}
	if w := warningFor(r, "strict"); w == nil || w.Class != typesystem.MappingOverrideFallback || !strings.Contains(w.Reason, "not nullable") {
		t.Errorf("strict warning=%+v", w)
	}
	for _, col := range []string{"label", "only_tokens"} {
		if w := warningFor(r, col); w != nil {
			t.Errorf("%s has warning %+v, want none", col, w)
		}
	}

	fs, err := NewFileSchema(header, r.Types())
	if err != nil {
		t.Fatal(err)
	}
	rec, err := FileDocsToRecord(memory.NewGoAllocator(), fs, []map[string]any{
		{"amount": "N/A", "day": " 2024-01-17", "label": "N/A", "only_tokens": "NA", "strict": "nan"},
	})
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	defer rec.Release()
	if !rec.Column(0).IsNull(0) || !rec.Column(4).IsNull(0) {
		t.Error("markers in typed columns must be NULL")
	}
	if got := rec.Column(1).(*array.Date32).Value(0).ToTime(); !got.Equal(time.Date(2024, 1, 17, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("day=%v", got)
	}
	if got := rec.Column(2).(*array.String).Value(0); got != "N/A" {
		t.Errorf("label=%q, want the marker kept as text", got)
	}
}

func TestFileTypeScannerOverridesStayLossless(t *testing.T) {
	header := []string{"code", "signed", "big_id", "amount", "fits"}
	overrides := map[string]string{"code": "int64", "signed": "int32", "big_id": "float64", "amount": "float64", "fits": "nullable<int64>"}
	r := scanText(t, overrides, header,
		[]string{"007", "+5", "9007199254740993", "1.5e3", "1"},
		[]string{"12", "6", "1", "-2.25", " 2 "},
	)
	want := map[string]string{
		"code":   "string", // int64 would drop the leading zeros of "007"
		"signed": "string",
		"big_id": "string", // float64 would round it
		"amount": "float64",
		"fits":   "nullable<int64>",
	}
	for col, typ := range want {
		if got := typeOf(r, col); got != typ {
			t.Errorf("%s=%s, want %s", col, got, typ)
		}
	}
	if w := warningFor(r, "code"); w == nil || w.Class != typesystem.MappingOverrideFallback || !strings.Contains(w.Reason, `"007" in row 1 is not a valid int64`) {
		t.Errorf("code warning=%+v", w)
	}
}

func TestFileTypeScannerWarnsForLocaleFormats(t *testing.T) {
	header := []string{"eu_date", "us_amount", "pct", "phone", "plain"}
	r := scanText(t, nil, header,
		[]string{"15/01/2024", "1,234.50", "12.5%", "+374 10 123456", "abc"},
		[]string{"16/01/2024", "2,000", "-3%", "+374 10 654321", "15/01/2024"},
	)
	for _, col := range header {
		if got := typeOf(r, col); got != "nullable<string>" {
			t.Errorf("%s=%s, want nullable<string>", col, got)
		}
	}
	if w := warningFor(r, "eu_date"); w == nil || !strings.Contains(w.Reason, "non-ISO format") {
		t.Errorf("eu_date warning=%+v", w)
	}
	for _, col := range []string{"us_amount", "pct"} {
		if w := warningFor(r, col); w == nil || !strings.Contains(w.Reason, "formatted numbers") {
			t.Errorf("%s warning=%+v", col, w)
		}
	}
	for _, col := range []string{"phone", "plain"} { // not every value looks like a date or number
		if w := warningFor(r, col); w != nil {
			t.Errorf("%s has warning %+v, want none", col, w)
		}
	}
}

// TestFileTypeScannerTypesBankingCSV is the shape of a real upload whose
// dates and amounts were stored as strings: ISO dates, plain decimals,
// integer counts and codes with leading zeros.
func TestFileTypeScannerTypesBankingCSV(t *testing.T) {
	header := []string{"row_id", "date", "settlement_date", "account_code", "currency", "x_balance", "balance_delta", "balance_change_pct", "transaction_count", "balance_band"}
	rows := [][]string{
		{"1", "2026-01-01", "2026-01-03", "0101-CHECKING", "AMD", "189606.0", "9834.47", "5.4705", "27", "00-LOW"},
		{"2", "2026-01-01", "2026-01-04", "0102-CURRENT", "USD", "238248.14", "-604.38", "-0.253", "13", "00-LOW"},
		{"3", "2026-01-02", "2026-01-03", "0110-PETTY", "EUR", "289980.78", "16467.09", "6.0206", "28", "01-MEDIUM"},
	}
	for _, overrides := range []map[string]string{nil, {"row_id": "int64"}} {
		r := scanText(t, overrides, header, rows...)
		want := map[string]string{
			"date": "nullable<date>", "settlement_date": "nullable<date>",
			"account_code": "nullable<string>", "currency": "nullable<string>", "balance_band": "nullable<string>",
			"x_balance": "nullable<float64>", "balance_delta": "nullable<float64>", "balance_change_pct": "nullable<float64>",
			"transaction_count": "nullable<int64>",
		}
		for col, typ := range want {
			if got := typeOf(r, col); got != typ {
				t.Errorf("overrides=%v: %s=%s, want %s", overrides, col, got, typ)
			}
		}
		if len(r.Warnings) != 0 {
			t.Errorf("overrides=%v: warnings=%+v, want none", overrides, r.Warnings)
		}
	}
}

func TestFileTypeScannerJSONNumbersAreExact(t *testing.T) {
	// S3 JSON files decode numbers as json.Number, so integers stay integers
	// and IDs beyond 2^53 are never rounded through float64.
	s, err := NewFileTypeScanner(false, map[string]string{"qty": "int32", "ratio": "int64"})
	if err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": json.Number("9007199254740993"), "amount": json.Number("10.25"), "qty": json.Number("3"), "ratio": json.Number("1.5"), "huge": json.Number("123456789012345678901")},
		{"id": json.Number("2"), "amount": json.Number("7"), "qty": json.Number("4"), "ratio": json.Number("2"), "huge": json.Number("98765432109876543210")},
	}
	for _, row := range rows {
		s.Observe(row)
	}
	r := s.Result([]string{"id", "amount", "qty", "ratio", "huge"})
	want := map[string]string{
		"id":     "nullable<int64>",
		"amount": "nullable<float64>",
		"qty":    "int32",
		"ratio":  "string", // 1.5 is not an int64
		"huge":   "nullable<string>",
	}
	for col, typ := range want {
		if got := typeOf(r, col); got != typ {
			t.Errorf("%s=%s, want %s", col, got, typ)
		}
	}
	if w := warningFor(r, "huge"); w == nil || !strings.Contains(w.Reason, "cannot be stored exactly") {
		t.Errorf("huge warning=%+v", w)
	}

	fs, err := NewFileSchema(nil, r.Types())
	if err != nil {
		t.Fatal(err)
	}
	rec, err := FileDocsToRecord(memory.NewGoAllocator(), fs, rows)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	defer rec.Release()
	for i, f := range fs.Schema.Fields() {
		switch f.Name {
		case "id":
			if got := rec.Column(i).(*array.Int64).Value(0); got != 9007199254740993 {
				t.Errorf("id=%d, want the exact value", got)
			}
		case "huge":
			if got := rec.Column(i).(*array.String).Value(0); got != "123456789012345678901" {
				t.Errorf("huge=%q", got)
			}
		case "ratio":
			if got := rec.Column(i).(*array.String).Value(0); got != "1.5" {
				t.Errorf("ratio=%q", got)
			}
		}
	}
}
