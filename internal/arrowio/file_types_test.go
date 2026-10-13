package arrowio

import (
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
		{"1e5", "string"},
		{"NaN", "string"},
		{"3.14", "float64"},
		{"-0.5", "float64"},
		{"0.5", "float64"},
		{".5", "string"},
		{"5.", "string"},
		{"1,234.50", "string"},
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
		{" 42", "string"},
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
		[]string{"2", "2024-01-16", "2024-01-16 10:30:00", "10.5", "ACC1", "false", "", "N/A", "0.5"},
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
	if w := warningFor(r, "mixed"); w == nil || !strings.Contains(w.Reason, `"N/A" in row 2`) {
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
		[]string{"2", "N/A", "", "", "y", ""},
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
	if w := warningFor(r, "bad"); w == nil || w.Class != typesystem.MappingOverrideFallback || w.LogicalType != "date" || !strings.Contains(w.Reason, `"N/A" in row 2 is not a valid date`) {
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
