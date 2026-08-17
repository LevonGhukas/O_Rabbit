package jobopts

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestNormalizedSourceMode(t *testing.T) {
	tests := []struct {
		name string
		in   Options
		want string
	}{
		{
			name: "empty defaults to table",
			in:   Options{},
			want: "table",
		},
		{
			name: "table uppercase normalized",
			in: Options{
				SourceMode: " TABLE ",
			},
			want: "table",
		},
		{
			name: "sql alias normalized",
			in: Options{
				SourceMode: "sql",
			},
			want: "query",
		},
		{
			name: "query stays query",
			in: Options{
				SourceMode: "query",
			},
			want: "query",
		},
		{
			name: "unknown preserved",
			in: Options{
				SourceMode: "custom",
			},
			want: "custom",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.NormalizedSourceMode(); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestNormalizedPartitionStrategy(t *testing.T) {
	tests := []struct {
		name string
		in   Options
		want string
	}{
		{"empty", Options{}, "single"},
		{"single", Options{PartitionStrategy: "single"}, "single"},
		{"legacy int range", Options{PartitionStrategy: "int_range"}, "ordered_cursor"},
		{"cursor alias", Options{PartitionStrategy: "cursor"}, "ordered_cursor"},
		{"ordered cursor", Options{PartitionStrategy: "ordered_cursor"}, "ordered_cursor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.NormalizedPartitionStrategy(); got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
	}
}

func TestEffectiveCursorColumn(t *testing.T) {
	if got := (Options{
		CursorColumn: "created_at",
		IDColumn:     "id",
	}).EffectiveCursorColumn(); got != "created_at" {
		t.Fatalf("got %q", got)
	}

	if got := (Options{
		IDColumn: "id",
	}).EffectiveCursorColumn(); got != "id" {
		t.Fatalf("got %q", got)
	}
}

func TestParseDefaults(t *testing.T) {
	got, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.PartitionStrategy != "single" {
		t.Fatalf("partition strategy=%q", got.PartitionStrategy)
	}

	if got.SourceMode != "table" {
		t.Fatalf("source mode=%q", got.SourceMode)
	}

	if got.CursorColumn != "ID" {
		t.Fatalf("cursor column=%q", got.CursorColumn)
	}

	if got.TargetRowsPerTask != 200_000 {
		t.Fatalf("target rows=%d", got.TargetRowsPerTask)
	}

	if got.MinTasksMultiplier != 2 {
		t.Fatalf("min multiplier=%d", got.MinTasksMultiplier)
	}

	if got.TargetFileBytes != 256*1024*1024 {
		t.Fatalf("target file bytes=%d", got.TargetFileBytes)
	}

	if got.PlannedTasks != 0 {
		t.Fatalf("planned tasks=%d; zero means planner inference", got.PlannedTasks)
	}
}

func TestParseEmptyObjectSameAsNil(t *testing.T) {
	fromNil, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}

	fromEmpty, err := Parse(json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Parse({}): %v", err)
	}

	if !reflect.DeepEqual(fromNil, fromEmpty) {
		t.Fatalf("Parse(nil)=%+v Parse({})=%+v", fromNil, fromEmpty)
	}
}

func TestParseInvalidJSON(t *testing.T) {
	_, err := Parse(json.RawMessage(`{invalid`))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestParseLegacyCursorAlias(t *testing.T) {
	raw := json.RawMessage(`{
		"partition_strategy":"int_range",
		"id_column":"order_id"
	}`)

	got, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if got.PartitionStrategy != "ordered_cursor" {
		t.Fatalf("strategy=%q", got.PartitionStrategy)
	}

	if got.CursorColumn != "order_id" {
		t.Fatalf("cursor=%q", got.CursorColumn)
	}
}

func TestMergeInto(t *testing.T) {
	opts := Options{
		PartitionStrategy: "int_range",
		SourceMode:        "sql",
		Query:             "select * from orders",
		IDColumn:          "id",
	}

	got := opts.MergeInto(nil)

	if got["partition_strategy"] != "ordered_cursor" {
		t.Fatalf("partition_strategy=%v", got["partition_strategy"])
	}

	if got["source_mode"] != "query" {
		t.Fatalf("source_mode=%v", got["source_mode"])
	}

	if got["cursor_column"] != "id" {
		t.Fatalf("cursor_column=%v", got["cursor_column"])
	}
}

func TestParseNilEqualsEmptyJSON(t *testing.T) {
	nilOpts, err := Parse(nil)
	if err != nil {
		t.Fatalf("Parse(nil): %v", err)
	}

	emptyOpts, err := Parse(json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Parse({}): %v", err)
	}

	if !reflect.DeepEqual(nilOpts, emptyOpts) {
		t.Fatalf("nil and empty JSON differ:\n%+v\n%+v", nilOpts, emptyOpts)
	}
}
