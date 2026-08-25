package connectors

import (
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestQuoteOracleMultipartIdent(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "orders", want: `"ORDERS"`},
		{in: "sales.orders", want: `"SALES"."ORDERS"`},
		{in: `"Sales"."Orders"`, want: `"Sales"."Orders"`},
	}
	for _, tc := range tests {
		got, err := quoteOracleMultipartIdent(tc.in)
		if err != nil {
			t.Fatalf("quoteOracleMultipartIdent(%q): %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("quoteOracleMultipartIdent(%q)=%q want %q", tc.in, got, tc.want)
		}
	}
}

func TestQuoteOracleMultipartIdentEscapesQuotedParts(t *testing.T) {
	got, err := quoteOracleMultipartIdent(`"HR"."O""Rayelly"`)
	if err != nil || got != `"HR"."O""Rayelly"` {
		t.Fatalf("quoteOracleMultipartIdent=%q, %v", got, err)
	}
	if _, err := quoteOracleMultipartIdent(`hr..orders`); err == nil {
		t.Fatal("expected empty part error")
	}
	if _, err := quoteOracleMultipartIdent(`hr..orders`); err == nil {
		t.Fatal("empty qualified identifier part must remain invalid")
	}
}

func TestBuildOracleAmbiguousNumberProbeQueries(t *testing.T) {
	fractionalSQL, boundsSQL, err := buildOracleAmbiguousNumberProbeQueries("sales.orders", "order_id")
	if err != nil {
		t.Fatalf("buildOracleAmbiguousNumberProbeQueries: %v", err)
	}
	if fractionalSQL != `SELECT 1 FROM "SALES"."ORDERS" WHERE "ORDER_ID" != TRUNC("ORDER_ID") AND ROWNUM = 1` {
		t.Fatalf("fractionalSQL=%q", fractionalSQL)
	}
	if boundsSQL != `SELECT MIN("ORDER_ID"), MAX("ORDER_ID") FROM "SALES"."ORDERS"` {
		t.Fatalf("boundsSQL=%q", boundsSQL)
	}
}

func TestBuildOracleAmbiguousNumberProbeQueriesQuotesUnusualNames(t *testing.T) {
	fractionalSQL, _, err := buildOracleAmbiguousNumberProbeQueries(`"Order Items"`, `O"Rayelly`)
	if err != nil || !strings.Contains(fractionalSQL, `"O""Rayelly"`) {
		t.Fatalf("unusual identifier SQL=%q err=%v", fractionalSQL, err)
	}
}

func TestClassifyOracleCursorType(t *testing.T) {
	tests := []struct {
		name         string
		typeName     string
		precision    int64
		scale        int64
		hasPrecision bool
		hasScale     bool
		want         oracleCursorTypeClass
	}{
		{
			name:         "number18",
			typeName:     "NUMBER",
			precision:    18,
			scale:        0,
			hasPrecision: true,
			hasScale:     true,
			want:         oracleCursorTypeClass{Domain: CursorDomainInt64, Orderable: true, RangeCapable: true},
		},
		{
			name:         "number20",
			typeName:     "NUMBER",
			precision:    20,
			scale:        0,
			hasPrecision: true,
			hasScale:     true,
			want:         oracleCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false},
		},
		{
			name:         "number_decimal",
			typeName:     "NUMBER",
			precision:    10,
			scale:        2,
			hasPrecision: true,
			hasScale:     true,
			want:         oracleCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false},
		},
		{
			name:     "number_missing_precision_scale",
			typeName: "NUMBER",
			want:     oracleCursorTypeClass{Domain: CursorDomainDecimal, Orderable: true, RangeCapable: false},
		},
		{
			name:     "date",
			typeName: "DATE",
			want:     oracleCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true},
		},
		{
			name:     "timestamp",
			typeName: "TIMESTAMP(6)",
			want:     oracleCursorTypeClass{Domain: CursorDomainTimestamp, Orderable: true, RangeCapable: true},
		},
		{
			name:     "timestamp_with_timezone",
			typeName: "TIMESTAMP WITH TIME ZONE",
			want:     oracleCursorTypeClass{},
		},
		{
			name:     "timestamp_with_local_timezone",
			typeName: "TIMESTAMP WITH LOCAL TIME ZONE",
			want:     oracleCursorTypeClass{},
		},
		{
			name:     "varchar2",
			typeName: "VARCHAR2",
			want:     oracleCursorTypeClass{Domain: CursorDomainString, Orderable: true, RangeCapable: false},
		},
		{
			name:     "blob",
			typeName: "BLOB",
			want:     oracleCursorTypeClass{},
		},
		{
			name:     "float",
			typeName: "FLOAT",
			want:     oracleCursorTypeClass{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyOracleCursorType(tc.typeName, tc.precision, tc.scale, tc.hasPrecision, tc.hasScale)
			if got != tc.want {
				t.Fatalf("classifyOracleCursorType(%q)=%+v want %+v", tc.typeName, got, tc.want)
			}
		})
	}
}

type oracleTestStringer string

func (s oracleTestStringer) String() string { return string(s) }

func TestParseOracleNumberAsInt64Strict(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		want    int64
		wantErr bool
	}{
		{name: "string", value: "123", want: 123},
		{name: "plus_string", value: "+123", want: 123},
		{name: "minus_string", value: "-123", want: -123},
		{name: "trimmed_string", value: " 123 ", want: 123},
		{name: "decimal_rejected", value: "123.0", wantErr: true},
		{name: "scientific_rejected", value: "1e3", wantErr: true},
		{name: "max_int64", value: "9223372036854775807", want: math.MaxInt64},
		{name: "overflow_positive", value: "9223372036854775808", wantErr: true},
		{name: "min_int64", value: "-9223372036854775808", want: math.MinInt64},
		{name: "overflow_negative", value: "-9223372036854775809", wantErr: true},
		{name: "bytes", value: []byte("123"), want: 123},
		{name: "uint64_max_safe", value: uint64(math.MaxInt64), want: math.MaxInt64},
		{name: "uint64_overflow", value: uint64(math.MaxInt64) + 1, wantErr: true},
		{name: "float64_rejected", value: float64(123), wantErr: true},
		{name: "stringer", value: oracleTestStringer("456"), want: 456},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseOracleNumberAsInt64Strict(tc.value)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseOracleNumberAsInt64Strict(%#v) expected error", tc.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOracleNumberAsInt64Strict(%#v): %v", tc.value, err)
			}
			if got != tc.want {
				t.Fatalf("parseOracleNumberAsInt64Strict(%#v)=%d want %d", tc.value, got, tc.want)
			}
		})
	}
}

func TestValidateOracleAmbiguousNumberProbeResult(t *testing.T) {
	tests := []struct {
		name            string
		cursorColumn    string
		fractionalFound bool
		minv            any
		maxv            any
		wantClass       oracleCursorTypeClass
		wantErrContains string
	}{
		{
			name:         "safe_strings",
			cursorColumn: "ID",
			minv:         "1",
			maxv:         "9",
			wantClass:    oracleInt64CursorTypeClass(),
		},
		{
			name:         "safe_mixed_runtime_types",
			cursorColumn: "ID",
			minv:         int64(-5),
			maxv:         uint64(10),
			wantClass:    oracleInt64CursorTypeClass(),
		},
		{
			name:            "fractional",
			cursorColumn:    "ID",
			fractionalFound: true,
			wantErrContains: "contains fractional values",
		},
		{
			name:            "overflow",
			cursorColumn:    "ID",
			minv:            "1",
			maxv:            "9223372036854775808",
			wantErrContains: "not safely representable as int64",
		},
		{
			name:            "decimal_string",
			cursorColumn:    "ID",
			minv:            "1",
			maxv:            "123.0",
			wantErrContains: "not safely representable as int64",
		},
		{
			name:            "scientific_notation",
			cursorColumn:    "ID",
			minv:            "1",
			maxv:            "1e3",
			wantErrContains: "not safely representable as int64",
		},
		{
			name:         "empty_table",
			cursorColumn: "ID",
			wantClass:    oracleInt64CursorTypeClass(),
		},
		{
			name:            "incomplete_bounds",
			cursorColumn:    "ID",
			minv:            nil,
			maxv:            "10",
			wantErrContains: "incomplete min/max bounds",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := validateOracleAmbiguousNumberProbeResult(tc.cursorColumn, tc.fractionalFound, tc.minv, tc.maxv)
			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("validateOracleAmbiguousNumberProbeResult expected error")
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error=%q want substring %q", err.Error(), tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("validateOracleAmbiguousNumberProbeResult: %v", err)
			}
			if got != tc.wantClass {
				t.Fatalf("class=%+v want %+v", got, tc.wantClass)
			}
		})
	}
}

func TestBuildOracleCursorQuery(t *testing.T) {
	ts := time.Date(2026, 5, 6, 10, 11, 12, 0, time.UTC)
	tests := []struct {
		name     string
		query    CursorQuery
		wantSQL  string
		wantArgs []any
	}{
		{
			name: "no_bounds",
			query: CursorQuery{
				Table:        "sales.orders",
				CursorColumn: "order_id",
				CursorDomain: CursorDomainInt64,
			},
			wantSQL:  `SELECT * FROM "SALES"."ORDERS" ORDER BY "ORDER_ID" ASC`,
			wantArgs: []any{},
		},
		{
			name: "lower_only",
			query: CursorQuery{
				Table:          "sales.orders",
				CursorColumn:   "order_id",
				CursorDomain:   CursorDomainInt64,
				LowerBound:     "100",
				LowerExclusive: true,
			},
			wantSQL:  `SELECT * FROM "SALES"."ORDERS" WHERE "ORDER_ID" > :1 ORDER BY "ORDER_ID" ASC`,
			wantArgs: []any{int64(100)},
		},
		{
			name: "upper_only",
			query: CursorQuery{
				Table:          "sales.orders",
				CursorColumn:   "order_id",
				CursorDomain:   CursorDomainInt64,
				UpperBound:     "250",
				UpperInclusive: true,
			},
			wantSQL:  `SELECT * FROM "SALES"."ORDERS" WHERE "ORDER_ID" <= :1 ORDER BY "ORDER_ID" ASC`,
			wantArgs: []any{int64(250)},
		},
		{
			name: "both_bounds_timestamp",
			query: CursorQuery{
				Table:          "sales.orders",
				CursorColumn:   "created_at",
				CursorDomain:   CursorDomainTimestamp,
				LowerBound:     "2026-05-01T00:00:00Z",
				UpperBound:     "2026-05-06T10:11:12Z",
				LowerExclusive: true,
				UpperInclusive: true,
			},
			wantSQL: `SELECT * FROM "SALES"."ORDERS" WHERE "CREATED_AT" > :1 AND "CREATED_AT" <= :2 ORDER BY "CREATED_AT" ASC`,
			wantArgs: []any{
				time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
				ts,
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSQL, gotArgs, err := buildOracleCursorQuery(tc.query)
			if err != nil {
				t.Fatalf("buildOracleCursorQuery: %v", err)
			}
			if gotSQL != tc.wantSQL {
				t.Fatalf("SQL=%q want %q", gotSQL, tc.wantSQL)
			}
			if !reflect.DeepEqual(gotArgs, tc.wantArgs) {
				t.Fatalf("args=%#v want %#v", gotArgs, tc.wantArgs)
			}
		})
	}
}
