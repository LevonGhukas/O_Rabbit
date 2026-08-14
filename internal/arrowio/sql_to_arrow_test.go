package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func buildFloat64Array(t *testing.T, values ...any) (*array.Float64, []error) {
	t.Helper()

	plan := planFloat64("col")
	builder := plan.Builder(memory.NewGoAllocator())
	t.Cleanup(builder.Release)

	errs := make([]error, 0, len(values))
	for _, v := range values {
		errs = append(errs, plan.Append(builder, v))
	}

	raw := builder.NewArray()
	arr, ok := raw.(*array.Float64)
	if !ok {
		raw.Release()
		t.Fatalf("NewArray() type = %T, want *array.Float64", raw)
	}
	t.Cleanup(arr.Release)
	return arr, errs
}

func buildBoolArray(t *testing.T, values ...any) (*array.Boolean, []error) {
	t.Helper()

	plan := planBool("col")
	builder := plan.Builder(memory.NewGoAllocator())
	t.Cleanup(builder.Release)

	errs := make([]error, 0, len(values))
	for _, v := range values {
		errs = append(errs, plan.Append(builder, v))
	}

	raw := builder.NewArray()
	arr, ok := raw.(*array.Boolean)
	if !ok {
		raw.Release()
		t.Fatalf("NewArray() type = %T, want *array.Boolean", raw)
	}
	t.Cleanup(arr.Release)
	return arr, errs
}

func TestPlanFloat64AppendConversions(t *testing.T) {
	arr, errs := buildFloat64Array(t,
		float32(1.25),
		int64(2),
		int32(3),
		int(4),
		"5.5",
		[]byte("6.75"),
		"nope",
	)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Append(%d) unexpected error: %v", i, err)
		}
	}

	want := []float64{1.25, 2, 3, 4, 5.5, 6.75}
	for i, v := range want {
		if arr.IsNull(i) {
			t.Fatalf("value %d unexpectedly null", i)
		}
		if got := arr.Value(i); got != v {
			t.Fatalf("value %d = %v, want %v", i, got, v)
		}
	}

	if !arr.IsNull(6) {
		t.Fatalf("parse failure should append null")
	}
}

func TestPlanFloat64AppendUnsupportedType(t *testing.T) {
	_, errs := buildFloat64Array(t, true)
	if errs[0] == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestPlanBoolAppendConversions(t *testing.T) {
	arr, errs := buildBoolArray(t,
		true,
		int64(1),
		int32(0),
		int(5),
		"1",
		"true",
		"false",
		[]byte("true"),
		[]byte("false"),
	)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Append(%d) unexpected error: %v", i, err)
		}
	}

	want := []bool{true, true, false, true, true, true, false, true, false}
	for i, v := range want {
		if arr.IsNull(i) {
			t.Fatalf("value %d unexpectedly null", i)
		}
		if got := arr.Value(i); got != v {
			t.Fatalf("value %d = %v, want %v", i, got, v)
		}
	}
}

func TestPlanBoolAppendUnsupportedType(t *testing.T) {
	_, errs := buildBoolArray(t, 1.5)
	if errs[0] == nil {
		t.Fatal("expected unsupported type error")
	}
}

func TestPlanForSQLColumnTypeOracle(t *testing.T) {
	tests := []struct {
		name       string
		dbType     string
		precision  int64
		scale      int64
		hasDecimal bool
		wantType   arrow.DataType
	}{
		{name: "number_int64", dbType: "NUMBER", precision: 18, scale: 0, hasDecimal: true, wantType: arrow.PrimitiveTypes.Int64},
		{name: "number_string", dbType: "NUMBER", precision: 20, scale: 0, hasDecimal: true, wantType: arrow.BinaryTypes.String},
		{name: "varchar2", dbType: "VARCHAR2", wantType: arrow.BinaryTypes.String},
		{name: "raw", dbType: "RAW", wantType: arrow.BinaryTypes.Binary},
		{name: "blob", dbType: "BLOB", wantType: arrow.BinaryTypes.Binary},
		{name: "date", dbType: "DATE", wantType: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
		{name: "timestamp", dbType: "TIMESTAMP", wantType: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := planForSQLColumnType("col", tc.dbType, tc.precision, tc.scale, tc.hasDecimal)
			if !arrow.TypeEqual(plan.DataType, tc.wantType) {
				t.Fatalf("type=%s want %s", plan.DataType, tc.wantType)
			}
		})
	}
}
