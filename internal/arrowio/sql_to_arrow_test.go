package arrowio

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func buildFloat64Array(t *testing.T, vals ...any) (*array.Float64, []error) {
	t.Helper()
	plan := planFloat64("c_float")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()

	errs := make([]error, len(vals))
	for i, v := range vals {
		errs[i] = plan.Append(b, v)
	}

	arr := b.NewArray().(*array.Float64)
	return arr, errs
}

func buildBoolArray(t *testing.T, vals ...any) (*array.Boolean, []error) {
	t.Helper()
	plan := planBool("c_bool")
	b := plan.Builder(memory.DefaultAllocator)
	defer b.Release()

	errs := make([]error, len(vals))
	for i, v := range vals {
		errs[i] = plan.Append(b, v)
	}

	arr := b.NewArray().(*array.Boolean)
	return arr, errs
}

func TestPlanFloat64AppendConversions(t *testing.T) {
	arr, errs := buildFloat64Array(t,
		float64(1.5),
		float32(2.5),
		int64(3),
		int32(4),
		int16(5),
		int8(6),
		int(7),
		uint64(8),
		uint32(9),
		uint16(10),
		uint8(11),
		uint(12),
		[]byte("13.5"),
		"14.5",
	)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("Append(%d) unexpected error: %v", i, err)
		}
	}

	want := []float64{1.5, 2.5, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13.5, 14.5}
	for i, v := range want {
		if arr.IsNull(i) {
			t.Fatalf("value %d unexpectedly null", i)
		}
		if got := arr.Value(i); got != v {
			t.Fatalf("value %d = %v, want %v", i, got, v)
		}
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
		{name: "number_decimal", dbType: "NUMBER", precision: 20, scale: 0, hasDecimal: true, wantType: &arrow.Decimal128Type{Precision: 20, Scale: 0}},
		{name: "varchar2", dbType: "VARCHAR2", wantType: arrow.BinaryTypes.String},
		{name: "raw", dbType: "RAW", wantType: arrow.BinaryTypes.Binary},
		{name: "blob", dbType: "BLOB", wantType: arrow.BinaryTypes.Binary},
		{name: "date", dbType: "DATE", wantType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
		{name: "timestamp", dbType: "TIMESTAMP", wantType: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: ""}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			plan := PlanForSQLColumn("oracle", "col", tc.dbType, tc.precision, tc.scale, tc.hasDecimal)
			if !arrow.TypeEqual(plan.DataType, tc.wantType) {
				t.Fatalf("type=%s want %s", plan.DataType, tc.wantType)
			}
		})
	}
}
