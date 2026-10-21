package icebergpart

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
)

// oneColumn builds a single-column batch; build appends the values.
func oneColumn(t *testing.T, name string, dt arrow.DataType, build func(array.Builder)) arrow.RecordBatch {
	t.Helper()
	b := array.NewBuilder(memory.DefaultAllocator, dt)
	defer b.Release()
	build(b)
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: name, Type: dt, Nullable: true}}, nil), []arrow.Array{col}, int64(col.Len()))
}

func micros(t *testing.T, ts string) arrow.Timestamp {
	t.Helper()
	v, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		t.Fatal(err)
	}
	return arrow.Timestamp(v.UnixMicro())
}

// decode returns the single group's partition value as the table would store
// it, using the transform's result type for the given source type.
func decode(t *testing.T, g *Group, transform string, src iceberg.Type) any {
	t.Helper()
	tr, err := iceberg.ParseTransform(transform)
	if err != nil {
		t.Fatal(err)
	}
	lit, err := g.Contract.Values[0].Literal(tr.ResultType(src))
	if err != nil {
		t.Fatal(err)
	}
	if lit == nil {
		return nil
	}
	return lit.Any()
}

func TestSplitMatchesIcebergTransforms(t *testing.T) {
	ny := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "America/New_York"}
	cases := []struct {
		name      string
		transform string
		dt        arrow.DataType
		src       iceberg.Type
		build     func(array.Builder)
		want      any
	}{
		// Expected bucket values come from the Iceberg spec's hash table:
		// hash(34L)=2017239379, hash("iceberg")=1210000089.
		{"bucket long", "bucket[16]", arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64,
			func(b array.Builder) { b.(*array.Int64Builder).Append(34) }, int32(3)},
		{"bucket int hashes like long", "bucket[16]", arrow.PrimitiveTypes.Int32, iceberg.PrimitiveTypes.Int32,
			func(b array.Builder) { b.(*array.Int32Builder).Append(34) }, int32(3)},
		{"bucket string", "bucket[16]", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String,
			func(b array.Builder) { b.(*array.StringBuilder).Append("iceberg") }, int32(9)},
		{"truncate negative int", "truncate[10]", arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64,
			func(b array.Builder) { b.(*array.Int64Builder).Append(-1) }, int64(-10)},
		{"truncate string", "truncate[3]", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String,
			func(b array.Builder) { b.(*array.StringBuilder).Append("iceberg") }, "ice"},
		{"identity string", "identity", arrow.BinaryTypes.String, iceberg.PrimitiveTypes.String,
			func(b array.Builder) { b.(*array.StringBuilder).Append("eu-west") }, "eu-west"},
		{"month date leap day", "month", arrow.FixedWidthTypes.Date32, iceberg.PrimitiveTypes.Date,
			func(b array.Builder) { b.(*array.Date32Builder).Append(arrow.Date32(19782)) }, int32((2024-1970)*12 + 1)},
		{"year date", "year", arrow.FixedWidthTypes.Date32, iceberg.PrimitiveTypes.Date,
			func(b array.Builder) { b.(*array.Date32Builder).Append(arrow.Date32(19782)) }, int32(2024 - 1970)},
		// 21:00 in New York on Jan 1 is 02:00 UTC on Jan 2; timestamptz
		// partitions use UTC, so the day is 2024-01-02 (19724).
		{"day zoned timestamp uses UTC", "day", ny, iceberg.PrimitiveTypes.TimestampTz,
			func(b array.Builder) {
				b.(*array.TimestampBuilder).Append(micros(t, "2024-01-01T21:00:00-05:00"))
			}, int32(19724)},
		{"hour zoned timestamp uses UTC", "hour", ny, iceberg.PrimitiveTypes.TimestampTz,
			func(b array.Builder) {
				b.(*array.TimestampBuilder).Append(micros(t, "2024-01-01T21:00:00-05:00"))
			}, int32(19724*24 + 2)},
		{"day local timestamp at midnight", "day", arrow.FixedWidthTypes.Timestamp_us, iceberg.PrimitiveTypes.Timestamp,
			func(b array.Builder) { b.(*array.TimestampBuilder).Append(micros(t, "2024-01-02T00:00:00Z")) }, int32(19724)},
		// One nanosecond before the epoch belongs to 1969-12-31, not 1970-01-01.
		{"day before epoch in nanoseconds", "day", arrow.FixedWidthTypes.Timestamp_ns, iceberg.PrimitiveTypes.Timestamp,
			func(b array.Builder) { b.(*array.TimestampBuilder).Append(-1) }, int32(-1)},
		{"null source is a null partition", "bucket[16]", arrow.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Int64,
			func(b array.Builder) { b.AppendNull() }, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := NewPartitioner(Spec{{Source: "c", Transform: tc.transform}})
			if err != nil {
				t.Fatal(err)
			}
			rec := oneColumn(t, "c", tc.dt, tc.build)
			defer rec.Release()
			groups, err := p.Split(rec)
			if err != nil {
				t.Fatal(err)
			}
			if len(groups) != 1 {
				t.Fatalf("groups=%d want 1", len(groups))
			}
			if got := decode(t, groups[0], tc.transform, tc.src); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("partition value=%#v (%T) want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

func TestSplitGroupsRowsInFirstSeenOrder(t *testing.T) {
	p, err := NewPartitioner(Spec{{Source: "region"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := oneColumn(t, "region", arrow.BinaryTypes.String, func(b array.Builder) {
		sb := b.(*array.StringBuilder)
		sb.Append("b")
		sb.Append("a")
		sb.Append("b")
		sb.AppendNull()
	})
	defer rec.Release()
	groups, err := p.Split(rec)
	if err != nil {
		t.Fatal(err)
	}
	var rows [][]int64
	for _, g := range groups {
		rows = append(rows, g.Rows)
	}
	if !reflect.DeepEqual(rows, [][]int64{{0, 2}, {1}, {3}}) {
		t.Fatalf("rows=%v", rows)
	}

	part, err := SelectRows(context.Background(), rec, groups[0].Rows)
	if err != nil {
		t.Fatal(err)
	}
	defer part.Release()
	col := part.Column(0).(*array.String)
	if part.NumRows() != 2 || col.Value(0) != "b" || col.Value(1) != "b" {
		t.Fatalf("selected rows=%v", part)
	}
}

func TestContractRoundTripWidensToTableType(t *testing.T) {
	p, err := NewPartitioner(Spec{{Source: "id"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := oneColumn(t, "id", arrow.PrimitiveTypes.Int32, func(b array.Builder) { b.(*array.Int32Builder).Append(7) })
	defer rec.Release()
	groups, err := p.Split(rec)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := groups[0].Contract.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseContract(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !c.Spec.Equal(Spec{{Source: "id", Transform: "identity"}}) {
		t.Fatalf("spec=%v", c.Spec)
	}
	// The table column may be a long while this file's column is an int.
	lit, err := c.Values[0].Literal(iceberg.PrimitiveTypes.Int64)
	if err != nil {
		t.Fatal(err)
	}
	if lit.Any() != int64(7) {
		t.Fatalf("value=%#v want int64(7)", lit.Any())
	}
}

func TestSpecEqual(t *testing.T) {
	a := Spec{{Source: "id", Name: "id_bucket", Transform: "Bucket[16]"}, {Source: "ts", Transform: "day"}}
	if !a.Equal(Spec{{Source: "id", Transform: "bucket[16]"}, {Source: "ts", Name: "ts_day", Transform: "day"}}) {
		t.Fatal("specs that differ only in field names and case must be equal")
	}
	if a.Equal(Spec{{Source: "id", Transform: "bucket[8]"}, {Source: "ts", Transform: "day"}}) {
		t.Fatal("different bucket counts must not be equal")
	}
	if a.Equal(Spec{{Source: "id", Transform: "bucket[16]"}}) {
		t.Fatal("different field counts must not be equal")
	}
}

func TestSplitRejectsInvalidSources(t *testing.T) {
	rec := oneColumn(t, "f", arrow.PrimitiveTypes.Float64, func(b array.Builder) { b.(*array.Float64Builder).Append(1.5) })
	defer rec.Release()
	for spec, want := range map[string]string{
		"bucket[4]": "cannot be applied",
		"missing":   "not a unique top-level column",
	} {
		source, transform := "f", spec
		if spec == "missing" {
			source, transform = "nope", "identity"
		}
		p, err := NewPartitioner(Spec{{Source: source, Transform: transform}})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Split(rec); err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("%s: error=%v want %q", spec, err, want)
		}
	}
}

func TestParseSpecJSON(t *testing.T) {
	spec, err := ParseSpecJSON(`[{"source":" id ","transform":"BUCKET[4]"},{"source":"region"}]`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, Spec{{Source: "id", Transform: "bucket[4]"}, {Source: "region", Transform: "identity"}}) {
		t.Fatalf("spec=%#v", spec)
	}
	if spec, err := ParseSpecJSON(""); err != nil || spec != nil {
		t.Fatalf("empty spec=%v err=%v", spec, err)
	}
	if _, err := ParseSpecJSON(`[{"source":"id","transform":"nope"}]`); err == nil {
		t.Fatal("invalid transform must be rejected")
	}
}
