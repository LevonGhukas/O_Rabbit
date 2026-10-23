package icebergpart

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

// IcebergType is the Iceberg type of an Arrow column, as iceberg-go maps it,
// except that a timestamp in any zone is timestamptz. Arrow stores zoned
// timestamps as UTC instants, which is exactly Iceberg's timestamptz, but
// iceberg-go only accepts the UTC zone names and O_Rabbit keeps the source
// zone (for example America/New_York).
func IcebergType(dt arrow.DataType) (iceberg.Type, error) {
	if ts, ok := dt.(*arrow.TimestampType); ok && ts.TimeZone != "" {
		return iceberg.PrimitiveTypes.TimestampTz, nil
	}
	return icetable.ArrowTypeToIceberg(dt, true)
}

// Partitioner assigns rows to partition tuples.
type Partitioner struct {
	spec       Spec
	transforms []iceberg.Transform
}

// Group is the rows of one record batch that share a partition tuple.
type Group struct {
	// Key identifies the tuple; equal tuples have equal keys across batches.
	Key      string
	Contract Contract
	Rows     []int64
}

// NewPartitioner validates spec and returns a partitioner for it.
func NewPartitioner(spec Spec) (*Partitioner, error) {
	spec = spec.Normalize()
	if len(spec) == 0 {
		return nil, fmt.Errorf("empty iceberg partition spec")
	}
	transforms := make([]iceberg.Transform, len(spec))
	for i, f := range spec {
		t, err := iceberg.ParseTransform(f.Transform)
		if err != nil {
			return nil, fmt.Errorf("partition field %q: %w", f.Source, err)
		}
		transforms[i] = t
	}
	return &Partitioner{spec: spec, transforms: transforms}, nil
}

type boundField struct {
	col     arrow.Array
	srcType iceberg.Type
}

func (p *Partitioner) bind(rec arrow.RecordBatch) ([]boundField, error) {
	out := make([]boundField, len(p.spec))
	for i, f := range p.spec {
		idx := rec.Schema().FieldIndices(f.Source)
		if len(idx) != 1 {
			return nil, fmt.Errorf("partition source column %q is not a unique top-level column of the output", f.Source)
		}
		col := rec.Column(idx[0])
		srcType, err := IcebergType(col.DataType())
		if err != nil {
			return nil, fmt.Errorf("partition source column %q: %w", f.Source, err)
		}
		if !p.transforms[i].CanTransform(srcType) {
			return nil, fmt.Errorf("partition transform %s cannot be applied to column %q of type %s", p.transforms[i], f.Source, srcType)
		}
		out[i] = boundField{col: col, srcType: srcType}
	}
	return out, nil
}

// Split groups the rows of rec by partition tuple, in order of first
// appearance. Every row lands in exactly one group.
func (p *Partitioner) Split(rec arrow.RecordBatch) ([]*Group, error) {
	if rec == nil || rec.NumRows() == 0 {
		return nil, nil
	}
	fields, err := p.bind(rec)
	if err != nil {
		return nil, err
	}
	var (
		groups = map[string]*Group{}
		order  []*Group
		values = make([]*Value, len(fields))
		key    strings.Builder
	)
	for row := 0; row < int(rec.NumRows()); row++ {
		key.Reset()
		for i, f := range fields {
			v, err := p.value(i, f, row)
			if err != nil {
				return nil, err
			}
			values[i] = v
			if v == nil {
				key.WriteString("null|")
				continue
			}
			key.WriteString(v.Type)
			key.WriteByte(':')
			key.WriteString(hex.EncodeToString(v.Data))
			key.WriteByte('|')
		}
		k := key.String()
		g, ok := groups[k]
		if !ok {
			g = &Group{Key: k, Contract: Contract{Version: contractVersion, Spec: p.spec, Values: append([]*Value(nil), values...)}}
			groups[k] = g
			order = append(order, g)
		}
		g.Rows = append(g.Rows, int64(row))
	}
	return order, nil
}

func (p *Partitioner) value(i int, f boundField, row int) (*Value, error) {
	lit, err := arrowLiteral(f.col, row)
	if err != nil {
		return nil, fmt.Errorf("partition source column %q: %w", p.spec[i].Source, err)
	}
	if lit == nil {
		return nil, nil
	}
	if !lit.Type().Equals(f.srcType) {
		if lit, err = lit.To(f.srcType); err != nil {
			return nil, fmt.Errorf("partition source column %q: %w", p.spec[i].Source, err)
		}
	}
	out := p.transforms[i].Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: lit})
	if !out.Valid {
		if _, void := p.transforms[i].(iceberg.VoidTransform); void {
			return nil, nil
		}
		return nil, fmt.Errorf("partition transform %s produced no value for column %q value %s", p.transforms[i], p.spec[i].Source, lit)
	}
	return encodeLiteral(out.Val)
}

// arrowLiteral converts one Arrow value to an Iceberg literal. Temporal values
// are normalized to Iceberg's units (days, microseconds); a nil literal is a
// null value.
func arrowLiteral(col arrow.Array, row int) (iceberg.Literal, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch arr := col.(type) {
	case *array.Boolean:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Int8:
		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Int16:
		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Int32:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Int64:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Uint8:
		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Uint16:
		return iceberg.NewLiteral(int32(arr.Value(row))), nil
	case *array.Uint32:
		return iceberg.NewLiteral(int64(arr.Value(row))), nil
	case *array.Uint64:
		v := arr.Value(row)
		if v > math.MaxInt64 {
			return nil, fmt.Errorf("value %d does not fit an Iceberg long", v)
		}
		return iceberg.NewLiteral(int64(v)), nil
	case *array.Float32:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Float64:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.String:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.LargeString:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.Binary:
		return iceberg.NewLiteral(append([]byte(nil), arr.Value(row)...)), nil
	case *array.LargeBinary:
		return iceberg.NewLiteral(append([]byte(nil), arr.Value(row)...)), nil
	case *extensions.UUIDArray:
		return iceberg.NewLiteral(arr.Value(row)), nil
	case *array.FixedSizeBinary:
		return iceberg.NewLiteral(append([]byte(nil), arr.Value(row)...)), nil
	case *array.Date32:
		return iceberg.NewLiteral(iceberg.Date(arr.Value(row))), nil
	case *array.Date64:
		return iceberg.NewLiteral(iceberg.Date(floorDiv(int64(arr.Value(row)), 86_400_000))), nil
	case *array.Time32:
		unit := arr.DataType().(*arrow.Time32Type).Unit
		return iceberg.NewLiteral(iceberg.Time(toMicros(int64(arr.Value(row)), unit))), nil
	case *array.Time64:
		unit := arr.DataType().(*arrow.Time64Type).Unit
		return iceberg.NewLiteral(iceberg.Time(toMicros(int64(arr.Value(row)), unit))), nil
	case *array.Timestamp:
		// Arrow stores zoned timestamps as UTC instants, which is also what
		// Iceberg's timestamptz transforms use, so no zone conversion applies.
		unit := arr.DataType().(*arrow.TimestampType).Unit
		return iceberg.NewLiteral(iceberg.Timestamp(toMicros(int64(arr.Value(row)), unit))), nil
	case *array.Decimal32:
		return decimalLiteral(decimal128.FromI64(int64(arr.Value(row))), arr.DataType().(*arrow.Decimal32Type).Scale), nil
	case *array.Decimal64:
		return decimalLiteral(decimal128.FromI64(int64(arr.Value(row))), arr.DataType().(*arrow.Decimal64Type).Scale), nil
	case *array.Decimal128:
		return decimalLiteral(arr.Value(row), arr.DataType().(*arrow.Decimal128Type).Scale), nil
	default:
		return nil, fmt.Errorf("arrow type %s is not supported as a partition source", col.DataType())
	}
}

func decimalLiteral(v decimal128.Num, scale int32) iceberg.Literal {
	return iceberg.NewLiteral(iceberg.Decimal{Val: v, Scale: int(scale)})
}

func toMicros(v int64, unit arrow.TimeUnit) int64 {
	switch unit {
	case arrow.Second:
		return v * 1_000_000
	case arrow.Millisecond:
		return v * 1_000
	case arrow.Nanosecond:
		// Floor, not truncate, so instants before 1970 stay in the right
		// day/hour bucket.
		return floorDiv(v, 1_000)
	default:
		return v
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// SelectRows returns the rows of rec at the given indices, in order. The
// caller releases the result.
func SelectRows(ctx context.Context, rec arrow.RecordBatch, rows []int64) (arrow.RecordBatch, error) {
	if int64(len(rows)) == rec.NumRows() {
		// Split returns rows in ascending order, so a group holding every row
		// is the batch itself.
		rec.Retain()
		return rec, nil
	}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(rows, nil)
	idx := b.NewInt64Array()
	defer idx.Release()
	out, err := compute.Take(ctx, *compute.DefaultTakeOptions(), compute.NewDatumWithoutOwning(rec), compute.NewDatumWithoutOwning(idx))
	if err != nil {
		return nil, fmt.Errorf("select partition rows: %w", err)
	}
	return out.(*compute.RecordDatum).Value, nil
}
