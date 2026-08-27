package arrowio

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// RecordForConfiguredTarget preserves unsupported FlightSQL timestamp[ns]
// scalars as versioned exact text before Parquet writing. Other Arrow values
// remain producer-native; nested values are never generically stringified.
func RecordForConfiguredTarget(rec arrow.RecordBatch) (*arrow.Schema, arrow.RecordBatch, error) {
	if rec == nil || rec.Schema() == nil {
		return nil, nil, fmt.Errorf("nil FlightSQL record or schema")
	}
	fields := rec.Schema().Fields()
	arrays := make([]arrow.Array, len(fields))
	changed := false
	for i, field := range fields {
		arrays[i] = rec.Column(i)
		ts, ok := field.Type.(*arrow.TimestampType)
		if !ok || ts.Unit == arrow.Microsecond {
			continue
		}
		if ts.Unit != arrow.Nanosecond {
			return nil, nil, fmt.Errorf("column %q: no exact fallback for %s", field.Name, ts)
		}
		source, ok := rec.Column(i).(*array.Timestamp)
		if !ok {
			return nil, nil, fmt.Errorf("column %q: timestamp schema/value mismatch", field.Name)
		}
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		for row := 0; row < source.Len(); row++ {
			if source.IsNull(row) {
				builder.AppendNull()
				continue
			}
			v := time.Unix(0, int64(source.Value(row))).UTC()
			layout := "2006-01-02 15:04:05.000000000"
			if ts.TimeZone != "" {
				layout += "Z07:00"
			}
			builder.Append(v.Format(layout))
		}
		arrays[i] = builder.NewArray()
		builder.Release()
		md := field.Metadata
		md = appendFieldMetadata(md, "orabbit.representation", "fallback")
		md = appendFieldMetadata(md, "orabbit.fallback.encoding", "arrow_timestamp_ns_text_v1")
		if ts.TimeZone != "" {
			md = appendFieldMetadata(md, "orabbit.source.timezone", ts.TimeZone)
		}
		fields[i] = arrow.Field{Name: field.Name, Type: arrow.BinaryTypes.String, Nullable: field.Nullable, Metadata: md}
		changed = true
	}
	if !changed {
		if err := ValidateArrowSchemaForConfiguredTarget(rec.Schema()); err != nil {
			return nil, nil, err
		}
		return rec.Schema(), rec, nil
	}
	schemaMetadata := rec.Schema().Metadata()
	schema := arrow.NewSchema(fields, &schemaMetadata)
	return schema, array.NewRecordBatch(schema, arrays, rec.NumRows()), nil
}

func appendFieldMetadata(md arrow.Metadata, key, value string) arrow.Metadata {
	keys, values := md.Keys(), md.Values()
	for i := range keys {
		if keys[i] == key {
			values[i] = value
			return arrow.NewMetadata(keys, values)
		}
	}
	return arrow.NewMetadata(append(keys, key), append(values, value))
}
