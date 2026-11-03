package icebergreg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	icecatalog "github.com/apache/iceberg-go/catalog"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// ExistingColumnTypes loads the run's target table from the catalog and
// returns its top-level columns as logical types, keyed by column name. A
// table that does not exist yet returns an empty map and no error. Planning
// uses it to keep the type an existing column already has, because Iceberg
// cannot change a column from, for example, string to decimal or map.
//
// Only table metadata is read; no data files are opened.
func ExistingColumnTypes(ctx context.Context, cfg RunConfig, bucket string, regS3 s3io.Config) (map[string]typesystem.LogicalType, error) {
	cfg = cfg.Normalize()
	if !cfg.Enabled || strings.TrimSpace(cfg.Table) == "" {
		return map[string]typesystem.LogicalType{}, nil
	}
	if cfg.URI == "" && cfg.ConfigYAML != "" {
		ice, err := ParseIceYAMLBytes([]byte(cfg.ConfigYAML))
		if err != nil {
			return nil, fmt.Errorf("parse ice config: %w", err)
		}
		cfg.URI = strings.TrimSpace(ice.URI)
		if cfg.BearerToken == "" {
			cfg.BearerToken = strings.TrimSpace(ice.BearerToken)
		}
	}
	cat, ident, err := newRESTCatalog(ctx, bucket, cfg, regS3, cfg.Table)
	if err != nil {
		return nil, err
	}
	tbl, err := cat.LoadTable(ctx, ident)
	if errors.Is(err, icecatalog.ErrNoSuchTable) {
		return map[string]typesystem.LogicalType{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load iceberg table %s: %w", cfg.Table, err)
	}
	out := map[string]typesystem.LogicalType{}
	for _, field := range tbl.Schema().Fields() {
		if t, ok := LogicalTypeForIceberg(field.Type, !field.Required); ok {
			out[field.Name] = t
		}
	}
	return out, nil
}

// LogicalTypeForIceberg translates an Iceberg type into the logical type
// whose storage produces it again, so files written with it match the
// table. ok is false for types O_Rabbit does not write.
func LogicalTypeForIceberg(t iceberg.Type, nullable bool) (typesystem.LogicalType, bool) {
	out, ok := logicalTypeForIceberg(t)
	if ok && nullable {
		out = typesystem.Nullable(out)
	}
	return out, ok
}

func logicalTypeForIceberg(t iceberg.Type) (typesystem.LogicalType, bool) {
	kind := func(k typesystem.Kind) (typesystem.LogicalType, bool) { return typesystem.LogicalType{Kind: k}, true }
	switch v := t.(type) {
	case iceberg.BooleanType:
		return kind(typesystem.KindBool)
	case iceberg.Int32Type:
		return kind(typesystem.KindInt32)
	case iceberg.Int64Type:
		return kind(typesystem.KindInt64)
	case iceberg.Float32Type:
		return kind(typesystem.KindFloat32)
	case iceberg.Float64Type:
		return kind(typesystem.KindFloat64)
	case iceberg.DecimalType:
		return typesystem.Decimal(int32(v.Precision()), int32(v.Scale())), true
	case iceberg.DateType:
		return kind(typesystem.KindDate)
	case iceberg.TimeType:
		return kind(typesystem.KindTime)
	case iceberg.TimestampType:
		return kind(typesystem.KindTimestamp)
	case iceberg.TimestampTzType:
		return typesystem.LogicalType{Kind: typesystem.KindTimestampTZ, Timezone: "UTC"}, true
	case iceberg.StringType:
		return kind(typesystem.KindString)
	case iceberg.UUIDType:
		return kind(typesystem.KindUUID)
	case iceberg.BinaryType:
		return kind(typesystem.KindBinary)
	case *iceberg.ListType:
		element, ok := LogicalTypeForIceberg(v.Element, !v.ElementRequired)
		if !ok {
			return typesystem.LogicalType{}, false
		}
		return typesystem.ArrayOf(element), true
	case *iceberg.MapType:
		key, ok := LogicalTypeForIceberg(v.KeyType, false)
		if !ok {
			return typesystem.LogicalType{}, false
		}
		value, ok := LogicalTypeForIceberg(v.ValueType, !v.ValueRequired)
		if !ok {
			return typesystem.LogicalType{}, false
		}
		return typesystem.MapOf(key, value), true
	case *iceberg.StructType:
		fields := make([]typesystem.LogicalField, len(v.FieldList))
		for i, f := range v.FieldList {
			ft, ok := LogicalTypeForIceberg(f.Type, !f.Required)
			if !ok {
				return typesystem.LogicalType{}, false
			}
			fields[i] = typesystem.LogicalField{Name: f.Name, Type: ft}
		}
		return typesystem.LogicalType{Kind: typesystem.KindStruct, Fields: fields}, true
	}
	return typesystem.LogicalType{}, false
}
