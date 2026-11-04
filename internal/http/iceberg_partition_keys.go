package httpapi

import (
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
)

const (
	partitionKeysField = "iceberg.partition_keys"
	identityTransform  = "identity"
)

// icebergOptionsWithPartitionKeys returns the run's Iceberg options with
// iceberg.partition_keys applied as identity fields of partition_spec, the
// setting workers split rows by and the table is partitioned by.
//
// partition_keys is shorthand for that spec, so both may only be given when
// they describe the same partitioning. selectColumns, when set, limits the
// exported columns, so every key must be one of them.
func icebergOptionsWithPartitionKeys(req runSubmitIcebergRequest, selectColumns []string) (icebergreg.RunOptions, error) {
	options := req.Options
	if len(req.PartitionKeys) == 0 {
		return options, nil
	}
	if !req.Enabled {
		return options, invalidSubmitField(partitionKeysField, "iceberg.partition_keys needs iceberg.enabled: without Iceberg registration the output is not partitioned", nil)
	}
	fields, err := identityPartitionFields(req.PartitionKeys, selectColumns)
	if err != nil {
		return options, err
	}
	if options.PartitionSpec != nil && len(*options.PartitionSpec) > 0 {
		if !partitionSpec(*options.PartitionSpec).Equal(partitionSpec(fields)) {
			return options, invalidSubmitField(partitionKeysField, "iceberg.partition_keys and iceberg.options.partition_spec describe different partitions; set only one", map[string]any{
				"partition_keys": req.PartitionKeys,
			})
		}
		return options, nil
	}
	options.PartitionSpec = &fields
	return options, nil
}

func identityPartitionFields(keys, selectColumns []string) ([]icebergreg.PartitionFieldConfig, error) {
	fields := make([]icebergreg.PartitionFieldConfig, 0, len(keys))
	seen := make(map[string]bool, len(keys))
	for i, raw := range keys {
		key := strings.TrimSpace(raw)
		field := fmt.Sprintf("%s[%d]", partitionKeysField, i)
		switch {
		case key == "":
			return nil, invalidSubmitField(field, "iceberg.partition_keys entries must be column names", nil)
		case seen[strings.ToLower(key)]:
			return nil, invalidSubmitField(field, fmt.Sprintf("iceberg.partition_keys lists %q more than once", key), nil)
		case len(selectColumns) > 0 && !containsFold(selectColumns, key):
			return nil, invalidSubmitField(field, fmt.Sprintf("iceberg.partition_keys column %q is not in source.select_columns", key), nil)
		}
		seen[strings.ToLower(key)] = true
		fields = append(fields, icebergreg.PartitionFieldConfig{Source: key, Transform: identityTransform})
	}
	return fields, nil
}

func partitionSpec(fields []icebergreg.PartitionFieldConfig) icebergpart.Spec {
	spec := make(icebergpart.Spec, len(fields))
	for i, f := range fields {
		spec[i] = icebergpart.Field{Source: f.Source, Name: f.Name, Transform: f.Transform}
	}
	return spec
}

func containsFold(values []string, want string) bool {
	for _, v := range values {
		if strings.EqualFold(strings.TrimSpace(v), want) {
			return true
		}
	}
	return false
}
