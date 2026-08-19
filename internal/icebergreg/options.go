package icebergreg

import (
	"fmt"
	"regexp"
	"strings"

	iceberg "github.com/apache/iceberg-go"
)

var truncateMetricsMode = regexp.MustCompile(`^truncate\([1-9][0-9]*\)$`)

func normalizePartitionFields(fields []PartitionFieldConfig) {
	for i := range fields {
		fields[i].Source = strings.TrimSpace(fields[i].Source)
		fields[i].Name = strings.TrimSpace(fields[i].Name)
		fields[i].Transform = strings.ToLower(strings.TrimSpace(fields[i].Transform))
		if fields[i].Transform == "" {
			fields[i].Transform = "identity"
		}
	}
}

func normalizeSortFields(fields []SortFieldConfig) {
	for i := range fields {
		fields[i].Source = strings.TrimSpace(fields[i].Source)
		fields[i].Transform = strings.ToLower(strings.TrimSpace(fields[i].Transform))
		fields[i].Direction = strings.ToLower(strings.TrimSpace(fields[i].Direction))
		fields[i].NullOrder = strings.ToLower(strings.TrimSpace(fields[i].NullOrder))
		if fields[i].Transform == "" {
			fields[i].Transform = "identity"
		}
		if fields[i].Direction == "" {
			fields[i].Direction = "asc"
		}
		if fields[i].NullOrder == "" {
			if fields[i].Direction == "desc" {
				fields[i].NullOrder = "nulls-last"
			} else {
				fields[i].NullOrder = "nulls-first"
			}
		}
		fields[i].NullOrder = strings.ReplaceAll(fields[i].NullOrder, "_", "-")
	}
}

func normalizeUpsert(cfg *UpsertConfig) {
	cfg.Mode = strings.ToLower(strings.TrimSpace(cfg.Mode))
	if cfg.Enabled && cfg.Mode == "" {
		cfg.Mode = "merge-on-read"
	}
	for i := range cfg.Keys {
		cfg.Keys[i] = strings.TrimSpace(cfg.Keys[i])
	}
}

func normalizeOptions(partitionSpec []PartitionFieldConfig, sortOrder []SortFieldConfig, schemaEvolution, distributionMode, metricsMode *string, upsert *UpsertConfig) {
	normalizePartitionFields(partitionSpec)
	normalizeSortFields(sortOrder)
	*schemaEvolution = strings.ToLower(strings.TrimSpace(*schemaEvolution))
	if *schemaEvolution == "" {
		*schemaEvolution = "strict"
	}
	*distributionMode = strings.ToLower(strings.TrimSpace(*distributionMode))
	*metricsMode = strings.ToLower(strings.TrimSpace(*metricsMode))
	normalizeUpsert(upsert)
}

func (c *IceYAML) normalizeTableOptions() {
	normalizeOptions(c.PartitionSpec, c.SortOrder, &c.SchemaEvolution, &c.DistributionMode, &c.MetricsMode, &c.Upsert)
}

func (c *RunConfig) normalizeTableOptions() {
	normalizeOptions(c.PartitionSpec, c.SortOrder, &c.SchemaEvolution, &c.DistributionMode, &c.MetricsMode, &c.Upsert)
}

func validatePartitionFields(fields []PartitionFieldConfig) error {
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field.Source == "" {
			return fmt.Errorf("partition_spec.source is required")
		}
		name := field.Name
		if name == "" {
			name = field.Source
		}
		if _, ok := seen[name]; ok {
			return fmt.Errorf("partition_spec contains duplicate name %q", name)
		}
		seen[name] = struct{}{}
		if err := validateTransform(field.Transform); err != nil {
			return fmt.Errorf("partition_spec %q: %w", field.Source, err)
		}
	}
	return nil
}

func validateSortFields(fields []SortFieldConfig) error {
	for _, field := range fields {
		if field.Source == "" {
			return fmt.Errorf("sort_order.source is required")
		}
		if err := validateTransform(field.Transform); err != nil {
			return fmt.Errorf("sort_order %q: %w", field.Source, err)
		}
		if field.Direction != "asc" && field.Direction != "desc" {
			return fmt.Errorf("sort_order %q direction must be asc or desc", field.Source)
		}
		if field.NullOrder != "nulls-first" && field.NullOrder != "nulls-last" {
			return fmt.Errorf("sort_order %q null_order must be nulls-first or nulls-last", field.Source)
		}
	}
	return nil
}

func validateTransform(transform string) error {
	allowed := transform == "identity" || transform == "year" || transform == "month" || transform == "day" || transform == "hour" || strings.HasPrefix(transform, "bucket[") || strings.HasPrefix(transform, "truncate[")
	if !allowed {
		return fmt.Errorf("unsupported transform %q", transform)
	}
	if _, err := iceberg.ParseTransform(transform); err != nil {
		return fmt.Errorf("unsupported transform %q: %w", transform, err)
	}
	return nil
}

func validateMetadataRetention(cfg MetadataRetentionConfig) error {
	if cfg.PreviousVersionsMax < 0 {
		return fmt.Errorf("metadata_retention.previous_versions_max must be positive")
	}
	if cfg.MinSnapshotsToKeep < 0 {
		return fmt.Errorf("metadata_retention.min_snapshots_to_keep must be positive")
	}
	if cfg.MaxSnapshotAgeMS < 0 {
		return fmt.Errorf("metadata_retention.max_snapshot_age_ms must be positive")
	}
	return nil
}

func validateOptions(partitionSpec []PartitionFieldConfig, sortOrder []SortFieldConfig, schemaEvolution string, targetFileSize int64, distributionMode, metricsMode string, retention MetadataRetentionConfig, upsert UpsertConfig, vending CredentialVendingConfig) error {
	if err := validatePartitionFields(partitionSpec); err != nil {
		return err
	}
	if err := validateSortFields(sortOrder); err != nil {
		return err
	}
	if schemaEvolution != "strict" && schemaEvolution != "additive" {
		return fmt.Errorf("schema_evolution must be strict or additive")
	}
	if targetFileSize < 0 {
		return fmt.Errorf("target_file_size must be zero or positive")
	}
	if distributionMode != "" && distributionMode != "none" && distributionMode != "hash" && distributionMode != "range" {
		return fmt.Errorf("distribution_mode must be none, hash, or range")
	}
	if metricsMode != "" && metricsMode != "none" && metricsMode != "counts" && metricsMode != "full" && !truncateMetricsMode.MatchString(metricsMode) {
		return fmt.Errorf("metrics_mode must be none, counts, full, or truncate(N)")
	}
	if err := validateMetadataRetention(retention); err != nil {
		return err
	}
	if upsert.Enabled {
		if len(upsert.Keys) == 0 {
			return fmt.Errorf("upsert.keys is required when upsert.enabled=true")
		}
		seenKeys := make(map[string]struct{}, len(upsert.Keys))
		for _, key := range upsert.Keys {
			if key == "" {
				return fmt.Errorf("upsert.keys cannot contain an empty column")
			}
			if _, exists := seenKeys[key]; exists {
				return fmt.Errorf("upsert.keys contains duplicate column %q", key)
			}
			seenKeys[key] = struct{}{}
		}
		if upsert.Mode != "copy-on-write" && upsert.Mode != "merge-on-read" {
			return fmt.Errorf("upsert.mode must be copy-on-write or merge-on-read")
		}
	}
	if vending.Required && !vending.Enabled {
		return fmt.Errorf("credential_vending.required needs credential_vending.enabled=true")
	}
	return nil
}

func (c IceYAML) validateTableOptions() error {
	return validateOptions(c.PartitionSpec, c.SortOrder, c.SchemaEvolution, c.TargetFileSize, c.DistributionMode, c.MetricsMode, c.MetadataRetention, c.Upsert, c.CredentialVending)
}

func (c RunConfig) validateTableOptions() error {
	if err := validateOptions(c.PartitionSpec, c.SortOrder, c.SchemaEvolution, c.TargetFileSize, c.DistributionMode, c.MetricsMode, c.MetadataRetention, c.Upsert, c.CredentialVending); err != nil {
		return err
	}
	if c.Engine == "ice" && c.Upsert.Enabled {
		return fmt.Errorf("upsert is supported only with engine=rest-go")
	}
	if c.Engine == "ice" && c.CredentialVending.Required {
		return fmt.Errorf("required credential vending is supported only with engine=rest-go")
	}
	return nil
}
