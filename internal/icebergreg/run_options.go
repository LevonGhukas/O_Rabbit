package icebergreg

import (
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

// RunOptions contains values supplied for one run. Pointer fields distinguish
// an omitted value from an explicit false, zero, or empty list.
type RunOptions struct {
	URI               *string                     `json:"uri,omitempty" yaml:"uri,omitempty"`
	BearerToken       *string                     `json:"bearer_token,omitempty" yaml:"bearer_token,omitempty"`
	S3                RunS3Options                `json:"s3,omitempty" yaml:"s3,omitempty"`
	PartitionSpec     *[]PartitionFieldConfig     `json:"partition_spec,omitempty" yaml:"partition_spec,omitempty"`
	SortOrder         *[]SortFieldConfig          `json:"sort_order,omitempty" yaml:"sort_order,omitempty"`
	SchemaEvolution   *string                     `json:"schema_evolution,omitempty" yaml:"schema_evolution,omitempty"`
	TargetFileSize    *int64                      `json:"target_file_size,omitempty" yaml:"target_file_size,omitempty"`
	DistributionMode  *string                     `json:"distribution_mode,omitempty" yaml:"distribution_mode,omitempty"`
	MetricsMode       *string                     `json:"metrics_mode,omitempty" yaml:"metrics_mode,omitempty"`
	MetadataRetention RunMetadataRetentionOptions `json:"metadata_retention,omitempty" yaml:"metadata_retention,omitempty"`
	Upsert            RunUpsertOptions            `json:"upsert,omitempty" yaml:"upsert,omitempty"`
	CredentialVending RunCredentialVendingOptions `json:"credential_vending,omitempty" yaml:"credential_vending,omitempty"`
}

type RunS3Options struct {
	Endpoint        *string `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	Region          *string `json:"region,omitempty" yaml:"region,omitempty"`
	PathStyleAccess *bool   `json:"path_style_access,omitempty" yaml:"path_style_access,omitempty"`
	AccessKeyID     *string `json:"access_key_id,omitempty" yaml:"access_key_id,omitempty"`
	SecretAccessKey *string `json:"secret_access_key,omitempty" yaml:"secret_access_key,omitempty"`
}

type RunMetadataRetentionOptions struct {
	DeleteAfterCommit   *bool  `json:"delete_after_commit,omitempty" yaml:"delete_after_commit,omitempty"`
	PreviousVersionsMax *int   `json:"previous_versions_max,omitempty" yaml:"previous_versions_max,omitempty"`
	MinSnapshotsToKeep  *int   `json:"min_snapshots_to_keep,omitempty" yaml:"min_snapshots_to_keep,omitempty"`
	MaxSnapshotAgeMS    *int64 `json:"max_snapshot_age_ms,omitempty" yaml:"max_snapshot_age_ms,omitempty"`
}

type RunUpsertOptions struct {
	Enabled *bool     `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Keys    *[]string `json:"keys,omitempty" yaml:"keys,omitempty"`
	Mode    *string   `json:"mode,omitempty" yaml:"mode,omitempty"`
}

type RunCredentialVendingOptions struct {
	Enabled  *bool `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Required *bool `json:"required,omitempty" yaml:"required,omitempty"`
}

// ApplyRunOptions overlays one run's explicit values on a resolved default
// config. It intentionally applies fields one by one so false and zero remain
// valid overrides.
func ApplyRunOptions(cfg RunConfig, options RunOptions) RunConfig {
	if options.URI != nil {
		cfg.URI = *options.URI
	}
	if options.BearerToken != nil {
		cfg.BearerToken = *options.BearerToken
	}
	if options.S3.Endpoint != nil {
		cfg.S3.Endpoint = *options.S3.Endpoint
	}
	if options.S3.Region != nil {
		cfg.S3.Region = *options.S3.Region
	}
	if options.S3.PathStyleAccess != nil {
		cfg.S3.PathStyleAccess = *options.S3.PathStyleAccess
	}
	if options.S3.AccessKeyID != nil {
		cfg.S3.AccessKeyID = *options.S3.AccessKeyID
	}
	if options.S3.SecretAccessKey != nil {
		cfg.S3.SecretAccessKey = *options.S3.SecretAccessKey
	}
	if options.PartitionSpec != nil {
		cfg.PartitionSpec = append([]PartitionFieldConfig(nil), (*options.PartitionSpec)...)
	}
	if options.SortOrder != nil {
		cfg.SortOrder = append([]SortFieldConfig(nil), (*options.SortOrder)...)
	}
	if options.SchemaEvolution != nil {
		cfg.SchemaEvolution = *options.SchemaEvolution
	}
	if options.TargetFileSize != nil {
		cfg.TargetFileSize = *options.TargetFileSize
	}
	if options.DistributionMode != nil {
		cfg.DistributionMode = *options.DistributionMode
	}
	if options.MetricsMode != nil {
		cfg.MetricsMode = *options.MetricsMode
	}
	if options.MetadataRetention.DeleteAfterCommit != nil {
		cfg.MetadataRetention.DeleteAfterCommit = *options.MetadataRetention.DeleteAfterCommit
	}
	if options.MetadataRetention.PreviousVersionsMax != nil {
		cfg.MetadataRetention.PreviousVersionsMax = *options.MetadataRetention.PreviousVersionsMax
	}
	if options.MetadataRetention.MinSnapshotsToKeep != nil {
		cfg.MetadataRetention.MinSnapshotsToKeep = *options.MetadataRetention.MinSnapshotsToKeep
	}
	if options.MetadataRetention.MaxSnapshotAgeMS != nil {
		cfg.MetadataRetention.MaxSnapshotAgeMS = *options.MetadataRetention.MaxSnapshotAgeMS
	}
	if options.Upsert.Enabled != nil {
		cfg.Upsert.Enabled = *options.Upsert.Enabled
	}
	if options.Upsert.Keys != nil {
		cfg.Upsert.Keys = append([]string(nil), (*options.Upsert.Keys)...)
	}
	if options.Upsert.Mode != nil {
		cfg.Upsert.Mode = *options.Upsert.Mode
	}
	if options.CredentialVending.Enabled != nil {
		cfg.CredentialVending.Enabled = *options.CredentialVending.Enabled
	}
	if options.CredentialVending.Required != nil {
		cfg.CredentialVending.Required = *options.CredentialVending.Required
	}
	return cfg.Normalize()
}

// ResolveRunConfigWithOptions resolves base S3 settings, optional file
// defaults, and explicit run options into the immutable run snapshot.
func ResolveRunConfigWithOptions(enabled bool, engine, table string, baseS3 s3io.Config, defaults IceYAML, options RunOptions) (RunConfig, error) {
	cfg := ResolveRunConfig(enabled, engine, table, baseS3, defaults)
	cfg = ApplyRunOptions(cfg, options)
	if enabled && strings.TrimSpace(cfg.URI) == "" {
		return RunConfig{}, fmt.Errorf("iceberg uri is required")
	}
	if err := cfg.validateTableOptions(); err != nil {
		return RunConfig{}, err
	}
	return cfg, nil
}
