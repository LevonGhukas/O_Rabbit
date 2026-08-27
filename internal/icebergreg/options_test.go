package icebergreg

import (
	"context"
	"os"
	"strings"
	"testing"

	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
)

func optionTestSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "created_at", Type: iceberg.PrimitiveTypes.Timestamp},
		iceberg.NestedField{ID: 3, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
}

func TestParseIceYAMLTableOptions(t *testing.T) {
	cfg, err := ParseIceYAMLBytes([]byte(`
uri: http://catalog:8181
partition_spec:
  - source: created_at
    name: created_day
    transform: day
sort_order:
  - source: created_at
    direction: DESC
    null_order: nulls_last
schema_evolution: additive
target_file_size: 134217728
distribution_mode: hash
metrics_mode: truncate(32)
metadata_retention:
  delete_after_commit: true
  previous_versions_max: 10
  min_snapshots_to_keep: 3
  max_snapshot_age_ms: 604800000
upsert:
  enabled: true
  keys: [id]
  mode: merge-on-read
credential_vending:
  enabled: true
  required: true
`))
	if err != nil {
		t.Fatalf("ParseIceYAMLBytes: %v", err)
	}
	if cfg.SchemaEvolution != "additive" || cfg.TargetFileSize != 134217728 || cfg.DistributionMode != "hash" || cfg.MetricsMode != "truncate(32)" {
		t.Fatalf("unexpected scalar options: %+v", cfg)
	}
	if len(cfg.PartitionSpec) != 1 || cfg.PartitionSpec[0].Transform != "day" {
		t.Fatalf("partition_spec=%+v", cfg.PartitionSpec)
	}
	if len(cfg.SortOrder) != 1 || cfg.SortOrder[0].Direction != "desc" || cfg.SortOrder[0].NullOrder != "nulls-last" {
		t.Fatalf("sort_order=%+v", cfg.SortOrder)
	}
	if !cfg.Upsert.Enabled || cfg.Upsert.Mode != "merge-on-read" || len(cfg.Upsert.Keys) != 1 {
		t.Fatalf("upsert=%+v", cfg.Upsert)
	}
	if !cfg.CredentialVending.Required || cfg.MetadataRetention.PreviousVersionsMax != 10 {
		t.Fatalf("vending=%+v retention=%+v", cfg.CredentialVending, cfg.MetadataRetention)
	}
}

func TestResolveRunConfigWithOptionsOverridesDefaults(t *testing.T) {
	uri := "http://run-catalog:8181"
	token := ""
	pathStyle := false
	targetFileSize := int64(0)
	distribution := "none"
	metrics := "counts"
	deleteAfterCommit := false
	upsertEnabled := false
	vendingRequired := false
	emptyPartitions := []PartitionFieldConfig{}
	emptyKeys := []string{}

	cfg, err := ResolveRunConfigWithOptions(true, "rest-go", "analytics.orders", s3io.Config{
		Endpoint:        "http://target-s3:9000",
		Region:          "target-region",
		ForcePathStyle:  true,
		AccessKeyID:     "target-key",
		SecretAccessKey: "target-secret",
	}, IceYAML{
		URI:              "http://default-catalog:8181",
		BearerToken:      "default-token",
		PartitionSpec:    []PartitionFieldConfig{{Source: "created_at", Transform: "day"}},
		TargetFileSize:   128 * 1024 * 1024,
		DistributionMode: "hash",
		MetricsMode:      "full",
		MetadataRetention: MetadataRetentionConfig{
			DeleteAfterCommit: true,
		},
		Upsert:            UpsertConfig{Enabled: true, Keys: []string{"id"}, Mode: "merge-on-read"},
		CredentialVending: CredentialVendingConfig{Enabled: true, Required: true},
	}, RunOptions{
		URI:              &uri,
		BearerToken:      &token,
		PartitionSpec:    &emptyPartitions,
		TargetFileSize:   &targetFileSize,
		DistributionMode: &distribution,
		MetricsMode:      &metrics,
		S3: RunS3Options{
			PathStyleAccess: &pathStyle,
		},
		MetadataRetention: RunMetadataRetentionOptions{
			DeleteAfterCommit: &deleteAfterCommit,
		},
		Upsert: RunUpsertOptions{
			Enabled: &upsertEnabled,
			Keys:    &emptyKeys,
		},
		CredentialVending: RunCredentialVendingOptions{
			Required: &vendingRequired,
		},
	})
	if err != nil {
		t.Fatalf("ResolveRunConfigWithOptions: %v", err)
	}
	if cfg.URI != uri || cfg.BearerToken != "" {
		t.Fatalf("catalog settings=%q token=%q", cfg.URI, cfg.BearerToken)
	}
	if cfg.S3.Endpoint != "http://target-s3:9000" || cfg.S3.PathStyleAccess {
		t.Fatalf("S3=%+v", cfg.S3)
	}
	if len(cfg.PartitionSpec) != 0 || cfg.TargetFileSize != 0 {
		t.Fatalf("partition_spec=%+v target_file_size=%d", cfg.PartitionSpec, cfg.TargetFileSize)
	}
	if cfg.DistributionMode != "none" || cfg.MetricsMode != "counts" {
		t.Fatalf("distribution=%q metrics=%q", cfg.DistributionMode, cfg.MetricsMode)
	}
	if cfg.MetadataRetention.DeleteAfterCommit || cfg.Upsert.Enabled || len(cfg.Upsert.Keys) != 0 {
		t.Fatalf("retention=%+v upsert=%+v", cfg.MetadataRetention, cfg.Upsert)
	}
	if !cfg.CredentialVending.Enabled || cfg.CredentialVending.Required {
		t.Fatalf("credential_vending=%+v", cfg.CredentialVending)
	}
}

func TestResolveRunConfigWithOptionsRequiresFinalURI(t *testing.T) {
	_, err := ResolveRunConfigWithOptions(true, "rest-go", "analytics.orders", s3io.Config{}, IceYAML{}, RunOptions{})
	if err == nil || !strings.Contains(err.Error(), "uri is required") {
		t.Fatalf("error=%v", err)
	}
}

func TestParseIceYAMLRejectsInvalidTableOptions(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{name: "partition source", yaml: "partition_spec:\n  - transform: day\n", want: "partition_spec.source"},
		{name: "sort direction", yaml: "sort_order:\n  - source: id\n    direction: sideways\n", want: "direction"},
		{name: "schema mode", yaml: "schema_evolution: destructive\n", want: "schema_evolution"},
		{name: "distribution", yaml: "distribution_mode: random\n", want: "distribution_mode"},
		{name: "metrics", yaml: "metrics_mode: truncate(0)\n", want: "metrics_mode"},
		{name: "upsert keys", yaml: "upsert:\n  enabled: true\n", want: "upsert.keys"},
		{name: "duplicate upsert keys", yaml: "upsert:\n  enabled: true\n  keys: [id, id]\n", want: "duplicate column"},
		{name: "vending", yaml: "credential_vending:\n  required: true\n", want: "credential_vending"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseIceYAMLBytes([]byte(test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v want substring %q", err, test.want)
			}
		})
	}
}

func TestBuildPartitionSpecAndSortOrder(t *testing.T) {
	schema := optionTestSchema()
	spec, err := buildPartitionSpec(schema, []PartitionFieldConfig{
		{Source: "id", Transform: "identity"},
		{Source: "created_at", Name: "created_day", Transform: "day"},
	})
	if err != nil {
		t.Fatalf("buildPartitionSpec: %v", err)
	}
	if spec.Len() != 2 {
		t.Fatalf("partition fields=%d want 2", spec.Len())
	}

	order, err := buildSortOrder(schema, []SortFieldConfig{{
		Source: "created_at", Transform: "identity", Direction: "desc", NullOrder: "nulls-last",
	}}, icetable.InitialSortOrderID)
	if err != nil {
		t.Fatalf("buildSortOrder: %v", err)
	}
	if order.Len() != 1 || order.OrderID() != icetable.InitialSortOrderID {
		t.Fatalf("sort order=%s", order.String())
	}
}

func TestTableOptionProperties(t *testing.T) {
	props := tableOptionProperties(RunConfig{
		TargetFileSize:   128 * 1024 * 1024,
		DistributionMode: "range",
		MetricsMode:      "full",
		MetadataRetention: MetadataRetentionConfig{
			DeleteAfterCommit:   true,
			PreviousVersionsMax: 7,
			MinSnapshotsToKeep:  3,
			MaxSnapshotAgeMS:    1000,
		},
		Upsert: UpsertConfig{Enabled: true, Mode: "copy-on-write", Keys: []string{"id"}},
	})
	want := map[string]string{
		icetable.WriteTargetFileSizeBytesKey:         "134217728",
		distributionModeProperty:                     "range",
		icetable.DefaultWriteMetricsModeKey:          "full",
		icetable.MetadataDeleteAfterCommitEnabledKey: "true",
		icetable.MetadataPreviousVersionsMaxKey:      "7",
		icetable.MinSnapshotsToKeepKey:               "3",
		icetable.MaxSnapshotAgeMsKey:                 "1000",
		icetable.WriteDeleteModeKey:                  "copy-on-write",
		writeUpdateModeProperty:                      "copy-on-write",
		writeMergeModeProperty:                       "copy-on-write",
	}
	for key, value := range want {
		if props[key] != value {
			t.Fatalf("property %s=%q want %q", key, props[key], value)
		}
	}
}

func TestCredentialVendingRequiredRemovesStaticCredentials(t *testing.T) {
	cfg := s3io.Config{
		Endpoint:        "http://minio:9000",
		Region:          "us-east-1",
		ForcePathStyle:  true,
		AccessKeyID:     "static-key",
		SecretAccessKey: "static-secret",
	}
	withFallback := icebergRegistrationS3Props(cfg, false)
	if withFallback["s3.access-key-id"] != "static-key" || withFallback["s3.secret-access-key"] != "static-secret" {
		t.Fatalf("static fallback missing: %+v", withFallback)
	}
	required := icebergRegistrationS3Props(cfg, true)
	if required["s3.access-key-id"] != "" || required["s3.secret-access-key"] != "" {
		t.Fatalf("required vending leaked static credentials: %+v", required)
	}
	if required["s3.endpoint"] == "" || required["s3.region"] == "" {
		t.Fatalf("required vending lost non-secret S3 settings: %+v", required)
	}
}

func TestSchemaWithIdentifierFields(t *testing.T) {
	schema, err := schemaWithIdentifierFields(optionTestSchema(), []string{"id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(schema.IdentifierFieldIDs) != 1 || schema.IdentifierFieldIDs[0] != 1 {
		t.Fatalf("identifier ids=%v", schema.IdentifierFieldIDs)
	}
	if _, err := schemaWithIdentifierFields(optionTestSchema(), []string{"missing"}); err == nil {
		t.Fatal("expected missing identifier error")
	}
}

func TestUpsertExpressionsRejectDuplicateAndNullKeys(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues([]int64{1, 1}, nil)
	values := builder.NewArray()
	builder.Release()
	record := array.NewRecordBatch(schema, []arrow.Array{values}, 2)
	values.Release()
	defer record.Release()

	_, err := upsertExpressionsFromRecord(optionTestSchema(), record, []string{"id"}, map[string]struct{}{})
	if err == nil || !strings.Contains(err.Error(), "duplicate key") {
		t.Fatalf("error=%v", err)
	}

	nullBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	nullBuilder.AppendNull()
	nullValues := nullBuilder.NewArray()
	nullBuilder.Release()
	nullRecord := array.NewRecordBatch(schema, []arrow.Array{nullValues}, 1)
	nullValues.Release()
	defer nullRecord.Release()
	_, err = upsertExpressionsFromRecord(optionTestSchema(), nullRecord, []string{"id"}, map[string]struct{}{})
	if err == nil || !strings.Contains(err.Error(), "contains NULL") {
		t.Fatalf("error=%v", err)
	}
}

func TestUpsertExpressionsBuildBindableCompositeFilter(t *testing.T) {
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String},
	}, nil)
	idBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	idBuilder.AppendValues([]int64{1, 2}, nil)
	ids := idBuilder.NewArray()
	idBuilder.Release()
	nameBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	nameBuilder.AppendValues([]string{"a", "b"}, nil)
	names := nameBuilder.NewArray()
	nameBuilder.Release()
	record := array.NewRecordBatch(arrowSchema, []arrow.Array{ids, names}, 2)
	ids.Release()
	names.Release()
	defer record.Release()

	expressions, err := upsertExpressionsFromRecord(optionTestSchema(), record, []string{"id", "name"}, map[string]struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if len(expressions) != 2 {
		t.Fatalf("expressions=%d want 2", len(expressions))
	}
	filter := joinExpressions(expressions, false)
	if _, err := iceberg.BindExpr(optionTestSchema(), filter, true); err != nil {
		t.Fatalf("bind upsert filter: %v", err)
	}
}

func TestResolveRunConfigPersistsTableOptions(t *testing.T) {
	cfg := ResolveRunConfig(true, "rest-go", "analytics.orders", s3io.Config{}, IceYAML{
		SchemaEvolution:   "additive",
		TargetFileSize:    123,
		DistributionMode:  "hash",
		MetricsMode:       "counts",
		PartitionSpec:     []PartitionFieldConfig{{Source: "id", Transform: "identity"}},
		CredentialVending: CredentialVendingConfig{Enabled: true},
	})
	if cfg.SchemaEvolution != "additive" || cfg.TargetFileSize != 123 || len(cfg.PartitionSpec) != 1 || !cfg.CredentialVending.Enabled {
		t.Fatalf("run config=%+v", cfg)
	}
}

func TestParquetRecordReaderStreamsMultipleFiles(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	paths := make([]string, 0, 2)
	for _, values := range [][]int64{{1, 2}, {3}} {
		writer, path, err := parquetio.NewTempFileWriterInDir(schema, parquetio.Options{}, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		column := builder.NewArray()
		builder.Release()
		record := array.NewRecordBatch(schema, []arrow.Array{column}, int64(len(values)))
		column.Release()
		if err := writer.Write(record); err != nil {
			record.Release()
			t.Fatal(err)
		}
		record.Release()
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, path)
		defer os.Remove(path)
	}

	reader, err := newParquetRecordReader(context.Background(), iceio.LocalFS{}, paths)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Release()
	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	if err := reader.Err(); err != nil {
		t.Fatal(err)
	}
	if rows != 3 {
		t.Fatalf("rows=%d want 3", rows)
	}
}

type optionTestCatalog struct {
	metadata         icetable.Metadata
	metadataLocation string
}

func (c *optionTestCatalog) LoadTable(context.Context, icetable.Identifier) (*icetable.Table, error) {
	return nil, nil
}

func (c *optionTestCatalog) CommitTable(_ context.Context, _ icetable.Identifier, requirements []icetable.Requirement, updates []icetable.Update) (icetable.Metadata, string, error) {
	for _, requirement := range requirements {
		if err := requirement.Validate(c.metadata); err != nil {
			return nil, "", err
		}
	}
	builder, err := icetable.MetadataBuilderFromBase(c.metadata, c.metadataLocation)
	if err != nil {
		return nil, "", err
	}
	for _, update := range updates {
		if err := update.Apply(builder); err != nil {
			return nil, "", err
		}
	}
	c.metadata, err = builder.Build()
	return c.metadata, c.metadataLocation, err
}

func newOptionTestTable(t *testing.T, schema *iceberg.Schema) *icetable.Table {
	t.Helper()
	location := t.TempDir()
	metadata, err := icetable.NewMetadata(schema, iceberg.UnpartitionedSpec, icetable.UnsortedSortOrder, location, iceberg.Properties{"format-version": "2"})
	if err != nil {
		t.Fatal(err)
	}
	catalog := &optionTestCatalog{metadata: metadata, metadataLocation: location + "/metadata/v1.json"}
	return icetable.New(icetable.Identifier{"analytics", "orders"}, metadata, catalog.metadataLocation, func(context.Context) (iceio.IO, error) {
		return iceio.LocalFS{}, nil
	}, catalog)
}

func TestApplyAdditiveSchemaEvolution(t *testing.T) {
	current := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
	)
	source := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
	table := newOptionTestTable(t, current)
	tx := table.NewTransaction()
	if err := applySchemaOptions(tx, current, source, RunConfig{SchemaEvolution: "additive"}); err != nil {
		t.Fatal(err)
	}
	updated, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, _ := updated.Schema().FindFieldByName("id")
	if !id.Type.Equals(iceberg.PrimitiveTypes.Int64) {
		t.Fatalf("id type=%s want long", id.Type)
	}
	if _, ok := updated.Schema().FindFieldByName("name"); !ok {
		t.Fatal("additive evolution did not add name")
	}
}

func TestApplyStrictSchemaEvolutionRejectsNewColumn(t *testing.T) {
	current := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true})
	source := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
	tx := newOptionTestTable(t, current).NewTransaction()
	err := applySchemaOptions(tx, current, source, RunConfig{SchemaEvolution: "strict"})
	if err == nil || !strings.Contains(err.Error(), "rejects new column") {
		t.Fatalf("error=%v", err)
	}
}

func TestApplySchemaOptionsRejectsOptionalSourceForRequiredTableBeforeCommit(t *testing.T) {
	current := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "created_at", Type: iceberg.PrimitiveTypes.String, Required: true})
	source := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "created_at", Type: iceberg.PrimitiveTypes.String})
	err := applySchemaOptions(newOptionTestTable(t, current).NewTransaction(), current, source, RunConfig{SchemaEvolution: "additive"})
	if err == nil || !failure.IsFailure(err, failure.FailureSchemaIncompatible) {
		t.Fatalf("error=%v", err)
	}
}

func TestApplyPartitionSpecEvolution(t *testing.T) {
	table := newOptionTestTable(t, optionTestSchema())
	tx := table.NewTransaction()
	current := table.Spec()
	if err := applyPartitionSpec(tx, &current, []PartitionFieldConfig{{Source: "created_at", Name: "created_day", Transform: "day"}}, table.Schema()); err != nil {
		t.Fatal(err)
	}
	updated, err := tx.Commit(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	spec := updated.Spec()
	if spec.Len() != 1 {
		t.Fatalf("partition fields=%d want 1", spec.Len())
	}
}
