package icebergreg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"
)

// WorkerPartitionSpec returns the partition spec workers must split their
// output by, or nil when the run does not write a partitioned rest-go table.
// The ice CLI engine partitions on its own, so it receives no spec.
func (c RunConfig) WorkerPartitionSpec() icebergpart.Spec {
	c = c.Normalize()
	if !c.Enabled || c.Engine != "rest-go" || len(c.PartitionSpec) == 0 {
		return nil
	}
	spec := make(icebergpart.Spec, len(c.PartitionSpec))
	for i, f := range c.PartitionSpec {
		spec[i] = icebergpart.Field{Source: f.Source, Name: f.Name, Transform: f.Transform}
	}
	return spec.Normalize()
}

// WorkerPartitionSpecJSON is WorkerPartitionSpec encoded for the task
// assignment; it is empty when there is no spec.
func (c RunConfig) WorkerPartitionSpecJSON() (string, error) {
	spec := c.WorkerPartitionSpec()
	if len(spec) == 0 {
		return "", nil
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// partitionedFile is a worker file prepared for registration from its footer.
type partitionedFile struct {
	path      string
	size      int64
	rows      int64
	partition map[int]any
	// inferable is true when iceberg-go can derive the same partition values
	// from the file's column statistics.
	inferable   bool
	columnSizes map[int]int64
	valueCounts map[int]int64
	nullCounts  map[int]int64
}

// registerPartitionedFiles adds worker files to a partitioned table as data
// files, or replaces the table's data files with them on a full refresh, in
// the transaction's single snapshot. Only Parquet footers are read: the files
// Iceberg references are exactly the verified worker artifacts.
//
// Every file must carry the partition contract a worker writes for spec. When
// all transforms preserve order and the footers have column statistics,
// iceberg-go infers the partition values and full column metrics itself.
// Otherwise (bucket or void transforms, or missing statistics) the files are
// added with the partition values from their contracts plus count metrics;
// such files have no lower/upper bounds, so engines prune them by partition
// but not by column ranges.
func registerPartitionedFiles(ctx context.Context, tx *icetable.Transaction, reg RunConfig, table string, existing []iceberg.DataFile, fullRefresh bool, newFiles []string, props iceberg.Properties) error {
	staged, err := tx.StagedTable()
	if err != nil {
		return err
	}
	spec, schema := staged.Spec(), staged.Schema()
	want := reg.WorkerPartitionSpec()
	if len(want) == 0 {
		return failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf(
			"iceberg table %s is partitioned (%s) but the run has no partition_spec, so the workers did not split its files by partition; set partition_spec to the table's spec and run the export again",
			table, spec))
	}
	fs, err := staged.FS(ctx)
	if err != nil {
		return err
	}
	files := make([]partitionedFile, 0, len(newFiles))
	inferable := true
	for field := range spec.Fields() {
		if !field.Transform.PreservesOrder() {
			inferable = false
		}
	}
	for _, path := range newFiles {
		f, err := readPartitionedFile(fs, path, want, spec, schema)
		if err != nil {
			return err
		}
		inferable = inferable && f.inferable
		files = append(files, f)
	}

	if inferable {
		existingPaths := make([]string, len(existing))
		for i, df := range existing {
			existingPaths[i] = df.FilePath()
		}
		return applyRESTGoFileMutation(ctx, tx, fullRefresh, existingPaths, newFiles, props)
	}

	dataFiles := make([]iceberg.DataFile, len(files))
	for i, f := range files {
		df, err := f.dataFile(spec)
		if err != nil {
			return err
		}
		dataFiles[i] = df
	}
	if fullRefresh {
		if err := tx.ReplaceDataFilesWithDataFiles(ctx, existing, dataFiles, props); err != nil {
			return fmt.Errorf("iceberg partitioned full refresh replace: %w", err)
		}
		return nil
	}
	if err := tx.AddDataFiles(ctx, dataFiles, props); err != nil {
		return fmt.Errorf("iceberg partitioned append: %w", err)
	}
	return nil
}

// readPartitionedFile reads a worker file's footer: its partition contract,
// schema, row count and column statistics.
func readPartitionedFile(fs iceio.IO, path string, want icebergpart.Spec, spec iceberg.PartitionSpec, schema *iceberg.Schema) (partitionedFile, error) {
	input, err := fs.Open(path)
	if err != nil {
		return partitionedFile{}, fmt.Errorf("open parquet %s: %w", path, err)
	}
	reader, err := file.NewParquetReader(input)
	if err != nil {
		_ = input.Close()
		return partitionedFile{}, fmt.Errorf("read parquet footer %s: %w", path, err)
	}
	defer reader.Close()
	md := reader.MetaData()

	raw := md.KeyValueMetadata().FindValue(icebergpart.FooterKey)
	if raw == nil {
		return partitionedFile{}, failure.NewFailure(failure.FailureDataIntegrity, false, true, fmt.Errorf(
			"parquet %s has no partition contract: it was written by a worker older than protocol 8 or before partition_spec was set, and may hold several partitions; run the export again",
			path))
	}
	contract, err := icebergpart.ParseContract(*raw)
	if err != nil {
		return partitionedFile{}, failure.NewFailure(failure.FailureDataIntegrity, false, true, fmt.Errorf("parquet %s: %w", path, err))
	}
	if !contract.Spec.Equal(want) {
		return partitionedFile{}, failure.NewFailure(failure.FailureConfigurationInvalid, false, true, fmt.Errorf(
			"parquet %s was split by partition spec (%s) but the registration uses (%s); partition_spec cannot change after the export, so run the export again",
			path, contract.Spec, want))
	}

	arrowSchema, err := pqarrow.FromParquet(md.Schema, &pqarrow.ArrowReadProperties{}, md.KeyValueMetadata())
	if err != nil {
		return partitionedFile{}, fmt.Errorf("read parquet schema %s: %w", path, err)
	}
	if err := checkFileSchema(arrowSchema, schema); err != nil {
		return partitionedFile{}, failure.NewFailure(failure.FailureSchemaIncompatible, false, true, fmt.Errorf("parquet %s: %w", path, err))
	}

	stat, err := input.Stat()
	if err != nil {
		return partitionedFile{}, fmt.Errorf("stat parquet %s: %w", path, err)
	}
	// iceberg-go's AddFiles converts the file schema strictly (it rejects
	// zoned timestamps outside UTC), so such files take the explicit path.
	_, strictErr := icetable.ArrowSchemaToIceberg(arrowSchema, true, schema.NameMapping())
	f := partitionedFile{path: path, size: stat.Size(), rows: md.NumRows, partition: map[int]any{}, inferable: strictErr == nil}
	for field := range spec.Fields() {
		source, ok := schema.FindFieldByID(field.SourceID)
		if !ok {
			return partitionedFile{}, fmt.Errorf("partition field %s: source column id %d is not in the table schema", field.Name, field.SourceID)
		}
		idx := contractIndex(contract.Spec, source.Name, field.Transform)
		if idx < 0 {
			return partitionedFile{}, fmt.Errorf("parquet %s: partition contract has no value for %s(%s)", path, field.Transform, source.Name)
		}
		lit, err := contract.Values[idx].Literal(field.Transform.ResultType(source.Type))
		if err != nil {
			return partitionedFile{}, failure.NewFailure(failure.FailureDataIntegrity, false, true, fmt.Errorf("parquet %s partition %s: %w", path, field.Name, err))
		}
		if lit == nil {
			f.partition[field.FieldID] = nil
			continue
		}
		f.partition[field.FieldID] = lit.Any()
		// Without min/max statistics iceberg-go would infer a null partition.
		f.inferable = f.inferable && hasMinMax(md, source.Name)
	}
	f.columnSizes, f.valueCounts, f.nullCounts = countMetrics(md, schema)
	return f, nil
}

func contractIndex(spec icebergpart.Spec, source string, transform iceberg.Transform) int {
	for i, f := range spec {
		t, err := iceberg.ParseTransform(f.Transform)
		if err == nil && f.Source == source && t.Equals(transform) {
			return i
		}
	}
	return -1
}

// checkFileSchema rejects files whose top-level columns are missing from the
// table or have a type the table column cannot read. iceberg-go's AddFiles
// does the same check, but AddDataFiles trusts its caller.
func checkFileSchema(fileSchema *arrow.Schema, table *iceberg.Schema) error {
	present := make(map[string]bool, len(fileSchema.Fields()))
	for _, field := range fileSchema.Fields() {
		present[field.Name] = true
		col, ok := table.FindFieldByName(field.Name)
		if !ok {
			return fmt.Errorf("column %q is not in the table schema", field.Name)
		}
		if _, primitive := col.Type.(iceberg.PrimitiveType); !primitive {
			continue
		}
		got, err := icebergpart.IcebergType(field.Type)
		if err != nil {
			return fmt.Errorf("column %q: %w", field.Name, err)
		}
		if !got.Equals(col.Type) && !promotable(got, col.Type) {
			return fmt.Errorf("column %q has type %s but the table column is %s", field.Name, got, col.Type)
		}
	}
	for _, col := range table.Fields() {
		if col.Required && !present[col.Name] {
			return fmt.Errorf("required column %q is missing", col.Name)
		}
	}
	return nil
}

// promotable reports the Iceberg type promotions a reader applies.
func promotable(from, to iceberg.Type) bool {
	switch f := from.(type) {
	case iceberg.Int32Type:
		_, ok := to.(iceberg.Int64Type)
		return ok
	case iceberg.Float32Type:
		_, ok := to.(iceberg.Float64Type)
		return ok
	case iceberg.DecimalType:
		t, ok := to.(iceberg.DecimalType)
		return ok && t.Scale() == f.Scale() && t.Precision() >= f.Precision()
	}
	return false
}

func hasMinMax(md *metadata.FileMetaData, column string) bool {
	idx := md.Schema.ColumnIndexByName(column)
	if idx < 0 {
		return false
	}
	for i := 0; i < md.NumRowGroups(); i++ {
		rg := md.RowGroup(i)
		if rg.NumRows() == 0 {
			continue
		}
		cc, err := rg.ColumnChunk(idx)
		if err != nil {
			return false
		}
		set, err := cc.StatsSet()
		if err != nil || !set {
			return false
		}
		stats, err := cc.Statistics()
		if err != nil || stats == nil || !stats.HasMinMax() {
			return false
		}
	}
	return true
}

// countMetrics sums per-column sizes and value/null counts of top-level
// primitive columns over all row groups. A column's null count is reported
// only when every row group has one.
func countMetrics(md *metadata.FileMetaData, schema *iceberg.Schema) (sizes, values, nulls map[int]int64) {
	sizes, values, nulls = map[int]int64{}, map[int]int64{}, map[int]int64{}
	incomplete := map[int]bool{}
	for i := 0; i < md.NumRowGroups(); i++ {
		rg := md.RowGroup(i)
		for c := 0; c < rg.NumColumns(); c++ {
			cc, err := rg.ColumnChunk(c)
			if err != nil {
				continue
			}
			path := cc.PathInSchema()
			if len(path) != 1 {
				continue
			}
			field, ok := schema.FindFieldByName(path[0])
			if !ok {
				continue
			}
			sizes[field.ID] += cc.TotalCompressedSize()
			values[field.ID] += cc.NumValues()
			stats, err := cc.Statistics()
			if set, setErr := cc.StatsSet(); err != nil || setErr != nil || !set || stats == nil || !stats.HasNullCount() {
				incomplete[field.ID] = true
				continue
			}
			nulls[field.ID] += stats.NullCount()
		}
	}
	for id := range incomplete {
		delete(nulls, id)
	}
	return sizes, values, nulls
}

func (f partitionedFile) dataFile(spec iceberg.PartitionSpec) (iceberg.DataFile, error) {
	b, err := iceberg.NewDataFileBuilder(spec, iceberg.EntryContentData, f.path, iceberg.ParquetFile, f.partition, nil, nil, f.rows, f.size)
	if err != nil {
		return nil, fmt.Errorf("data file %s: %w", f.path, err)
	}
	return b.ColumnSizes(f.columnSizes).ValueCounts(f.valueCounts).NullValueCounts(f.nullCounts).Build(), nil
}

// currentDataFiles returns the live data files of the table's current
// snapshot. Delete files are excluded: they are not data files to replace.
func currentDataFiles(ctx context.Context, tbl *icetable.Table) ([]iceberg.DataFile, error) {
	snap := tbl.CurrentSnapshot()
	if snap == nil {
		return nil, nil
	}
	fs, err := tbl.FS(ctx)
	if err != nil {
		return nil, fmt.Errorf("open table fs: %w", err)
	}
	manifests, err := snap.Manifests(fs)
	if err != nil {
		return nil, fmt.Errorf("list manifests: %w", err)
	}
	var out []iceberg.DataFile
	for _, mf := range manifests {
		if mf.ManifestContent() != iceberg.ManifestContentData {
			continue
		}
		entries, err := mf.FetchEntries(fs, true)
		if err != nil {
			return nil, fmt.Errorf("fetch manifest entries: %w", err)
		}
		for _, e := range entries {
			out = append(out, e.DataFile())
		}
	}
	return out, nil
}
