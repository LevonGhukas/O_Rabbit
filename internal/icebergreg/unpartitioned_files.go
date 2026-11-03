package icebergreg

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	iceberg "github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	icetable "github.com/apache/iceberg-go/table"

	"github.com/LevonGhukas/O_Rabbit/internal/failure"
)

// registerUnpartitionedFiles adds newFiles to an unpartitioned table, or on a
// full refresh replaces the existing data files with them, in the
// transaction's single snapshot.
//
// iceberg-go converts each file's schema strictly and rejects zoned
// timestamps outside UTC, which workers write for sources in another time
// zone. When any file is rejected that way, every file is registered as an
// explicit data file with count metrics instead, as partitioned tables do, so
// a source time zone never blocks a registration.
func registerUnpartitionedFiles(ctx context.Context, tx *icetable.Transaction, existing []iceberg.DataFile, fullRefresh bool, newFiles []string, props iceberg.Properties) error {
	staged, err := tx.StagedTable()
	if err != nil {
		return err
	}
	fs, err := staged.FS(ctx)
	if err != nil {
		return err
	}
	schema := staged.Schema()
	files := make([]unpartitionedFile, 0, len(newFiles))
	strict := true
	for _, path := range newFiles {
		f, err := readUnpartitionedFile(fs, path, schema)
		if err != nil {
			return err
		}
		strict = strict && f.inferable
		files = append(files, f)
	}

	if strict {
		existingPaths := make([]string, len(existing))
		for i, df := range existing {
			existingPaths[i] = df.FilePath()
		}
		return applyRESTGoFileMutation(ctx, tx, fullRefresh, existingPaths, newFiles, props)
	}

	spec := staged.Spec()
	dataFiles := make([]iceberg.DataFile, len(files))
	for i, f := range files {
		// iceberg-go checks file schemas itself, but AddDataFiles trusts its
		// caller.
		if f.schemaErr != nil {
			return failure.NewFailure(failure.FailureSchemaIncompatible, false, true, fmt.Errorf("parquet %s: %w", f.path, f.schemaErr))
		}
		df, err := f.dataFile(spec)
		if err != nil {
			return err
		}
		dataFiles[i] = df
	}
	if fullRefresh {
		if err := tx.ReplaceDataFilesWithDataFiles(ctx, existing, dataFiles, props); err != nil {
			return fmt.Errorf("iceberg full refresh replace: %w", err)
		}
		return nil
	}
	if err := tx.AddDataFiles(ctx, dataFiles, props); err != nil {
		return fmt.Errorf("iceberg incremental append: %w", err)
	}
	return nil
}

// unpartitionedFile is a worker file prepared for registration from its
// footer. schemaErr is why the table cannot read the file, if it cannot.
type unpartitionedFile struct {
	partitionedFile
	schemaErr error
}

// readUnpartitionedFile reads a worker file's footer: its schema, row count
// and column statistics. inferable reports whether iceberg-go accepts the
// file schema itself.
func readUnpartitionedFile(fs iceio.IO, path string, schema *iceberg.Schema) (unpartitionedFile, error) {
	input, err := fs.Open(path)
	if err != nil {
		return unpartitionedFile{}, fmt.Errorf("open parquet %s: %w", path, err)
	}
	reader, err := file.NewParquetReader(input)
	if err != nil {
		_ = input.Close()
		return unpartitionedFile{}, fmt.Errorf("read parquet footer %s: %w", path, err)
	}
	defer reader.Close()
	md := reader.MetaData()

	arrowSchema, err := pqarrow.FromParquet(md.Schema, &pqarrow.ArrowReadProperties{}, md.KeyValueMetadata())
	if err != nil {
		return unpartitionedFile{}, fmt.Errorf("read parquet schema %s: %w", path, err)
	}
	stat, err := input.Stat()
	if err != nil {
		return unpartitionedFile{}, fmt.Errorf("stat parquet %s: %w", path, err)
	}
	_, strictErr := icetable.ArrowSchemaToIceberg(arrowSchema, true, schema.NameMapping())
	f := partitionedFile{path: path, size: stat.Size(), rows: md.NumRows, partition: map[int]any{}, inferable: strictErr == nil}
	f.columnSizes, f.valueCounts, f.nullCounts = countMetrics(md, schema)
	return unpartitionedFile{partitionedFile: f, schemaErr: checkFileSchema(arrowSchema, schema)}, nil
}
