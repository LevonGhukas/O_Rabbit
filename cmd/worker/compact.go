package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// compactionTaskType is the partition spec type of a compaction task. Its
// inputs are table files of one partition; the task rewrites them into fewer,
// larger files that the master swaps in.
const compactionTaskType = "iceberg_compact"

// compactionInput is one file a compaction task rewrites, with the size and
// row count the table records for it.
type compactionInput struct {
	Key     string `json:"key"`
	Size    int64  `json:"size"`
	Records int64  `json:"records"`
}

type objectOpener interface {
	OpenObject(ctx context.Context, key string) (io.ReadCloser, bool, error)
}

// compactIcebergFiles runs a compaction task: it reads every input and writes
// its rows, in input order, through the task's Parquet writer.
func compactIcebergFiles(ctx context.Context, log *slog.Logger, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache) (sourceExtract, error) {
	res := sourceExtract{OutputPart: ps.OutputPart}
	if len(ps.Inputs) == 0 {
		return res, fmt.Errorf("compaction task has no inputs")
	}
	u, ms, err := clients.S3(ctx, s3io.Config{
		Endpoint:       t.S3Endpoint,
		Region:         t.S3Region,
		Bucket:         t.S3Bucket,
		ForcePathStyle: t.S3ForcePathStyle,
		Credentials:    taskCredentialsFromContext(ctx).S3,
	})
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("init s3: %w", err)
	}
	target := t.TargetFileBytes
	if ps.TargetFileBytes > 0 {
		target = ps.TargetFileBytes
	}
	pw, err := newParquetOutputWriter(ctx, t.GetIcebergPartitionSpecJson(), target)
	if err != nil {
		return res, err
	}
	start := time.Now()
	rows, err := compactInputs(ctx, u, ps.Inputs, pw)
	res.ConvertMS = time.Since(start).Milliseconds()
	res.Rows = rows
	if err != nil {
		pw.Abort()
		return res, err
	}
	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	log.Info("compaction task rewrote files", slog.String("task_id", t.TaskId), slog.Int("inputs", len(ps.Inputs)), slog.Int("outputs", len(res.ParquetFiles)), slog.Int64("rows", rows))
	return res, nil
}

// compactInputs copies the rows of every input into pw. Each input must still
// be the file the master planned (same size and row count), and all inputs
// must share one schema; otherwise the task fails and nothing is replaced.
func compactInputs(ctx context.Context, store objectOpener, inputs []compactionInput, pw parquetOutputWriter) (int64, error) {
	var (
		total  int64
		schema *arrow.Schema
	)
	for _, in := range inputs {
		path, err := downloadInput(ctx, store, in)
		if err != nil {
			return total, err
		}
		rows, fileSchema, err := copyParquetRows(ctx, path, schema, pw)
		_ = os.Remove(path)
		if err != nil {
			return total, fmt.Errorf("compaction input %s: %w", in.Key, err)
		}
		if schema == nil {
			schema = fileSchema
		}
		if rows != in.Records {
			return total, fmt.Errorf("compaction input %s has %d rows but the table records %d", in.Key, rows, in.Records)
		}
		total += rows
	}
	return total, nil
}

// downloadInput copies one input into the task workspace, checking its size.
func downloadInput(ctx context.Context, store objectOpener, in compactionInput) (string, error) {
	body, found, err := store.OpenObject(ctx, in.Key)
	if err != nil {
		return "", fmt.Errorf("open compaction input %s: %w", in.Key, err)
	}
	if !found {
		return "", fmt.Errorf("compaction input %s no longer exists", in.Key)
	}
	defer body.Close()
	f, err := os.CreateTemp(workspaceDirFromContext(ctx), "orabbit_compact_*.parquet")
	if err != nil {
		return "", err
	}
	n, copyErr := io.Copy(f, body)
	closeErr := f.Close()
	if copyErr != nil || closeErr != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("download compaction input %s: %w", in.Key, firstErr(copyErr, closeErr))
	}
	if n != in.Size {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("compaction input %s is %d bytes but the table records %d", in.Key, n, in.Size)
	}
	return f.Name(), nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// copyParquetRows streams one local Parquet file into pw. Schema metadata is
// dropped: it carries the input's footer, not the output's.
func copyParquetRows(ctx context.Context, path string, want *arrow.Schema, pw parquetOutputWriter) (int64, *arrow.Schema, error) {
	pr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return 0, nil, err
	}
	defer pr.Close()
	fr, err := pqarrow.NewFileReader(pr, pqarrow.ArrowReadProperties{BatchSize: 64 * 1024}, memory.DefaultAllocator)
	if err != nil {
		return 0, nil, err
	}
	read, err := fr.Schema()
	if err != nil {
		return 0, nil, err
	}
	schema := arrow.NewSchema(read.Fields(), nil)
	if want != nil {
		fp, err := artifact.SchemaFingerprint(schema)
		if err != nil {
			return 0, nil, err
		}
		wantFP, err := artifact.SchemaFingerprint(want)
		if err != nil {
			return 0, nil, err
		}
		if fp != wantFP {
			return 0, nil, fmt.Errorf("schema differs from the other inputs")
		}
		// Write every input with the first input's schema object.
		schema = want
	}
	records, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, nil, err
	}
	defer records.Release()
	var rows int64
	for records.Next() {
		rec := records.RecordBatch()
		out := array.NewRecordBatch(schema, rec.Columns(), rec.NumRows())
		err := pw.Write(schema, out)
		out.Release()
		if err != nil {
			return rows, nil, err
		}
		rows += rec.NumRows()
	}
	if err := records.Err(); err != nil {
		return rows, nil, err
	}
	return rows, schema, nil
}
