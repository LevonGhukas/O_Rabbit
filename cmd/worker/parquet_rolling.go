package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/artifact"
	"github.com/LevonGhukas/O_Rabbit/internal/parquetio"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/compress"
)

type parquetOutputFile struct {
	Path              string
	Bytes             int64
	Rows              int64
	LogicalBytes      int64
	SHA256            string
	SchemaFingerprint string
}

type parquetRollingWriter struct {
	ctx             context.Context
	targetFileBytes int64

	current             *parquetio.Writer
	currentPath         string
	currentRows         int64
	currentLogicalBytes int64
	currentEncodedBytes int64
	currentSchema       *arrow.Schema

	files   []parquetOutputFile
	closeMS int64
}

func newParquetRollingWriterWithContext(ctx context.Context, targetFileBytes int64) *parquetRollingWriter {
	return &parquetRollingWriter{
		ctx:             ctx,
		targetFileBytes: targetFileBytes,
	}
}

func (w *parquetRollingWriter) Write(schema *arrow.Schema, rec arrow.RecordBatch) error {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	if err := w.ensureOpen(schema); err != nil {
		return err
	}
	// A record batch is the rollover boundary. Use encoded bytes accumulated
	// from completed Parquet row groups to decide whether this batch belongs in
	// the current file. This permits a bounded overshoot and avoids eagerly
	// creating a tiny tail file after every target crossing.
	recBytes := recordLogicalBytes(rec)
	if w.shouldRollBefore(recBytes) {
		if err := w.closeCurrent(); err != nil {
			return err
		}
		if err := w.ensureOpen(schema); err != nil {
			return err
		}
	}
	if err := w.current.Write(rec); err != nil {
		return err
	}
	w.currentRows += rec.NumRows()
	w.currentLogicalBytes += recBytes
	// This excludes the footer, which is intentionally covered by the
	// tolerance below. It tracks compression rather than Arrow memory layout.
	w.currentEncodedBytes = w.current.CompressedBytes()
	return nil
}

const parquetRollOvershootFraction = int64(20) // permit up to 20% at a batch boundary

func recordLogicalBytes(rec arrow.RecordBatch) int64 {
	var total int64
	for i := 0; i < int(rec.NumCols()); i++ {
		arr := rec.Column(i)
		if arr != nil && arr.Data() != nil {
			total += int64(arr.Data().SizeInBytes())
		}
	}
	return total
}

func (w *parquetRollingWriter) shouldRollBefore(nextLogicalBytes int64) bool {
	if w.current == nil || w.currentRows == 0 {
		return false
	}
	target := w.targetFileBytes
	if target <= 0 {
		target = 256 * 1024 * 1024
	}
	// Predict only from this file's observed compression ratio. A conservative
	// fallback treats the next batch as incompressible until a ratio is known.
	predictedNext := nextLogicalBytes
	if w.currentLogicalBytes > 0 && w.currentEncodedBytes > 0 {
		predictedNext = nextLogicalBytes * w.currentEncodedBytes / w.currentLogicalBytes
	}
	maxBytes := target + target*parquetRollOvershootFraction/100
	// Do not split a merely growing file too early. Once it is close to target,
	// however, start the next batch in a fresh file if it would exceed bounded
	// overshoot. A single large batch is still allowed through intact.
	nearTarget := w.currentEncodedBytes >= target-target*parquetRollOvershootFraction/100
	return nearTarget && w.currentEncodedBytes+predictedNext > maxBytes
}

func (w *parquetRollingWriter) Close() error {
	return w.closeCurrent()
}

func (w *parquetRollingWriter) Abort() {
	if w.current != nil {
		_ = w.current.Close()
		w.current = nil
	}
	if strings.TrimSpace(w.currentPath) != "" {
		_ = os.Remove(w.currentPath)
		w.currentPath = ""
	}
	for _, f := range w.files {
		if strings.TrimSpace(f.Path) != "" {
			_ = os.Remove(f.Path)
		}
	}
	w.files = nil
	w.currentRows = 0
	w.currentLogicalBytes = 0
	w.currentEncodedBytes = 0
	w.currentSchema = nil
}

func (w *parquetRollingWriter) Files() []parquetOutputFile {
	return append([]parquetOutputFile(nil), w.files...)
}

func (w *parquetRollingWriter) TotalBytes() int64 {
	var total int64
	for _, f := range w.files {
		total += f.Bytes
	}
	return total
}

func (w *parquetRollingWriter) TotalLogicalBytes() int64 {
	var total int64
	for _, f := range w.files {
		total += f.LogicalBytes
	}
	return total
}

func (w *parquetRollingWriter) CloseMS() int64 {
	return w.closeMS
}

func (w *parquetRollingWriter) ensureOpen(schema *arrow.Schema) error {
	if w.current != nil {
		return nil
	}
	pw, path, err := parquetio.NewTempFileWriterInDir(schema, parquetio.Options{Compression: compress.Codecs.Snappy}, workspaceDirFromContext(w.ctx))
	if err != nil {
		return err
	}
	w.current = pw
	w.currentPath = path
	w.currentRows = 0
	w.currentLogicalBytes = 0
	w.currentSchema = schema
	return nil
}

func (w *parquetRollingWriter) closeCurrent() error {
	if w.current == nil {
		return nil
	}
	closeStart := time.Now()
	if err := w.current.Close(); err != nil {
		w.closeMS += time.Since(closeStart).Milliseconds()
		return err
	}
	w.closeMS += time.Since(closeStart).Milliseconds()

	info, err := artifact.ValidateLocalParquet(w.ctx, w.currentPath, w.currentRows, w.currentSchema)
	if err != nil {
		return err
	}
	w.files = append(w.files, parquetOutputFile{
		Path:              w.currentPath,
		Bytes:             info.ByteSize,
		Rows:              w.currentRows,
		LogicalBytes:      w.currentLogicalBytes,
		SHA256:            info.SHA256,
		SchemaFingerprint: info.SchemaFingerprint,
	})
	w.current = nil
	w.currentPath = ""
	w.currentRows = 0
	w.currentLogicalBytes = 0
	w.currentEncodedBytes = 0
	w.currentSchema = nil
	return nil
}

func buildTaskParquetObjectKeys(runPrefix string, partNo int64, fileCount int) []string {
	if fileCount <= 0 {
		return nil
	}
	base := strings.TrimSuffix(strings.TrimSpace(runPrefix), "/")
	keys := make([]string, 0, fileCount)
	for i := 0; i < fileCount; i++ {
		keys = append(keys, fmt.Sprintf("%s/part-%06d-%03d.parquet", base, partNo, i))
	}
	return keys
}

func parquetFileSizeStats(files []parquetOutputFile) (minBytes, maxBytes, avgBytes int64) {
	if len(files) == 0 {
		return 0, 0, 0
	}
	minBytes = files[0].Bytes
	maxBytes = files[0].Bytes
	var total int64
	for _, f := range files {
		if f.Bytes < minBytes {
			minBytes = f.Bytes
		}
		if f.Bytes > maxBytes {
			maxBytes = f.Bytes
		}
		total += f.Bytes
	}
	return minBytes, maxBytes, total / int64(len(files))
}

type workspaceDirKey struct{}

func withWorkspaceDir(ctx context.Context, dir string) context.Context {
	return context.WithValue(ctx, workspaceDirKey{}, dir)
}

func workspaceDirFromContext(ctx context.Context) string {
	if dir, ok := ctx.Value(workspaceDirKey{}).(string); ok {
		return dir
	}
	return ""
}
