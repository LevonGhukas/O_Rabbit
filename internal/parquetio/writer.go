package parquetio

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type Writer struct {
	f      *os.File
	pw     *pqarrow.FileWriter
	schema *arrow.Schema
}

type Options struct {
	Compression compress.Compression
}

func NewTempFileWriter(schema *arrow.Schema, opts Options) (*Writer, string, error) {
	return NewTempFileWriterInDir(schema, opts, "")
}

func NewTempFileWriterInDir(schema *arrow.Schema, opts Options, dir string) (*Writer, string, error) {
	if schema == nil {
		return nil, "", fmt.Errorf("nil schema")
	}
	if opts.Compression == 0 {
		opts.Compression = compress.Codecs.Snappy
	}

	f, err := os.CreateTemp(dir, "orabbit_*.parquet")
	if err != nil {
		return nil, "", err
	}
	path := f.Name()

	props := parquet.NewWriterProperties(parquet.WithCompression(opts.Compression))
	arrProps := pqarrow.DefaultWriterProps()

	pw, err := pqarrow.NewFileWriter(schema, f, props, arrProps)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return nil, "", err
	}

	return &Writer{f: f, pw: pw, schema: schema}, path, nil
}

func (w *Writer) Write(rec arrow.RecordBatch) error {
	if w == nil || w.pw == nil {
		return fmt.Errorf("parquet writer is nil")
	}
	return w.pw.Write(rec)
}

func (w *Writer) Schema() *arrow.Schema {
	if w == nil {
		return nil
	}
	return w.schema
}

func (w *Writer) Close() error {
	if w == nil {
		return nil
	}
	var err error
	if w.pw != nil {
		if e := w.pw.Close(); err == nil {
			err = e
		}
		w.pw = nil
	}
	if w.f != nil {
		// pqarrow.FileWriter may close the underlying file; ignore double-close.
		if e := w.f.Close(); err == nil && e != nil && !errors.Is(e, os.ErrClosed) {
			err = e
		}
		w.f = nil
	}
	return err
}

type FileMeta struct {
	Bytes  int64
	SHA256 string
}

func ComputeFileMeta(path string) (FileMeta, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileMeta{}, err
	}
	defer f.Close()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return FileMeta{}, err
	}
	return FileMeta{Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil))}, nil
}
