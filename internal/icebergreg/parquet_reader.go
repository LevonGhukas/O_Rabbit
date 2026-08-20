package icebergreg

import (
	"context"
	"fmt"
	"sync/atomic"

	iceio "github.com/apache/iceberg-go/io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

type parquetRecordReader struct {
	ctx    context.Context
	fs     iceio.IO
	paths  []string
	index  int
	schema *arrow.Schema

	input   iceio.File
	current array.RecordReader
	err     error
	refs    atomic.Int64
}

func newParquetRecordReader(ctx context.Context, fs iceio.IO, paths []string) (*parquetRecordReader, error) {
	reader := &parquetRecordReader{ctx: ctx, fs: fs, paths: append([]string(nil), paths...)}
	reader.refs.Store(1)
	if err := reader.openNext(); err != nil {
		reader.Release()
		return nil, err
	}
	return reader, nil
}

func (r *parquetRecordReader) openNext() error {
	if r.index >= len(r.paths) {
		return nil
	}
	path := r.paths[r.index]
	r.index++

	input, err := r.fs.Open(path)
	if err != nil {
		return fmt.Errorf("open parquet %s: %w", path, err)
	}
	parquetReader, err := file.NewParquetReader(input)
	if err != nil {
		input.Close()
		return fmt.Errorf("read parquet metadata %s: %w", path, err)
	}
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 64 * 1024}, memory.DefaultAllocator)
	if err != nil {
		input.Close()
		return err
	}
	records, err := arrowReader.GetRecordReader(r.ctx, nil, nil)
	if err != nil {
		input.Close()
		return err
	}
	if r.schema == nil {
		r.schema = records.Schema()
	} else if !r.schema.Equal(records.Schema()) {
		records.Release()
		input.Close()
		return fmt.Errorf("parquet schemas differ inside one registration")
	}
	r.input = input
	r.current = records
	return nil
}

func (r *parquetRecordReader) closeCurrent() {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	if r.input != nil {
		_ = r.input.Close()
		r.input = nil
	}
}

func (r *parquetRecordReader) Retain() {
	r.refs.Add(1)
}

func (r *parquetRecordReader) Release() {
	if r.refs.Add(-1) == 0 {
		r.closeCurrent()
	}
}

func (r *parquetRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *parquetRecordReader) Next() bool {
	for r.current != nil {
		if r.current.Next() {
			return true
		}
		if err := r.current.Err(); err != nil {
			r.err = err
			r.closeCurrent()
			return false
		}
		r.closeCurrent()
		if err := r.openNext(); err != nil {
			r.err = err
			return false
		}
	}
	return false
}

func (r *parquetRecordReader) RecordBatch() arrow.RecordBatch {
	if r.current == nil {
		return nil
	}
	return r.current.RecordBatch()
}

func (r *parquetRecordReader) Record() arrow.RecordBatch {
	return r.RecordBatch()
}

func (r *parquetRecordReader) Err() error {
	return r.err
}
