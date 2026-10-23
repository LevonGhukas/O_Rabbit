package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergpart"

	"github.com/apache/arrow-go/v18/arrow"
)

// parquetOutputWriter is a task's Parquet output: one rolling sequence of
// files, or one per Iceberg partition tuple.
type parquetOutputWriter interface {
	Write(schema *arrow.Schema, rec arrow.RecordBatch) error
	Close() error
	Abort()
	Files() []parquetOutputFile
	TotalBytes() int64
	TotalLogicalBytes() int64
	CloseMS() int64
}

// maxOpenPartitionWriters bounds the Parquet files a task keeps open when its
// rows span many partitions. Each open file buffers a row group in memory.
// When the bound is reached the least recently written file is finished; a
// later row of that partition starts a new file, so high partition
// cardinality costs more, smaller files rather than unbounded memory.
const maxOpenPartitionWriters = 32

// newTaskParquetWriter returns the writer for t's output. A task for a
// partitioned Iceberg table writes one partition tuple per file.
func newTaskParquetWriter(ctx context.Context, t *grpcpb.TaskAssignment) (parquetOutputWriter, error) {
	return newParquetOutputWriter(ctx, t.GetIcebergPartitionSpecJson(), t.TargetFileBytes)
}

func newParquetOutputWriter(ctx context.Context, partitionSpecJSON string, targetFileBytes int64) (parquetOutputWriter, error) {
	spec, err := icebergpart.ParseSpecJSON(partitionSpecJSON)
	if err != nil {
		return nil, err
	}
	if len(spec) == 0 {
		return newParquetRollingWriterWithContext(ctx, targetFileBytes), nil
	}
	return newPartitionedParquetWriter(ctx, targetFileBytes, spec, maxOpenPartitionWriters)
}

type partitionedParquetWriter struct {
	ctx             context.Context
	targetFileBytes int64
	partitioner     *icebergpart.Partitioner
	maxOpen         int

	slots map[string]*partitionSlot
	order []*partitionSlot // first-seen order, so file order is deterministic
	open  []*partitionSlot // slots with an open file, least recently written first
}

type partitionSlot struct {
	writer *parquetRollingWriter
}

func newPartitionedParquetWriter(ctx context.Context, targetFileBytes int64, spec icebergpart.Spec, maxOpen int) (*partitionedParquetWriter, error) {
	p, err := icebergpart.NewPartitioner(spec)
	if err != nil {
		return nil, err
	}
	if maxOpen < 1 {
		maxOpen = 1
	}
	return &partitionedParquetWriter{
		ctx:             ctx,
		targetFileBytes: targetFileBytes,
		partitioner:     p,
		maxOpen:         maxOpen,
		slots:           map[string]*partitionSlot{},
	}, nil
}

func (w *partitionedParquetWriter) Write(schema *arrow.Schema, rec arrow.RecordBatch) error {
	if rec == nil || rec.NumRows() == 0 {
		return nil
	}
	groups, err := w.partitioner.Split(rec)
	if err != nil {
		return fmt.Errorf("split rows by iceberg partition: %w", err)
	}
	for _, g := range groups {
		slot, err := w.slotFor(g)
		if err != nil {
			return err
		}
		part, err := icebergpart.SelectRows(w.ctx, rec, g.Rows)
		if err != nil {
			return err
		}
		err = slot.writer.Write(schema, part)
		part.Release()
		if err != nil {
			return err
		}
	}
	return nil
}

// slotFor returns the writer of g's partition and marks it most recently
// used, finishing the least recently used open file when the bound is reached.
func (w *partitionedParquetWriter) slotFor(g *icebergpart.Group) (*partitionSlot, error) {
	slot, ok := w.slots[g.Key]
	if !ok {
		footer, err := g.Contract.Marshal()
		if err != nil {
			return nil, err
		}
		slot = &partitionSlot{writer: newParquetRollingWriterWithContext(w.ctx, w.targetFileBytes)}
		slot.writer.footer = map[string]string{icebergpart.FooterKey: footer}
		w.slots[g.Key] = slot
		w.order = append(w.order, slot)
	}
	for i, s := range w.open {
		if s == slot {
			w.open = append(append(w.open[:i:i], w.open[i+1:]...), slot)
			return slot, nil
		}
	}
	if len(w.open) >= w.maxOpen {
		lru := w.open[0]
		w.open = w.open[1:]
		if err := lru.writer.Close(); err != nil {
			return nil, err
		}
	}
	w.open = append(w.open, slot)
	return slot, nil
}

func (w *partitionedParquetWriter) Close() error {
	var errs []error
	for _, s := range w.order {
		errs = append(errs, s.writer.Close())
	}
	w.open = nil
	return errors.Join(errs...)
}

func (w *partitionedParquetWriter) Abort() {
	for _, s := range w.order {
		s.writer.Abort()
	}
	w.open = nil
}

func (w *partitionedParquetWriter) Files() []parquetOutputFile {
	var out []parquetOutputFile
	for _, s := range w.order {
		out = append(out, s.writer.Files()...)
	}
	return out
}

func (w *partitionedParquetWriter) TotalBytes() int64 {
	var total int64
	for _, s := range w.order {
		total += s.writer.TotalBytes()
	}
	return total
}

func (w *partitionedParquetWriter) TotalLogicalBytes() int64 {
	var total int64
	for _, s := range w.order {
		total += s.writer.TotalLogicalBytes()
	}
	return total
}

func (w *partitionedParquetWriter) CloseMS() int64 {
	var total int64
	for _, s := range w.order {
		total += s.writer.CloseMS()
	}
	return total
}
