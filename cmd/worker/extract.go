package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/grpcpb"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type sourceExtract struct {
	DBConnectMS    int64
	QueryMS        int64
	ConvertMS      int64
	ParquetCloseMS int64
	Rows           int64
	MaxCursor      string
	CursorDomain   string
	ParquetPath    string
	ParquetFiles   []parquetOutputFile
	ParquetBytes   int64
	LogicalBytes   int64
	PartitionLower string
	PartitionUpper string
	OutputPart     int64
}

// sourceQueryTimeout bounds one partition's source query. It is set once from
// the worker configuration at startup; 0 means no limit.
var sourceQueryTimeout = 2 * time.Hour

func withSourceQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if sourceQueryTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, sourceQueryTimeout)
}

// extractSQLCursorTask reads an ordered-cursor partition from a SQL source and writes a local Parquet file.
func extractSQLCursorTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache, sourceEngine string) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "sql_cursor_range" && ps.Type != "sql_cursor_single" && ps.Type != "mssql_int_range" && ps.Type != "sql_int_range" {
		return res, fmt.Errorf("unsupported partition type %q for %s", ps.Type, sourceEngine)
	}
	sourceMode := strings.ToLower(strings.TrimSpace(ps.SourceMode))
	if sourceMode == "" {
		sourceMode = "table"
	}
	sourceQuery := ""
	if sourceMode == "query" {
		sourceQuery = strings.TrimSpace(t.SourceSql)
		if sourceQuery == "" {
			return res, fmt.Errorf("query mode task is missing source_sql")
		}
		log.Info("query mode task execution",
			slog.String("task_id", t.TaskId),
			slog.String("run_id", t.RunId),
			slog.String("source_engine", sourceEngine),
			slog.String("query_hash", ps.QueryHash),
		)
	}
	res.CursorDomain = ps.CursorDomain
	res.PartitionLower = strings.TrimSpace(ps.Lower)
	res.PartitionUpper = strings.TrimSpace(ps.Upper)
	res.OutputPart = ps.OutputPart
	if res.OutputPart <= 0 {
		res.OutputPart = int64(t.TaskIndex)
	}

	src, ms, err := clients.SQLReader(ctx, sourceEngine, taskCredentialsFromContext(ctx).SourceDSN)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open %s: %w", sourceEngine, err)
	}

	qctx, cancel := withSourceQueryTimeout(ctx)
	defer cancel()

	queryStart := time.Now()
	rows, cols, colTypes, cursorIdx, err := src.QueryCursor(qctx, connectors.CursorQuery{
		Table:          ps.Table,
		SourceQuery:    sourceQuery,
		CursorColumn:   ps.CursorColumn,
		CursorDomain:   connectors.NormalizeCursorDomain(ps.CursorDomain),
		LowerBound:     ps.Lower,
		UpperBound:     ps.Upper,
		LowerExclusive: ps.LowerExclusive,
		UpperInclusive: ps.UpperInclusive,
		WhereClause:    ps.WhereClause,
		SelectColumns:  ps.SelectColumns,
		ColumnTypes:    ps.ColumnTypes,
	})
	if err != nil {
		return res, fmt.Errorf("query cursor partition: %w", err)
	}
	defer rows.Close()
	res.QueryMS = time.Since(queryStart).Milliseconds()

	alloc := memory.NewGoAllocator()
	var (
		pw       = newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)
		rowsRead int64
		lastProg time.Time
	)

	convertStart := time.Now()
	total, actualMaxCursor, err := arrowio.RowsToRecordBatchesEngineWithOverrides(sourceEngine, rows, cols, colTypes, ps.ColumnTypes, 50_000, alloc, cursorIdx, connectors.NormalizeCursorDomain(ps.CursorDomain), func(schema *arrow.Schema, rec arrow.RecordBatch) error {
		rowsRead += rec.NumRows()
		if time.Since(lastProg) > 5*time.Second {
			lastProg = time.Now()
			if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     rowsRead,
			}); err != nil {
				return err
			}
		}
		return pw.Write(schema, rec)
	})
	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = total
	res.MaxCursor = actualMaxCursor
	if err != nil {
		pw.Abort()
		return res, fmt.Errorf("read to arrow/parquet: %w", err)
	}
	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

// extractFlightSQLTask runs a FlightSQL query and writes a local Parquet file.
func extractFlightSQLTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "single" {
		return res, fmt.Errorf("unsupported partition type %q for flightsql", ps.Type)
	}
	res.OutputPart = int64(t.TaskIndex)

	src, ms, err := clients.FlightSQL(ctx, taskCredentialsFromContext(ctx).SourceDSN)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open flightsql: %w", err)
	}

	qctx, cancel := withSourceQueryTimeout(ctx)
	defer cancel()

	pw := newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)
	var (
		rowsRead int64
		lastProg time.Time
	)

	convertStart := time.Now()
	total, err := src.StreamQuery(qctx, t.SourceSql, func(schema *arrow.Schema, rec arrow.RecordBatch) error {
		rowsRead += rec.NumRows()
		if time.Since(lastProg) > 5*time.Second {
			lastProg = time.Now()
			if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
				WorkerId:     workerID,
				TaskId:       t.TaskId,
				RunId:        t.RunId,
				AttemptId:    t.AttemptId,
				FencingToken: t.FencingToken,
				RowsRead:     rowsRead,
			}); err != nil {
				return err
			}
		}
		return pw.Write(schema, rec)
	})
	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = total
	if err != nil {
		pw.Abort()
		return res, fmt.Errorf("read flightsql to parquet: %w", err)
	}
	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

func extractDocumentTask(ctx context.Context, log *slog.Logger, cp grpcpb.ControlPlaneClient, workerID string, t *grpcpb.TaskAssignment, ps partitionSpec, clients *clientCache, sourceEngine string) (sourceExtract, error) {
	res := sourceExtract{}

	if ps.Type != "single" && ps.Type != "sql_cursor_range" && ps.Type != "sql_cursor_single" {
		return res, fmt.Errorf("unsupported partition type %q for %s", ps.Type, sourceEngine)
	}
	res.OutputPart = int64(t.TaskIndex)

	src, ms, err := clients.DocumentReader(ctx, sourceEngine, taskCredentialsFromContext(ctx).SourceDSN)
	res.DBConnectMS = ms
	if err != nil {
		return res, fmt.Errorf("open %s: %w", sourceEngine, err)
	}

	qctx, cancel := withSourceQueryTimeout(ctx)
	defer cancel()

	collection := ps.Table
	if collection == "" {
		collection = strings.TrimSpace(t.SourceSql)
	}
	if collection == "" {
		return res, fmt.Errorf("%s requires collection name in partition_spec.table or source_sql", sourceEngine)
	}

	const batchSize = 10000
	var (
		pw             *parquetRollingWriter
		schema         *arrow.Schema
		rowsRead       int64
		lastProg       time.Time
		schemaInferred bool
	)
	pw = newParquetRollingWriterWithContext(ctx, t.TargetFileBytes)

	convertStart := time.Now()

	var filter map[string]any
	if ps.Type == "sql_cursor_range" || ps.Type == "sql_cursor_single" {
		f, err := src.BuildCursorFilter(connectors.CursorQuery{
			CursorColumn:   ps.CursorColumn,
			CursorDomain:   connectors.NormalizeCursorDomain(ps.CursorDomain),
			LowerBound:     ps.Lower,
			UpperBound:     ps.Upper,
			LowerExclusive: ps.LowerExclusive,
			UpperInclusive: ps.UpperInclusive,
		})
		if err != nil {
			return res, fmt.Errorf("build cursor filter: %w", err)
		}
		filter = f
		res.CursorDomain = ps.CursorDomain
		res.PartitionLower = ps.Lower
		res.PartitionUpper = ps.Upper
	}
	if sourceEngine == "s3" {
		if recordPath := strings.TrimSpace(ps.RecordPath); recordPath != "" {
			if filter == nil {
				filter = make(map[string]any)
			}
			filter["record_path"] = recordPath
		}
		if fileFormat := strings.TrimSpace(ps.FileFormat); fileFormat != "" {
			if filter == nil {
				filter = make(map[string]any)
			}
			filter["format"] = fileFormat
		}
	}

	it, err := src.StreamDocuments(qctx, collection, filter, batchSize)
	if err != nil {
		return res, fmt.Errorf("%s stream: %w", sourceEngine, err)
	}
	defer it.Close()
	var fieldOrder []string
	if ordered, ok := it.(connectors.OrderedDocumentIterator); ok {
		fieldOrder = ordered.FieldOrder()
	}

	var (
		docBuf []map[string]any
		alloc  = memory.NewGoAllocator()
	)

	for it.Next(qctx) {
		doc, err := it.Decode()
		if err != nil {
			return res, fmt.Errorf("decode %s document: %w", sourceEngine, err)
		}
		docBuf = append(docBuf, doc)

		if !schemaInferred && len(docBuf) >= batchSize {
			schema, err = arrowio.InferMongoSchemaWithFieldOrder(docBuf, fieldOrder)
			if err != nil {
				return res, fmt.Errorf("infer schema: %w", err)
			}
			schemaInferred = true
		}

		if schemaInferred && len(docBuf) >= batchSize {
			if err := writeMongoDocBatch(alloc, pw, schema, docBuf); err != nil {
				return res, err
			}
			rowsRead += int64(len(docBuf))
			docBuf = docBuf[:0]

			if time.Since(lastProg) > 5*time.Second {
				lastProg = time.Now()
				if err := reportProgressBestEffort(ctx, log, cp, &grpcpb.ReportTaskProgressRequest{
					WorkerId:     workerID,
					TaskId:       t.TaskId,
					RunId:        t.RunId,
					AttemptId:    t.AttemptId,
					FencingToken: t.FencingToken,
					RowsRead:     rowsRead,
				}); err != nil {
					return res, err
				}
			}
		}
	}

	if err := it.Err(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("%s cursor error: %w", sourceEngine, err)
	}

	// Flush remaining docs.
	if len(docBuf) > 0 {
		if !schemaInferred {
			schema, err = arrowio.InferMongoSchemaWithFieldOrder(docBuf, fieldOrder)
			if err != nil {
				return res, fmt.Errorf("infer schema: %w", err)
			}
		}
		if err := writeMongoDocBatch(alloc, pw, schema, docBuf); err != nil {
			return res, err
		}
		rowsRead += int64(len(docBuf))
	}

	res.ConvertMS = time.Since(convertStart).Milliseconds()
	res.Rows = rowsRead

	if err := pw.Close(); err != nil {
		pw.Abort()
		return res, fmt.Errorf("close parquet: %w", err)
	}
	res.ParquetCloseMS = pw.CloseMS()
	res.ParquetFiles = pw.Files()
	res.ParquetBytes = pw.TotalBytes()
	res.LogicalBytes = pw.TotalLogicalBytes()
	if len(res.ParquetFiles) == 0 {
		return res, nil
	}
	return res, nil
}

func writeMongoDocBatch(alloc memory.Allocator, pw *parquetRollingWriter, schema *arrow.Schema, docs []map[string]any) error {
	rec, err := arrowio.MongoDocsToRecord(alloc, schema, docs)
	if err != nil {
		return fmt.Errorf("convert batch to arrow: %w", err)
	}
	if rec == nil {
		return nil
	}
	defer rec.Release()
	return pw.Write(schema, rec)
}
