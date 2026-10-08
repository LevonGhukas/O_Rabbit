package icebergreg

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/failure"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
	"github.com/apache/arrow-go/v18/arrow"
	iceberg "github.com/apache/iceberg-go"
	icetable "github.com/apache/iceberg-go/table"
)

func inferRunIcebergSchema(ctx context.Context, req RunRequest, tableName string) (*iceberg.Schema, error) {
	mode := normalizedRunRequestSourceMode(req.SourceMode)
	if mode == "query" {
		if !connectors.SupportsQueryMode(req.SourceEngine) {
			engine := connectors.NormalizeSourceEngine(req.SourceEngine)
			if engine == "" {
				engine = strings.TrimSpace(req.SourceEngine)
			}
			return nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query mode is not supported for %s", engine)
		}
	} else if !connectors.SupportsOrderedCursor(req.SourceEngine) && !connectors.SupportsDocumentReader(req.SourceEngine) {
		return nil, fmt.Errorf("auto-create Iceberg table is only implemented for SQL ordered-cursor and document sources; create %s before running registration", tableName)
	}

	var iceSchema *iceberg.Schema
	if len(req.DurableIcebergSchema) > 0 {
		var persisted iceberg.Schema
		if err := json.Unmarshal(req.DurableIcebergSchema, &persisted); err != nil {
			return nil, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("invalid durable source schema: %w", err))
		}
		if len(persisted.Fields()) == 0 {
			return nil, failure.NewFailure(failure.FailureConfigurationUnavailable, false, true, fmt.Errorf("durable source schema has no fields"))
		}
		iceSchema = &persisted
	}

	var arrSchema *arrow.Schema

	if iceSchema == nil && connectors.SupportsDocumentReader(req.SourceEngine) {
		db, err := connectors.OpenDocumentReader(ctx, req.SourceEngine, req.SourceDSN)
		if err != nil {
			return nil, fmt.Errorf("open document source for iceberg auto-create: %w", err)
		}
		defer db.Close()

		it, err := db.StreamDocuments(ctx, req.SourceTable, documentFilter(req.SourceEngine, req.RecordPath, req.FileFormat), 1000)
		if err != nil {
			return nil, fmt.Errorf("stream documents for iceberg auto-create: %w", err)
		}
		defer it.Close()
		var fieldOrder []string
		if ordered, ok := it.(connectors.OrderedDocumentIterator); ok {
			fieldOrder = ordered.FieldOrder()
		}

		var docBuf []map[string]any
		for it.Next(ctx) {
			doc, err := it.Decode()
			if err != nil {
				return nil, fmt.Errorf("decode document for iceberg auto-create: %w", err)
			}
			docBuf = append(docBuf, doc)
			if len(docBuf) >= 1000 {
				break
			}
		}

		arrSchema, err = arrowio.InferMongoSchemaWithFieldOrder(docBuf, fieldOrder)
		if err != nil {
			return nil, fmt.Errorf("infer document schema for iceberg auto-create: %w", err)
		}
	} else if iceSchema == nil {
		db, err := connectors.OpenIntRangeReader(ctx, req.SourceEngine, req.SourceDSN)
		if err != nil {
			return nil, fmt.Errorf("open source for iceberg auto-create: %w", err)
		}
		defer db.Close()

		cols, colTypes, err := describeSourceSchemaForAutoCreate(ctx, db, req)
		if err != nil {
			return nil, err
		}
		_, arrSchema, err = arrowio.PlansFromSQLEngineWithOverrides(req.SourceEngine, cols, colTypes, req.ColumnTypes)
		if err != nil {
			return nil, fmt.Errorf("sql->arrow schema: %w", err)
		}
	}

	if iceSchema == nil {
		var err error
		iceSchema, err = icetable.ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
		if err != nil {
			return nil, fmt.Errorf("arrow->iceberg schema: %w", err)
		}
	}

	return iceSchema, nil
}

// InferDurableIcebergSchema snapshots the source/query schema in Iceberg's
// stable JSON representation before a zero-artifact run enters its durable
// commit boundary.
func InferDurableIcebergSchema(ctx context.Context, engine, dsn, mode, table, query, recordPath, fileFormat string) (json.RawMessage, error) {
	schema, _, err := InferDurableIcebergSchemaWithWarnings(ctx, engine, dsn, mode, table, query, recordPath, fileFormat, nil)
	return schema, err
}

// InferDurableIcebergSchemaWithWarnings snapshots the durable source schema and
// returns the document inference warnings produced by that same sample.
func InferDurableIcebergSchemaWithWarnings(ctx context.Context, engine, dsn, mode, table, query, recordPath, fileFormat string, columnTypes map[string]string) (json.RawMessage, []typesystem.TypeWarning, error) {
	if connectors.SupportsDocumentReader(engine) {
		reader, err := connectors.OpenDocumentReader(ctx, engine, dsn)
		if err != nil {
			return nil, nil, fmt.Errorf("open document source for durable schema: %w", err)
		}
		defer reader.Close()
		inference, err := arrowio.InferMongoSchemaFromReader(ctx, reader, table, documentFilter(engine, recordPath, fileFormat))
		if err != nil {
			return nil, nil, fmt.Errorf("infer document durable schema: %w", err)
		}
		iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(inference.Schema, false)
		if err != nil {
			return nil, nil, fmt.Errorf("arrow->iceberg durable schema: %w", err)
		}
		schema, err := json.Marshal(iceSchema)
		if err != nil {
			return nil, nil, err
		}
		return schema, arrowio.MongoTypeWarnings(inference), nil
	}
	reader, err := connectors.OpenIntRangeReader(ctx, engine, dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open source for durable schema: %w", err)
	}
	defer reader.Close()
	req := RunRequest{SourceEngine: engine, SourceMode: mode, SourceTable: table, SourceQuery: query}
	cols, sqlTypes, err := describeSourceSchemaForAutoCreate(ctx, reader, req)
	if err != nil {
		return nil, nil, err
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("durable source schema has no columns")
	}
	_, arrSchema, err := arrowio.PlansFromSQLEngineWithOverrides(engine, cols, sqlTypes, columnTypes)
	if err != nil {
		return nil, nil, fmt.Errorf("sql->arrow durable schema: %w", err)
	}
	iceSchema, err := icetable.ArrowSchemaToIcebergWithFreshIDs(arrSchema, false)
	if err != nil {
		return nil, nil, fmt.Errorf("arrow->iceberg durable schema: %w", err)
	}
	schema, err := json.Marshal(iceSchema)
	return schema, nil, err
}

func normalizedRunRequestSourceMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "table":
		return "table"
	case "query", "sql":
		return "query"
	default:
		return strings.ToLower(strings.TrimSpace(raw))
	}
}

func describeSourceSchemaForAutoCreate(ctx context.Context, reader connectors.TableReader, req RunRequest) ([]string, []*sql.ColumnType, error) {
	mode := normalizedRunRequestSourceMode(req.SourceMode)
	if mode == "query" {
		query := strings.TrimSpace(req.SourceQuery)
		if query == "" {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: source query is empty")
		}
		queryReader, ok := reader.(connectors.SourceQueryReader)
		if !ok {
			engine := connectors.NormalizeSourceEngine(req.SourceEngine)
			if engine == "" {
				engine = strings.TrimSpace(req.SourceEngine)
			}
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query mode is not supported for %s", engine)
		}
		cols, colTypes, err := queryReader.DescribeQuery(ctx, query)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: describe query result: %w", err)
		}
		if len(cols) == 0 {
			return nil, nil, fmt.Errorf("cannot infer Iceberg schema for query-mode run: query returned no columns")
		}
		return cols, colTypes, nil
	}

	cols, colTypes, err := reader.DescribeTable(ctx, req.SourceTable)
	if err != nil {
		return nil, nil, fmt.Errorf("describe table for iceberg auto-create: %w", err)
	}
	if len(req.SelectColumns) > 0 {
		selMap := make(map[string]int)
		for idx, c := range cols {
			selMap[strings.ToLower(c)] = idx
		}
		filteredCols := make([]string, 0, len(req.SelectColumns))
		filteredColTypes := make([]*sql.ColumnType, 0, len(req.SelectColumns))
		for _, sc := range req.SelectColumns {
			scClean := strings.TrimSpace(sc)
			if idx, ok := selMap[strings.ToLower(scClean)]; ok {
				filteredCols = append(filteredCols, cols[idx])
				filteredColTypes = append(filteredColTypes, colTypes[idx])
			}
		}
		if len(filteredCols) > 0 {
			return filteredCols, filteredColTypes, nil
		}
	}
	return cols, colTypes, nil
}
