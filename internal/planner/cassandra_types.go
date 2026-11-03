package planner

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LevonGhukas/O_Rabbit/internal/arrowio"
	"github.com/LevonGhukas/O_Rabbit/internal/connectors"
	"github.com/LevonGhukas/O_Rabbit/internal/crypto"
	"github.com/LevonGhukas/O_Rabbit/internal/dataset"
	"github.com/LevonGhukas/O_Rabbit/internal/db"
	"github.com/LevonGhukas/O_Rabbit/internal/icebergreg"
	"github.com/LevonGhukas/O_Rabbit/internal/jobopts"
	"github.com/LevonGhukas/O_Rabbit/internal/s3io"
	"github.com/LevonGhukas/O_Rabbit/internal/typesystem"
)

// cassandraDecimalPrecision is the precision chosen for Cassandra decimals:
// the most Iceberg's decimal holds, so later, larger values still fit the
// column without changing its type. Only the scale comes from the values.
const cassandraDecimalPrecision = 38

// cassandraEmptyDecimalScale is the scale used when a decimal column has no
// values yet to measure.
const cassandraEmptyDecimalScale = 18

// existingTable is the target table as planning found it.
type existingTable struct {
	// Columns holds the table's columns; empty when the table does not exist.
	Columns map[string]typesystem.LogicalType
	// Known is false when the table could not be checked and the dataset
	// already has committed data, so earlier runs' types must be assumed.
	Known bool
}

// targetTableLookupTimeout bounds reading the target table's schema during
// planning, so an unreachable catalog cannot stall the run.
const targetTableLookupTimeout = 30 * time.Second

// plannedTargetTable reads the run's target table for the automatic column
// types. When the table cannot be read and the dataset already has committed
// data, Known is false so earlier runs' types are kept.
func plannedTargetTable(ctx context.Context, st *db.Store, k crypto.Key, job db.Job, srcEngine string, o jobopts.Options, registrationConfig json.RawMessage, runID string) existingTable {
	_, hasData, _, _, err := loadDatasetState(ctx, st, k, job, srcEngine, o)
	if err != nil {
		hasData = true // cannot tell: assume earlier runs wrote data
	}
	unknown := existingTable{Columns: map[string]typesystem.LogicalType{}, Known: !hasData}
	cfg, err := icebergreg.ParseRunConfig(registrationConfig)
	if err != nil || !cfg.Enabled {
		// No catalog table to compare with. A dataset with data keeps the
		// types its files already have.
		return unknown
	}
	conn, err := st.GetConnection(ctx, job.TargetConnectionID)
	if err != nil {
		return unknown
	}
	target, err := dataset.ParseTarget(conn.MetadataJSON)
	if err != nil {
		return unknown
	}
	lookupCtx, cancel := context.WithTimeout(ctx, targetTableLookupTimeout)
	defer cancel()
	cols, err := icebergreg.ExistingColumnTypes(lookupCtx, cfg, target.Bucket, s3io.Config{Endpoint: target.Endpoint, Region: target.Region, Bucket: target.Bucket, ForcePathStyle: target.ForcePathStyle})
	if err != nil {
		slog.Warn("target table schema unavailable for automatic column types", "run_id", runID, "table", cfg.Table, "error", err)
		return unknown
	}
	return existingTable{Columns: cols, Known: true}
}

// withAutomaticColumnTypes returns the user's overrides with the automatic
// types added. An automatic type replaces a "source" placeholder for the
// same column, whatever the placeholder's spelling of the name.
func withAutomaticColumnTypes(user, auto map[string]string) map[string]string {
	if len(auto) == 0 {
		return user
	}
	out := make(map[string]string, len(user)+len(auto))
	for k, v := range user {
		out[k] = v
	}
	for col, t := range auto {
		for k := range out {
			if strings.EqualFold(strings.TrimSpace(k), col) {
				delete(out, k)
			}
		}
		out[col] = t
	}
	return out
}

// cassandraAutomaticColumnTypes chooses the types of Cassandra columns whose
// native type the schema alone cannot give (TID-945), for columns the user
// left automatic (no override, or the "source" placeholder):
//
//   - decimal: decimal(38,S), with S read from the values, because Cassandra
//     decimals declare no precision or scale;
//   - varint, map and tuple are already native from the schema;
//   - uuid and timeuuid stay canonical UUID text: ClickHouse cannot read
//     Parquet UUID columns yet. A "uuid" column type stores them natively.
//
// A column the target table already has keeps exactly the table's type,
// because Iceberg cannot change a column from string to decimal, uuid or map;
// a column stored as text by an earlier release therefore stays text, with a
// warning explaining how to switch.
func cassandraAutomaticColumnTypes(ctx context.Context, scanner connectors.DecimalColumnScanner, table, query string, cols []string, colTypes []*sql.ColumnType, overrides map[string]string, existing existingTable) (map[string]string, []typesystem.TypeWarning) {
	auto := map[string]string{}
	var warnings []typesystem.TypeWarning
	type pending struct {
		column   string
		nullable bool
		source   string
	}
	var decimals []pending

	for i, col := range cols {
		if i >= len(colTypes) || colTypes[i] == nil {
			continue
		}
		cqlType := strings.ToLower(strings.TrimSpace(colTypes[i].DatabaseTypeName()))
		if !cassandraTypeChangedByTID945(cqlType) {
			continue
		}
		// Unknown nullability counts as nullable, as in source inference
		// (SourceLogicalTypeForSQLColumn); Cassandra regular columns are.
		nullable, known := colTypes[i].Nullable()
		if !known {
			nullable = true
		}
		if raw, ok := arrowio.LookupColumnOverride(overrides, col); ok {
			isSource, placeholderNullable := typesystem.IsSourceOverride(raw)
			if !isSource {
				continue // the user's own type wins
			}
			nullable = placeholderNullable
		}
		inferred, err := arrowio.LogicalTypeForCassandraColumn(cqlType, 0, 0, false)
		if err != nil {
			continue
		}

		if current, ok := existing.Columns[col]; ok {
			current.Nullable = current.Nullable || nullable
			auto[col] = current.String()
			if w, legacy := legacyTextColumnWarning(col, cqlType, inferred, current); legacy {
				warnings = append(warnings, w)
			}
			continue
		}
		if !existing.Known {
			// The dataset has data, but the table could not be checked:
			// write what earlier runs wrote, so this run cannot conflict.
			legacy := cassandraLegacyType(inferred)
			if storageTarget(legacy, true) == storageTarget(nativeStorage(cqlType, inferred), false) {
				continue // stored the same way either way
			}
			legacy.Nullable = nullable
			auto[col] = legacy.String()
			warnings = append(warnings, typesystem.TypeWarning{
				Column: col, SourceType: cqlType, LogicalType: legacy.String(), StorageType: "utf8", TargetType: legacy.String(),
				Class:  typesystem.MappingSemanticFallback,
				Reason: "the target table could not be checked, so the type earlier runs used is kept to avoid a schema conflict; it is stored natively once the table can be read",
			})
			continue
		}

		if inferred.Kind == typesystem.KindUnknown && cqlType == "decimal" {
			decimals = append(decimals, pending{column: col, nullable: nullable, source: cqlType})
		}
	}

	if len(decimals) == 0 {
		return auto, warnings
	}
	names := make([]string, len(decimals))
	for i, d := range decimals {
		names[i] = d.column
	}
	var stats map[string]connectors.DecimalColumnStats
	var scanErr error
	if scanner == nil {
		scanErr = connectors.ErrColumnProbeUnsupported
	} else {
		stats, scanErr = scanner.ScanDecimalColumns(ctx, table, query, names)
	}
	for _, d := range decimals {
		text := typesystem.LogicalType{Kind: typesystem.KindString, Nullable: d.nullable}
		warn := func(reason string) {
			warnings = append(warnings, typesystem.TypeWarning{
				Column: d.column, SourceType: d.source, LogicalType: "decimal", StorageType: "utf8", TargetType: "string",
				Class: typesystem.MappingUnsupportedFallback, Reason: reason,
			})
		}
		if scanErr != nil {
			auto[d.column] = text.String()
			warn(fmt.Sprintf("the decimal values could not be read to choose a precision and scale (%v), so they are stored as exact text; choose a decimal(p,s) column type to store them as numbers", scanErr))
			continue
		}
		s := stats[d.column]
		scale := s.Scale
		if s.Values == 0 {
			scale = cassandraEmptyDecimalScale
		}
		if s.IntegerDigits+scale > cassandraDecimalPrecision {
			auto[d.column] = text.String()
			warn(fmt.Sprintf("values need %d digits before and %d after the decimal point, more than the %d digits an Iceberg decimal holds, so they are stored as exact text", s.IntegerDigits, s.Scale, cassandraDecimalPrecision))
			continue
		}
		t := typesystem.Decimal(cassandraDecimalPrecision, int32(scale))
		t.Nullable = d.nullable
		auto[d.column] = t.String()
		if s.Values == 0 {
			warnings = append(warnings, typesystem.TypeWarning{
				Column: d.column, SourceType: d.source, LogicalType: t.String(), StorageType: t.String(), TargetType: t.String(),
				Class:  typesystem.MappingSafePromotion,
				Reason: fmt.Sprintf("the column has no values yet to measure, so decimal(%d,%d) is used; a value with more than %d decimal places fails the run instead of being rounded", cassandraDecimalPrecision, scale, scale),
			})
		}
	}
	return auto, warnings
}

// cassandraTypeChangedByTID945 reports CQL types whose storage TID-945
// changed from text to a native type, directly or inside a collection.
func cassandraTypeChangedByTID945(cqlType string) bool {
	for _, token := range []string{"decimal", "varint", "uuid", "map<", "tuple<"} {
		if strings.Contains(cqlType, token) {
			return true
		}
	}
	return false
}

// cassandraLegacyType is the type earlier releases stored a Cassandra column
// as: text for decimal, varint, uuid, map and tuple, also inside lists.
func cassandraLegacyType(t typesystem.LogicalType) typesystem.LogicalType {
	switch t.Kind {
	case typesystem.KindArray:
		element := cassandraLegacyType(*t.Element)
		element.Nullable = t.Element.Nullable
		return typesystem.ArrayOf(element)
	case typesystem.KindDecimal, typesystem.KindUUID, typesystem.KindMap, typesystem.KindStruct, typesystem.KindUnknown:
		return typesystem.LogicalType{Kind: typesystem.KindString}
	}
	return typesystem.LogicalType{Kind: t.Kind}
}

// legacyTextColumnWarning explains a column an existing table stores as
// text although a new table would store it natively.
func legacyTextColumnWarning(col, cqlType string, inferred, current typesystem.LogicalType) (typesystem.TypeWarning, bool) {
	stored, wanted := storageTarget(current, true), storageTarget(nativeStorage(cqlType, inferred), false)
	if stored == wanted || !strings.Contains(stored, "string") {
		return typesystem.TypeWarning{}, false
	}
	if cqlType == "decimal" {
		wanted = "a decimal" // its scale is chosen from the values on a new table
	}
	return typesystem.TypeWarning{
		Column: col, SourceType: cqlType, LogicalType: current.String(), StorageType: stored, TargetType: stored,
		Class:  typesystem.MappingSemanticFallback,
		Reason: fmt.Sprintf("the existing table stores this column as %s, and Iceberg cannot change it to %s, so it is kept as %s; write to a new target table (or drop this one) to store it natively", stored, wanted, stored),
	}, true
}

// nativeStorage is the type a new table stores the column as. A decimal's
// scale is only known after the scan, so any decimal stands for it.
func nativeStorage(cqlType string, inferred typesystem.LogicalType) typesystem.LogicalType {
	if inferred.Kind == typesystem.KindUnknown && cqlType == "decimal" {
		return typesystem.Decimal(cassandraDecimalPrecision, 0)
	}
	return inferred
}

// storageTarget is the type a column is stored as when it is written with
// t as its column type (asColumnType) or with t inferred from the source.
// The difference matters for UUIDs: a uuid column type is stored natively,
// an inferred UUID as canonical text.
func storageTarget(t typesystem.LogicalType, asColumnType bool) string {
	t.Nullable = false
	var mapping typesystem.MappingResult
	var err error
	if asColumnType {
		_, mapping, err = arrowio.PlanForOverride("c", t.String(), t)
	} else {
		_, mapping, err = arrowio.PlanForLogicalType("c", t)
	}
	if err != nil {
		return t.String()
	}
	return mapping.Target
}
