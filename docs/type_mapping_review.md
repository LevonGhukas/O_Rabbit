# Type Mapping Review

## 1. Current Architecture

The implemented pipeline is an extraction-and-publication pipeline, not a ClickHouse `s3()` table-function pipeline:

```text
SQL source or S3 document object
  -> Go connector / database driver
  -> Apache Arrow schema + record batches
  -> Apache Parquet files
  -> S3 objects
  -> Iceberg registration (iceberg-go REST path or Altinity `ice` CLI)
  -> ClickHouse DataLakeCatalog / Iceberg reader at query time
```

Evidence:

- Source engines, including `s3` as a document reader, are registered in `internal/connectors/source.go:102-195`.
- SQL extraction calls `RowsToRecordBatchesEngineWithOverrides`, then writes Arrow batches to Parquet in `cmd/worker/main.go:1069-1123`.
- Document/S3 extraction buffers up to 10,000 decoded documents, infers an Arrow schema once, then writes Parquet in `cmd/worker/main.go:1222-1367`.
- Parquet is written by `pqarrow.NewFileWriter` in `internal/parquetio/writer.go:31-63` and uploaded by the worker.
- The Iceberg REST implementation reads registered Parquet through `pqarrow`; the Altinity path invokes `ice insert` with S3 URIs (`internal/icebergreg/parquet_reader.go:30-75`, `internal/icebergreg/icecli.go:191-250`).
- The supplied ClickHouse compose file creates a `DataLakeCatalog` database with an S3 warehouse, not an S3 table function (`docker-compose.clickhouse-altinity.yml:43-57`).

There is no canonical project type model. `arrowio.ColumnPlan` is the de facto intermediate representation: it couples an Arrow type, builder, and value-conversion function (`internal/arrowio/sql_to_arrow.go:17-22`). Iceberg schema is derived from Arrow by the external `iceberg-go` converter (`internal/icebergreg/manager.go:1221-1232`).

## 2. Current Type-Mapping Flow

### SQL-source execution order

1. A connector returns `*sql.ColumnType` metadata from the database driver. The worker receives it with result rows at `cmd/worker/main.go:1069-1086`. For ClickHouse sources, the connector calls `rows.ColumnTypes()`; see `internal/connectors/clickhouse.go:68-74` and `:111-117`.
2. Optional user `column_types` overrides are carried in job options (`internal/jobopts/jobopts.go:31-45`, `:174-179`) and passed both to the source query and Arrow conversion (`cmd/worker/main.go:1070-1082`, `:1097`).
3. `PlansFromSQLEngineWithOverrides` obtains name, database type name, decimal precision/scale, and nullable flag from `sql.ColumnType` (`internal/arrowio/sql_to_arrow.go:38-75`, `:454-476`).
4. `PlanForSQLColumn` normalizes the engine/type name, extracts an embedded `(precision,scale)`, and dispatches to dialect planners (`internal/arrowio/type_plan.go:38-76`). The planner set is:

   - MySQL/MariaDB: `internal/arrowio/mysql_to_arrow.go:7-95`
   - PostgreSQL: `internal/arrowio/postgres_to_arrow.go:7-88`
   - MSSQL: `internal/arrowio/mssql_to_arrow.go:7-76`
   - Oracle: `internal/arrowio/oracle_to_arrow.go:7-68`
   - ClickHouse: `internal/arrowio/clickhouse_to_arrow.go:7-115`
   - Trino: `internal/arrowio/trino_to_arrow.go:7-75`
   - Cassandra: `internal/arrowio/cassandra_to_arrow.go:7-69`
   - generic/SQLite fallback: `internal/arrowio/sqlite_to_arrow.go:9-83`.
5. An override replaces both the plan and Arrow field type. It recognizes a small ClickHouse-like target-type grammar (`UInt*`, `Int*`, `Float*`, `Decimal`, dates/timestamps, `Array`, `Binary`) in `PlanForTargetType` (`internal/arrowio/type_plan.go:78-171`). The override sets nullability solely by whether the override string starts with `Nullable(` (`internal/arrowio/sql_to_arrow.go:465-473`), rather than preserving source nullability.
6. Row values are scanned as `any`, passed to each `ColumnPlan.Append`, and emitted in Arrow record batches (`internal/arrowio/sql_to_arrow.go:343-446`). Conversion helpers perform parsing/coercion (`asInt64`, `asUint64`, `asFloat64`, `asBool` at `internal/arrowio/sql_to_arrow.go:101-285`; timestamp/decimal/list helpers at `internal/arrowio/type_plan.go:631-916`).
7. The Arrow schema and batches are encoded to Parquet. The project does not supply an explicit Parquet schema; Arrow's `pqarrow` writer derives it (`internal/parquetio/writer.go:45-55`).
8. File validation reopens Parquet and translates its stored schema back to Arrow; it checks only that this Arrow schema fingerprint equals the expected Arrow schema (`internal/artifact/integrity.go:73-121`). The fingerprint canonically records Arrow kind, time unit/timezone, decimal precision/scale, field nullability, nested fields, and map/list metadata (`internal/artifact/integrity.go:161-190`).
9. For an auto-created Iceberg table, the master either uses a persisted Iceberg schema or independently repeats source-to-Arrow inference, then calls `icetable.ArrowSchemaToIcebergWithFreshIDs` (`internal/icebergreg/manager.go:1149-1235`). This is a second schema-inference stage, separate from the worker's schema.
10. Iceberg schema evolution delegates promotion compatibility to `iceberg.PromoteType` (`internal/icebergreg/catalog_options.go:125-175`). The repository does not map Iceberg types to ClickHouse DDL.

### S3-document execution order

1. `OpenS3` parses `s3://bucket/key`, creates an AWS S3 client, and chooses a format from explicit configuration or extension (`internal/connectors/s3.go:37-83`, `:197-228`). Unsupported explicit formats fail; an unknown extension defaults to CSV.
2. `StreamDocuments` reads the S3 object using the AWS SDK (`internal/connectors/s3.go:99-114`). CSV/JSON/XML/Excel are decoded in Go; Parquet is decoded by Arrow's `pqarrow.NewFileReader` (`:115-194`).
3. For Parquet, `arrowValueToAny` immediately erases several Arrow distinctions while converting each value to `map[string]any`: signed widths become `int64`, unsigned widths `uint64`, Float32 `float64`, dates/timestamps formatted strings, and all unsupported Arrow arrays `ValueStr` text (`internal/connectors/s3.go:642-690`).
4. The worker samples the first 10,000 S3 documents and calls `InferMongoSchemaWithFieldOrder` (`cmd/worker/main.go:1282-1302`; trailing smaller input at `:1325-1336`). Despite its name, this function is used for every document source, including S3.
5. `InferMongoSchemaWithFieldOrder` assigns each field the type of its first non-null observed value; an all-null field becomes String (`internal/arrowio/mongo_to_arrow.go:31-88`). It then converts values to that inferred schema via `MongoDocsToRecord` (`:107-146`) and fixed-type appenders (`:149-364`).
6. The Arrow result follows the same Parquet/S3/Iceberg/ClickHouse route as SQL sources.

### Current defaults for unknown or unsupported values

- Unknown SQL type: generic planner generally produces String (`internal/arrowio/sqlite_to_arrow.go:81-83`).
- Unsupported explicit target type: `PlanForTargetType` delegates to generic planning, which normally produces String (`internal/arrowio/type_plan.go:169-170`, `internal/arrowio/sqlite_to_arrow.go:81-83`).
- Unsupported document value: inferred type becomes String and the value is formatted/JSON-marshaled where possible (`internal/arrowio/mongo_to_arrow.go:263-273`, `:379-408`).
- Type mismatch in many narrow appenders silently becomes NULL: Int8/16/32, UInt8/16/32, Float32, Date32, Time64, Decimal128, and List (`internal/arrowio/type_plan.go:178-241`, `:265-328`, `:352-371`, `:426-475`, `:510-540`, `:587-625`). Int64/UInt64/Float64/Bool instead return an error (`:244-262`, `:331-349`, `:374-414`).

## 3. How S3 Types Are Read by ClickHouse

### What the repository actually does

No repository code executes a ClickHouse `s3()`, `s3Cluster()`, S3 table engine, `SELECT ... FROM s3(...)`, or ClickHouse DDL containing an S3 table schema. A repository-wide search finds S3 only in Go/AWS, Iceberg configuration, and `DataLakeCatalog` configuration. Therefore the exact ClickHouse S3 function, inferred schema, and resulting ClickHouse column type requested by a direct-S3 query are **Not determinable from the current codebase**, because that direct query path is not implemented.

The indirect ClickHouse access path is:

- `docker-compose.clickhouse-altinity.yml:49-57` enables the experimental Iceberg database and creates `ice` with `ENGINE = DataLakeCatalog(...)`, `storage_endpoint`, and `warehouse = 's3://bucket1'`.
- `internal/icebergreg/icecli.go:210-230` supplies Altinity `ice insert` a list of existing Parquet S3 URIs. It explicitly avoids `ice insert -p`; the comment says the auto-create path would read the first S3 object (`:210-212`). The master instead creates/verifies the Iceberg table before insertion (`internal/icebergreg/manager.go:633-660`).
- In the `rest-go` registration engine, `newParquetRecordReader` reads Parquet through `iceberg-go` filesystem IO and Arrow and rejects a non-equal Arrow schema among input files (`internal/icebergreg/parquet_reader.go:30-75`).

Thus, the repository determines Parquet/Arrow/Iceberg schema before ClickHouse sees the Iceberg table. ClickHouse type translation for a DataLakeCatalog/Iceberg table is performed by the deployed Altinity ClickHouse server, not by repository code. Exact behavior is version/configuration dependent; the repo pins an Altinity image in one compose file (`25.3.8.10042.altinitystable`, `docker-compose.clickhouse-altinity.yml:2-3`) and a different image in another (`25.8.9.20207.altinityantalya`, `docker-compose.yaml:89-90`).

### Actual S3 source formats

The S3 connector supports CSV, JSON, XML, Excel, and Parquet; only Parquet carries an explicit physical/logical schema. CSV, JSON, XML, and Excel are application-decoded documents whose source logical types are inferred from values. Parquet read uses Arrow's Parquet reader and its schema conversion (`internal/connectors/s3.go:172-191`). Avro, ORC, and Arrow IPC are not supported by this connector (`internal/connectors/s3.go:199-208`).

For a Parquet S3 object, source physical/logical types are parsed by Arrow's `pqarrow` library; the repository neither inspects Parquet physical/logical annotations nor preserves their complete original form. For example, it converts Date32 to `YYYY-MM-DD`, Timestamp to RFC3339 text, and otherwise falls through to `ValueStr` (`internal/connectors/s3.go:677-690`). Exact physical Parquet encoding and logical annotations in a particular S3 object are **Not determinable from the current codebase**.

Different S3 files/partitions can produce incompatible schemas. The S3 connector operates on one object/key per run. The worker samples a bounded prefix of one stream and locks the type. The REST Iceberg reader detects `arrow.Schema.Equal` differences only among its registration file set (`internal/icebergreg/parquet_reader.go:66-72`). The Altinity `ice` implementation's cross-file compatibility behavior is external and **Not determinable from the current codebase**.

## 4. Current Type Mapping Matrix

The following maps current repository behavior. “ClickHouse” is an inferred final outcome only when an Iceberg-compatible mapping is normally available; no repository code produces ClickHouse DDL or validates it.

| Source/storage type | Type detected while reading S3 | Internal/project type | Final ClickHouse type | Potential information loss |
|---|---|---|---|---|
| SQL signed 8/16/32/64 | Driver `DatabaseTypeName`, `ColumnType` | Arrow Int8/16/32/64 | Indirect through Arrow→Iceberg; exact CH type not repository-controlled | Narrow appenders cast without bounds check; invalid conversion can become NULL. |
| SQL unsigned 8/16/32/64 | Driver type name / dialect planner | Arrow UInt8/16/32/64 | Indirect; exact CH mapping not repository-controlled | Narrow appenders wrap values above width; signed/unsigned interpretation relies on metadata. |
| MySQL `BIGINT UNSIGNED` | Driver metadata | Arrow UInt64 (`internal/arrowio/mysql_to_arrow.go:36-40`) | Indirect | None intended in Arrow; later Iceberg/ClickHouse compatibility is not checked here. |
| `DECIMAL/NUMERIC(p,s)`, p ≤ 38 | Driver metadata or parsed text | Arrow Decimal128(p,s) | Indirect | p > 38 clamps to 38; float/text coercion can round; malformed value becomes NULL. |
| `DATE` | SQL metadata; Parquet Date32 is turned into date string | Arrow Date32 | Indirect | Project clamps to 1900-01-01..2299-12-31. |
| timestamp/date-time | SQL metadata; Parquet Timestamp becomes RFC3339 text | Arrow Timestamp(microsecond, optional `UTC`) | Indirect | Nanoseconds truncated to microseconds; zone name/offset semantics collapse to UTC or empty zone; values clamped to 1900..2299. |
| Parquet signed integer | Arrow `array.Int*` | `int64` document value, then Arrow Int64 by first-value inference | Indirect | Original width is erased. |
| Parquet unsigned integer | Arrow `array.Uint*` | `uint64` document value, then first-value inference cannot recognize uint64 and usually becomes Arrow String | Indirect | Width and numeric type can be lost; String path changes semantics. |
| Parquet Float32 | Arrow `array.Float32` | `float64`, then Arrow Float64 | Indirect | Float32 identity/width is lost, although numeric value is representable. |
| Parquet Float64 | Arrow `array.Float64` | `float64`, then Arrow Float64 | Indirect | None intended before external conversion. |
| Parquet Decimal, FixedSizeBinary, struct/list/map/UUID/enum-like | Unsupported array usually `ValueStr` | String inferred from first non-null value | Indirect | Logical and structural type, binary fidelity, annotations, and reversible decoding are not assured. |
| S3 CSV | CSV text | Document values are strings; field schema first-value String | Indirect | No typed source schema; empty cells are not proven distinct from source NULL. |
| S3 JSON/XML | decoder values | first non-null Go value type | Indirect | Value-based inference; heterogeneous fields are coerced or nullified. |
| S3 Excel | blank→nil, integer parse→int64, float parse→float64, else String (`internal/connectors/s3.go:610-627`) | first-value inferred Arrow type | Indirect | Excel cell formatting/type semantics are discarded. |

## 5. Data-Loss and Correctness Risks

| Current behavior | Why unsafe/lossy | Safer target rule and layer | Files/functions to change |
|---|---|---|---|
| Int8/16/32 and UInt8/16/32 append with Go casts (`internal/arrowio/type_plan.go:190-195`, `:212-217`, `:234-239`, `:277-282`, `:299-304`, `:321-326`). | Out-of-range values wrap; parse failures become NULL silently. | Validate min/max before every narrowing conversion. Return a typed conversion error or select a schema-level wider/fallback type; never write NULL for non-NULL invalid source value. | `internal/arrowio/type_plan.go`; introduce validation in new canonical type package. |
| Generic NUMBER(scale 0, precision ≤18) becomes Int64 (`internal/arrowio/sqlite_to_arrow.go:55-66`). | Decimal semantics and declared precision are replaced with signed integer; negative scale/large values are ambiguous. | Keep declared Decimal(p,0), unless source metadata explicitly declares a signed integer. | `internal/arrowio/sqlite_to_arrow.go`; dialect planners. |
| Precision is capped to 38 in all dialect planners and Decimal append parses float values at target scale (`internal/arrowio/type_plan.go:510-540`, `:686-727`). | Decimal(p>38) loses digits; float→decimal has already lost decimal exactness; currency symbols/commas are silently stripped. | Preserve source decimal unscaled integer/scale when supported; otherwise use a reversible decimal fallback with exact lexical/unscaled form. Reject rather than clean ambiguous values. | `internal/arrowio/type_plan.go`; all dialect planners. |
| Float32 is explicitly cast from Float64 (`internal/arrowio/type_plan.go:352-371`); document inference widens Float32 to Float64 (`internal/connectors/s3.go:665-668`). | Float64→Float32 rounds; source float width is not retained in document flow. | Map declared Float32 only from Float32/bits-preserving input; preserve Parquet Arrow schema instead of `any`; do not infer Float32 from value. | `internal/arrowio/type_plan.go`; `internal/connectors/s3.go`; document pipeline. |
| Date and timestamp values clamp to a ClickHouse range (`internal/arrowio/type_plan.go:416-447`, `:478-505`). | Values outside 1900..2299 are silently changed. | Do not constrain extraction to a presumed downstream range. Use exact wider timestamp/date mapping if target supports it, else schema-level reversible fallback. | `internal/arrowio/type_plan.go`. |
| All timestamps are Arrow microseconds; string time parsing truncates beyond six fractional digits (`internal/arrowio/type_plan.go:478-505`, `:759-805`). | Nanoseconds are lost. | Carry source temporal unit (s/ms/us/ns), precision, zone/offset representation in canonical metadata; choose DateTime64(9) only after confirmed target compatibility, otherwise fallback. | `internal/arrowio/type_plan.go`; new canonical model. |
| Zoned data maps to `UTC` or no timezone based on string matching (`internal/arrowio/*_to_arrow.go`, e.g. PostgreSQL `:73-78`; `asSafeString` UTC-formats times at `type_plan.go:646-648`). | Original IANA zone and original offset are lost. `TIMESTAMP WITH LOCAL TIME ZONE` has source-specific semantics not represented. | Store instant plus source timezone/semantic annotation, or use a reversible timestamp envelope; do not infer zone from a substring. | Dialect planners; `internal/arrowio/type_plan.go`. |
| SQL fields preserve source `Nullable()` initially, but an override changes nullability to `Nullable(...)` only (`internal/arrowio/sql_to_arrow.go:64-72`, `:465-473`). Document fields are always nullable (`internal/arrowio/mongo_to_arrow.go:83-88`). | Overrides can make a nullable source non-nullable or vice versa, without a value check. | Separate override type from nullability policy. Preserve source nullability unless an explicit validated override requests change. | `internal/arrowio/sql_to_arrow.go`; options/API validation. |
| Empty strings are accepted as strings; several type parsers turn empty/zero-date values into NULL (`internal/arrowio/type_plan.go:729-757`, `:809-829`). | Empty, invalid sentinel, NULL, and special values such as `infinity` can collapse into NULL or clamped values. | Represent source NULL only as Arrow null. Treat invalid/sentinel source values as conversion errors or typed fallback values with source marker. | `internal/arrowio/type_plan.go`. |
| UUID, FixedString, enum, JSON, map, tuple, dynamic, large integers, and geographic ClickHouse source types map to String (`internal/arrowio/clickhouse_to_arrow.go:109-111`). | Semantic type, fixed length, enum labels/codes, binary encoding, nested structure, and large integer exactness are discarded. | Support UUID, fixed binary/string constraints, Enum dictionary metadata, Map, Struct/Tuple; use reversible fallback for unsupported types. | `internal/arrowio/clickhouse_to_arrow.go`; canonical type parser. |
| PostgreSQL JSON/JSONB/XML/UUID/network types become String (`internal/arrowio/postgres_to_arrow.go:80-84`); MSSQL `uniqueidentifier` and UDTs become String (`internal/arrowio/mssql_to_arrow.go:67-71`). | A string may not be a canonical or reversible source representation. | Use native UUID where values are valid; record JSON serialization/version; use binary or vendor codec fallback for UDT/XML/network types. | PostgreSQL/MSSQL planners and serializers. |
| Binary accepts String and arbitrary values via `fmt.Sprint` (`internal/arrowio/type_plan.go:562-584`). | Text-to-bytes and `fmt.Sprint` are not guaranteed reversible and conflate binary vs UTF-8 text. | Binary only accepts byte-preserving input. Else fail/fallback with source encoding metadata. | `internal/arrowio/type_plan.go`. |
| S3 Parquet conversion through `arrowValueToAny` reduces types to Go scalars/text (`internal/connectors/s3.go:642-690`). | This is the largest S3-specific loss boundary: decimal, fixed binary, UUID, nested, maps, lists, dictionary/low-cardinality, enums, and Arrow metadata can be lost before inference. | Keep Arrow record batches/schema from `pqarrow` through Parquet writing; do not use `map[string]any` for typed Parquet. | `internal/connectors/s3.go`; `cmd/worker/main.go:1200-1367`. |
| Document schema uses first non-null sample value, max 10,000 records (`internal/arrowio/mongo_to_arrow.go:38-58`; `cmd/worker/main.go:1282-1295`). | Type depends on value order and sample size; later incompatible values are formatted, defaulted to zero, or made NULL. Different runs/files can infer different schemas. | Require supplied schema or compute a deterministic union from authoritative source metadata/full schema; reject incompatible evolution unless an explicit widening/fallback rule applies. | `internal/arrowio/mongo_to_arrow.go`; worker document path; S3 options. |
| Mongo integer field is always Arrow Int64, and incompatible value appends as zero (`internal/arrowio/mongo_to_arrow.go:161-166`, `:289-293`, `:366-376`). | Int32 width is lost; incompatible values become `0`, indistinguishable from source zero. | Preserve BSON int32/int64 distinction or deliberately widen with validation. On mismatch return error/fallback, never zero. | `internal/arrowio/mongo_to_arrow.go`. |
| Schema fingerprint proves Arrow round-trip only (`internal/artifact/integrity.go:97-121`). | It does not prove source→Arrow losslessness, Parquet logical annotations, Iceberg conversion, Altinity `ice`, or ClickHouse interpretation. | Validate canonical schema and values before write; persist canonical schema/fallback metadata with artifact and Iceberg table properties; add target compatibility checks. | `internal/artifact/integrity.go`; `internal/icebergreg/manager.go`; artifact metadata definitions. |

## 6. Recommended Lossless Mapping Strategy

Introduce a canonical internal schema model rather than letting each planner directly select Arrow. Arrow remains the transport/Parquet representation, but not the policy representation.

Suggested model (new package `internal/typemap`):

```text
CanonicalField {
  name, nullable, source_system, source_declared_type, source_type_parameters,
  logical_kind, physical_kind, signed, bit_width,
  decimal_precision, decimal_scale,
  temporal_unit, timezone_semantics, timezone_name,
  fixed_length, enum_labels, children,
  mapping_class: exact | wider | fallback,
  fallback_codec, fallback_version
}
```

Required mapping rules:

- Preserve signedness and width: Int8/16/32/64 and UInt8/16/32/64 map exactly when the destination supports the same type. Validate every value against declared bounds. Never infer unsignedness from observed positive values.
- For UInt64, preserve UInt64 only after confirming every destination path (Arrow, Parquet, Iceberg, deployed ClickHouse) supports it. If an intermediate type cannot represent it, use the fallback—not Int64, Float64, or a decimal guessed from values.
- Preserve Float32 versus Float64. Do not downcast Float64. Treat NaN, ±Inf, and signed zero as value-level compatibility cases in tests.
- Preserve Decimal(p,s) only when `p` and `s` are within every stage's supported range. Never obtain an exact Decimal from Float. Decimal p>38, negative scale, or opaque vendor numeric should use an exact fallback that stores unscaled integer + scale or canonical lexical format.
- Preserve Date, time, timestamp unit, and timezone semantics distinctly. Do not clamp at Arrow conversion time. Any target-specific date/time range check belongs in the compatibility stage and must route incompatibilities to fallback/error.
- Preserve nullability independently from the base type. A target-schema override must explicitly carry a nullability policy and must be validated against actual data.
- Map UTF-8 String and Binary as separate canonical kinds. FixedString must retain byte width and padding semantics. Do not decode arbitrary bytes as UTF-8.
- Map UUID to canonical 16-byte UUID plus textual presentation only as metadata; map to native UUID only with valid 16-byte values and stable byte order.
- Preserve Enum labels and numeric codes, LowCardinality/dictionary semantics, and nested field metadata. LowCardinality is a storage optimization, not a reason to erase the underlying type.
- Represent Array, Tuple/Struct, Map, and nested records recursively. Map keys must be non-null and carry key type. Do not serialize them to text when an exact Arrow/Parquet/Iceberg mapping exists.
- For CSV/JSON/XML/Excel, do not claim losslessness from value guessing. Require an explicit schema for lossless operation; otherwise classify the run as inferred/best-effort and record the inference sample, coercions, and fallback decisions.

This fits the existing code: replace `ColumnPlan` selection as the primary decision with `source metadata -> CanonicalField -> Arrow plan + Iceberg schema + target compatibility result`. Existing dialect planners can initially be refactored into source-type parsers; `ColumnPlan` appenders can become rendering/validation adapters.

## 7. Fallback Strategy for Unsupported Types

Use a schema-level/per-column fallback selected before data extraction. Do not silently change a column's physical type per row.

```text
if exact lossless ClickHouse/Iceberg/Parquet mapping exists:
    use the exact mapping
else if a declared safe wider mapping exists and every value is representable:
    use the wider mapping and record it
else:
    use a reversible fallback envelope
```

Recommended fallback envelope for each fallback column:

```text
<column>__orabbit_fallback: Nullable(Binary)     // exact encoded payload
<column>__orabbit_fallback_meta: LowCardinality(String)
```

`__orabbit_fallback_meta` identifies a versioned codec and canonical source type, for example `orabbit.type.v1;source=oracle.NUMBER(50,0);codec=decimal-unscaled-be-v1`. Prefer a table-level `orabbit.type_mapping.v1` Iceberg property/sidecar manifest to avoid repeating metadata per row; retain the column metadata only if rows can legitimately carry different codecs.

Rules:

- **Original source type:** Persist exact declared source type, parameters, source engine, canonical type JSON, and mapping decision in an artifact sidecar and Iceberg table/snapshot properties. The present `schema_fingerprint` is insufficient because it records only the post-conversion Arrow schema.
- **Serialization:** Use source-type-specific reversible codecs, not plain String by default: fixed-width/big integer as sign + big-endian magnitude; decimal as sign + unscaled integer + scale; binary as raw bytes; UUID as 16 raw bytes plus byte-order metadata; timestamp as epoch count in original unit plus timezone semantic/name; enum as code plus label dictionary; structured values as canonical typed CBOR/MessagePack-like envelope with field schema. JSON text is acceptable only when the source type is JSON and a canonical JSON codec/version preserves required semantics; it is not a universal fallback.
- **NULL distinction:** Arrow/Parquet null means source NULL. A non-null empty `Binary` payload is a serialized value. Never encode source NULL as `"null"`, zero bytes, or an empty string.
- **Reconstruction:** A decoder dispatches from `fallback_codec` and canonical source type to rebuild the original typed value. Include codec version and test decoder compatibility.
- **Nested values:** Recursively typed serialization must include field IDs/names, element types, null markers, map key/value types, and ordering. Do not depend on Go `fmt.Sprint`, `ValueStr`, or non-canonical JSON.
- **Heterogeneous S3 files:** Reconcile all file schemas before registering/reading. Exact equal schemas are accepted; documented safe widenings produce one canonical schema; incompatible types select a single schema-level fallback only with explicit policy, otherwise fail the run before writing/registering.
- **Logging/metrics:** Emit `type_mapping_exact_total`, `type_mapping_widened_total`, `type_mapping_fallback_total`, `type_mapping_rejected_total`, `type_mapping_value_violation_total`, labelled by source engine/type, canonical type, destination type, column, and codec. Log source schema fingerprint, canonical mapping fingerprint, run/artifact ID, and each non-exact decision. Do not log raw sensitive values.

Plain String is acceptable only for an explicitly textual source type with UTF-8 validity preserved, or a documented source-type-specific canonical textual codec. It is not safe for arbitrary binary, UUID bytes, precision decimals, nested values, or vendor values.

## 8. Proposed Architecture

```text
Source file/schema or database metadata
     ↓
Source type parser
     ↓
Canonical internal type
     ↓
Lossless compatibility check
     ↓
Exact ClickHouse/Iceberg/Parquet type
     OR safe wider type (recorded)
     OR reversible fallback representation
     ↓
Arrow record schema -> Parquet -> S3 -> Iceberg schema -> ClickHouse catalog read
```

Integration points:

1. At SQL planning, parse `sql.ColumnType` into canonical fields rather than immediately returning `ColumnPlan`; retain precision, scale, nullable, declared type, signedness, and source dialect.
2. At S3 Parquet ingestion, obtain canonical fields directly from the Arrow/Parquet schema rather than `arrowValueToAny`. Route typed Arrow batches directly into the writer when no transformation is necessary.
3. At JSON/CSV/XML/Excel ingestion, accept a required schema contract for lossless mode. An inference mode may remain, but must be explicitly marked non-lossless and must use deterministic union/widening, not first-value selection.
4. Render canonical fields to Arrow with a value validator. Use that same canonical schema to generate/validate the Iceberg schema, preserve it in artifact metadata, and compare it to all S3 file schemas before registration.
5. Add a ClickHouse/Iceberg compatibility adapter configured for the deployed ClickHouse version. It must be the only place that maps canonical types to final ClickHouse types or invokes fallback.

## 9. Required Code Changes

| File | Change |
|---|---|
| `internal/arrowio/type_plan.go` | Split `ColumnPlan` policy from Arrow rendering. Replace silent NULL/casts/clamps and permissive string/binary conversion with checked conversion results. Remove timestamp/date clamping from generic extraction. |
| `internal/arrowio/sql_to_arrow.go` | Add canonical schema construction from `sql.ColumnType`; make overrides explicit typed contracts with separate nullable policy; propagate conversion diagnostics. |
| `internal/arrowio/mysql_to_arrow.go`, `postgres_to_arrow.go`, `mssql_to_arrow.go`, `oracle_to_arrow.go`, `clickhouse_to_arrow.go`, `trino_to_arrow.go`, `cassandra_to_arrow.go`, `sqlite_to_arrow.go` | Refactor into source-type parsers returning canonical types. Implement exact UUID/binary/enum/nested rules or explicit fallback; do not default ambiguous values to String. |
| `internal/connectors/s3.go` | Add a typed Parquet route that retains Arrow schema/arrays. Replace `arrowValueToAny` loss boundary for Parquet. Require a declared schema or deterministic schema reconciliation for textual/document formats. Remove unknown-extension→CSV fallback in lossless mode. |
| `internal/arrowio/mongo_to_arrow.go` | Replace first-non-null inference with canonical schema reconciliation; preserve BSON type distinctions; reject/fallback mismatches rather than zero/String formatting. Keep BSON Extended JSON only as a versioned fallback codec. |
| `cmd/worker/main.go` | Use canonical schema before opening `parquetRollingWriter`; for S3 Parquet avoid map/document conversion; collect and publish mapping diagnostics and canonical schema metadata. |
| `internal/parquetio/writer.go` | Accept canonical schema metadata and write/validate a mapping manifest/Parquet key-value metadata where supported. |
| `internal/artifact/integrity.go` | Extend artifact metadata/fingerprint to include canonical source schema, mapping plan, fallback codecs, and value-validation summary; verify it after Parquet readback. |
| `internal/icebergreg/manager.go` and `parquet_reader.go` | Create Iceberg schema from canonical schema, validate all input file schemas against it before both `rest-go` and `ice` registration, and persist mapping/fallback metadata in Iceberg properties. Do not independently re-infer a schema differently from the worker. |
| `internal/icebergreg/icecli.go` | Pass only a prevalidated, schema-homogeneous file set to Altinity `ice`; record the mapping manifest/version in registration receipt. |
| `internal/jobopts/jobopts.go`, `internal/http/api_runs.go` | Replace/augment `map[string]string column_types` with a versioned source-schema/mapping contract and explicit lossless/inference policy. |
| New `internal/typemap/` | Add canonical type AST, dialect parsers, Arrow/Iceberg/ClickHouse renderers, lossless compatibility checker, fallback codec registry, schema reconciliation, diagnostics, and reconstruction API. |

## 10. Tests to Add

Add table-driven tests in `internal/typemap`, unit tests beside existing `internal/arrowio/*_test.go`, Parquet round-trip tests in `internal/parquetio`, and integration/contract tests in `internal/icebergreg`.

- Every signed/unsigned integer width: min, max, min-1, max+1, signed/unsigned boundaries, and values beyond UInt64/Int64. Verify exact mapping, safe widening, or fallback—never wrap/NULL/zero.
- Decimal: p/s combinations including scale 0, p=38, p>38, negative scale if source supports it, trailing zeros, min/max unscaled values, and decimal supplied by float. Ensure only exact source decimal representations qualify as lossless.
- Floating point: Float32/Float64 boundaries, NaN, infinities, negative zero, and Float64 values not representable in Float32.
- Time: Date boundaries, DateTime/DateTime64 precisions 0/3/6/9, nanoseconds, source values outside 1900..2299, UTC offsets, IANA zone metadata, DST transitions, and local-time semantics.
- Null/empty: NULL, empty string, empty binary, `"NULL"`, zero date, `infinity`, invalid date/timestamp, and missing document field must remain distinguishable according to the declared source type.
- Text/binary/UUID: invalid UTF-8 bytes, arbitrary blobs, FixedString padding/length, 16-byte UUID byte order, canonical UUID text, invalid UUID, and source Unicode strings.
- Complex types: arrays with null elements, nested arrays, maps with typed keys/values, tuples/structs with optional fields, JSON objects with number precision, enum label/code pairs, LowCardinality/dictionary input, and nested BSON values.
- Unsupported/custom values: Oracle NUMBER(50), Cassandra varint/UDT, MSSQL SQL_VARIANT/geometry, ClickHouse Int128/Variant/Dynamic, vendor types, and malformed source metadata. Assert deterministic fallback codec and metadata.
- S3 schemas: two or more Parquet files with equal schemas, safe-width differences, incompatible signed/unsigned values, Decimal scale mismatch, nested schema change, and textual files with heterogeneous values outside the sample. Ensure a single reconciled schema or a pre-write failure.
- ClickHouse/Iceberg compatibility: fixture canonical schemas for every supported final type against the pinned ClickHouse images; validate expected generated Iceberg/ClickHouse mapping where the integration supports it.
- Round-trip invariant for every mapping classified lossless:

  ```text
  source value
    -> mapped/serialized representation
    -> ClickHouse-compatible representation
    -> reconstructed value
  reconstructed value == source value
  ```

  Include canonical schema metadata and fallback codec/version in the round-trip assertion, not merely displayed values.

## 11. Final Recommended Mapping Matrix

| Canonical source type | Exact target rule | Safe wider rule | Otherwise reversible fallback |
|---|---|---|---|
| Int8/16/32/64 | Same signed Arrow/Parquet/Iceberg/ClickHouse-compatible type | Wider signed integer only | signed integer binary codec with bit width |
| UInt8/16/32/64 | Same unsigned type after full-path compatibility check | Wider unsigned only | unsigned magnitude binary codec with bit width |
| Decimal(p,s) | Exact Decimal(p,s) when all stages support p/s | Only a declared Decimal(P,s) with P≥p | unscaled integer + scale codec |
| Float32/Float64 | Same floating width | Float32→Float64 only if policy allows representation widening | IEEE bit-pattern codec for unsupported float semantics |
| Date | Exact date type with supported range | Wider temporal type only if date semantics preserved | signed days + calendar/semantic metadata |
| Timestamp(unit, zone semantic) | Exact precision and supported timezone semantic | Greater precision only | epoch count + original unit + zone semantic/name codec |
| UTF-8 String | String after UTF-8 validation | none required | raw bytes plus declared encoding if non-UTF-8 |
| Binary / FixedString(n) | Binary; FixedString only when length semantics supported | Binary plus fixed-length metadata | raw bytes + fixed-length/padding metadata |
| UUID | Native UUID only with documented byte order | 16-byte Binary plus UUID metadata | 16-byte UUID codec |
| Enum | Native enum only when full label/code dictionary is preserved | String plus dictionary metadata only if policy accepts semantic widening | code + label dictionary codec |
| Array | Recursive exact Array | Recursive safe widening only | typed nested envelope |
| Tuple/Struct | Recursive exact Struct/Tuple | Add only optional fields with schema evolution rules | typed nested envelope with field schema |
| Map | Exact Map only with compatible non-null keys | recursive safe widening | typed ordered key/value envelope |
| JSON/object | Native JSON only if semantic/number fidelity is demonstrated | canonical JSON with explicit codec/version | typed structured envelope |
| LowCardinality | Underlying exact type plus dictionary metadata | Remove only the storage optimization, retaining type | underlying/fallback codec |
| Unsupported/custom | none | none unless a proof of losslessness exists | source-specific binary or structured codec, never generic `fmt.Sprint` |

The current project already has useful seams—dialect planners, Arrow writing, schema fingerprinting, Iceberg creation, and registration validation—but it needs one canonical schema and one compatibility decision before values are coerced. That change prevents the present value-driven S3 inference and silent conversion paths from determining storage semantics.
