# Type-Mapping Roadmap

## Purpose and non-negotiable outcome

This roadmap plans a lossless type-conversion contract for the ingestion path:

```text
source database -> driver metadata/raw value -> source type interpretation
-> canonical type contract -> Arrow arrays -> Parquet/S3 -> Iceberg
-> Altinity ClickHouse DataLakeCatalog
```

No supported source value may be silently wrapped, truncated, clamped, replaced by `NULL`/zero, or otherwise corrupted because an exact downstream type is unavailable. A lack of native Arrow, Parquet, Iceberg, or ClickHouse support is a reason to select a documented lossless fallback, not to fail ingestion or alter the source value.

This document is an implementation plan only. It does not change the current mapping behavior.

## 1. Current architecture and decision ownership

### Existing flow

1. SQL connectors obtain metadata with `rows.ColumnTypes()` after `SELECT ... LIMIT 0` or the extraction query. Examples: `internal/connectors/postgres.go:100-124`, `internal/connectors/mysql.go:56-80`, and `internal/connectors/mariadb.go:38-46`.
2. `arrowio.PlansFromSQLEngine` reads `DatabaseTypeName`, `DecimalSize`, and `Nullable`, then calls `PlanForSQLColumn` (`internal/arrowio/sql_to_arrow.go:39-74`; `internal/arrowio/type_plan.go:39-75`).
3. The PostgreSQL and MySQL/MariaDB dialect planners return an Arrow `ColumnPlan`: `internal/arrowio/postgres_to_arrow.go:7-88` and `internal/arrowio/mysql_to_arrow.go:7-95`. MariaDB deliberately shares the MySQL planner today (`PlanForSQLColumn`, `type_plan.go:56-60`).
4. A `ColumnPlan` pairs an Arrow data type, builder, and an `Append` conversion function (`internal/arrowio/sql_to_arrow.go:17-22`). SQL rows are appended in `RowsToRecordBatchesEngineWithOverrides` (`internal/arrowio/sql_to_arrow.go:410-446`).
5. MongoDB is a document path. `MongoDB.StreamDocuments` decodes each BSON document into `map[string]any` (`internal/connectors/mongodb.go:209-223`). The worker buffers up to 10,000 documents and calls `InferMongoSchemaWithFieldOrder`, then `MongoDocsToRecord` (`cmd/worker/main.go:1222-1367`; `internal/arrowio/mongo_to_arrow.go:31-146`).
6. `internal/parquetio.Writer` creates a `pqarrow.FileWriter`; its Arrow schema determines the Parquet schema (`internal/parquetio/writer.go:31-63`). The worker uploads the files to S3.
7. For auto-created tables, `inferRunIcebergSchema` independently derives Arrow schema, then calls `icetable.ArrowSchemaToIcebergWithFreshIDs` (`internal/icebergreg/manager.go:1149-1235`). The REST registration reader requires exact Arrow-schema equality across files (`internal/icebergreg/parquet_reader.go:30-75`).
8. ClickHouse is configured as an Iceberg `DataLakeCatalog`, not via a repository-owned source-to-ClickHouse type translator (`docker-compose.clickhouse-altinity.yml:43-57`). The pinned development image is `altinity/clickhouse-server:25.3.8.10042.altinitystable`; another compose file pins a different Altinity version. The repository therefore cannot presently prove the final ClickHouse type for every Arrow/Iceberg type.

### Ownership after the redesign

| Layer | Owns | Must not own |
|---|---|---|---|
| Source adapter/type parser | Interpret source type names, source metadata, and database-specific value semantics. | Target-specific lossy coercions. |
| Canonical schema contract | Source type metadata, logical semantics, fallback choice, and lossless compatibility decisions. | Physical Parquet encoding details. |
| Shared Arrow renderer/converters | Bounds-checked conversion from a canonical field to Arrow values. | Guessing source semantics from Go values. |
| Parquet writer | Serialize the already-selected Arrow contract without another semantic conversion. | Reinterpreting values or selecting fallbacks. |
| Iceberg adapter | Produce and validate an Iceberg schema from the canonical contract/Arrow schema. | Independently sampling a different source schema. |
| ClickHouse compatibility adapter | Verify the deployed Altinity version exposes the chosen Iceberg representation correctly. | Driving unsafe source conversion; it may select a lossless fallback only. |

## 2. Findings verified in the current code

### Cross-cutting P0 findings

| Finding | Current status | Why it matters | Current locations |
|---|---|---|---|
| Narrow integer appenders cast directly. | Resolved in Phase 1A. | Values beyond Int8/16/32 or UInt8/16/32 must report a conversion decision instead of wrapping. | `internal/arrowio/type_plan.go` |
| Decimal planning caps precision at 38; append failure writes null. | Resolved for SQL scalar decimals in Phase 1B; MongoDB and array work remain open. | A valid source decimal can lose digits or become indistinguishable from source `NULL`. | `internal/arrowio/type_plan.go`; PostgreSQL/MySQL planners |
| Date and timestamps clamp to 1900–2299. | Resolved in Phase 1C for shared Date32/Timestamp(us)/Time64 conversion. | Valid source values must not be silently changed to a different date/time. | `internal/arrowio/type_plan.go` |
| Binary appender accepts text and `fmt.Sprint`. | Resolved in Phase 1D. | Arbitrary values must not become non-reversible bytes. | `internal/arrowio/type_plan.go` |
| Document inference is first-non-null-wins. | Still open (Phase 4). | Later values can be stringified, nullified, or converted to zero. Worker and Iceberg creation sample different sizes. | `mongo_to_arrow.go:38-58,289-361,366-408`; `cmd/worker/main.go:1282-1338`; `manager.go:1177-1232` |

### Other important findings

- SQL schema fields initially preserve `ColumnType.Nullable()`, but a target-type override resets nullability based only on whether the string starts with `Nullable(` (`internal/arrowio/sql_to_arrow.go:455-476`).
- PostgreSQL-specific unknown types, and MySQL/MariaDB unknown types, fall through to the generic String plan. This is sometimes a safe presentation choice but carries neither the original source type nor a fallback codec (`postgres_to_arrow.go:80-87`, `mysql_to_arrow.go:90-94`, `sqlite_to_arrow.go:81-82`).
- PostgreSQL arrays are recognized by `T[]` and `_T`, but current list parsing cannot preserve array dimensions or a rich element type where an element falls back (`postgres_to_arrow.go:11-21`; `type_plan.go:587-624`).
- MongoDB preserves neither BSON Binary subtype nor BSON Timestamp increment. It maps ObjectId to String and Decimal128 to fixed `(38,18)` (`mongo_to_arrow.go:198-252`). Nested values, arrays, Regex, JavaScript, MinKey, and MaxKey use generic textual rendering (`mongo_to_arrow.go:263-273,379-408`).
- Passing Arrow schema through `pqarrow` is appropriate serialization behavior, but Arrow-to-Iceberg and Iceberg-to-ClickHouse type support needs an explicit tested matrix. The present schema fingerprint only proves post-write Arrow-schema equality, not semantic source-value preservation.

## 3. Canonical type conversion policy

Every field must receive an explicit policy before rows are converted. The policy contains source metadata, a selected physical representation, and either an exact native mapping or a versioned lossless fallback.

### Level 1 — Native lossless mapping

Use a native Arrow/Iceberg type only when the field's declared semantics and each value are exactly representable through the configured Parquet/Iceberg/ClickHouse path.

Examples: PostgreSQL `BIGINT -> Int64`; MySQL `DOUBLE -> Float64`; BSON Boolean -> Boolean; a safe PostgreSQL `DATE -> Date32` mapping.

### Level 2 — Structured lossless mapping

Use an Arrow/Iceberg struct/list/binary-plus-metadata representation where a primitive loses meaning. Examples include BSON Timestamp `{seconds, increment}`, a spatial payload plus SRID/type metadata, and recursively represented homogeneous BSON objects/lists.

### Level 3 — Canonical lossless fallback

When the exact structure cannot safely cross all target stages, use a stable, versioned String or Binary codec. Examples: PostgreSQL interval and range text; network canonical text; canonical UUID text; BSON Extended JSON; raw binary with an explicit subtype. The fallback must carry enough metadata to interpret or reconstruct the source value.

### Level 4 — Reject only unreadable source values

Unsupported downstream typing alone must not reject a row. Reject only when the driver cannot retrieve the source value or it is itself unreadable/corrupt. When possible, retain a raw source representation and emit a bounded diagnostic.

### Canonical contract and metadata

Introduce an internal schema object before `ColumnPlan` selection. The exact package name is an implementation choice; a package near `internal/arrowio` is likely appropriate because `ColumnPlan` is currently the de facto contract.

Minimum field metadata:

- source engine and original declared type name;
- nullability, precision, scale, signedness, bit width, fixed length, charset/collation where material;
- temporal unit and instant/local-wall-clock/offset semantics;
- enum/set labels; array dimensions; geometry type/SRID; PostgreSQL domain/extension identity;
- BSON type, Binary subtype, ObjectId/UUID encoding, and fallback codec/version;
- mapping class: `native`, `structured`, or `fallback`.

Store operational metadata first in the internal canonical schema. Mirror non-sensitive, durable interpretation metadata into Arrow field metadata and an Iceberg table/snapshot property or an artifact sidecar. Arrow metadata is close to the physical schema but may not survive every external path; Iceberg properties are durable but table-level; a sidecar can preserve complete versioned policy without relying on a consumer's metadata support. Avoid logging raw values.

## 4. Shared conversion-infrastructure roadmap

| Item | Priority | Intended behavior and fallback | Likely implementation locations | Tests |
|---|---|---|---|---|
| Checked integer conversion — implemented (Phase 1A) | P0 | Checked native integer conversion rejects overflow/underflow rather than wrapping or nulling. | `internal/arrowio/type_plan.go`; canonical renderer. | Every signed/unsigned min/max, one-over/under, null, string/driver forms. |
| Decimal contract — implemented for PostgreSQL/MySQL/MariaDB scalar numeric declarations (Phase 1B) | P0 | Native Decimal128 only for supported exact `(p,s)`. Unsupported/unbounded declarations use versioned canonical decimal text; non-null native conversion failures return errors. MongoDB and array work remain deferred. | `type_plan.go`; PostgreSQL/MySQL planners. | p=1/38/beyond-38, scale boundaries/negative scale, exact strings, invalid driver payload, source-null distinction. |
| Temporal contract — shared Date32/Timestamp(us)/Time64 safety implemented (Phase 1C) | P0 | Shared conversion preserves Arrow-representable dates/timestamps, rejects sub-microsecond values and invalid Time64 time-of-day values, and returns typed errors instead of clamping or nulling. Source-specific instant/local/offset/duration semantics and fallback remain later work. | `internal/arrowio/type_plan.go`; source planners. | Outside 1900–2299, micro/nanoseconds, Time64 bounds, timezone/offset semantics, PostgreSQL `timetz`, MySQL negative/large `TIME`. |
| Byte-exact binary conversion — implemented (Phase 1D) | P0 | Shared Binary accepts only exact `[]byte` inputs, preserving bytes unchanged; a non-byte input returns a typed error and never uses `fmt.Sprint`. MongoDB-specific Binary behavior remains deferred. | `internal/arrowio/type_plan.go`; Mongo Binary path. | Empty/embedded NUL/invalid UTF-8/arbitrary bytes, mutation after append, non-byte inputs, SQL error propagation. |
| Controlled conversion outcome — implemented for shared SQL plans (Phase 1E) | P0 | Shared scalar/list plans return typed errors rather than appending null/zero/default values for non-null conversion failures. Existing source-specific fallbacks remain later work. | `internal/arrowio/type_plan.go`; SQL row loop. | Invalid Float/Boolean/String/List/legacy Decimal inputs and SQL error propagation. |
| Nullability policy — SQL cleanup implemented (Phase 1E) | P1 | Source `ColumnType.Nullable()` remains authoritative for a type override unless existing `Nullable(...)` explicitly requests nullable output. Policy metadata retains source nullability. Mongo missing/null semantics remain deferred. | `internal/arrowio/sql_to_arrow.go`. | Nullable/non-null SQL, normal and `Nullable(...)` overrides, policy metadata. |
| One durable schema source | P1 | Worker, auto-create, and registration consume the same persisted canonical schema, instead of separate 10,000/1,000 document samples. | `cmd/worker/main.go`; `internal/icebergreg/manager.go`. | Worker/table schema equality; late-field and multi-file cases. |
| Schema diagnostics — minimally implemented (Phase 1E) | P2 | Passive `MappingDiagnostics` exposes source/target mapping class and fallback codec/version without row values. Counters, warnings, and durable observability remain later work. | `internal/arrowio/mapping_diagnostics.go`. | Native/fallback/codec/MariaDB identity assertions; no row payloads. |

## 5. PostgreSQL roadmap

`planPostgresColumn` is the parser starting point (`internal/arrowio/postgres_to_arrow.go:7-88`); the connector currently supplies driver metadata only (`internal/connectors/postgres.go:100-124`). Add catalog-assisted metadata only where driver names cannot carry needed semantics, such as enum labels, domains, composites, extension identity, and array dimensions.

| Source type/group | Current behavior | Target strategy and lossless fallback | Priority | Notes/tests |
|---|---|---|---|---|
| `smallint`, `integer`, `bigint`, serial aliases | ✅ Checked native-width conversion (Phase 1). | Native exact integers with checked conversion. Fallback canonical decimal/text only if an anomalous driver value cannot fit the declared type. | P0 | Test extrema and invalid forms. |
| `numeric`, `decimal`, `money` | ✅ Declared Decimal128 or canonical decimal fallback (Phase 1); values are never blindly capped or converted to null on failure. | Preserve declared `(p,s)` where exact. For values/metadata outside Decimal128, canonical decimal codec. Treat money as documented locale-independent numeric representation plus source type metadata. | P0 | Test p>38, no declared precision, scale, currency-formatted driver values. |
| `real`, `double precision` | Float32/64; Float32 may downcast input. | Retain declared width and special-value behavior. Do not downcast unvalidated Float64 input. | P1 | NaN, infinities, signed zero. |
| `date`, `timestamp`, `timestamptz` | Shared Date32/Timestamp(us) safety implemented; PostgreSQL policy now records calendar-date, local-wall-clock, and instant semantics. | Preserve date/range/precision. Model `timestamptz` as instant plus documented source timezone semantics; canonical timestamp fallback when target cannot retain range/unit. | P0 | Extremes, submicroseconds, offsets/DST. |
| `time`, `timetz`, `interval` | `time` remains Time64us with local-time metadata. `timetz` and interval are explicit text fallbacks. | Preserve `timetz` offset and interval months/days/time text exactly; use structured values only after compatibility proof. | P1 | Negative offsets, interval months, fractional seconds. |
| `uuid` | ✅ Phase 2A: validated PostgreSQL UUID text fallback with named `postgres-uuid-text` codec and source metadata. | Canonical UUID String initially; consider FixedSizeBinary(16) only after complete compatibility validation. | P1 | Valid/uppercase/invalid UUID; arrays remain deferred. |
| `json`, `jsonb`, `xml` | ✅ Phase 2A: strict source-text String plans. JSON/JSONB validate without remarshal and use distinct named codecs; XML preserves source text. | Canonical JSON text with source-type metadata; XML as text preserving encoding declaration where available. | P1 | Unicode, scalar/object/array JSON, large payloads. |
| enum, domain | ✅ Phase 2C: enum uses validated `postgres-enum-text` v1 String fallback with catalog labels; domains reuse their safely discovered base plan and retain domain identity. Unsupported domain bases use `postgres-domain-text` v1. | Enum String plus enum type/labels metadata. Domain underlying physical mapping plus domain identity/constraints metadata. | P2 | Query system catalogs; altered enum/domain. |
| composite | ✅ Phase 2C: exact String fallback using `postgres-composite-text` v1; catalog field summaries remain internal policy metadata. | Struct only with deterministic component metadata; otherwise canonical PostgreSQL text/Binary codec. | P2 | Null components, nested composites. |
| arrays/multidimensional arrays | ✅ Phase 2B: one-dimensional, explicitly supported primitive arrays use Arrow List. Unsupported elements and explicitly multidimensional declarations use exact `postgres-array-text` v1 String fallback. | Preserve PostgreSQL array text whenever dimensions/lower bounds or element semantics cannot be represented by Arrow List. | P1 | Multi-dimensional, null elements, escaped strings, exotic elements. |
| `inet`, `cidr`, `macaddr`, `macaddr8` | ✅ Phase 2A: validated PostgreSQL text fallbacks with distinct semantic metadata and named network/MAC codecs. | Canonical PostgreSQL text plus source type; no need for a custom Arrow extension in the first release. | P2 | IPv4/IPv6/prefix/MAC forms. |
| `bit`, `varbit` | ✅ Phase 2B: exact String fallback using `postgres-bit-text` v1 for every width, including `BIT(1)`. Declared width is retained when driver type metadata includes it. | Preserve exact bit string and declared width through canonical text/Binary. | P2 | Width >64, leading zeros. |
| geometric, range, multirange | ✅ Phase 2D exact String fallbacks: `postgres-geometry-text` v1, `postgres-range-text` v1, and `postgres-multirange-text` v1. | Structured geometry/range only after an explicit decoder contract. | P3 | Bounds, empty/infinite ranges. |
| `hstore`, PostGIS, extension/UDT types | ✅ Phase 2D exact fallbacks: `postgres-hstore-text` v1, `postgres-postgis-text` v1, `postgres-postgis-binary` v1 for already-byte payloads, and `postgres-extension-text` v1 for all remaining PostgreSQL types. | Source-specific stable text/Binary codec. | P3 | Extension version and SRID coverage. |

## 6. MySQL roadmap

`planMySQLColumn` currently recognizes common names (`internal/arrowio/mysql_to_arrow.go:7-95`), while the connector gets `ColumnType` metadata (`internal/connectors/mysql.go:56-80`). Add metadata queries only when driver names omit a required property such as enum labels, collation, geometry SRID, or actual unsigned declaration.

| Source type/group | Current behavior | Target strategy and lossless fallback | Priority | Notes/tests |
|---|---|---|---|---|
| Signed/unsigned integer families | ✅ Checked native widths, including UInt64. | Preserve declared signedness; UInt64 downstream compatibility remains Phase 5. | P0 | Each boundary and UInt64 max. |
| `decimal`, `numeric`, `dec`, `fixed` | ✅ Declared Decimal128 or canonical decimal fallback; invalid non-null conversion errors. | Same exact decimal/fallback contract as PostgreSQL. | P0 | p/s limits and exact driver values. |
| `float`, `double` | Float32/64. | Preserve width; define special-value compatibility through Parquet/Iceberg/ClickHouse integration tests. | P1 | NaN/Inf/signed zero. |
| `bit(n)`, boolean aliases | `BIT(1)` and `TINYINT(1)` become Bool; other bit values UInt64. | Preserve Boolean aliases only by declared semantic policy. Use exact fixed bit string codec for arbitrary `n`, especially n>64. | P1 | Leading zero and 65-bit values. |
| char/varchar/text | String without charset/collation/fixed-width metadata. | Decode to UTF-8 only when source charset conversion is verified; otherwise byte-preserving fallback. Retain material collation/fixed padding metadata. | P2 | Non-UTF8 text and trailing spaces. |
| binary/varbinary/blob | ✅ Exact-byte Binary plan. | Raw bytes only; subtype-free binary requires no text conversion. | P0 | NUL/invalid UTF-8/large blobs. |
| enum/set | String. | Enum canonical label plus type/labels metadata. `SET` canonical ordered label list or bitmask+labels metadata. | P1 | Empty and multi-label sets. |
| date/datetime/timestamp/time/year | Date/timestamp-us/time64/int16; clamp and timezone/session assumptions remain. | Preserve date/range/fractional precision. Treat MySQL `TIME` as signed duration, not wall-clock time. Make TIMESTAMP session-timezone policy explicit. | P0 | Negative/838-hour TIME, zero-date policy, session zone. |
| JSON | String. | Canonical JSON String with source type metadata; ensure it is not confused with arbitrary text. | P1 | Unicode and large documents. |
| geometry/spatial | Binary with no encoding/SRID validation. | WKB/EWKB binary plus geometry/SRID metadata, or canonical spatial text only if it is demonstrably reversible. | P2 | Geometry collections and SRIDs. |

## 7. MariaDB roadmap

MariaDB must retain shared rendering where semantics are identical, but not be treated as a permanent alias: `OpenMariaDB` embeds `MySQL` (`internal/connectors/mariadb.go:15-35`) and `PlanForSQLColumn` currently routes both engines to `planMySQLColumn` (`internal/arrowio/type_plan.go:56-58`). Introduce `planMariaDBColumn` or a MariaDB metadata normalizer that delegates only explicit shared cases.

| Scope | Target strategy and lossless fallback | Priority | Locations and tests |
|---|---|---|---|
| Shared integer, decimal, float, text/blob, temporal, enum/set, spatial types | Reuse MySQL canonical rules and checked renderers after MariaDB metadata normalization. | P0/P1 | New MariaDB planner/metadata tests beside `mysql_to_arrow_test.go`. |
| Unsigned/decimal/temporal metadata | Verify the MariaDB driver reports type, unsigned flag, precision/scale, and fractional temporal precision as expected; query metadata if it does not. | P0 | MariaDB integration container; boundaries and timezone/session tests. |
| JSON behavior | Preserve original declared type/alias and validation/storage semantics in metadata; use canonical JSON fallback rather than assuming MySQL-native JSON behavior. | P1 | Versioned MariaDB tests. |
| UUID-related values/features | Recognize configured/version-supported UUID declarations/functions only when schema metadata establishes intent; canonical UUID String is safe initial fallback. | P2 | UUID format/order tests. |
| `INET6` and network-related aliases | Add explicit recognition; canonical network String plus original type metadata is a safe initial mapping. | P2 | IPv4/IPv6 and invalid input tests. |
| MariaDB extensions/aliases | Maintain a versioned registry: known shared type, MariaDB-specific mapping, or named fallback codec. Never generic-string an unknown type without metadata. | P3 | Metadata registry tests per supported server version. |

## 8. MongoDB roadmap

### Deterministic schema strategy

The existing first-non-null rule (`InferMongoSchemaWithFieldOrder`) is not reliable for schemaless data. Use a staged strategy that favors no data loss over maximum native nesting.

1. **Phase-1 deterministic union:** scan a configured sample or explicit user schema; merge observed types deterministically rather than by order. Preserve stable simple scalars natively. If a field is heterogeneous, nested, or has an unsupported BSON value, select a canonical Extended JSON/BSON fallback for that field before rows are written. Persist the sample/policy fingerprint.
2. **One schema authority:** persist this canonical document schema and use it for the worker, Iceberg auto-create, and registration. Eliminate the current independent 10,000-row worker and 1,000-row manager inference.
3. **Phase-2 recursive support:** add homogeneous Struct/List inference and deterministic numeric widening. For mixed arrays/documents, retain the phase-1 fallback unless a tagged union is deliberately introduced.
4. **Phase-3 evolution:** define late-field policy: add a nullable field through an explicit Iceberg evolution transaction, or retain unmatched late fields in a documented fallback envelope. Never silently drop them.
5. **User schema:** allow an explicit schema contract to avoid sampling ambiguity for production collections.

| BSON type/case | Current behavior | Target strategy and fallback | Priority |
|---|---|---|---|
| String, Boolean, Null | Native String/Bool; missing and null collapse to Arrow null. | Native types; optionally carry field-presence state in fallback/evolution policy where required. | P1 |
| Int32, Int64, Double | Int32 -> Int64; mismatches may become zero/null. | Preserve width or deterministic validated widening. Mixed numeric/non-numeric fields use field fallback. | P0 |
| Date | Timestamp(ms, UTC). | Native instant timestamp only after range/unit compatibility check; preserve BSON Date identity in metadata. | P1 |
| BSON Timestamp | Converted to milliseconds from seconds, increment lost. | Structured `{seconds, increment}` where supported; otherwise canonical Extended JSON/Binary. | P1 |
| Decimal128 | Forced Decimal128(38,18). | Exact Decimal128 only when representable; otherwise canonical decimal/BSON fallback. | P0 |
| Binary and UUID subtypes | Raw bytes; subtype discarded. | Binary plus subtype/UUID-representation metadata, or BSON-compatible fallback envelope. | P1 |
| ObjectId | Plain hexadecimal String. | Canonical ObjectId String plus explicit BSON-type metadata initially; optionally fixed 12-byte binary after compatibility proof. | P1 |
| Nested object/document | Generic Extended JSON text. | Phase 1 canonical Extended JSON fallback; Phase 2 recursive Struct when stable/deterministic. | P1 |
| Arrays of primitives/objects/mixed | Generic JSON text. | Homogeneous list only after deterministic element type; mixed array uses canonical Extended JSON/BSON. | P1 |
| Regex, JavaScript, CodeWithScope | Generic String. | Canonical Extended JSON/BSON codec preserving options/code/scope. | P2 |
| MinKey, MaxKey, Undefined | Sentinel String. | Tagged Extended JSON/BSON fallback, never bare sentinel text without type metadata. | P2 |
| Late/deep fields and heterogeneous types | First sample fixes schema; later values can be corrupted. | Schema union/evolution/fallback policy above, with maximum depth and explicit deep-object fallback. | P0 |

Relevant implementation sites are `internal/connectors/mongodb.go:137-223` for sampling/streaming and `internal/arrowio/mongo_to_arrow.go:31-408` for inference/rendering. Keep raw BSON-type information available until the canonical policy is applied.

## 9. Arrow, Parquet, Iceberg, and ClickHouse compatibility plan

Current dependency versions are Arrow Go v18 and `github.com/apache/iceberg-go v0.5.0` (`go.mod:8-9`). Compatibility must be proven against the deployed Altinity images, not inferred from Arrow availability alone.

| Canonical representation | Arrow | Parquet | Iceberg | ClickHouse | Safe now? | Validation/action |
|---|---|---|---|---|---|---|
| Boolean, Int8/16/32/64, Float32/64, UTF-8 String, Binary | Native | Native | Expected common support | Version-dependent external mapping | Partially | Round-trip and ClickHouse type/value assertions. |
| UInt8/16/32/64 | Arrow-native | Native physical representation | Must verify converter/version behavior | Must verify especially UInt64 | Unclear | P0 matrix tests, including UInt64 max. |
| Decimal128(p,s), p<=38 | Arrow-native | Decimal logical type | Must verify Iceberg converter constraints | Must verify Decimal exposure | Unclear | Boundary tests by p/s; fallback when any stage rejects. |
| Date32, Time64, Timestamp(us, timezone) | Arrow-native | Logical annotations | Must verify mapping semantics | Time/timezone support varies | Unclear | Value/range/timezone integration tests; no clamping. |
| List/Struct | Arrow-native | Nested encoding | Iceberg nested types expected but converter/version must be tested | Reader support/version-specific | Unclear | Test null elements/fields and nested schema. |
| Fixed-size UUID/ObjectId Binary | Arrow-capable | Fixed/byte array support | Compatibility uncertain | Compatibility uncertain | Unclear | Prefer canonical String until end-to-end proof. |
| BSON Timestamp struct | Arrow Struct | Nested encoding | Requires converter validation | Requires reader validation | Unclear | Start with Extended JSON fallback if matrix fails. |
| Canonical String fallback | Native | Native | Commonly portable | Commonly readable | Preferred fallback | Validate UTF-8/codec metadata. |
| Canonical Binary fallback | Native | Native | Must verify Binary mapping | Must verify binary exposure | Unclear | Use only after binary integration coverage; String fallback remains safer for textual codecs. |

Build this as an automated matrix. For every canonical mapping, create an Arrow record, write it with `internal/parquetio.Writer`, register it via both supported registration paths where applicable, read the Iceberg table, and query `DESCRIBE` plus exact values through `DataLakeCatalog`. A successful query alone is insufficient.

ClickHouse compatibility may choose between two equally lossless representations, such as canonical String versus Binary. It must never cause an earlier source conversion to clamp, wrap, or discard semantics.

## 10. Test and observability roadmap

### Test layers

1. **Unit tests:** planner and renderer tests per mapping: min/max, out-of-range, source-null, invalid payload, precision/scale, timezone, Unicode, NUL bytes, and unusual BSON values. Extend `internal/arrowio/*_to_arrow_test.go` and `type_plan_test.go`.
2. **Source integration tests:** run PostgreSQL, MySQL, MariaDB, and MongoDB containers containing every supported edge type. Exercise connector metadata and the real worker extraction path, not hand-built Go values alone.
3. **Parquet verification:** independently read generated files with Arrow/Parquet and assert exact schema, values, nullability, decimal values, timestamp unit, bytes, and fallback metadata.
4. **Iceberg verification:** assert table schema/properties and values after registration. Test both REST Go and Altinity `ice` registration where each is supported.
5. **ClickHouse verification:** query through Altinity `DataLakeCatalog`, assert exposed type and semantic equality to source truth.
6. **Golden datasets:** add durable fixtures following repository conventions, proposed as `testdata/types/postgres`, `testdata/types/mysql`, `testdata/types/mariadb`, and `testdata/types/mongodb`. Each contains source DDL/documents, expected canonical policy, expected fallback metadata, and expected visible values.

### Observability

Emit bounded schema-level diagnostics/counters for `type_mapping_native`, `type_mapping_structured`, `type_mapping_fallback`, `type_mapping_unreadable`, and `type_mapping_value_violation`, labeled by source, table/collection, field, declared source type, canonical representation, fallback codec, and reason. Log no raw values. Persist mapping policy/fingerprint with the artifact and Iceberg table/snapshot so readers can interpret a fallback later.

## 11. Migration and backward compatibility

Existing Iceberg tables may already contain String columns that a new policy would map to numeric, UUID, Binary, Struct, or List. Changing a physical type may be incompatible with Iceberg schema evolution and can make historical files ambiguous.

- Apply the new policy by default to newly created tables only after the compatibility matrix passes.
- For an existing table, retain its current physical representation unless an explicit migration creates a new versioned table or a compatible additive column/backfill is approved.
- Do not silently change Decimal precision, UUID/ObjectId representation, Mongo field inference behavior, or fallback codec for an existing table.
- Version the type-mapping policy and fallback codecs. Store the version durably with each table/artifact, and make a migration tool choose whether to rewrite historical data.
- A policy change that only adds metadata without changing physical values is generally safer, but still needs reader compatibility verification.

## 12. Prioritized implementation phases

### Phase 0 — canonical contract (P0)

Define lossless, structured, and fallback policy; conversion outcomes; metadata storage; mapping-policy version; and one authoritative durable schema. Decide the initial fallback codec formats and their reconstruction guarantees.

**Status:** Implemented as internal, non-persisted policy metadata in Phase 0. `internal/arrowio.TypePolicy` is attached to PostgreSQL, MySQL, and MariaDB SQL `ColumnPlan` values without altering Arrow fields, Parquet output, Iceberg schemas, or conversion behavior. Durable schema authority, codecs, and conversion outcomes remain later-phase work.

### Phase 1 — eliminate shared silent corruption (P0) — ✅ Completed

Implement checked integer conversion, exact-decimal/fallback behavior, unclamped temporal conversion, byte-exact binary conversion, explicit failure/fallback outcomes, and diagnostics. Do this before adding exotic source mappings.

**Status:** ✅ Completed

- ✅ Phase 1A — checked signed/unsigned integer conversion
- ✅ Phase 1B — decimal safety
- ✅ Phase 1C — temporal conversion safety
- ✅ Phase 1D — byte-exact binary conversion
- ✅ Phase 1E — shared conversion, SQL nullability, and passive diagnostics cleanup

Shared SQL plans now return explicit errors for non-null conversion failures instead of silently appending null/zero/default values. Source-specific semantic mappings remain deferred: PostgreSQL to Phase 2; MySQL/MariaDB to Phase 3; MongoDB conversion and inference to Phase 4; and Iceberg/ClickHouse compatibility to Phase 5. Durable fallback execution, mapping metadata persistence, and worker-level counters/logging are not implemented here.

### Phase 2 — PostgreSQL core correctness (P0/P1) — ✅ Completed

- ✅ Phase 2A — semantic scalar mappings: UUID, JSON/JSONB, XML, interval, INET/CIDR, MACADDR/MACADDR8, and PostgreSQL temporal-policy metadata. `timetz` now uses an offset-preserving text fallback.
- ✅ Phase 2B — arrays and bit/varbit: safe primitive one-dimensional arrays remain Arrow List; unsupported and multidimensional arrays use `postgres-array-text` v1; bit strings use `postgres-bit-text` v1.
- ✅ Phase 2C — enums, domains, composites: connector catalog enrichment classifies unambiguous user-defined types; enums and composites use named exact-text fallbacks, while domains reuse safe base mappings with domain identity metadata.
- ✅ Phase 2D — ranges, multiranges, geometric types, hstore, PostGIS, and remaining extensions use named exact text/binary fallbacks; no payload parser or Arrow extension type was introduced.

Phase 2C adds batched PostgreSQL catalog reads for unambiguous user-defined types. Phase 2D completes PostgreSQL coverage with named, exact fallbacks for every remaining advanced or unknown PostgreSQL type. It introduces no Arrow extension types, Iceberg changes, or ClickHouse changes. Phase 2B intentionally changes PostgreSQL `BIT(1)` from Boolean and `BIT(n)` from UInt64 to String, eliminating loss of bit-string identity, leading zeroes, and values wider than 64 bits. Arrays previously treated as List now use String fallback when their element type or explicit dimensionality cannot be represented exactly.

### Phase 3 — MySQL and MariaDB core correctness (P0/P1) — ✅ Completed

MySQL and MariaDB now have separate planner entry points with shared MySQL-family rules. Declared signed/unsigned integer widths (including UInt64), FLOAT/DOUBLE, decimal aliases, date, datetime, timestamp, and YEAR use checked native renderers where exact. DATETIME/TIMESTAMP declarations above microsecond precision use named text fallbacks; TIMESTAMP policy records session-timezone-dependent instant semantics.

BIT is now a width-preserving String fallback (`mysql-bit-text`/`mariadb-bit-text` v1), including `BIT(1)`; ordinary `TINYINT(1)` remains numeric because intent cannot be inferred reliably. MySQL-family TIME is now a signed-duration String fallback (`mysql-time-text`/`mariadb-time-text` v1), preserving negative values, hours above 24, and fractional seconds. JSON, ENUM, SET, geometry, and unknown extensions have engine-specific versioned fallback policies. Geometry remains byte-exact Binary; JSON is validated text without re-marshalling; ENUM/SET retain exact driver text. Native textual and binary families continue to use String and exact []byte Binary respectively.

The current `database/sql.ColumnType` path supplies type name, decimal size, and nullability. The planner records discoverable declaration metadata (signedness, bit width, fixed length, temporal semantics) but does not issue per-column catalog queries. Charset/collation, enum/set labels, and geometry SRID are not exposed by that path and remain optional future connector enrichment rather than guessed metadata. MariaDB UUID and network fallbacks are selected only when those explicit type names are reported.

### Phase 4 — MongoDB deterministic handling (P0/P1)

Replace first-value inference; unify schema authority; implement mixed-field fallback, numeric widening, BSON Timestamp, Decimal128, Binary subtype, ObjectId, nested documents, arrays, and missing/null policy.

### Phase 5 — Iceberg/ClickHouse matrix (P1)

Automate end-to-end type/value compatibility for every canonical representation against supported Arrow/Iceberg/Altinity versions. Gate native mappings on this matrix.

### Phase 6 — advanced and rare types (P2/P3)

Add PostgreSQL ranges/multiranges/composites/PostGIS/extensions, MySQL/MariaDB spatial and extensions, and richer BSON typed structures only where the matrix demonstrates a reliable native path.

## Definition of Done

The work is complete only when:

- every recognized source type has an explicit native/structured/fallback policy, and unknown types have a named lossless fallback;
- unsupported downstream types neither fail ingestion automatically nor silently lose information;
- integer conversion cannot wrap, decimal conversion cannot truncate/null a valid value, temporal values cannot clamp, and binary remains byte-exact;
- timezone semantics, BSON-specific semantics, PostgreSQL array dimensions, and fallback interpretation metadata are documented and tested where needed;
- MongoDB heterogeneous and late fields follow deterministic, observable behavior;
- source, Parquet, Iceberg, and ClickHouse tests verify semantic values, not just successful reads;
- every fallback is observable at schema level; and
- policy versioning and existing-table migration behavior are documented and enforced.
