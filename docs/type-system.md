# ORabbit Logical Type System

`internal/typesystem.LogicalType` is ORabbit's canonical, source-independent description of type semantics. It gives future source mappers, Arrow mappers, Iceberg mappers, validation, logs, and APIs one vocabulary without requiring any of them to import each other.

It represents only logical kinds and their structure: nullability, decimal precision/scale, timezone-aware timestamps, array elements, map key/value types, and ordered struct fields. `SourceTypeName` is optional provenance metadata and is not semantic equality.

It intentionally does not represent database spellings (`VARCHAR`, `JSONB`, `NUMBER`, `DATETIME64`), physical Arrow/Iceberg types, or source-engine behavior. It has no Arrow, Iceberg, or database-driver imports.

Canonical kinds are: `unknown`, `string`, `bool`, signed and unsigned integers from 8 to 64 bits, `float32`, `float64`, `decimal`, `date`, `time`, `timestamp`, `timestamp_tz`, `uuid`, `binary`, `json`, `array`, `struct`, and `map`.

The future fallback contract is:

```text
recognized + natively supported -> typed conversion
recognized but unsupported in Arrow/Iceberg -> lossless string fallback + warning
unrecognized -> lossless string fallback + warning
```

## Runtime conversion core

`Convert(value, target)` now provides Arrow-independent canonical values for the scalar kinds, dates/times/timestamps, UUIDs, binary data, and recursive arrays. It recursively dereferences pointers and interfaces; source `nil` remains `nil` so schema-level nullability enforcement stays at the row/schema boundary.

Failed supported conversions return `*ConversionError`; they never become zero values, false, empty strings, or nulls. Integer conversions reject floats and validate destination bounds. `DecimalValue` retains an exact `big.Int` unscaled value plus scale; it rejects rounding and precision overflow. `timestamp_tz` normalizes its output to UTC, while offset-free timestamp strings are explicitly parsed as UTC by Go's `time.Parse`.

Normal `string` conversion is intentionally separate from `ToLosslessString`. The latter uses `base64:` for bytes, RFC3339Nano UTC for times, scalar canonical text, and `json:` plus stable JSON for conservative structured values. It rejects values that cannot be represented safely instead of falling back to `fmt.Sprint`.

`unknown`, `struct`, `map`, and `json` currently use this conservative lossless representation. Native structured conversion is intentionally deferred, as is migration of legacy Arrow planners and their source-specific behavior.

Destination-specific code remains responsible for warnings when it selects the fallback representation; the conversion core intentionally returns only the value or a conversion error.

## Destination mapping

`internal/arrowio.ArrowTypeForLogicalType` maps the canonical logical type to Arrow and returns dependency-neutral mapping metadata. `internal/icebergreg.IcebergMappingForLogicalType` records the expected Iceberg representation for the installed `github.com/apache/iceberg-go` bridge. `ResolveStorageMapping` combines them and verifies the selected Arrow type through `ArrowSchemaToIcebergWithFreshIDs`, so the Arrow schema cannot silently become a different Iceberg type.

| Logical | Resolved Arrow | Expected Iceberg | Classification |
| --- | --- | --- | --- |
| `int64` | `int64` | `long` | exact |
| `int8` / `int16` | native Arrow width | `int` | safe promotion |
| `uint8` / `uint16` | native Arrow width | `int` | safe promotion |
| `uint32` | `uint32` | `long` | safe promotion |
| `uint64` | `string`* | `string`* | semantic fallback |
| `decimal(p,s)`, `p <= 38` | `decimal128(p,s)` | `decimal(p,s)` | exact |
| `decimal(p,s)`, `p > 38` | `string`* | `string`* | semantic fallback |
| `timestamp_tz` with a UTC alias | `timestamp[us, UTC]` | `timestamptz` | exact |
| `uuid` | `string`* | `string`* | semantic fallback |
| `json`, `struct`, `map` | `string`* | `string`* | semantic fallback |
| `array<T>` | `list<resolved T>` | `list<resolved T>` | inherited from `T` |
| `unknown` | `string`* | `string`* | unsupported fallback |

`*` means the storage representation is a fallback. It is deliberately selected on both sides of the current Arrow-to-Iceberg bridge. Arrow alone can express `uint64`, but Iceberg `long` cannot safely hold its complete range, so the resolver uses `string` instead. Arrow Decimal256 is likewise not selected because the current bridge accepts Decimal128 only. Although the installed Iceberg library has a native UUID type, the current runtime converter emits UUID text and native Iceberg UUID requires Arrow UUID extension values; UUID therefore remains text until that end-to-end path is implemented.

For `timestamp_tz`, an empty logical timezone becomes Arrow `UTC`, matching the canonical conversion output. A non-UTC timezone is preserved by the standalone Arrow mapper, but the resolved storage mapper falls back to string because the current bridge only accepts `UTC`, `+00:00`, `Etc/UTC`, and `Z` for Iceberg `timestamptz`.

## Target type syntax

`typesystem.ParseType` is the authoritative parser for explicit ORabbit target types. Preferred canonical syntax is case-insensitive:

```text
string
bool
int8 | int16 | int32 | int64
uint8 | uint16 | uint32 | uint64
float32 | float64
decimal(p,s)
date | time | timestamp | timestamp_tz[zone]
uuid | binary | json
array<T> | nullable<T>
```

Whitespace and nesting are supported, such as `array < nullable < string > >`. Repeated nullable wrappers normalize idempotently to one nullable logical type.

Legacy compatibility syntax is accepted but is not the preferred API: ClickHouse-style `Array(T)`, `Nullable(T)`, and `LowCardinality(T)`; `Numeric(p,s)`, `Number(p,s)`, `Money`, and `SmallMoney`; `DateTime`, `DateTime64`, `Time64`; SQL spelling aliases such as `VARCHAR`, `BYTEA`, and `UNIQUEIDENTIFIER`. `LowCardinality` is stripped because it has no independent logical meaning. `XML` currently normalizes to `string` because the logical vocabulary has no XML kind.

Unknown source-database types may still use the documented lossless string fallback. Unknown **explicit target type strings** are configuration errors: `ParseType("FooBar")`, `array<>`, and malformed decimals return errors rather than silently becoming `unknown` or `string`. Decimal precision is retained without a parser limit; for example, `decimal(50,10)` parses successfully and storage resolution later chooses its explicit fallback.

## PostgreSQL migration

PostgreSQL is the first migrated source engine: PostgreSQL metadata flows through `arrowio.LogicalTypeForPostgresColumn`, `ArrowTypeForLogicalType`, `typesystem.Convert`, and canonical Arrow appenders. Invalid values now return conversion errors rather than wrapping narrow integers, producing zero values, or silently appending nulls. UUID and JSON remain logical `uuid`/`json` and use their explicit storage fallback; unsupported PostgreSQL semantic types (including `INET`, `TIMETZ`, and `VARBIT`) remain `unknown` with their source type name and use lossless-string fallback. Unconstrained `NUMERIC` is also `unknown`; no `decimal(38,10)` constraint is invented. PostgreSQL text arrays permit null elements, so their migrated element type is marked nullable because source column metadata does not expose element nullability.

MySQL/MariaDB now use same migrated path. `BIGINT UNSIGNED` remains logical `uint64` but storage plan uses string, preserving MaxUint64 exactly. JSON is semantic fallback; ENUM, SET, wide BIT, spatial, TIME, unknown, and unconstrained decimal are `unknown` fallback. MySQL TIME remains string fallback because valid MySQL durations exceed KindTime time-of-day range.

## MSSQL migration

MSSQL now follows the same `LogicalTypeForMSSQLColumn -> PlanForLogicalType -> Convert -> canonical Arrow append` path. `UNIQUEIDENTIFIER` is logical `uuid` and resolves to canonical UUID text in Arrow/Iceberg string storage. `DATETIMEOFFSET` is `timestamp_tz[UTC]`; `TIMESTAMP` and `ROWVERSION` are binary row-version values, not temporal timestamps. XML, `SQL_VARIANT`, `HIERARCHYID`, and spatial values remain `unknown` and take lossless string fallback; driver-reported `JSON` is interpreted semantically as `json` and takes semantic string fallback. Decimal precision is retained exactly, with precision above 38 resolved to storage fallback. MSSQL dates and timestamps no longer inherit ClickHouse range clamping.

## Oracle migration

Oracle now follows `LogicalTypeForOracleColumn -> PlanForLogicalType -> Convert -> canonical Arrow append`. `NUMBER` is `unknown` without reliable precision/scale; when metadata is present it retains the exact decimal precision (including precision above 38, which uses storage fallback). Oracle `DATE` is a `timestamp`, preserving its time component. `TIMESTAMP WITH TIME ZONE` and `TIMESTAMP WITH LOCAL TIME ZONE` are `timestamp_tz[UTC]`; LOCAL TIME ZONE has already been normalized according to Oracle/session semantics before ORabbit canonicalizes the materialized instant to UTC. `RAW`, `LONG RAW`, and `BLOB` are binary. `BFILE`, ROWID variants, XMLTYPE, intervals, custom, and unsupported Oracle types remain `unknown` lossless-string fallback. Oracle temporal values do not inherit ClickHouse clamping.

## ClickHouse migration

ClickHouse now follows `LogicalTypeForClickHouseColumn -> PlanForLogicalType -> Convert -> canonical Arrow append`. `Nullable(T)` becomes logical nullability and `LowCardinality(T)` has no independent logical semantic; both wrappers compose recursively with arrays. `UInt64` remains logical `uint64` but uses exact decimal text in string storage. Decimal precision is preserved: `Decimal256(s)` is logical `decimal(76,s)` and uses storage fallback. DateTime timezone arguments are parsed and preserved; UTC aliases use native timestamptz, while non-UTC zones use string fallback under the current Iceberg bridge. UUID and JSON are semantic fallbacks. Time/Time64 are conservatively unknown because ClickHouse duration-like values may exceed canonical time-of-day semantics. IPs, enums, structured/dynamic/variant types, wide integers, and geo types are unknown fallback. Migrated ClickHouse dates and timestamps no longer clamp to the legacy 1900–2299 range.
