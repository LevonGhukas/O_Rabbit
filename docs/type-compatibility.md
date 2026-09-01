# ORabbit Current Type Compatibility

## Summary

This is a static baseline of the current `internal/arrowio` behavior. SQL mapping starts with `database/sql.ColumnType.DatabaseTypeName`, plus `DecimalSize` and `Nullable` when available, in `internal/arrowio/sql_to_arrow.go:PlansFromSQLEngine`. `internal/arrowio/type_plan.go:PlanForSQLColumn` selects the per-engine planner; its `ColumnPlan.Append` controls runtime coercion. Explicit `column_types` overrides instead use `PlanForTargetType`.

The documented vocabularies compared here are the official [PostgreSQL type list](https://www.postgresql.org/docs/current/datatype.html), [MySQL type reference](https://dev.mysql.com/doc/refman/en/data-types.html), [SQL Server type list](https://learn.microsoft.com/en-us/sql/t-sql/data-types/data-types-transact-sql), [Oracle type reference](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html), [ClickHouse data types](https://clickhouse.com/docs/reference/data-types), [Trino types](https://trino.io/docs/current/language/types.html), [Cassandra CQL types](https://cassandra.apache.org/doc/3.11/cassandra/cql/types.html), [SQLite storage classes and affinities](https://www.sqlite.org/datatype3.html), and [MongoDB BSON types](https://www.mongodb.com/docs/manual/reference/bson-types/). Those references establish source vocabulary only; this code is authoritative for ORabbit behavior.

`Exact` means the planner intentionally preserves the shown logical kind within the limits of its Arrow representation. `SafePromotion` means a wider Arrow representation is selected. `SemanticFallback` preserves a textual representation while losing the logical kind. `UnsupportedFallback` is an unrecognized source type that reaches the generic string fallback. `PotentiallyLossy` is a recognized conversion that can truncate, clamp, or otherwise change a value. `Unknown` is not determined from code.

Potential Iceberg implications are limited to what is visible here: `internal/icebergreg/manager.go:inferRunIcebergSchema` passes the Arrow schema to external dependency `icetable.ArrowSchemaToIcebergWithFreshIDs`. The precise Arrow-to-Iceberg mapping is **Not determined from code** in this repository.

## PostgreSQL

Migration status: PostgreSQL is now the first migrated engine. The table below is retained as the Milestone 1 legacy baseline; current PostgreSQL execution instead uses `LogicalTypeForPostgresColumn -> PlanForLogicalType -> typesystem.Convert`. It no longer clamps unconstrained `NUMERIC`, treats UUID/JSON as native strings semantically, or silently nulls/wraps failed conversions. Unsupported semantic types, including `TIMETZ`, `INET`, and `VARBIT`, are `KindUnknown` and use an observable lossless-string fallback. Other engine sections remain legacy behavior.

Planner: `internal/arrowio/postgres_to_arrow.go:planPostgresColumn`. It explicitly recognizes arrays by the `[]` and `_` forms before scalar matching.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `INT2`, `SMALLINT`, `SMALLSERIAL`; `INT4`, `INTEGER`, `INT`, `SERIAL`; `INT8`, `BIGINT`, `BIGSERIAL` | `Int16`, `Int32`, `Int64` | Direct numeric coercion; Exact | Invalid values for narrow targets can become null; see NULL section. |
| `FLOAT4`, `REAL`; `FLOAT8`, `DOUBLE PRECISION`, `FLOAT` | `Float32`, `Float64` | Numeric coercion; Exact / PotentiallyLossy for `Float32` runtime narrowing | Invalid `Float32` values become null; `Float64` errors. |
| `NUMERIC`, `DECIMAL`; `MONEY` | `Decimal128(p,s)` (missing decimal metadata uses scale 10); `Decimal128(19,2)` | Decimal text/number conversion; PotentiallyLossy | Arrow decimal precision is capped at 38. |
| `BOOL`, `BOOLEAN`; `BIT`; `VARBIT` | `Bool`; `Bool` only for exact `BIT(1)`, otherwise `UInt64`; `String` | Boolean text accepts any non-truthy string as false; PotentiallyLossy | `VARBIT` is an explicit SemanticFallback. |
| `DATE`; `TIMESTAMP`; `TIMESTAMPTZ`; `TIME`, `TIMETZ` | `Date32`; timestamp microseconds (UTC for `TIMESTAMPTZ`); `Time64us` | Date/time parsers accept selected Go/string forms; PotentiallyLossy (microsecond precision and date/tz normalization) | Invalid date/time values append null. |
| `BYTEA` | `Binary` | Bytes retained; Exact | A string runtime value is encoded as bytes by `planBinary`. |
| `UUID`, `JSON`, `JSONB`, `XML`, `TEXT`, `VARCHAR`, `CHAR`, `BPCHAR`, `NAME`, `CITEXT`, `INET`, `CIDR`, `MACADDR`, `MACADDR8` | `String` | Native strings pass through; non-strings are formatted; SemanticFallback for non-text logical kinds | JSON/JSONB/XML/network addresses lose Arrow logical type. |
| Other documented types, including geometric, range, composite, domain, enum, `pg_lsn`, text-search and pseudo types | Generic planner, normally `String` | UnsupportedFallback | `planGenericSQLColumn` only has generic numeric/boolean/date/binary recognition; otherwise string. |

## MySQL / MariaDB

Migration status: MySQL/MariaDB now use LogicalType and shared conversion. BIGINT UNSIGNED resolves to string storage; JSON is semantic fallback; ENUM/SET, spatial values, wide BIT, TIME durations, unconstrained decimal, and unknown types are explicit unknown fallback. Other engine sections remain legacy.

Planner: `internal/arrowio/mysql_to_arrow.go:planMySQLColumn`; MariaDB is dispatched to this same planner by `PlanForSQLColumn`.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `TINYINT`; `SMALLINT`; `MEDIUMINT`, `INT`, `INTEGER`; `BIGINT`; `YEAR`; `UNSIGNED`/`ZEROFILL` variants | Signed/unsigned 8, 16, 32, or 64-bit integer; `YEAR` -> `Int16` | Direct casts after broad integer parsing; Exact by declared width, but runtime narrowing is unchecked | `TINYINT(1)` (exact spelling) maps to `Bool` unless unsigned. |
| `BOOL`, `BOOLEAN`; `BIT(1)`; other `BIT` | `Bool`; `UInt64` | Boolean coercion or integer conversion; Exact / PotentiallyLossy | Other bit widths are widened to `UInt64`. |
| `FLOAT`; `DOUBLE`, `DOUBLE PRECISION`, `REAL`; decimal aliases `DECIMAL`, `NUMERIC`, `DEC`, `FIXED` | `Float32`, `Float64`, `Decimal128` | Numeric/decimal text coercion; PotentiallyLossy where decimal precision exceeds 38 or narrow float is used | Invalid narrow/decimal values append null. |
| `DATE`; `DATETIME`; `TIMESTAMP`; `TIME` | `Date32`; timestamp microseconds (UTC for `TIMESTAMP`); `Time64us` | Date/time conversion; PotentiallyLossy | Invalid temporal values append null. |
| `BINARY`, `VARBINARY`, BLOB variants; geometry family | `Binary` | Bytes retained; strings/non-bytes become textual bytes; geometry is binary fallback | Geometry is recognized but no geometric Arrow type is preserved. |
| `JSON`, text/string types, `ENUM`, `SET` | `String` | SemanticFallback for JSON/enum/set | Native string values remain strings. |
| Other MySQL/MariaDB types | Generic planner, normally `String` | UnsupportedFallback | Actual generic special cases are in `internal/arrowio/sqlite_to_arrow.go:planGenericSQLColumn`. |

## MSSQL

Migration status: MSSQL now uses `LogicalTypeForMSSQLColumn -> PlanForLogicalType -> typesystem.Convert`; the table below is retained as the legacy baseline. Its migrated path rejects overflow and invalid boolean/decimal/temporal values rather than wrapping, silently nulling, or clamping. Decimal precision above 38, `UNIQUEIDENTIFIER`, and semantic JSON use explicit storage fallback. MSSQL temporal values do not use ClickHouse range clamping.

Planner: `internal/arrowio/mssql_to_arrow.go:planMSSQLColumn`.

Current migrated mappings: `UNIQUEIDENTIFIER -> uuid -> Arrow/Iceberg string` (semantic fallback); `DATETIMEOFFSET -> timestamp_tz[UTC]`; SQL Server `TIMESTAMP`/`ROWVERSION -> binary`; XML, `SQL_VARIANT`, `HIERARCHYID`, `GEOMETRY`, `GEOGRAPHY`, and unknown types -> `unknown ->` lossless string (unsupported fallback). `JSON`, when reported as a driver type name, is a semantic interpretation and uses `json ->` string fallback.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `TINYINT`, `SMALLINT`, `INT`, `INTEGER`, `BIGINT` | `UInt8`, `Int16`, `Int32`, `Int64` | Direct integer coercion; Exact by declared width, but narrow casts are unchecked | SQL Server `TINYINT` is deliberately unsigned. |
| `BIT`; `FLOAT` (precision-aware), `REAL`, `FLOAT24`, `FLOAT53`, `DOUBLE` | `Bool`, `Float32`, `Float64` | Numeric/boolean coercion; PotentiallyLossy for `Float32` | Invalid `Float32` values append null. |
| `DECIMAL`, `NUMERIC`, `NUMBER`, `MONEY`, `SMALLMONEY` | `Decimal128`; fixed money scales | Decimal coercion; PotentiallyLossy beyond precision 38 | Invalid decimal appends null. |
| `DATE`; `DATETIME`, `DATETIME2`, `SMALLDATETIME`, `DATETIMEOFFSET`; `TIME` | `Date32`, timestamp microseconds, `Time64us` | Temporal conversion; PotentiallyLossy | Offset timestamps are normalized to UTC only when runtime conversion yields a time. |
| `BINARY`, `VARBINARY`, `IMAGE`, `ROWVERSION`, `TIMESTAMP` | `Binary` | Binary preservation; `TIMESTAMP` is treated as binary, not temporal | Exact for bytes; strings become byte content. |
| `UNIQUEIDENTIFIER`, character types, `XML`, `JSON`, `SQL_VARIANT`, `HIERARCHYID`, geography/geometry | `String` | SemanticFallback | Logical structure is lost. |
| Other documented/user-defined types | Generic planner, normally `String` | UnsupportedFallback | |

## Oracle

Migration status: Oracle now uses `LogicalTypeForOracleColumn -> PlanForLogicalType -> typesystem.Convert`; the table below is retained as the legacy baseline. It preserves `NUMBER` metadata without inventing `decimal(38,10)`, rejects invalid/overflow values, and never applies ClickHouse temporal clamping.

Planner: `internal/arrowio/oracle_to_arrow.go:planOracleColumn`. It removes `DB_TYPE_` before matching.

Current migrated mappings: Oracle `DATE -> timestamp`; `TIMESTAMP WITH TIME ZONE` and `TIMESTAMP WITH LOCAL TIME ZONE -> timestamp_tz[UTC]`. LOCAL TIME ZONE values are normalized by Oracle/session semantics before driver materialization; ORabbit preserves the resulting instant in UTC and cannot reconstruct the inserted timezone. `RAW`/`LONG RAW`/`BLOB -> binary`. `BFILE`, `ROWID`, `UROWID`, `XMLTYPE`, intervals, and custom/unknown types -> `unknown ->` lossless string fallback.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `NUMBER(p,0)` p<=4, <=9, <=18, <=38; other `NUMBER`; `FLOAT`, `BINARY_FLOAT`, `BINARY_DOUBLE`, `DOUBLE` | `Int16`, `Int32`, `Int64`, `Decimal128(p,0)`; decimal `(38,10)` when metadata absent; `Float32`/`Float64` | Exact for bounded integral ranges; PotentiallyLossy for unspecified number, precision cap, or float | Precision >38 returns `String`. |
| `DATE`; timestamp names; `WITH [LOCAL] TIME ZONE` and `*TZ` | timestamp microseconds; UTC-marked for time-zone forms | Oracle `DATE` becomes a timestamp, not Arrow date; PotentiallyLossy | Invalid runtime values append null. |
| `RAW`, `LONG RAW`, `BLOB`, `BFILE` | `Binary` | Binary representation; Exact for bytes | |
| `VARCHAR`, `VARCHAR2`, `NVARCHAR2`, `CHAR`, `NCHAR`, `CLOB`, `NCLOB`, `LONG`, `ROWID`, `UROWID`, `XMLTYPE`, intervals | `String` | SemanticFallback for XML, row IDs and intervals | |
| Oracle supplied, user-defined, spatial/media/any types and other unknown names | Generic planner, normally `String` | UnsupportedFallback | |

## ClickHouse

Migration status: ClickHouse now uses `LogicalTypeForClickHouseColumn -> PlanForLogicalType -> typesystem.Convert`; the table below is retained as the legacy baseline. Nested `Nullable` and `LowCardinality` wrappers are parsed semantically, arrays preserve nullable elements, and runtime conversion is strict. The migrated path removes ClickHouse's former 1900–2299 date/timestamp clamping.

Planner: `internal/arrowio/clickhouse_to_arrow.go:planClickHouseColumn`. It unwraps outer `Nullable(...)` and `LowCardinality(...)` but schema nullability remains from source metadata rather than the type wrapper.

Current migrated mappings: `UInt64` is logical `uint64` with string storage fallback; `Decimal256(s)` is logical `decimal(76,s)` with fallback. DateTime timezone arguments are preserved: UTC aliases resolve to native timestamptz, non-UTC zones resolve to string fallback. UUID/JSON are semantic fallbacks. ClickHouse `Time`/`Time64` are unknown fallback because they can be duration-like rather than canonical time-of-day. IPs, enums, tuples/maps/nested values, dynamic/variant/object values, wide integers, geo, aggregate/special, and unknown types are `unknown` lossless-string fallback.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `UInt8`..`UInt64`, `Int8`..`Int64`; `Float32`, `BFloat16`, `Float64`; `Bool`, `Boolean` | Corresponding Arrow integer/float/bool | Broad runtime coercers; `BFloat16` -> `Float32` is SafePromotion | All narrow integer casts are unchecked. |
| `Decimal`, `Decimal32`, `Decimal64`, `Decimal128` | `Decimal128` precision 38 max | Decimal coercion; PotentiallyLossy if source capacity/scale does not fit | |
| `Date`, `Date32`, `DateTime`, `DateTime64`, `Time`, `Time64` | `Date32`, timestamp microseconds, `Time64us` | Temporal conversion; PotentiallyLossy (microsecond target and explicit 1900–2299 clamp) | Invalid runtime values append null. |
| `Array(T)` | `List<T>` | Recursive planner + slice/PG-array/JSON-array parser; SafePromotion | Invalid non-slice value appends a null list. |
| `UUID`, IPs, strings, enums, `JSON`, `Tuple`, `Map`, `Dynamic`, `Variant`, 128/256-bit ints, geo values | `String` | SemanticFallback | This includes source numeric types larger than Arrow mappings offered here. |
| `Nested`, aggregate-function, special and unknown types | Generic planner, normally `String` | UnsupportedFallback | |

## Trino

Migration status: Trino now uses `LogicalTypeForTrinoColumn -> PlanForLogicalType -> typesystem.Convert`. Decimal metadata is retained without a precision-38 invention; UUID/JSON are semantic fallback, arrays resolve recursively, and time-with-time-zone, ROW/MAP/IP/interval, and unknown plugin types are unknown fallback.

Planner: `internal/arrowio/trino_to_arrow.go:planTrinoColumn`.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `BOOLEAN`, `BOOL`; `TINYINT`, `SMALLINT`, `INTEGER`, `INT`, `BIGINT`; `REAL`, `DOUBLE`, `FLOAT` | Corresponding signed integer/float/bool | Broad runtime coercers; Exact by planned type | Narrow integer casts are unchecked. |
| `DECIMAL`, `NUMERIC`, `NUMBER` | `Decimal128` | PotentiallyLossy beyond precision 38 | |
| `DATE`; `TIMESTAMP*`; `TIME*` | `Date32`, timestamp microseconds (UTC when name says zone/TZ), `Time64us` | PotentiallyLossy | |
| `ARRAY(T)`; `VARBINARY` | `List<T>`; `Binary` | Recursive list or byte conversion | Invalid list input appends null. |
| `UUID`, character types, `JSON`, `IPADDRESS`, `ROW`, `MAP`, interval | `String` | SemanticFallback | Trino structural types are documented but no structural Arrow mapping exists except arrays. |
| Other built-in/plugin types | Generic planner, normally `String` | UnsupportedFallback | Connector-dependent source vocabulary is not statically determinable. |

## Cassandra

Migration status: Cassandra now uses `LogicalTypeForCassandraColumn -> PlanForLogicalType -> typesystem.Convert`. `varint`, unconstrained decimal, collections/UDTs, duration, inet, and unknown types use unknown lossless fallback. The connector preserves blobs as bytes, emits `uint64` as exact decimal text, UUIDs canonically, and uses conservative lossless serialization rather than `fmt.Sprintf`.

Planner: `internal/arrowio/cassandra_to_arrow.go:planCassandraColumn`. Actual Cassandra scan values first pass through `internal/connectors/cassandra.go:cassandraToDriverValue`.

| Source types / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| `tinyint`, `smallint`, `int`, `integer`, `bigint`, `varint`, `counter` | `Int8`, `Int16`, `Int32`, `Int64` | `cassandraToDriverValue` widens several integers to `int64`; `varint` is mapped to `Int64`; PotentiallyLossy | `uint64` is directly cast to `int64` before Arrow conversion. |
| `float`, `double`, `decimal` | `Float32`, `Float64`, `Decimal128` (default scale 10) | Numeric conversion; PotentiallyLossy for decimal capacity/default scale | |
| `boolean`; `date`, `time`, `timestamp`; `blob` | `Bool`; `Date32`, `Time64us`, UTC timestamp; `Binary` | Standard converter paths | Invalid temporal/decimal values append null. |
| `uuid`, `timeuuid`, character types, `inet`, `list`, `set`, `map`, `tuple`, `udt`, `frozen` | `String` | SemanticFallback | CQL collection/UDT structure is stringified by the connector if it is not a `driver.Value`. |
| `duration`, custom types and other unknown CQL types | Generic planner, normally `String` | UnsupportedFallback | |

## SQLite

Migration status: SQLite now uses `LogicalTypeForSQLiteColumn -> PlanForLogicalType -> typesystem.Convert`. Explicit declarations map conservatively; dynamic SQLite values must still satisfy their declared logical type. Unknown/empty declarations and unconstrained numeric use unknown fallback rather than loose affinity or substring inference.

Planner: `internal/arrowio/sqlite_to_arrow.go:planSQLiteColumn`. SQLite’s dynamic storage classes mean the type name alone cannot establish the runtime value kind.

| Source type / aliases recognized | Arrow type | Value conversion / classification | Fallback and notes |
| --- | --- | --- | --- |
| Generic integer names recognized by `internal/connectors/source.go:ClassifySQLIntegerType`, including `INTn`/`UINTn` tokens | Width-selected integer | Direct coercion; PotentiallyLossy for unchecked narrowing | Source type affinity is not otherwise interpreted. |
| `BIT`, `BOOL`, `BOOLEAN`; `REAL`, float names; decimal/number/money names; `DATE`, `TIME`, date/time/timestamp names; binary names | Bool, float, decimal/int64, date/time/timestamp, binary | Generic conversion rules | Decimal `(p,0)` with p<=18 becomes `Int64`. |
| All other declared names and unrecognized affinity names | `String` | UnsupportedFallback | This includes SQLite’s flexible type declarations. |

## MongoDB

Mongo has no declared-column planner. `internal/arrowio/mongo_to_arrow.go:InferMongoSchemaWithFieldOrder` assigns a field from its first non-null observed Go/BSON value; fields that are only null become `String`. `MongoDocsToRecord` then uses that fixed Arrow schema for every buffered document.

| BSON/Go value recognized | Arrow type | Runtime conversion / classification | Later incompatible value |
| --- | --- | --- | --- |
| `string`; `ObjectID` | `String` | Native string Exact; ObjectID -> 24-character hex SemanticFallback | String plan formats a non-string with `mongoValueToString`. |
| `int32`, `int64`, `int` | `Int64` | SafePromotion for `int32`; `mongoToInt64` otherwise returns 0 | Any unexpected later value becomes **0** without an error. |
| `float64`; `bool` | `Float64`; `Bool` | Exact for same Go type | Any incompatible non-null value becomes Arrow null. |
| `time.Time`, `primitive.DateTime`, `primitive.Timestamp` | UTC `Timestamp(ms)` | Timestamp uses milliseconds; BSON timestamp uses seconds field `T` and discards increment `I`; PotentiallyLossy | Incompatible values become null. |
| `primitive.Decimal128` | `Decimal128(38,18)` | Decimal string parsing; PotentiallyLossy | Parse/type mismatch becomes null. |
| `primitive.Binary`, `[]byte` | `Binary` | Exact for matching binary kind | Incompatible values become null. |
| document, array, regex, JavaScript, code-with-scope, min/max/undefined/symbol and all other values | `String` | Extended JSON / text formatter; SemanticFallback | Type information is not represented in Arrow schema. |

## String Fallbacks

Warnings: none of the following paths log or emit a conversion warning.

| File / function | Non-native-string input | Result / recoverability |
| --- | --- | --- |
| `internal/arrowio/type_plan.go:asSafeString` | `[]byte`, `[16]byte`, `time.Time`, `fmt.Stringer`, map/slice/struct, other values | Bytes become a Go string (16 bytes are UUID-formatted); time/`Stringer` are textual; maps/slices/structs use JSON when possible, otherwise `fmt.Sprint`. Original logical type is lost; valid byte payload can be reconstructed from Go string bytes, but the Arrow field is text. |
| `internal/arrowio/type_plan.go:planBinary` | `string`, all non-byte values | String is converted with `[]byte`; other values use `fmt.Sprint` bytes. Text can be recovered, but original binary/logical semantics are lost. |
| `internal/arrowio/mongo_to_arrow.go:mongoValueToArrowType` and `mongoBuilderFromArrowType` | `ObjectID`, unmatched inferred-string values | ObjectID hex or `fmt.Sprint`; logical BSON type is lost. |
| `internal/arrowio/mongo_to_arrow.go:mongoValueToString` | BSON document/array, regex, JavaScript, code-with-scope, min/max/undefined/symbol, default values | Extended JSON when marshaling succeeds; otherwise custom JSON/text/`fmt.Sprint`. Extended JSON preserves more BSON information than ordinary text, but Arrow schema remains `String`; fallback `fmt.Sprint` is not a reliable reversible encoding. |
| `internal/connectors/cassandra.go:cassandraToDriverValue` | `[]byte`, `gocql.UUID`, unsupported non-`driver.Value` | Bytes -> string, UUID -> `String()`, other values -> `fmt.Sprintf`. Used before Arrow planning; semantic type information is lost. |
| `internal/connectors/source.go:asStringValue` | bytes, `fmt.Stringer`, other non-nil cursor values | Trimmed string / `String()` / `fmt.Sprint` for cursor persistence. Reversibility and source type preservation are not guaranteed. |
| `internal/connectors/mssql_cdc.go:MSSQLCDC.pollChanges` | CDC row `[]byte` values | Bytes become a Go string before `CDCRecord` is JSON-marshaled. This treats opaque binary as text and has no warning. |
| `internal/connectors/mongodb.go:MongoDB.DiscoverCursorStats` | Mongo cursor min/max values | Defaults to `fmt.Sprint`, with explicit ObjectID/Decimal/date textual forms. It persists comparison bounds as text; reverse interpretation is domain-dependent. |
| `internal/connectors/s3.go:parseScalar` | XML text that is not a parseable `int64` or `float64` | Returns the trimmed Go string. This is parser fallback rather than an Arrow conversion; the original XML type annotation is not preserved. |

Native `string` source values are not fallbacks: `planString` and Mongo’s `string` branch append them directly.

## Implicit NULL Fallbacks

Normal `nil` input is excluded. These non-null inputs append Arrow null and return no error.

| File / converter | Target Arrow type | Non-null condition | Tested |
| --- | --- | --- | --- |
| `internal/arrowio/type_plan.go:planInt8`, `planInt16`, `planInt32` | narrow signed integer | `asInt64` rejects input (for example invalid numeric text or oversized `uint64`) | Added in `TestCurrentImplicitNullFallbacks`. |
| `internal/arrowio/type_plan.go:planUint8`, `planUint16`, `planUint32` | narrow unsigned integer | `asUint64` rejects input (for example negative integer or invalid text) | Added in `TestCurrentImplicitNullFallbacks`. |
| `internal/arrowio/type_plan.go:planFloat32` | `Float32` | `asFloat64` rejects input | Added in `TestCurrentImplicitNullFallbacks`. |
| `internal/arrowio/type_plan.go:planDate32`, `planTime64`, `planTimestampUs`, `planDecimal128`, `planList` | date, time, timestamp, decimal, list | Helper rejects input / list input is not a slice-like representation | Added in `TestCurrentImplicitNullFallbacks`. |
| `internal/arrowio/mongo_to_arrow.go:mongoValueToArrowType` / `mongoBuilderFromArrowType` | inferred `Float64`, `Bool`, timestamp, decimal, binary | Later document value does not have the inferred compatible type, or decimal cannot parse | Added in `TestCurrentMongoIncompatibleValuesBecomeNull`. |

`planInt64`, `planUint64`, `planFloat64`, and `planBool` instead return an error for values their helper rejects.

## Default-Zero Conversions

| File / function | Condition | Current result | Tested |
| --- | --- | --- | --- |
| `internal/arrowio/mongo_to_arrow.go:mongoToInt64` | Value is not `int32`, `int64`, or `int` after an `Int64` Mongo schema is inferred | `0` is appended, no error | Added in `TestCurrentMongoUnexpectedIntegerValueBecomesZero`. |
| `internal/arrowio/type_plan.go:asTime64Microseconds` | Colon-separated text has non-numeric hour/minute/second/fraction components | Parse errors are ignored; parsed components default to zero and helper reports success | Added in `TestCurrentTimeTextParseErrorsBecomeZeroComponents`. |
| `internal/arrowio/sql_to_arrow.go:parseTruthyText` via `asBool` | Any unrecognized non-empty string or multi-byte `[]byte` boolean text | `false`, with `ok=true`, rather than error/null | Added in `TestCurrentBooleanTextFallbackIsFalse`. |

Several numeric helpers return `(0,false)` on failure (`asInt64`, `asUint64`, parsing helpers), but their callers either append null or return an error; they are not value-level default-zero outputs.

## Unchecked Integer Casts

| File / function | Source domain -> target | Check before cast | Risk / tested |
| --- | --- | --- | --- |
| `internal/arrowio/type_plan.go:planInt8`, `planInt16`, `planInt32` | accepted `int64` -> narrow signed | No target-range check | Wrap/truncate. Tested in `TestCurrentUncheckedNarrowIntegerCastsWrap`. |
| `internal/arrowio/type_plan.go:planUint8`, `planUint16`, `planUint32` | accepted `uint64` -> narrow unsigned | No target-range check | Wrap/truncate. Tested in `TestCurrentUncheckedNarrowIntegerCastsWrap`. |
| `internal/arrowio/sql_to_arrow.go:asInt64` | `uint` -> `int64` | No `MaxInt64` check (unlike `uint64`) | On 64-bit platforms, high values can change sign; not separately tested because `uint` width is architecture-dependent. |
| `internal/connectors/source.go:asInt64Value` | `uint` -> `int64` | No `MaxInt64` check | Same architecture-dependent sign risk; used for cursor encoding. |
| `internal/connectors/cassandra.go:cassandraToDriverValue` | `uint64` -> `int64` | No check | Values above `MaxInt64` change sign before Arrow conversion; not unit-tested directly. |

No float-to-integer cast is implemented by `asInt64`/`asUint64`: floats are rejected. Numeric strings are parsed to 64-bit first, then unchecked only when a narrow target plan casts them.

## Existing Safety Gaps

- Silent-null behavior is target-dependent and inconsistent with error-returning `Int64`, `UInt64`, `Float64`, and `Bool` plans.
- Narrow integer plans do not verify range before direct Go casts.
- Mongo schema inference uses the first non-null document value and later integer incompatibility becomes zero instead of null/error.
- BSON document/collection/special values and many database logical types are represented as `String` without warnings.
- Time parsing ignores component parse errors once colon structure is present; boolean parsing maps arbitrary text to false.
- Planner recognition is based on driver-reported type strings. Driver spelling/value behavior for all database-specific types is **Not determined from code**.

## Regression Tests Added

- `internal/arrowio/type_plan_safety_test.go`: current generic/dialect mappings, target override fallback, string fallbacks, implicit nulls, default boolean/time behavior, and narrow-cast wrapping.
- `internal/arrowio/mongo_to_arrow_test.go`: schema inference and later incompatible Mongo value behavior (null versus integer zero).
