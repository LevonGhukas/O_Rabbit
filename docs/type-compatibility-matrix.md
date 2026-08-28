# Type compatibility matrix

This is a test-oriented record of the Phase 5 compatibility work. It applies
only to the versions below and must not be read as a compatibility promise for
other server or library versions.

## Tested software

| Component | Version |
|---|---|
| Arrow Go / Parquet (`pqarrow`) | `v18.5.2-0.20260220015023-a886a5722b87` |
| Iceberg Go | `v0.5.0` |
| Altinity ClickHouse, dedicated DataLakeCatalog compose | `25.3.8.10042.altinitystable` |
| Altinity ClickHouse, isolated-stack test | `25.8.9.20207.altinityantalya` |

The Arrow/Parquet and Arrow-to-Iceberg results below are covered by
`internal/parquetio/compatibility_matrix_test.go` and
`internal/icebergreg/compatibility_matrix_test.go`. The latter calls
`icetable.ArrowSchemaToIcebergWithFreshIDs`, the production conversion path.

| Canonical representation | Arrow | Parquet | Iceberg v0.5.0 | ClickHouse | Status | Notes |
|---|---|---|---|---|---|---|
| Boolean | `bool` | exact round-trip | `boolean` | unverified | Supported to Iceberg | Nullable schema/value tested. |
| Int8 / Int16 / Int32 | native widths | exact extrema round-trip | `int` | unverified | Supported to Iceberg | Iceberg intentionally widens narrow signed values. |
| Int64 | `int64` | exact extrema round-trip | `long` | unverified | Supported to Iceberg | |
| UInt8 / UInt16 | native widths | exact extrema round-trip | `int` | unverified | Requires fallback | Iceberg has no unsigned type; values above signed `int32` range are unsafe downstream. |
| UInt32 | `uint32` | exact max round-trip | `int` | unverified | Requires fallback | `4294967295` cannot be represented by Iceberg `int`. |
| UInt64 | `uint64` | exact `18446744073709551615` round-trip | `long` | unverified | Requires fallback | Full UInt64 cannot be represented by signed Iceberg `long`; no planner change has been made before end-to-end registration/query proof. |
| Float32 / Float64 | native | exact finite-value round-trip | `float` / `double` | unverified | Supported to Iceberg | NaN/Inf remain an integration follow-up. |
| String and text fallback codecs | `utf8` | exact text round-trip | `string` | unverified | Supported to Iceberg | Covers canonical-decimal, PostgreSQL UUID/array/bit/range, MySQL time/bit, Mongo ObjectId/Extended JSON/Decimal128 text. |
| Binary / uniform Mongo Binary | `binary` | exact bytes, including NUL and invalid UTF-8 | `binary` | unverified | Supported to Iceberg | ClickHouse byte equality still needs a DataLakeCatalog query. |
| Decimal128(10,2), Decimal128(38,0) | native decimal | exact negative/boundary round-trip | matching decimal | unverified | Supported to Iceberg | ClickHouse decimal exposure remains unverified. |
| Date32 | `date32` | pre-1900/post-2299 round-trip | `date` | unverified | Supported to Iceberg | No clamping is introduced. |
| Timestamp(us) | `timestamp[us]` | exact microsecond round-trip | `timestamp` | unverified | Supported to Iceberg | Empty Arrow timezone maps to Iceberg local timestamp. |
| MongoDB Date / Timestamp(ms) | `timestamp[ms, UTC]` | exact millisecond round-trip | `timestamptz` | unverified | Supported to Iceberg | UTC source instant semantics retained by schema conversion. |
| Time64(us) | `time64[us]` | exact time-of-day round-trip | `time` | unverified | Supported to Iceberg | |
| PostgreSQL primitive List | `list<int32>` | null, empty, and null-element round-trip | `list<int>` | unverified | Supported to Iceberg | List element field label is normalized by Parquet, but type/nullability and values survive. |

## Integration command and current limitation

The opt-in stack isolates MinIO, Iceberg REST, and ClickHouse from the
master/worker compose file:

```sh
docker compose -f docker-compose.type-compatibility.yml up -d
ORABBIT_RUN_INTEGRATION=1 go test -tags=integration ./internal/icebergreg \
  -run TestTypeCompatibilityRESTAndClickHouse -v
```

The test uploads each real Writer output to MinIO, calls the production
`runRESTGoRegister` path, and reloads the table through the REST catalog. All
registration and schema reload cases passed against REST catalog `0.2.0`.

ClickHouse `DESCRIBE` and value queries were attempted against Altinity
`25.8.9.20207.altinityantalya`. They fail before a read with:

```text
Table cannot have empty namespace: compat_<table> (BAD_ARGUMENTS)
```

The catalog resolves a registered `compat.<table>` identifier to a table with
an empty namespace, so no ClickHouse types or values can yet be recorded. This
is an actual unsupported downstream result for the checked configuration, not
an unknown cell. The dedicated `25.3.8.10042.altinitystable` compose remains
unrun.

When namespace resolution is fixed, the required verification is:

```sh
docker compose exec clickhouse clickhouse-client --query 'DESCRIBE ice.<namespace>.<table>'
docker compose exec clickhouse clickhouse-client --query 'SELECT * FROM ice.<namespace>.<table>'
```

Phase 5 is still in progress. In particular, the unsigned mappings must not
be declared safe: the production Iceberg converter maps unsigned Arrow types
to signed Iceberg types, and ClickHouse cannot currently read the registered
tables to establish their final behavior.
