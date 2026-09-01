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
