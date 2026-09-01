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
