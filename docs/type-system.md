# ORabbit Logical Type System

`internal/typesystem.LogicalType` is ORabbit's canonical, source-independent description of type semantics. It gives future source mappers, Arrow mappers, Iceberg mappers, validation, logs, and APIs one vocabulary without requiring any of them to import each other.

It represents only logical kinds and their structure: nullability, decimal precision/scale, timezone-aware timestamps, array elements, map key/value types, and ordered struct fields. `SourceTypeName` is optional provenance metadata and is not semantic equality.

It intentionally does not represent database spellings (`VARCHAR`, `JSONB`, `NUMBER`, `DATETIME64`), physical Arrow/Iceberg types, database driver values, conversion rules, fallback warnings, or source-engine behavior. Existing planners and value conversion remain unchanged in Milestone 2.

Canonical kinds are: `unknown`, `string`, `bool`, signed and unsigned integers from 8 to 64 bits, `float32`, `float64`, `decimal`, `date`, `time`, `timestamp`, `timestamp_tz`, `uuid`, `binary`, `json`, `array`, `struct`, and `map`.

The future fallback contract is:

```text
recognized + natively supported -> typed conversion
recognized but unsupported in Arrow/Iceberg -> lossless string fallback + warning
unrecognized -> lossless string fallback + warning
```

Implementing that contract is explicitly outside Milestone 2. The package deliberately has no Arrow, Iceberg, or database-driver imports.
