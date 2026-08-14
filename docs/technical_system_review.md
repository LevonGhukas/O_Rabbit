# O_Rabbit Technical System Review

This document describes the implementation currently present in this repository as inspected on 2026-05-10. It is intended as source material for a formal capstone report. It is implementation-focused: it explains what the code already does, where that behavior lives, and which parts are currently absent or limited.

## Scope And Method

The review is based on the current runtime codepaths and deployment assets in:

- `cmd/master`, `cmd/worker`, `cmd/orabbit-client`
- `internal/planner`
- `internal/connectors`
- `internal/arrowio`
- `internal/parquetio`
- `internal/db`
- `internal/grpc`
- `internal/http`
- `internal/icebergreg`
- `internal/orabbitcli`
- `internal/ops`
- `proto/controlplane.proto`
- deployment files such as `docker-compose*.yml`, `Dockerfile.orabbit`, `.env.*.example`, `deploy-*.sh`, `.ice.yaml`, and `ice-rest-catalog.yaml`

No production code was changed for this review.

## 1. High-Level System Purpose

### What problem O_Rabbit solves

O_Rabbit is a distributed batch-ingestion system for exporting data from external databases into a lakehouse-oriented object storage layout. Its core purpose is to make large database exports operationally manageable by splitting work into explicit runs and tasks, executing extraction on workers, writing Parquet, and registering the resulting objects as Iceberg tables.

The core pipeline implemented in the repository is:

`source database -> master planning -> worker extraction -> Parquet -> S3/MinIO -> Iceberg registration -> external query engines`

### What kind of system it is

O_Rabbit is:

- a control plane plus worker data plane for batch exports
- a distributed extractor for SQL and SQL-like sources
- a Parquet file producer
- a run/task state tracker backed by SQLite
- an Iceberg registration orchestrator
- a CLI-driven operational tool

### What kind of system it is not

O_Rabbit is not:

- a full CDC platform
- a streaming ingestion engine
- a source-side replication system
- a transformation engine or SQL ELT framework
- a query engine itself
- a general-purpose workflow scheduler like Airflow

### Main user workflow

The implemented user workflow is:

1. The user creates or updates source and target connections through the HTTP API or `orabbit-client`.
2. The user creates or updates a job describing the source table/query, target information, and planning options.
3. The user starts a run for that job.
4. The master validates the job, validates the ordered cursor when applicable, loads `_state.json`, discovers source stats, and creates run tasks.
5. Workers poll the master for tasks over gRPC.
6. Workers read source rows, convert them into Arrow record batches, write Parquet files, and upload them to S3-compatible storage.
7. Workers report progress and final task results back to the master.
8. When all tasks succeed, the master writes a commit manifest and updates dataset state in object storage.
9. If Iceberg registration was enabled for the run, the master registers all committed Parquet objects into an Iceberg table.
10. Users query the registered table through external tools such as Altinity Ice and ClickHouse that are deployed alongside the data lake stack, not inside O_Rabbit itself.

## 2. Architecture Overview

### Runtime roles

| Runtime | Primary responsibilities | Main implementation |
| --- | --- | --- |
| `orabbit-master` binary | Persistence, planning, gRPC control plane, HTTP API, event streaming, commit, Iceberg registration | `cmd/master/main.go`, `internal/grpc/server.go`, `internal/http/server.go`, `internal/planner/planner.go` |
| `orabbit-worker` binary | Polling for tasks, source extraction, Arrow conversion, Parquet writing, S3 upload, result reporting | `cmd/worker/main.go`, `cmd/worker/clients.go`, `cmd/worker/parquet_rolling.go` |
| `orabbit-client` binary | User-facing CLI, interactive configuration, run submission, run watch, local daemon supervision, registration snapshot creation | `cmd/orabbit-client/main.go`, `internal/orabbitcli/*` |
| SQLite store | Durable control-plane state | `internal/db/store.go`, `internal/db/migrate.go` |
| Object storage | Parquet objects, commit metadata, dataset state, Iceberg registration state | `internal/s3io/uploader.go`, commit/state code in `internal/grpc/server.go` and `internal/icebergreg/manager.go` |
| Iceberg registration layer | Table creation or loading, file registration, registration checkpoints | `internal/icebergreg/*` |

### Mermaid architecture diagram

```mermaid
flowchart LR
    U[User / Operator]
    C[orabbit-client]
    H[HTTP API + SSE on master]
    G[gRPC control plane on master]
    M[orabbit-master]
    DB[(SQLite control DB)]
    W1[orabbit-worker]
    W2[orabbit-worker]
    SRC[(Source DB)]
    S3[(S3 / MinIO)]
    ICE[Iceberg REST Catalog / Altinity Ice]
    Q[ClickHouse / external query client]

    U --> C
    C --> H
    H --> M
    G --> M
    M --> DB
    W1 --> G
    W2 --> G
    W1 --> SRC
    W2 --> SRC
    W1 --> S3
    W2 --> S3
    M --> S3
    M --> ICE
    Q --> ICE
    Q --> S3
```

### End-to-end sequence diagram

```mermaid
sequenceDiagram
    participant User
    participant CLI as orabbit-client
    participant Master as orabbit-master
    participant Worker as orabbit-worker
    participant Source as Source DB
    participant S3 as MinIO/S3
    participant Ice as Iceberg catalog

    User->>CLI: run interactive / run submit
    CLI->>Master: HTTP create/update connections and job
    CLI->>Master: HTTP POST /jobs/{id}/runs
    Master->>Master: CreateRunAndTasks
    Master->>S3: read <prefix>/_state.json
    Master->>Source: validate cursor and discover stats
    Master->>Master: insert run + tasks
    Worker->>Master: RegisterWorker + Heartbeat + RequestTask
    Master-->>Worker: TaskAssignment
    Worker->>Source: QueryCursor / StreamQuery
    Worker->>Worker: SQL -> Arrow -> Parquet
    Worker->>S3: upload parquet object(s)
    Worker->>Master: ReportTaskProgress / ReportTaskResult
    Master->>Master: CompleteTask + TryFinalizeRun
    Master->>S3: write _commits/run-<id>.json
    Master->>S3: write _state.json
    Master->>Ice: register committed objects
    CLI->>Master: SSE watch stream
    Master-->>CLI: run/task/commit/registration events
```

## 3. Main Components And Code Structure

The repository uses binary entrypoints in `cmd/*` and reusable implementation packages under `internal/*`.

### Entry points

| Path | What it does | Key functions |
| --- | --- | --- |
| `cmd/master` | Builds the `orabbit-master` daemon | `main` in `cmd/master/main.go`, config parsing in `cmd/master/config.go` |
| `cmd/worker` | Builds the `orabbit-worker` daemon | `main`, `executeTask`, `extractSQLCursorTask`, `extractFlightSQLTask` in `cmd/worker/main.go` |
| `cmd/orabbit-client` | Builds the user CLI | `main` in `cmd/orabbit-client/main.go`, delegates to `orabbitcli.Main` |

Important naming note: the repository directories are `cmd/master` and `cmd/worker`, but the built binaries are named `orabbit-master` and `orabbit-worker`.

### Package-level structure

| Path | Role in pipeline | Key files / functions | Important design choices |
| --- | --- | --- | --- |
| `internal/planner` | Creates runs and tasks | `CreateRunAndTasks`, `autoTuneCursorPlanWithDecision`, `buildOrderedCursorRangeTasks` | Planning is centralized on the master and is state-aware through `_state.json` |
| `internal/connectors` | Source-engine abstraction | `source.go`, `postgres.go`, `mssql.go`, `clickhouse.go`, `oracle.go`, `flightsql.go` | SQL sources share a `TableReader` contract; FlightSQL is separate and Arrow-native |
| `internal/arrowio` | Converts SQL rows to Arrow records | `PlansFromSQL`, `RowsToRecordBatches` | Conservative type mapping prioritizes robustness over perfect fidelity |
| `internal/parquetio` | Writes Parquet temp files | `NewTempFileWriter`, `Writer.Write`, `Writer.Close` | Simple temp-file writer with Snappy compression |
| `internal/s3io` | S3/MinIO access and uploads | `New`, `UploadFileMultipart`, `GetObjectBytes`, `PutObjectBytes`, `ListKeys` | Uses AWS SDK v2 and supports multipart upload plus collision checks |
| `internal/db` | Control-plane persistence | `Open`, `CreateRun`, `AssignNextPendingTask`, `CompleteTask`, `TryFinalizeRun`, migrations | SQLite with WAL mode, one writer connection, busy retries |
| `internal/grpc` | Worker control plane | `RegisterWorker`, `RequestTask`, `ReportTaskProgress`, `ReportTaskResult`, `commitRun` | Pull-based task assignment; task results can already contain multiple object keys |
| `internal/http` | REST API, SSE, and remote operations backend | `Server.Handler`, `handleJobByID`, `handleRunEventsStream`, `handleDeployments`, `handleServers` | API covers both ingestion operations and server/deployment management |
| `internal/icebergreg` | Iceberg registration | `Manager.RegisterRun`, `prepareRESTGoTable`, `runRESTGoRegister`, `runIceCLIRegister` | Run-specific registration config snapshot avoids drift between submission time and completion time |
| `internal/orabbitcli` | CLI workflow implementation | `Main`, `cmdRunInteractive`, `promptRunConfig`, `buildIcebergRegistrationSnapshot`, `streamSSE` | CLI is the primary user entrypoint; interactive and file-based flows share config-building logic |
| `internal/ops` | Control-panel backend helpers | SSH, Docker, deployment, and config-editing services | Remote ops are backend-only in this repo; no browser frontend assets were found |
| `proto` | gRPC schema | `proto/controlplane.proto` | Task assignment carries target file hints and repeated result object keys |

### Frontend / control panel code status

No standalone browser frontend source tree was found in this repository. There is, however, a control-panel backend API:

- remote server inventory and SSH credentials in `internal/db/controlpanel.go`
- HTTP endpoints in `internal/http/servers.go`, `internal/http/deployments.go`, `internal/http/containers.go`, and `internal/http/configs.go`
- SSH/Docker/deploy/config services in `internal/ops/*`

This means the repository contains control-panel backend capabilities, but not a bundled web frontend implementation.

### Docs and example configs

Important non-code assets include:

- `README.md`
- `.ice.yaml`
- `ice-rest-catalog.yaml`
- `.env.master.example`, `.env.worker.example`, `.env.minio.example`, `.env.worker.on.master.example`
- `docker-compose.yaml`
- `docker-compose.master.yml`
- `docker-compose.worker.yml`
- `docker-compose.minio.yml`
- `docker-compose.postgres.yml`
- `docker-compose.ice-rest-catalog.yml`
- `deploy-master.sh`, `deploy-worker.sh`, `deploy-minio.sh`
- `docs/dataplane.drawio`

## 4. Job / Run / Task Model

### Persistent entities

The control-plane model is stored in SQLite and defined in `internal/db/store.go` and `internal/db/migrate.go`.

| Entity | Meaning | Main fields | Storage table |
| --- | --- | --- | --- |
| Connection | Source or target connection definition | `kind`, `engine`, `metadata_json`, encrypted secret blob | `connections` |
| Job | Export definition | source/target connections, `source_sql`, `incremental`, `hwm_column`, `options_json` | `jobs` |
| Run | One execution of a job | `status`, `dataset_key`, `correlation_id`, `registration_config_json` | `runs` |
| Task | One schedulable work unit in a run | `partition_spec_json`, `worker_id`, status, row/byte counters, `parquet_objects_json` | `tasks` |
| Event | Time-ordered run/task event for replay and SSE | `ts`, `level`, `message`, `fields_json` | `events` |
| HWM | Best-effort SQLite copy of max committed cursor | `hwm_value` | `hwm` |
| Worker | Worker liveness and capabilities | `addr`, `last_heartbeat`, `capabilities_json` | `workers` |
| Worker Instance | Physical tracking of worker nodes authenticated via gRPC | `worker_boot_id`, `pid`, `hostname`, `version` | `worker_instances` |

### Job

A job is the durable export definition. In code it is `db.Job` in `internal/db/store.go`. A job stores:

- connection IDs
- source SQL for FlightSQL jobs
- target namespace/table metadata
- incremental flag
- HWM column metadata
- planner/worker options in `options_json`

Important current implementation detail:

- for ordered-cursor sources, workers do not execute `job.SourceSQL`; they execute connector-generated table queries based on `options.table`, cursor metadata, and partition bounds
- `target_namespace`, `target_table`, and `write_mode` are persisted as job metadata, but physical object layout is driven primarily by dataset prefix derivation from `options.table` plus target prefix configuration

### Run

A run is one execution of a job. It is created in `planner.CreateRunAndTasks` and stored through `db.CreateRun`. Important run-level fields are:

- `status`: `PLANNING`, `RUNNING`, `SUCCEEDED`, `FAILED`, or `CANCELED`
- `dataset_key`: a stable identity for physical dataset location, built by `dataset.StorageKey`
- `correlation_id`: run-scoped identifier propagated to workers
- `registration_config_json`: persisted Iceberg registration snapshot

### Task

A task is one planned partition of work. For SQL ordered-cursor jobs, tasks are created with partition specs such as:

- `single`
- `sql_cursor_single`
- `sql_cursor_range`
- legacy `sql_int_range` / `mssql_int_range` aliases still appear in compatibility paths

The worker does not infer work from the job directly. It executes the exact `partition_spec_json` received in `TaskAssignment`.

### Task states

Implemented task states are:

| State | Meaning | Main transitions |
| --- | --- | --- |
| `PENDING` | Planned, not assigned | created by planner |
| `RUNNING` | Assigned to a worker | set by `AssignNextPendingTask` |
| `SUCCEEDED` | Completed successfully | set by `CompleteTask` |
| `FAILED` | Worker or commit failure | set by `CompleteTask` or stale-run failure logic |
| `CANCELED` | Canceled before execution or while run was canceled | set by `CancelRun` or `CompleteTask` coercion |

### Task assignment

Task assignment is pull-based:

- worker calls `ControlPlane.RequestTask`
- master calls `db.AssignNextPendingTask`
- the SQL query in `AssignNextPendingTask` enforces global per-run `max_in_flight_tasks`
- only one task is returned per request

Important detail: `max_in_flight_tasks` is global per run, not per worker. Enforcement happens in `internal/db/store.go` inside `AssignNextPendingTask`.

### How workers report results

Workers report:

- periodic progress through `ReportTaskProgress`
- terminal results through `ReportTaskResult`

The result payload already supports multiple object keys via `repeated string parquet_object_keys` in `proto/controlplane.proto`.

The master stores result objects in `tasks.parquet_objects_json` using `buildParquetObjectPayloads` in `internal/grpc/server.go`.

### How the master commits a run

Run finalization is split into two phases:

1. `db.TryFinalizeRun` decides whether all tasks are done and whether the run should become `SUCCEEDED` or `FAILED`.
2. If the run becomes `SUCCEEDED`, `internal/grpc/server.go:commitRun` writes:
   - a commit manifest at `<prefix>/_commits/run-<run-id>.json`
   - dataset state at `<prefix>/_state.json`
   - an SSE/event timeline entry `run committed`

Only after commit succeeds does the master publish the `run SUCCEEDED` event to clients.

## 5. Source Database Support

### Support matrix

| Source engine | Main file | Ordered-cursor extraction | Range splitting | Query style |
| --- | --- | --- | --- | --- |
| PostgreSQL | `internal/connectors/postgres.go` | yes | yes for `int64`, `uint64`, `date`, `timestamp` domains | generated SQL with optional bounded predicates and `ORDER BY` |
| MSSQL | `internal/connectors/mssql.go` | yes | yes for `int64`, `uint64`, `date`, `timestamp` domains | generated SQL with `WITH (NOLOCK)` and optional bounded predicates |
| ClickHouse | `internal/connectors/clickhouse.go` | yes | yes for `int64`, `uint64`, `date`, `timestamp` domains | generated SQL with optional bounded predicates and always `ORDER BY` |
| Oracle | `internal/connectors/oracle.go` | yes | yes for integer/date/timestamp cursors accepted by Oracle classifier | generated SQL with `:1`, `:2` bind parameters and `ORDER BY` |
| FlightSQL | `internal/connectors/flightsql.go` | no ordered-cursor path | no | source SQL streamed as Arrow through ADBC |

### Per-source behavior

| Source | Table/schema handling | Cursor validation | Stats discovery | Query generation | Important limitations |
| --- | --- | --- | --- | --- | --- |
| PostgreSQL | `splitPostgresTableIdent` defaults one-part names to `public` | `ValidateCursorColumn` checks type, nullability, and index presence best-effort | `MIN/MAX`, `pg_class.reltuples`, `pg_total_relation_size` in `DiscoverCursorStats` | `QueryCursor` builds `WHERE` bounds and optional ordered scan | no snapshot coordination across tasks; index detection is best-effort |
| MSSQL | `splitMSSQLTableIdent` defaults one-part names to `dbo` | `ValidateCursorColumn` checks type, nullability, and index presence best-effort | bounds plus `sys.dm_db_partition_stats`; bytes from allocation units | `QueryCursor` builds `WHERE` bounds with `@p1` style binds and uses `WITH (NOLOCK)` | `NOLOCK` can trade correctness for concurrency |
| ClickHouse | one-part names resolve against current DB in some metadata paths | `ValidateCursorColumn` checks type, nullability, and whether sorting key mentions the column | bounds, `system.parts` rows and compressed bytes, fallback `count()` | `QueryCursor` always orders by cursor column | “indexed” means sort-key participation, not a transactional index in the OLTP sense |
| Oracle | one-part table names use current schema semantics; quoted/unquoted handling implemented in `parseOracleObjectIdent` and related helpers | `ValidateCursorColumn` checks metadata, nullability, index presence, and may run a NUMBER safety probe | bounds plus `USER_TABLES` / `ALL_TABLES` `NUM_ROWS` and `AVG_ROW_LEN` | `buildOracleCursorQuery` uses bind parameters and ordered scan | many Oracle type families are intentionally unsupported as cursor columns |
| FlightSQL | user supplies a query, not a table/cursor plan | no ordered-cursor validation path | none in planner | `StreamQuery` executes source SQL and streams Arrow records | no incremental support, no auto-tune planning path |

### How ordered-cursor support is decided

Ordered-cursor support is engine-level and registered in `internal/connectors/source.go`. The core checks are:

- `SupportsOrderedCursor(engine)`
- `ClassifySQLCursorType(typeName)`
- `SupportsCursorRangeSplit(domain)`

Implemented range-capable domains are:

- `int64`
- `uint64`
- `date`
- `timestamp`

Implemented orderable but non-range-capable domains include:

- `decimal`
- `string`
- `uuid`

For non-range-capable ordered cursor columns, the planner still allows incremental export, but it falls back to a single ordered scan task rather than splitting ranges.

## 6. Oracle Support

Oracle support is materially implemented, even though the current `README.md` source-engine list is stale and omits it.

### Driver and connection handling

- Driver: `github.com/sijms/go-ora/v2`
- Open path: `internal/connectors/oracle.go:OpenOracle`
- DSN example in CLI defaults: `oracle://user:password@localhost:1521/ORCLCDB`
- SID note in CLI prompt help: `internal/orabbitcli/config.go` says SID-based URLs can use a driver-supported form such as `oracle://user:password@host:1521/?SID=ORCL`

The chosen implementation is a pure Go `database/sql` path, which fits the rest of the connector layer and avoids introducing a separate CGO-only stack into the worker path.

### Oracle metadata lookup

Cursor metadata is looked up in:

- `lookupColumnMeta`
- `lookupCursorIndexedBestEffort`
- `lookupTableStats`

Specifically:

- column metadata comes from `ALL_TAB_COLUMNS` or `USER_TAB_COLUMNS`
- index presence comes from `ALL_IND_COLUMNS` + `ALL_INDEXES` or `USER_IND_COLUMNS` + `USER_INDEXES`
- row count and average row length come from `ALL_TABLES` or `USER_TABLES`

### Oracle NUMBER handling

Oracle cursor validation is intentionally conservative. The key decision logic is:

- `classifyOracleCursorType`
- `shouldProbeOracleAmbiguousNumber`
- `probeOracleAmbiguousNumberCursor`
- `validateOracleAmbiguousNumberProbeResult`
- `parseOracleNumberAsInt64Strict`

Implemented safe cases:

- `NUMBER(p,0)` with `p <= 18` is accepted as `int64`
- integer family names such as `INTEGER`, `INT`, `SMALLINT` are accepted as `int64`
- `DATE` is accepted as timestamp-domain
- timestamp types without timezone are accepted as timestamp-domain

Rejected or unresolved cases:

- fractional `NUMBER`
- overly wide `NUMBER`
- timezone-aware timestamps
- RAW, ROWID/UROWID, BLOB/CLOB/NCLOB, FLOAT/BINARY_FLOAT/BINARY_DOUBLE as cursor columns

### Ambiguous NUMBER safety probe

If Oracle reports `NUMBER` without usable precision and scale metadata, the code may run a safety probe:

- `buildOracleAmbiguousNumberProbeQueries`
- `probeOracleAmbiguousNumberCursor`

The probe checks:

- whether fractional values exist
- whether min/max values are safely representable as `int64`

If those checks fail, cursor validation rejects the column rather than silently treating it as a split-capable integer.

### Oracle DATE and TIMESTAMP handling

Oracle date/time cursor columns are mapped into O_Rabbit timestamp-domain behavior:

- `DATE` -> orderable, range-capable timestamp-domain
- `TIMESTAMP` without timezone -> orderable, range-capable timestamp-domain

The worker-side Arrow mapping in `internal/arrowio/sql_to_arrow.go` writes date/time-like SQL values as Arrow `timestamp[ms, UTC]`.

### Unsupported Oracle types and features

Observed Oracle limitations in current code:

- no support for timezone-aware timestamp cursor columns
- no support for floating-point or LOB-like cursor columns
- no special Oracle snapshot-coordination mechanism across tasks
- no Oracle-specific partition-pruning logic beyond bounded cursor predicates

### What was validated manually

The current checkout contains manual-validation references but not a formal benchmark report with measured Oracle numbers. The only direct Oracle validation references visible in this checkout are:

- Oracle-oriented unit tests in `internal/connectors/oracle_test.go`
- manual validation cases in `docs/parquet_file_rolling_validation.md`
- planner review/test material referring to Oracle table names outside a formal benchmark log

## 7. Ordered-Cursor Planning

### Cursor column concept

The ordered cursor is the column used for:

- deterministic ordering
- incremental high-water mark tracking
- optional range splitting

The planner validates it through `validateCursorColumn`, which delegates to the source connector’s `ValidateCursorColumn`.

### Orderable vs range-capable

The distinction is explicit in `CursorColumnValidation`:

- `Orderable` means the column can drive ordered incremental export.
- `RangeCapable` means the column can also be split into bounded subranges.

Examples:

- `int64`, `uint64`, `date`, `timestamp` can be split
- `string`, `uuid`, and `decimal` can usually be ordered but are not split by the current planner

### High-water mark and `_state.json`

Dataset state is loaded in `internal/planner/planner.go:loadDatasetState`.

The authoritative state object is `<dataset-prefix>/_state.json`, which stores:

- `max_hwm_value`
- `max_part`
- `next_part`

Planning behavior:

- if `_state.json` exists, the planner uses its `max_hwm_value` as `fromHWM`
- if `_state.json` is missing, the planner treats the dataset as empty and plans a full export
- if `_state.json` is missing but SQLite still has an HWM for the job, the planner logs a warning event and resets the SQLite HWM to empty

This makes object storage state authoritative, with SQLite HWM as a best-effort mirror.

### Range task generation

Range tasks are generated by:

- `CreateRunAndTasks`
- `buildOrderedCursorRangeTasks`
- `connectors.SplitCursorRange`

The algorithm is:

1. validate cursor column
2. if range-capable, discover `MIN` and `MAX`
3. compute `startInclusive`
4. split the closed interval into `planned_tasks` ranges
5. emit `sql_cursor_range` tasks with lower/upper bounds

The task partition spec contains:

- table
- cursor column
- cursor domain
- lower and upper bounds
- bound inclusivity/exclusivity
- `output_part`

### What happens when the cursor is not range-capable

The planner emits a single `sql_cursor_single` task:

- no explicit upper bound
- lower bound is `fromHWM`
- lower bound is exclusive when an HWM exists

The worker then performs one ordered incremental scan for the entire remaining cursor domain.

### What happens when the table is empty or there are no new rows

For range-capable cursors, `buildOrderedCursorRangeTasks` can return no tasks when:

- `MIN` or `MAX` is missing
- `fromHWM` is already at or beyond the current max cursor

However, `CreateRunAndTasks` then falls back to creating one `sql_cursor_single` task anyway. The task executes, reads zero rows, produces zero Parquet files, and the run still commits a new `_state.json`.

So “no new rows” is represented as:

- one successful task
- zero Parquet files
- unchanged `max_hwm_value`

## 8. Auto-Tune Planner

### Knobs

The main planner and worker knobs live in `internal/jobopts/jobopts.go`.

| Knob | Meaning in current implementation | Primary code |
| --- | --- | --- |
| `planned_tasks` | number of planned cursor partitions; for SQL paths this is still closely related to file count, but no longer identical after rolling | `jobopts.Options`, `autoTuneCursorPlanWithDecision`, `buildOrderedCursorRangeTasks` |
| `max_in_flight_tasks` | global per-run scheduler concurrency cap | enforced in `db.AssignNextPendingTask` |
| `fetch_limit_rows` | worker-side Arrow record-batch target row count, not a SQL `LIMIT` clause | chosen in planner, consumed by `arrowio.RowsToRecordBatches` |
| `target_rows_per_task` | row-based planning hint used in fallback task-count logic | planner only |
| `target_file_bytes` | worker-side Parquet rolling threshold and planner file-size target | planner + SQL worker rolling |
| `max_rows_per_file` | worker-side Parquet rolling row threshold | SQL worker rolling |
| `chunk_size` | legacy manual span hint for old int-range planning | `buildOrderedCursorRangeTasks` legacy path |
| `min_tasks_multiplier` | lower bound multiplier for task count relative to concurrency | `autoTuneCursorPlanWithDecision` |

### Default parsing

`jobopts.Parse` fills defaults:

- `cursor_column` / legacy `id_column` default to `ID`
- `target_rows_per_task = 200000`
- `min_tasks_multiplier = 2`
- `target_file_bytes = 256 MiB`
- `max_rows_per_file = 5,000,000`
- when `auto_tune = false` and user omitted values:
  - `planned_tasks = 1`
  - `fetch_limit_rows = 50000`

### How current auto-tune works

The main logic is `internal/planner/planner.go:autoTuneCursorPlanWithDecision`.

Implemented decision factors:

- source row-count estimate
- source table-byte estimate
- source-local vs remote source heuristic
- local-target heuristic
- active worker count
- cursor domain range-splittability

Implemented algorithm summary:

1. If `max_in_flight_tasks` is not set, choose it from host CPU, memory, and locality via `heuristicMaxInFlightTasks`.
2. Estimate bytes per row as `table_bytes / row_count` when both are available.
3. Estimate total bytes as `bytes_per_row * row_count`.
4. Choose `target_rows_per_task`.
5. Derive `target_file_bytes`, including larger defaults for low-concurrency situations when table bytes are known.
6. Compute a minimum task count from `max_in_flight_tasks * min_tasks_multiplier`.
7. Prefer bytes-based task count when total-byte estimate exists.
8. Fall back to adaptive row-based task count otherwise.
9. Clamp task count between a minimum and a max cap.
10. If the cursor domain is not range-capable, force `planned_tasks = 1`.
11. If active workers are known and `max_in_flight_tasks` was not explicitly supplied, lower it to `min(planned_tasks, active_workers)`.
12. Choose `fetch_limit_rows` by row-count class:
    - `50000`
    - `250000`
    - `500000`

### Current limits of auto-tune

Current code does not use:

- column count
- estimated row width from schema
- previous run performance history
- measured Parquet bytes per row
- worker CPU/memory by host
- topology between DB, workers, and object storage

It therefore remains a heuristic planner rather than a feedback-driven one.

## 9. Worker Export Path

### Task reception

The worker main loop is in `cmd/worker/main.go:main`.

Implemented flow:

1. load worker config
2. connect to master over gRPC
3. call `RegisterWorker`
4. send heartbeats
5. repeatedly call `RequestTask`
6. call `executeTask` when a task is returned

The worker advertises capabilities such as Go version, OS, architecture, CPU count, PID, and timestamp in `RegisterWorkerRequest.capabilities_json`.

### How the worker opens the source DB

The worker uses a small per-process client cache in `cmd/worker/clients.go`:

- `clientCache.SQLReader`
- `clientCache.FlightSQL`
- `clientCache.S3`

For SQL sources, it opens the connector through `connectors.OpenIntRangeReader`, which is an alias of `OpenCursorReader`.

### SQL cursor query execution

The ordered SQL path is `extractSQLCursorTask` in `cmd/worker/main.go`.

It:

- decodes the partition spec
- opens the connector
- calls `QueryCursor`
- gets source column metadata
- converts rows to Arrow batches

### Arrow conversion

Arrow conversion is handled by `internal/arrowio/sql_to_arrow.go`.

The worker calls:

- `PlansFromSQL`
- `RowsToRecordBatches`

Important implementation facts:

- SQL types are conservatively mapped into Arrow
- decimal-like values are often preserved as strings rather than exact Arrow decimal types
- `RowsToRecordBatches` adapts batch size by estimated bytes, targeting about 64 MiB record batches
- `fetch_limit_rows` is the maximum desired batch row count, not a source SQL limit

### Parquet writing

Parquet writing is handled by:

- `internal/parquetio/writer.go`
- `cmd/worker/parquet_rolling.go` for the SQL rolling wrapper

Parquet files are written locally as temp files first. Compression defaults to Snappy.

### Upload

Uploads are performed through `s3io.Uploader.UploadFileMultipart`.

Important behavior:

- small files use single `PutObject`
- larger files use multipart upload
- upload metadata includes `run_id`, `task_id`, `part`, and `file_index`
- if an object key already exists with matching metadata, the upload can be skipped
- if an object key exists but metadata does not match, upload fails rather than silently overwriting

### HWM tracking

During row-to-Arrow conversion, the worker tracks the maximum cursor value seen in the task. This comes from `RowsToRecordBatches`, which uses `connectors.EncodeCursorValue` and `connectors.CompareCursorValues`.

The task result reports this as `max_hwm_value`. The master later derives run-level HWM from all task object payloads in `deriveMaxCursor`.

### Error handling

Important worker-side error behaviors:

- progress reporting is best-effort
- result reporting retries on busy master/SQLite contention
- task cancellation is detected through `ReportTaskProgress` or `ReportTaskResult` gRPC cancellation responses
- if extraction fails, the worker reports task failure
- if a task is canceled, the worker reports `CANCELED`

There is no general task retry mechanism for failed task execution in the current code.

## 10. Parquet File Rolling

### Previous behavior

Historically, one successful SQL worker task usually produced one Parquet file. That tightly coupled:

- planned task count
- scheduling parallelism
- approximate file count
- approximate file size

### Current rolling implementation

Current rolling behavior is implemented in:

- `cmd/worker/parquet_rolling.go`
- integrated into `extractSQLCursorTask`

Rolling is currently active for the ordered SQL cursor path. The FlightSQL path remains single-file-per-task.

### Rolling thresholds

A file rolls when, after finishing a record batch:

- `target_file_bytes > 0` and current temp-file size is `>= target_file_bytes`, or
- `max_rows_per_file > 0` and current file row count is `>= max_rows_per_file`

The threshold decision lives in `shouldRollParquetFile`.

### Important rolling rules

Implemented rules:

- rolling happens only between Arrow record batches
- the code does not split a batch mid-write
- empty tasks still produce zero files
- non-empty SQL tasks produce one or more files
- task-level row count, byte count, and max HWM are preserved across all rolled files

### Naming scheme

Object naming is implemented in `buildTaskParquetObjectKeys`.

Naming pattern:

- first file: `part-000123.parquet`
- rolled files: `part-000123-001.parquet`, `part-000123-002.parquet`, and so on

This keeps the logical task part number stable while making per-task rolled files collision-safe.

### Reporting multiple files per task

Multiple files per task are already supported end-to-end:

- gRPC result: `repeated string parquet_object_keys` in `proto/controlplane.proto`
- SQLite task record: stored in `tasks.parquet_objects_json`
- commit manifest: `objects` and `objects_v2` written by `commitRun`
- CLI reporting: counts all task objects in `internal/orabbitcli/reporting.go`

### Iceberg registration with rolled files

`internal/icebergreg/manager.go`:

- loads committed object keys from manifests or state
- parses logical part numbers with `parsePartNum`
- parses per-task file indices with `parsePartFileIndex`
- sorts by logical part and then file index

This means rolled files are visible to registration in deterministic order.

### Current limitation

Rolling is approximate by design:

- a file may exceed `target_file_bytes` because the trigger is checked only after a full record batch
- per-object `rows` and `bytes` in the stored task object payload are still task totals repeated on each object, not exact per-file metrics

## 11. S3 / MinIO Storage Layout

### Prefix derivation

Dataset prefixes are derived in `internal/dataset/dataset.go`.

Default pattern:

`<engine>/<table-name>`

The table-name normalizer keeps all identifier segments and converts punctuation into a stable path-safe form, for example:

`SalesDB.dbo.Orders -> SalesDB__dbo__Orders`

If the target connection metadata already specifies a prefix, that prefix is used instead.

### Object layout

The current layout uses run-scoped object locations and metadata commits, not a copy/promote stage.

| Object type | Path pattern | Producer |
| --- | --- | --- |
| Parquet parts | `<prefix>/_runs/run-<run-id>/part-000123.parquet` | worker |
| Rolled Parquet parts | `<prefix>/_runs/run-<run-id>/part-000123-001.parquet` | worker |
| Dataset state | `<prefix>/_state.json` | master commit |
| Commit manifest | `<prefix>/_commits/run-<run-id>.json` | master commit |
| Iceberg registration state | `<prefix>/_ice_state.json` | Iceberg manager |

### `_state.json`

`_state.json` stores the latest committed dataset checkpoint. It is written by `commitRun` and read by both the planner and Iceberg registration layer.

Important fields include:

- `last_committed_run_id`
- `committed_at`
- `max_hwm_value`
- `max_part`
- `next_part`
- `last_run_objects`

### How object keys are stored and reported

Workers report object keys through `ReportTaskResult`.

The master stores per-task object metadata in `tasks.parquet_objects_json`. These records are then aggregated by:

- `collectParquetKeys`
- `collectParquetObjectInfos`

The commit manifest stores both:

- `objects`: flat key list
- `objects_v2`: richer object maps

## 12. Iceberg Registration

### Registration engines

The system currently supports two registration engines:

| Engine | Main implementation | Behavior |
| --- | --- | --- |
| `rest-go` | `runRESTGoRegister` in `internal/icebergreg/manager.go` | uses `apache/iceberg-go` REST catalog client to load/create table and add files |
| `ice` | `runIceCLIRegister` in `internal/icebergreg/icecli.go` | shells out to Altinity `ice insert --force-no-copy` after preparing config and verifying table |

### Where registration config comes from

Registration config is assembled on the client side:

- `.ice.yaml` is parsed by `ParseIceYAML`
- `internal/orabbitcli/iceauto.go:buildIcebergRegistrationSnapshot` resolves the effective run snapshot
- the snapshot is persisted in `runs.registration_config_json`

This design is important: the master does not re-read `.ice.yaml` later. It uses the run’s persisted snapshot in `runIcebergRegistration`.

### Table creation and loading

For `rest-go`:

- `prepareRESTGoTable`
- `loadOrCreateRESTGoTable`
- `createRESTGoTable`

If the table does not exist, the manager can auto-create it for SQL ordered-cursor sources by:

1. opening the source connector
2. calling `DescribeTable`
3. converting SQL schema to Arrow via `arrowio.PlansFromSQL`
4. converting Arrow schema to Iceberg via `icetable.ArrowSchemaToIcebergWithFreshIDs`

This auto-create path is not implemented for non-ordered-cursor sources such as FlightSQL.

### How objects are collected

`collectCommittedKeys` in `internal/icebergreg/manager.go`:

1. prefers reading `_commits/run-*.json`
2. falls back to state object fields
3. falls back to listing `part-` objects if needed

Registration reads cumulative commit history, not only the latest run. It then uses `_ice_state.json` and `last_inserted_part` to avoid re-inserting already registered logical parts.

### Registration checkpointing

The Iceberg manager writes `_ice_state.json` with:

- `last_inserted_part`
- `last_run_id`
- `updated_at`

This allows later registrations to skip already-inserted logical parts.

### Query-layer integration

O_Rabbit itself does not provide a query engine. The repository’s query-layer integration is external:

- `ice-rest-catalog.yaml` configures an Iceberg REST catalog backed by the same S3-compatible storage
- `docker-compose.yaml` deploys an Altinity ClickHouse image alongside the catalog
- `.ice.yaml` configures the CLI-side registration endpoint and S3 connectivity

The intended model is:

- O_Rabbit writes files and registers Iceberg metadata
- external engines such as Altinity Ice and ClickHouse read/query the registered table

## 13. CLI Workflow

### Main CLI responsibilities

The user-facing CLI is implemented in `internal/orabbitcli`.

Implemented user workflows include:

- `stack start`
- `stack stop`
- `stack status`
- `run interactive`
- `run submit --file ...`
- `run watch <run-id>`
- `run cancel <run-id>`

### Interactive mode

Interactive run setup is driven by `cmdRunInteractive` and `promptRunConfig` in `internal/orabbitcli/main.go`.

Current behavior:

- normal mode prompts only for essential source, target, Iceberg, and review fields
- `--advanced` exposes storage overrides, worker/process settings, and manual performance knobs

This behavior is also described in the built-in help text under `internal/orabbitcli/help.go`.

### Normal mode vs advanced mode

Implemented distinction:

| Mode | What it asks for |
| --- | --- |
| normal | source engine, DSN, source table, cursor column, incremental toggle, S3 endpoint/bucket/credentials, Iceberg enablement, review |
| advanced | adds S3 region/prefix/path-style, local worker startup choice, master gRPC address for local workers, manual tuning fields, Iceberg engine/config path |

For ordered-cursor sources, normal mode keeps automatic performance tuning enabled and does not ask for low-level tuning values.

### Run-submit config mode

`run submit` is the non-interactive file-based path implemented in `internal/orabbitcli/runsubmit.go`.

It:

- parses strict JSON or YAML
- validates source-engine-specific rules
- upserts source and target connections
- upserts the job
- submits a run

Important validation rules already implemented:

- FlightSQL must use `source.sql`
- FlightSQL cannot use incremental mode
- FlightSQL cannot use auto-tune or manual planning knobs
- ordered-cursor engines require `job.id_column`
- when `auto_tune=true`, manual `planned_tasks`, `chunk_size`, and `fetch_limit` are forbidden

### Defaults and simplifications

Current CLI defaults include:

- auto-tune enabled in normal ordered-cursor flows
- Iceberg registration optional
- automatic local-worker startup when using a local master
- source-specific DSN templates in `internal/orabbitcli/config.go`

The interactive flow has clearly been simplified toward essential prompts in normal mode and low-level control only in advanced mode.

## 14. Frontend / Control Panel

### Implemented backend capabilities

The repository includes a control-panel backend API, not a full frontend.

Implemented backend features:

- server inventory and labels
- SSH credential storage
- SSH connection tests
- project-directory validation
- Docker inspection and container operations
- deployment execution for master, worker, and MinIO
- remote config file listing, reading, validation, and updating
- event streams for executions and deployments

Key implementation files:

- `internal/http/servers.go`
- `internal/http/deployments.go`
- `internal/http/containers.go`
- `internal/http/configs.go`
- `internal/http/controlpanel_helpers.go`
- `internal/db/controlpanel.go`
- `internal/ops/ssh/ssh.go`
- `internal/ops/docker/service.go`
- `internal/ops/deploy/service.go`
- `internal/ops/configs/service.go`

### What is not present

No browser frontend pages, JavaScript/TypeScript frontend application, or templated HTML UI were found in this checkout. If a web UI exists for the project, it is outside this repository snapshot.

## 15. Deployment

### Docker images

`Dockerfile.orabbit` builds separate daemon images:

- `master` image
- `worker` image

Important detail:

- the master image includes the Altinity `ice` binary
- the worker image does not

### Compose files

| File | Purpose |
| --- | --- |
| `docker-compose.yaml` | full local dev stack with master, workers, MinIO, Postgres, Iceberg REST catalog, and ClickHouse |
| `docker-compose.master.yml` | master-only deployment |
| `docker-compose.worker.yml` | worker-only deployment |
| `docker-compose.minio.yml` | MinIO deployment |
| `docker-compose.postgres.yml` | local Postgres deployment |
| `docker-compose.ice-rest-catalog.yml` | Iceberg REST catalog deployment |

### Deployment scripts

The root scripts:

- `deploy-master.sh`
- `deploy-worker.sh`
- `deploy-minio.sh`

are thin wrappers around the compose files and `.env` files.

### Environment files

Example environment files document the intended deployment split:

- `.env.master.example`
- `.env.worker.example`
- `.env.worker.on.master.example`
- `.env.minio.example`

### Multi-VPS assumptions

The repo clearly assumes a multi-host deployment model is possible:

- master and workers can be on different hosts
- MinIO can be separate
- workers connect to master by address in `ORABBIT_MASTER_GRPC_ADDR`
- storage endpoints must be reachable from all workers

The control-panel backend also assumes remote SSH access to project directories on deployment hosts.

### Security-related deployment behavior

Secret-at-rest handling is implemented in `internal/crypto/plain.go`.

Facts from current code:

- `ORABBIT_MASTER_KEY` is optional
- when present, secrets are encrypted with AES-256-GCM
- when absent, the code falls back to a plaintext compatibility format

This means encryption at rest is implemented, but only enforced when the operator configures a non-zero master key.

HTTP auth is optional and configured by `ORABBIT_HTTP_AUTH_TOKEN`. Health and readiness endpoints remain unauthenticated.

gRPC can run either:

- insecure
- TLS with explicit cert/key on the master and CA settings on workers

## 16. Observability And Reporting

### Event model

The system records run/task events in SQLite and replays them over SSE.

Important code:

- event model in `db.Event`
- `InsertEvent` and `ListEventsForRun`
- `httpapi.SSEHandler`
- `httpapi.Broadcaster`

### What is reported

| Signal | Where it comes from |
| --- | --- |
| task progress counters | worker `ReportTaskProgress`, stored via `UpdateTaskProgress` |
| task terminal status | worker `ReportTaskResult`, stored via `CompleteTask` |
| run terminal status | `TryFinalizeRun` and master commit path |
| commit timing | `commitRun` emits `run committed` with `commit_ms` |
| task benchmark timing | worker bench payloads in progress events |
| Iceberg registration timing | `launchIcebergRegistration` emits registration events |
| worker liveness | `RegisterWorker`, `Heartbeat`, `/workers` endpoint |

### CLI summary

The CLI watcher in `internal/orabbitcli/reporting.go` aggregates:

- per-task benchmark payloads
- commit timing
- registration timing
- parquet byte totals
- parquet file counts
- throughput and worker utilization summaries

It now correctly counts multiple Parquet objects per task.

### What is logged

Logging exists at:

- master startup and server failures
- worker registration and task completion
- planner validation/stats/auto-tune events
- commit and registration completion/failure
- remote operation/deployment streams for control-panel actions

### What is missing

There is no separate metrics stack in the repository:

- no Prometheus instrumentation
- no OpenTelemetry traces
- no long-term benchmark history store

Observability is currently event- and log-centric.

## 17. Correctness And Reliability

### Correctness measures already implemented

- nullable ordered cursor columns are rejected
- unsupported cursor types are rejected
- `_state.json` is treated as the authoritative HWM source
- object key collisions fail safely unless metadata matches
- task results are only committed through master-side manifest/state writes
- only one active run per dataset key is allowed via the `idx_runs_dataset_active` SQLite index

### How row counts are tracked

Rows are tracked at task level:

- progress counters in `tasks.rows_read`
- final task totals from worker result
- task-level totals reused in object payload metadata

There is no exact run-global validation against source-side `COUNT(*)` at commit time.

### Failed tasks and retries

Implemented behavior:

- a failed task marks the run failed when `TryFinalizeRun` sees any failed tasks
- pending tasks are canceled when the run is finalized as failed
- result reporting retries when master/SQLite is busy

Not implemented:

- general automatic retry of failed extraction tasks
- worker-side checkpoint resume inside a partially completed task

### Partial runs and cancellation

`CancelRun`:

- marks the run `CANCELED`
- cancels only `PENDING` tasks immediately
- running workers discover cancellation on later progress/result RPCs

This is cooperative cancellation, not hard remote process interruption.

### Known correctness risks visible in code

| Risk | Why it exists |
| --- | --- |
| no full CDC semantics | the system exports new rows by max cursor; it does not capture updates/deletes as change events |
| no source snapshot coordination across workers | range tasks open independent connections and queries; concurrent source changes can affect consistency |
| MSSQL `WITH (NOLOCK)` | may read uncommitted or inconsistent data |
| non-range-capable ordered cursors become single-task scans | correctness is preserved better than guessed splitting, but performance may degrade sharply |
| worker object payload rows/bytes are task totals repeated per file | per-file reporting is approximate |
| encryption-at-rest is optional | plaintext compatibility mode exists when `ORABBIT_MASTER_KEY` is unset |

## 18. Benchmarks And Validation Evidence

The current repository checkout does not include a formal benchmark report with measured timings or throughput numbers for the requested cases.

### What evidence is present

- `internal/planner/planner_test.go` contains a planner test case named `large table 16.6m rows uses 1m fallback`. This is planner test evidence, not a measured export benchmark.
- `docs/parquet_file_rolling_validation.md` lists manual validation cases including an Oracle-style `STAGE_ORDERS_5GB` scenario, but it does not contain measured numeric results.
- Oracle support has dedicated unit tests in `internal/connectors/oracle_test.go`.

### What was not found in this checkout

No code-backed measured benchmark numbers were found for:

- a PostgreSQL 16.6M-row export benchmark
- Oracle `STAGE_BIG`
- Oracle `STAGE_ORDERS_5GB`
- Oracle `LOAN_BALANCES`

Because those measured values are not present in the current repository snapshot, they should not be quoted as part of a formal capstone report unless they exist in an external benchmark log outside this repo.

## 19. Current Technical Limitations

The following limitations are directly supported by code inspection:

- no full CDC implementation
- no generalized update/delete reconciliation
- no source-consistent snapshot orchestration across multiple worker tasks
- no automatic retry policy for failed extraction tasks
- non-range-capable cursor columns fall back to single-task ordered scans
- FlightSQL has no incremental mode and no auto-tuned partitioned planning path
- auto-tune is heuristic only and does not use historical run feedback
- scheduler concurrency is not topology-aware
- worker placement is not source-location-aware
- Parquet rolling is approximate because it rolls only at record-batch boundaries
- per-file object metadata does not yet contain exact per-file row and byte counts
- Iceberg auto-create is only implemented for ordered-cursor SQL sources
- `write_mode` is stored on jobs but is not materially enforced by the current export/commit logic
- many Oracle cursor-column type families are intentionally unsupported
- control-panel deployment service does not implement `postgres` or `ice-rest-catalog` deployment components yet; `internal/ops/deploy/service.go` returns `ErrNotImplemented` for those
- the repository contains backend control-panel APIs but no bundled browser frontend

## Appendix: File-Level Responsibilities

### Master-side runtime

- `cmd/master/main.go`
  Starts the master process, loads the master key, opens SQLite, fails stale runs, starts HTTP and gRPC servers, and wires in Iceberg registration.
- `cmd/master/config.go`
  Defines master flags and environment variables.
- `internal/grpc/server.go`
  Implements worker registration, heartbeat, task assignment, task progress/result handling, commit, and post-run registration.
- `internal/http/server.go`
  Exposes the REST API for jobs, runs, connections, workers, SSE, and control-panel operations.
- `internal/planner/planner.go`
  Contains the central planning algorithm.

### Worker-side runtime

- `cmd/worker/main.go`
  Contains the worker loop, task execution, SQL extraction path, FlightSQL path, upload, and benchmark-event emission.
- `cmd/worker/clients.go`
  Reuses SQL, FlightSQL, and S3 clients inside one worker process.
- `cmd/worker/parquet_rolling.go`
  Implements SQL-task Parquet file rolling and rolled object-key naming.

### Data conversion and storage

- `internal/arrowio/sql_to_arrow.go`
  SQL-to-Arrow mapping and adaptive Arrow record batch creation.
- `internal/parquetio/writer.go`
  Temp-file Parquet writing with Snappy compression.
- `internal/s3io/uploader.go`
  S3/MinIO upload, object reads/writes, manifest/state IO, and listing.

### Persistence and control-plane state

- `internal/db/store.go`
  Models, CRUD operations, scheduling, run finalization, and HWM handling.
- `internal/db/migrate.go`
  Schema definitions and migrations.
- `internal/db/audit.go`
  Audited CRUD operations and run-start audit support.
- `internal/db/controlpanel.go`
  Control-panel backend storage.

### Registration and query-layer integration

- `internal/icebergreg/manager.go`
  Main registration manager, committed object discovery, table preparation, and checkpointing.
- `internal/icebergreg/icecli.go`
  Altinity `ice` CLI integration.
- `internal/icebergreg/icebergreg.go`
  Registration config parsing and normalization.

### CLI and operator workflow

- `internal/orabbitcli/main.go`
  Command routing, interactive workflow, local daemon startup, watch/cancel flows.
- `internal/orabbitcli/runsubmit.go`
  Non-interactive YAML/JSON submission path and validation.
- `internal/orabbitcli/reporting.go`
  SSE consumption and benchmark summary generation.
- `internal/orabbitcli/iceauto.go`
  Run-scoped Iceberg registration snapshot creation from `.ice.yaml`.
