<div align="center">

# O_Rabbit

**Distributed, resumable exports from databases and files to Parquet on S3, with optional Apache Iceberg registration.**

[![Release images](https://github.com/LevonGhukas/O_Rabbit/actions/workflows/release.yml/badge.svg)](https://github.com/LevonGhukas/O_Rabbit/actions/workflows/release.yml)
[![Vulnerability check](https://github.com/LevonGhukas/O_Rabbit/actions/workflows/vulncheck.yml/badge.svg)](https://github.com/LevonGhukas/O_Rabbit/actions/workflows/vulncheck.yml)
[![Go](https://img.shields.io/badge/Go-1.26-00ADD8?logo=go&logoColor=white)](go.mod)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Docker image](https://img.shields.io/badge/docker-levonghukas%2Forabbit-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/levonghukas/orabbit)
[![Apache Iceberg](https://img.shields.io/badge/Apache-Iceberg-1F6FEB)](https://iceberg.apache.org/)

[Quick start](#quick-start) •
[How it works](#how-it-works) •
[Sources](#supported-sources) •
[CLI](#using-the-cli) •
[HTTP API](#http-api) •
[Configuration](#configuration) •
[Deployment](#deployment) •
[Development](#development)

</div>

---

O_Rabbit reads database tables, SQL queries or uploaded files, writes the rows as
Parquet, and uploads the files to S3-compatible storage such as MinIO or AWS S3.
It can then register the files in an Apache Iceberg catalog, so engines like
ClickHouse, Trino or Spark can query them.

A **master** plans each export into tasks and tracks their state. **Workers**
pull tasks, read the source, write Parquet and upload it. Every run is durable:
it survives worker crashes and master restarts, can be canceled, and resumes
incremental exports from where the last run stopped.

## Table of contents

- [Features](#features)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Supported sources](#supported-sources)
- [Using the CLI](#using-the-cli)
- [Iceberg registration](#iceberg-registration)
- [HTTP API](#http-api)
- [Security model](#security-model)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Observability](#observability)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)
- [License](#license)

## Features

| | |
|---|---|
| ⚡ **Parallel exports** | Pull-based master/worker execution over gRPC. Large tables are split into cursor ranges and exported by many workers at once. |
| 🔁 **Incremental runs** | Ordered-cursor exports resume after the last committed value (high-water mark), with an optional look-back window for late-arriving rows. |
| 🛡️ **Reliable by design** | Task leases, retries with backoff, fencing tokens, master leadership fencing, crash recovery and artifact checksum verification. |
| 🧊 **Iceberg ready** | Optional registration through an Iceberg REST catalog (`rest-go`) or the Altinity `ice` CLI, with partitioning, sort order, schema evolution and upsert. |
| 🔢 **Type-safe output** | Source types map to Arrow/Parquet/Iceberg types; per-column type and nullability overrides are verified against the data and fall back with a warning instead of losing values. |
| 🎛️ **Auto-tuning** | Task count and concurrency are planned from table size and active workers, or set manually. |
| 🔐 **Secure defaults** | Encrypted secrets at rest, bearer auth, worker identities via mTLS certificates, short-lived task credentials, optional STS-scoped S3 access. |
| 🧰 **Operable** | REST API with OpenAPI spec, live SSE events, run diagnosis and recovery actions, Prometheus metrics and OpenTelemetry tracing. |

## Quick start

The fastest way to try O_Rabbit is the local Docker stack. It starts MinIO,
PostgreSQL, an Iceberg REST catalog, ClickHouse, one master and two workers,
all bound to `127.0.0.1`.

**Requirements:** Docker with Compose v2, Go 1.26 (for the CLI) and `openssl`.

```sh
git clone https://github.com/LevonGhukas/O_Rabbit.git
cd O_Rabbit

# 1. Secrets for the stack. Keep ORABBIT_MASTER_KEY: it encrypts stored secrets.
export ORABBIT_HTTP_AUTH_TOKEN="$(openssl rand -hex 32)"
export ORABBIT_WORKER_AUTH_TOKEN="$(openssl rand -hex 32)"
export ORABBIT_MASTER_KEY="$(openssl rand -base64 32)"

# 2. Start the stack and wait until the master is ready.
docker compose up -d --build
curl -fsS http://127.0.0.1:9100/ready

# 3. Create a small table to export.
docker compose exec postgres psql -U myuser -d mydb -c \
  "CREATE TABLE people (id bigint PRIMARY KEY, name text, created_at timestamptz DEFAULT now());
   INSERT INTO people (id, name) SELECT g, 'person ' || g FROM generate_series(1, 10000) g;"

# 4. Build the CLI.
make build-client
```

Save this as `run.yaml`. Hostnames are the Compose service names, because the
**workers** connect to the source and the target, not your machine:

```yaml
master:
  http: http://127.0.0.1:9100

source:
  name: local-postgres
  engine: postgres
  dsn: postgres://myuser:mypassword@postgres:5432/mydb?sslmode=disable

target:
  name: local-minio
  endpoint: http://minio:9000
  region: us-east-1
  bucket: bucket1
  prefix: exports
  force_path_style: true
  access_key_id: minioadmin
  secret_access_key: minioadmin

job:
  name: export-people
  target_namespace: demo
  target_table: people
  write_mode: overwrite
  incremental: false
  table: public.people
  cursor_column: id
  auto_tune: true
```

```sh
# 5. Submit the run and follow it live.
./orabbit-client run submit --file ./run.yaml
./orabbit-client run watch <run-id>
```

Open the MinIO console at <http://127.0.0.1:9001> (`minioadmin` / `minioadmin`)
to see the Parquet files under `bucket1/exports/`. Stop the stack with
`docker compose down`.

> [!WARNING]
> The local stack uses well-known credentials. Never expose it beyond your
> machine; see [Deployment](#deployment) for servers.

## How it works

```mermaid
flowchart LR
    CLI[orabbit-client] -->|REST and SSE| M[orabbit-master]
    UI[Other HTTP clients] -->|REST and SSE| M
    W1[orabbit-worker] -->|gRPC: pull, lease, progress, result| M
    W2[orabbit-worker] -->|gRPC: pull, lease, progress, result| M
    M -->|state| DB[(SQLite)]
    W1 --> SRC[(Sources)]
    W2 --> SRC
    W1 --> S3[(S3 / MinIO)]
    W2 --> S3
    M -->|optional| ICE[Iceberg catalog]
    M -->|optional| SSH[Remote hosts over SSH]
```

| Binary | Role |
|---|---|
| `orabbit-master` | Owns the SQLite state, plans runs, serves the REST/SSE API and the worker gRPC service, commits results, registers Iceberg tables, runs cleanup and remote operations. |
| `orabbit-worker` | Pulls leased tasks, reads the source, converts rows to Arrow and Parquet, uploads the files and reports progress and results. |
| `orabbit-client` | The CLI: runs guided or file-based exports, watches events, cancels and diagnoses runs, and can supervise a local master and workers. |

**Key concepts**

- **Connection**: a source or target, with metadata and an encrypted secret.
- **Job**: what to export (table or query, cursor, planning options) and where to.
- **Run**: one execution of a job. Planning splits it into **tasks**; each task
  attempt holds a lease and a fencing token, so a stale worker can never commit.
- **Snapshot**: when a run starts, the master records the job options and the
  connections' non-secret settings. Editing a job or connection only affects
  later runs; rotated credentials apply to runs in flight.
- **Commit**: when all tasks succeed, the master verifies every Parquet file
  (size and SHA-256), publishes the dataset and advances the high-water mark.
- **Dataset**: the target bucket and prefix. One dataset has at most one active run.

## Supported sources

| Engine | Aliases | Ordered-cursor (parallel) | Query mode |
|---|---|:---:|:---:|
| PostgreSQL | `postgresql`, `pg` | ✅ | ✅ |
| MySQL | | ✅ | ✅ |
| MariaDB | `mariadb-server` | ✅ | ✅ |
| Microsoft SQL Server | `sqlserver`, `ms-sql` | ✅ | ✅ |
| Oracle | `ora` | ✅ | ✅ |
| ClickHouse | `click-house`, `ch` | ✅ | ✅ |
| Trino | | ✅ | ✅ |
| Cassandra | `cql`, `cassandra-db` | ✅ | CQL passthrough |
| MongoDB | `mongo` | ✅ | MQL filter passthrough |
| FlightSQL | `flight-sql`, `adbc`, … | Full-query path | — |
| S3 files (CSV, JSON, XML, Excel, Parquet) | `file`, `minio` | Single task | — |

`GET /api/v1/source-engines` returns the live capability list. Engines without
ordered-cursor support are exported as one task.

> [!TIP]
> Give every source connection a **read-only database user**. PostgreSQL,
> MySQL, MariaDB and ClickHouse sessions are also opened read-only; for the
> other engines the database user is the only enforcement. See
> [Security model](#security-model).

## Using the CLI

Run `orabbit-client help <command> <subcommand>` for every flag.

**Guided run.** An interactive flow that can start a local master and workers,
asks for the source, target, performance and Iceberg settings, then streams
the run's events:

```sh
orabbit-client run interactive
orabbit-client run interactive --master-http http://master:9100 --master-grpc master:9102 --local-workers=false
```

**File-based run.** `run submit` takes strict YAML or JSON (unknown fields are
rejected); see the [quick start](#quick-start) for an example.

- `target` needs `endpoint`, `region` and `bucket` (MinIO accepts
  `us-east-1`). For AWS use the regional endpoint, for example
  `https://s3.eu-west-1.amazonaws.com`. `force_path_style` defaults to `false`;
  set it to `true` for MinIO.
- `job` needs `incremental` and `auto_tune`, and SQL sources need
  `cursor_column` (the older name `id_column` is still accepted).
- With `auto_tune: false`, set `max_in_flight_tasks` and `planned_tasks` or
  `target_rows_per_task`.
- `fetch_limit` (optional) is stored as the job option `fetch_size`; see
  [Write mode and fetch size](#write-mode-and-fetch-size).
- FlightSQL needs `source.sql`, `incremental: false`, `auto_tune: false` and no
  `cursor_column` or manual planning fields.

**Run lifecycle.**

```sh
orabbit-client run watch <run-id>      # live events; Ctrl+C stops watching, not the run
orabbit-client run cancel <run-id>
orabbit-client run diagnose <run-id>   # status, events and a suggested next action
orabbit-client run recover <run-id> --action reconcile_commit --reason "commit is overdue"
```

**Local stack without Docker.**

```sh
orabbit-client stack start master worker --count 4
orabbit-client stack status
orabbit-client stack stop
```

The client finds `orabbit-master` and `orabbit-worker` next to its own binary,
or use `--master-bin` and `--worker-bin`.

### Write mode and fetch size

**`write_mode`** says what a run does to the table already published for its
dataset:

| Job | `write_mode` | Each run |
|---|---|---|
| Full (`incremental: false`) | `overwrite` (default) | replaces the table with the new export |
| Incremental (`incremental: true`) | `append` (default) | adds the rows after the high-water mark |

An empty `write_mode` gets the default for the job's `incremental` flag. Two
combinations are rejected when a job is saved (400) and when a stored job is
run (422, `CONFIGURATION_INVALID`):

- `incremental: true` + `overwrite`: each run reads only new rows, so it would
  replace the table with just those rows (a run with no new rows would empty it).
- `incremental: false` + `append`: appending full exports is not supported yet.

**`fetch_size`** (job option, `options_json.fetch_size`) is how many rows the
source driver fetches per network round-trip. Larger values mean fewer
round-trips on large exports and a little more memory per worker; `0` keeps
the engine default. The job value wins over the same setting in the DSN.

| Engine | Applied as | Default |
|---|---|---|
| Oracle | DSN `PREFETCH_ROWS` | 5,000 (O_Rabbit sets it when the DSN does not; go-ora's own default is 25) |
| Cassandra | page size (DSN `page_size`) | 5,000 |
| ClickHouse | setting `max_block_size` | server default |
| MongoDB | cursor batch size | 10,000 |
| PostgreSQL, MySQL, MariaDB, SQL Server, Trino | not used: these drivers stream rows | — |

### Incremental exports and late-arriving rows

An incremental run resumes at the dataset's high-water mark, the largest
cursor value committed so far, and reads `cursor >= mark`. Rows committed
later with the same cursor value as the mark are therefore exported by the
next run. To avoid exporting the rows at the mark twice, each commit stores
the hashes of the rows sitting exactly at the new mark in `_state.json`
(`hwm_boundary`), and the next run drops boundary rows whose hash is stored.
Rows with identical content are counted, so a new identical row is still
exported.

- A dataset whose `_state.json` predates the boundary is seeded by its next
  run: rows at the mark are treated as already exported and their hashes are
  recorded. Late rows that arrived before that run cannot be told apart from
  exported ones and are not recovered.
- At most 10,000 distinct rows are kept per mark. Above that (typical for
  coarse cursors such as a `date` column) the run logs a `WARN`
  `hwm boundary resume` event and resumes strictly after the mark, so late
  rows with the mark's value can be missed.
- Cursors of unknown type always resume strictly after the mark.
- Only SQL sources track the boundary; MongoDB and file sources are unchanged.
- A row's hash covers every selected column, so changing the selected
  columns, their types, or a query whose result is not repeatable (for
  example one selecting `now()`) exports the rows at the mark once more.

Rows that become visible later with a *smaller* cursor value are still
missed, for example an `updated_at` set when a transaction starts but
committed after a run has read past it.

Set the job option `cursor_lookback` to re-read a window below the mark on
every run: a count for integer cursors (`"1000"`) or a duration for date and
timestamp cursors (`"30m"`, `"48h"`). Re-read rows are exported again, so
deduplicate downstream, for example with Iceberg upsert on the table's key.
String, UUID and decimal cursors do not support a look-back, and the
high-water mark never moves backwards.

### Deleted source rows

Incremental runs export rows whose cursor is above the high-water mark, so
they capture inserts and updates that move the cursor. **They do not
propagate deletes:** a row deleted at the source leaves nothing to read, and
its earlier copy stays in the exported Parquet files and the Iceberg table.

- `cursor_lookback` does not help: it re-reads rows that still exist.
- Iceberg upsert replaces the target row of every key that appears in a run.
  It never removes a key that is absent from the run, so it does not
  propagate deletes either.
- Change data capture is not supported: the job options
  `partition_strategy: cdc_stream` and `consistency_mode: STRONG_CDC`, and
  `consistency.mode: STRONG_CDC` on `/runs/submit`, are rejected. The
  proposed design is in [docs/cdc-design.md](docs/cdc-design.md).

Safe ways to remove deleted rows today:

- **Full refresh.** A run with `incremental: false` and `write_mode: overwrite`
  re-reads the whole source. With Iceberg registration it replaces the
  table's files in one snapshot, so readers never see a half-replaced table.
  Run one as often as deleted rows must disappear; between refreshes they
  stay visible.
- **Soft deletes.** Mark rows as deleted at the source (for example a
  `deleted_at` column) and move the cursor column when you do. The next
  incremental run exports the marked row, which replaces the old one under
  upsert; readers filter marked rows out. Remove rows physically only after a
  full refresh has run.
- Many teams combine both: incremental runs for freshness and a scheduled
  full refresh to drop deleted rows.

### Source reconciliation

An artifact checksum and the worker's row count prove that what was written is
intact, not that the worker read every row of its range. An optional check
closes that gap: at commit, after the artifacts verify and before anything is
published, the master runs `SELECT COUNT(*)` on the source for each task's
planned range and compares it with the rows the task wrote.

```json
"partition_strategy": "ordered_cursor",
"reconciliation": {"mode": "count", "consistency": "immutable", "on_mismatch": "review"}
```

| Key | Values (default first) | Meaning |
| --- | --- | --- |
| `mode` | `off`, `count` | `count` turns the check on. Absent means off. |
| `consistency` | `none`, `immutable` | `immutable` asserts that the rows of the planned range do not change while the run is in flight. |
| `on_mismatch` | `fail`, `review` | What a proven mismatch does. |

The count uses the same table or query, where clause, bounds and inclusivity as
extraction, taken from the task's planned range, never from the worker-reported
high-water mark. With `count`, the planner pins a single-task run to the maximum
cursor value it saw at plan time (an inclusive upper bound), so rows inserted
afterwards fall outside both the extraction and the count. Range-split runs
already carry plan-time upper bounds. Where no plan-time maximum is known
(cursors that cannot be range-split, or an empty source at plan time) the task
stays unbounded above, so rows added to the source during the run count as a
difference: with `consistency: immutable` that is a mismatch.

Each task ends as:

- `match`: source and output counts are equal.
- `mismatch`: they differ and `consistency` is `immutable`. This is proven data
  loss. `on_mismatch: fail` fails the run terminally; `review` fails it with
  `operator_action_required` and failure class `OPERATOR_ACTION_REQUIRED`. Either
  way nothing is committed, so the dataset state and the high-water mark do not
  move; investigate and re-run. There is no action to force-commit a held run.
- `indeterminate`: they differ and `consistency` is `none`. A mutable source may
  have changed, so this is recorded as a warning and the commit proceeds.
- `error`: the count query failed. The commit is retried; an error is never a
  mismatch.

Results are stored per task and appear in `GET /runs/{id}/diagnosis` as
`source_reconciliation` (counts per status and every task that did not match,
with bounds, source rows, output rows and detail).

Limits and cost:

- Needs `partition_strategy: ordered_cursor` and a source of postgres, mysql,
  mariadb, mssql, oracle, clickhouse or trino. Other engines are rejected when
  the job is saved.
- In query mode the job cannot also set `where_clause` (it is ignored in query
  mode today, see TID-982); put the filter in the query.
- One extra `COUNT(*)` per task on the source at commit time, so keep an index on
  the cursor column. The counts run inside the commit timeout
  (`ORABBIT_COMMIT_TIMEOUT`); a commit that exceeds it is interrupted and retried
  without recording a failure, so keep the total count time well below it. Pool jobs are counted from the master, which must reach the
  source (it already does for ordered-cursor planning).
- `consistency_mode: STRONG_SNAPSHOT` is not applied by the worker yet (TID-981),
  so it does not make a source immutable; only `reconciliation.consistency`
  controls how a difference is classified.
- Rows with a NULL cursor value are outside every bounded range: both extraction
  and the count exclude them, so reconciliation cannot detect them. The planner
  rejects cursors it knows are nullable; nullability the catalog does not report
  is not caught.
- It is not run again when a commit is retried after its manifest was written.

### Column types

Source types are mapped to Arrow, Parquet and Iceberg types automatically.
`source.column_types` overrides a column with a logical type such as `string`,
`int64`, `decimal(12,2)`, `date`, `timestamp_tz[UTC]` or `nullable<int64>`,
or with `source` / `nullable<source>` to change only nullability. Before any
task starts, the planner checks every override against the data. An override
that would lose values or violate NOT NULL falls back to a lossless type, and
the run gets a `type_warnings` entry explaining why.

## Iceberg registration

Registration runs after a successful commit. Configure it per run under
`iceberg.options`, optionally on top of a defaults file (`.ice.yaml`, see
[.ice.yaml.example](.ice.yaml.example)). Run values win, and the resolved
configuration is stored with the run, so retries never depend on the file.

```yaml
iceberg:
  enabled: true
  engine: rest-go              # or: ice (Altinity ice CLI)
  table: demo.people
  options:
    uri: http://catalog:8181
    schema_evolution: additive # default: strict
    target_file_size: 268435456
    partition_spec:
      - source: created_at
        transform: day
    upsert:
      enabled: true
      keys: [id]
      mode: merge-on-read
```

<details>
<summary><b>All registration options</b></summary>

```yaml
uri: http://catalog:8181
bearerToken: token

partition_spec:
  - source: created_at
    name: created_day
    transform: day         # identity, year, month, day, hour, bucket[N], truncate[N]

sort_order:
  - source: created_at
    direction: desc
    null_order: nulls_last

schema_evolution: additive # strict (default) or additive
target_file_size: 268435456
distribution_mode: range
metrics_mode: truncate(32)

metadata_retention:
  delete_after_commit: true
  previous_versions_max: 10
  min_snapshots_to_keep: 3
  max_snapshot_age_ms: 604800000

upsert:
  enabled: true
  keys: [id]
  mode: merge-on-read      # sets write.delete.mode for other engines
  max_keys: 500000         # default: 500000, or 10000 for composite keys

credential_vending:
  enabled: true
  required: true
```

- `additive` schema evolution adds optional columns and allows
  Iceberg-compatible type promotion.
- Upsert needs Iceberg format v2 and non-null, unique keys in every run.
  O_Rabbit deletes the replaced rows copy-on-write: it rewrites only the data
  files whose key ranges overlap the run's keys. iceberg-go v0.5 writes no
  position deletes for files registered without Parquet field IDs, so
  `mode: merge-on-read` only sets the table property that other engines use.
- An upsert run may carry at most `upsert.max_keys` rows. Larger runs fail
  before any row is read with a non-retryable `CONFIGURATION_INVALID`; split
  them into smaller incremental runs or use a full refresh.
- With required credential vending, static S3 keys are not passed to the
  catalog-backed table file system.
- Registration is zero-copy: Iceberg references the verified worker files
  themselves. For a partitioned `rest-go` table the workers receive the
  partition spec, split rows with the catalog's transforms and write one
  partition per file (at most 32 open files per task; more partitions mean
  more, smaller files). The master reads only Parquet footers. Files with
  only order-preserving transforms (identity, year, month, day, hour,
  truncate) keep full column metrics; with `bucket[N]` they carry partition
  values and counts but no column bounds.
- `partition_spec` cannot change between an export and its registration. A
  table that is partitioned while the run has no `partition_spec`, or files
  from workers older than protocol 8, fail registration with a
  non-retryable error and nothing is committed; set the spec and export again.

</details>

## HTTP API

The API listens on port `9100` and is described in
[`docs/openapi.yaml`](docs/openapi.yaml) (OpenAPI 3.1). A test fails if a
served route is missing from the spec.

```sh
curl -sS -H "Authorization: Bearer $ORABBIT_HTTP_AUTH_TOKEN" http://127.0.0.1:9100/api/v1/runs
```

- Every resource is served under **`/api/v1`**. Older unversioned and `/api/...`
  paths still work but return `Deprecation: true` and a
  `Link: </api/v1/...>; rel="successor-version"` header.
- `/healthz` (process alive) and `/ready` (database reachable and leadership
  fresh) need no token. `/status` and `/metrics` are unversioned.
- Errors use one envelope: `{"error": {"code", "message", "details",
  "failure_class", "retryable", "request_id"}}`.
- `GET /runs` and `GET /connections` accept `limit` (1-1000) and `cursor`; the
  next cursor is in the `X-Next-Cursor` header. Request bodies are limited to 4 MiB.
- Jobs are validated before they are saved (`POST /jobs`, `PUT /jobs/{id}`):
  a name, existing source and target connections, and an `options_json`
  object with known keys of the right type. Unknown keys are rejected with a
  "did you mean" hint, and a `400 invalid_input` lists every problem in
  `details.errors` as `{"field": "options_json.planned_tasks", "error": "..."}`.

<details>
<summary><b>Core routes</b> (relative to <code>/api/v1</code>)</summary>

| Method | Route | Purpose |
|---|---|---|
| `GET` | `/healthz`, `/ready`, `/status` | Liveness, readiness and leadership status (unversioned) |
| `GET` | `/metrics` | Prometheus metrics (unversioned) |
| `GET` | `/source-engines` | Connector capabilities |
| `GET`, `POST` | `/connections` | List or create connections |
| `GET`, `PUT`, `DELETE` | `/connections/{id}` | Read, replace or delete a connection |
| `GET`, `POST` | `/jobs` | List or create jobs |
| `GET`, `PUT`, `DELETE` | `/jobs/{id}` | Read, replace or delete a job |
| `POST` | `/jobs/{id}/runs` | Start a stored job |
| `POST` | `/runs/validate` | Validate a one-shot submission |
| `POST` | `/runs/submit` | Validate, upsert and start a one-shot run |
| `GET` | `/runs`, `/runs/{id}` | List runs or read one run |
| `POST` | `/runs/{id}/cancel` | Cancel a run |
| `GET` | `/runs/{id}/progress` | Aggregated progress |
| `GET` | `/runs/{id}/events`, `/runs/{id}/events/stream` | Stored events and live SSE |
| `GET` | `/runs/{id}/artifacts` | Committed artifact metadata |
| `GET` | `/runs/{id}/diagnosis` | Lifecycle diagnosis and suggested next action |
| `POST` | `/runs/{id}/recover` | Audited recovery action (`action`, `reason`) |
| `POST` | `/runs/{id}/registration/retry`, `/registration/cancel` | Retry or cancel Iceberg registration |
| `GET` | `/sse?run_id={id}` | General replay and live event stream |
| `GET` | `/workers` | Active workers (`?all=1` for all) |
| `POST` | `/workers/enrollment-tokens` | Create a worker enrollment token |
| `GET` | `/workers/identities` | List worker identities |
| `POST` | `/workers/identities/{id}/revoke` | Revoke a worker identity |
| `POST` | `/maintenance/submit` | Submit a maintenance operation |

</details>

<details>
<summary><b>Run failures</b></summary>

Planning failures on `/runs/submit` and `/jobs/{id}/runs` return `422`
(`run_planning_failed`: fix the request), `503` (`run_planning_failed`: a
dependency was unreachable, retrying may help) or `500` (`internal_error`).
A job saved with invalid `options_json` before validation existed returns
`422` with failure class `CONFIGURATION_INVALID` and the problems in
`details.errors`; fix the job with `PUT /jobs/{id}`.

A `FAILED` run explains itself in `GET /api/v1/runs/{id}`:

- `failure_phase`: `planning`, `extract` or `commit`
- `failure_class`: for example `AUTHENTICATION_FAILED`, `NETWORK_CONNECTION_FAILED`,
  `TIMEOUT`, `QUERY_SYNTAX_ERROR`, `DATA_INTEGRITY_ERROR` (see `internal/failure`)
- `error_summary`: `"<n> of <total> task(s) failed: <first task error>"`, with
  passwords in URLs redacted

</details>

<details>
<summary><b>Remote operations</b> (SSH servers, Docker, deployments)</summary>

The master can manage registered SSH servers: system info, Docker containers
and logs, configuration files and deployments (`/servers`, `/deployments`,
`/executions` under `/api/v1`). These routes are separate from the data API:

- They return 404 unless `ORABBIT_REMOTE_OPS_AUTH_TOKEN` is set, and accept only
  that token. The API token cannot reach them and vice versa.
- Every server needs a pinned SSH host key (`host_key_fingerprint`, such as
  `SHA256:...`). Verify it out of band with `ssh-keyscan <host> | ssh-keygen -lf -`.

</details>

<details>
<summary><b>gRPC service</b></summary>

Workers use `orabbit.v1.ControlPlane` on port `9102`: `RegisterWorker`,
`Heartbeat`, `RequestTask`, `RenewTaskLease`, `AcquireUploadCapacity`,
`ReleaseUploadCapacity`, `ReportTaskProgress`, `ReportTaskResult`,
`EnrollWorker`, `RenewWorkerCertificate` and `GetTaskCredentials`. The wire
contract is [proto/controlplane.proto](proto/controlplane.proto).

</details>

## Security model

- **Secrets at rest.** The master refuses to start without `ORABBIT_MASTER_KEY`
  (32-byte AES-256 key, base64 or hex). Every stored secret is encrypted with it.
  Losing or replacing it makes them unreadable, so back it up separately and
  use key rotation (see [Troubleshooting](#troubleshooting)) to change it.
- **Network defaults.** HTTP and gRPC bind to `127.0.0.1`. A non-loopback HTTP
  listener requires `ORABBIT_HTTP_AUTH_TOKEN` and must sit behind a
  TLS-terminating proxy. A non-loopback gRPC listener requires
  `ORABBIT_WORKER_AUTH_TOKEN` and gRPC TLS.
- **Worker identity.** With gRPC TLS, every worker holds a master-issued
  certificate:
  1. Create a token: `POST /api/v1/workers/enrollment-tokens`
     with an optional body such as `{"pool": "default", "ttl_seconds": 3600, "max_uses": 1}`.
  2. Start the worker once with `ORABBIT_WORKER_ENROLLMENT_TOKEN`. It creates a
     key and receives a certificate whose SAN is `spiffe://orabbit/worker/<uuid>`,
     stored in `ORABBIT_WORKER_IDENTITY_DIR`.
  3. The worker renews it at two thirds of `ORABBIT_WORKER_CERT_TTL` (default 24h).
     Revoke with `POST /api/v1/workers/identities/{id}/revoke`.
- **Task credentials.** Assignments carry no secrets. Only the worker holding a
  task's live lease can fetch its source DSN and S3 keys (`GetTaskCredentials`).
- **Worker pools.** A job runs in `worker_pool` (default `default`); a worker
  only receives tasks from the pool of its enrollment token. Use pools to keep
  trust zones apart.
- **Scoped S3 access.** With target metadata `"credential_mode": "sts"` and
  `"sts_role_arn"`, workers get temporary credentials limited to the run's
  own prefix (`<dataset prefix>/_runs/run-<run id>/`).
- **Source safety.** Query mode accepts a single `SELECT`/`WITH` statement;
  `where_clause` must be one boolean expression. A read-only database user is
  still the only control that holds on every engine.

Generate secrets without writing them to files under version control:

```sh
openssl rand -hex 32       # ORABBIT_HTTP_AUTH_TOKEN
openssl rand -hex 32       # ORABBIT_WORKER_AUTH_TOKEN (a different value)
openssl rand -base64 32    # ORABBIT_MASTER_KEY
```

To report a vulnerability, see [SECURITY.md](SECURITY.md).

## Configuration

Flags override environment variables. Example files:
[.env.master.example](.env.master.example),
[.env.worker.example](.env.worker.example),
[.env.worker.on.master.example](.env.worker.on.master.example),
[.env.minio.example](.env.minio.example) and
[.env.backup.example](.env.backup.example). Never commit the copied files.

<details>
<summary><b>Master environment</b></summary>

| Variable | Default | Purpose |
|---|---|---|
| `ORABBIT_MASTER_KEY` | required | Base64 or hex 32-byte AES-256 key for stored secrets |
| `ORABBIT_DB_PATH` | `./master.sqlite` | SQLite database path |
| `ORABBIT_HTTP_ADDR` | `127.0.0.1:9100` | HTTP listen address |
| `ORABBIT_GRPC_ADDR` | `127.0.0.1:9102` | gRPC listen address |
| `ORABBIT_HTTP_AUTH_TOKEN` | empty | Bearer token for the HTTP API and SSE |
| `ORABBIT_WORKER_AUTH_TOKEN` | empty | Bearer token for worker RPCs; required for non-loopback gRPC |
| `ORABBIT_REMOTE_OPS_AUTH_TOKEN` | empty | Enables the remote operations API and is its only token; must differ from the API token |
| `ORABBIT_TLS_CERT_FILE`, `ORABBIT_TLS_KEY_FILE` | empty | Master gRPC certificate and key |
| `ORABBIT_GRPC_INSECURE` | `false` | Disable gRPC TLS (loopback only) |
| `ORABBIT_GRPC_ALLOW_INSECURE_REMOTE` | `false` | Allow plaintext gRPC on a non-loopback listener (isolated private networks only) |
| `ORABBIT_WORKER_CERT_TTL` | `24h` | Worker certificate lifetime (1h-720h) |
| `ORABBIT_ICE_BIN` | `ice` | `ice` CLI used by `engine: ice` registration |
| `ORABBIT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, `ERROR` |
| `ORABBIT_LOG_FORMAT` | `json` | `json` or `text` |
| `ORABBIT_TASK_LEASE_DURATION` | `30s` | Attempt lease duration |
| `ORABBIT_TASK_LEASE_SCAN_INTERVAL` | `5s` | Expired-lease scan cadence |
| `ORABBIT_TASK_MAX_ATTEMPTS` | `3` | Attempts per task |
| `ORABBIT_TASK_RETRY_BACKOFF` | `1s` | Initial retry delay |
| `ORABBIT_TASK_RETRY_BACKOFF_MAX` | `30s` | Maximum retry delay |
| `ORABBIT_MAX_ACTIVE_RUNS` | `16` | Global limit of `RUNNING`/`COMMITTING` runs; extra runs wait in `PLANNING` |
| `ORABBIT_MAX_ACTIVE_TASKS` | `64` | Global limit of active task attempts |
| `ORABBIT_CATALOG_WORK_LIMIT` | `2` | Concurrent registration/reconciliation calls |
| `ORABBIT_UPLOAD_CAPACITY_LIMIT` | `8` | Concurrent uploading tasks across all workers |
| `ORABBIT_UPLOAD_CAPACITY_LEASE_TTL` | `2m` | Upload capacity lease lifetime |
| `ORABBIT_LEADERSHIP_LEASE_DURATION` | `15s` | Durable leadership lease |
| `ORABBIT_LEADERSHIP_RENEW_INTERVAL` | `5s` | Leadership renewal cadence |
| `ORABBIT_COMMIT_TIMEOUT` | `30m` | Bound on one publication attempt |
| `ORABBIT_COMMIT_MAX_ATTEMPTS` | `5` | Commit retries before operator action is needed |
| `ORABBIT_REGISTRATION_TIMEOUT` | `30m` | Bound on one catalog registration |
| `ORABBIT_REGISTRATION_LEASE_DURATION` | `30s` | Registration attempt lease (min `3s`) |
| `ORABBIT_REGISTRATION_MAX_ATTEMPTS` | `5` | Registration attempts |
| `ORABBIT_RECONCILIATION_LEASE_DURATION` | `30s` | Catalog observation lease (min `3s`) |
| `ORABBIT_RECONCILIATION_MAX_ATTEMPTS` | `5` | Catalog observation attempts |
| `ORABBIT_MULTIPART_CLEANUP_SCAN_INTERVAL` | `1m` | Abandoned-upload scan cadence |
| `ORABBIT_MULTIPART_ABANDONMENT_GRACE` | `15m` | Delay before multipart cleanup |
| `ORABBIT_MULTIPART_CLEANUP_MAX_ATTEMPTS` | `5` | Multipart cleanup retries |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_DRY_RUN` | `true` | **Nothing is deleted while `true`**; see [Troubleshooting](#troubleshooting) |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_SCAN_INTERVAL` | `5m` | Canceled-object scan cadence |
| `ORABBIT_CANCELED_OBJECT_RETENTION` | `168h` | Quarantine before deletion |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_MAX_ATTEMPTS` | `5` | Object cleanup retries |
| `ORABBIT_FULL_RUN_RETAIN_COUNT` | `1` | Full-refresh datasets kept after Iceberg publication |
| `ORABBIT_HISTORY_RETENTION` | `720h` | Prune history of finished runs older than this; `0` disables |
| `ORABBIT_HISTORY_PRUNE_INTERVAL` | `1h` | History pruning cadence |
| `ORABBIT_DB_READ_CONNS` | `4` | Read-only SQLite connections for lists, metrics and SSE |

</details>

<details>
<summary><b>Worker environment</b></summary>

| Variable | Default |
|---|---|
| `ORABBIT_WORKER_AUTH_TOKEN` | empty; must match the master |
| `ORABBIT_WORKER_ENROLLMENT_TOKEN` | empty; used once to obtain a certificate |
| `ORABBIT_WORKER_IDENTITY_DIR` | where the certificate and key are stored |
| `ORABBIT_SOURCE_QUERY_TIMEOUT` | `2h`; bound on one task's source query, `0` for none |
| `ORABBIT_LOG_LEVEL`, `ORABBIT_LOG_FORMAT` | `INFO`, `json` |
| `ORABBIT_WORKER_TEMP_ROOT` | OS temp directory + `orabbit-worker` |
| `ORABBIT_TEMP_SCAN_INTERVAL` | `5m` |
| `ORABBIT_TEMP_UNLOCKED_GRACE` | `30m` |
| `ORABBIT_TEMP_OFFLINE_RETENTION` | `168h` |
| `ORABBIT_TEMP_MAX_ENTRIES` | `100` |
| `ORABBIT_TEMP_MAX_BYTES_PER_SCAN` | `10737418240` (10 GiB) |
| `ORABBIT_TEMP_MIN_FREE_BYTES` | `1073741824` (1 GiB) |
| `ORABBIT_TEMP_MAX_MANAGED_BYTES` | `107374182400` (100 GiB) |
| `ORABBIT_TEMP_DRY_RUN` | `false` |

Control-plane flags: `-master`, `-worker-id`, `-worker-addr`, `-insecure`,
`-tls-ca`, `-tls-server-name`, `-identity-dir`, `-enrollment-token`,
`-worker-auth-token` and `-poll`. Prefer the environment variable over the
token flag so the token does not appear in process arguments.
`docker-compose.worker.yml` maps `ORABBIT_MASTER_GRPC_ADDR`,
`ORABBIT_GRPC_INSECURE`, `ORABBIT_TLS_CA_FILE`, `ORABBIT_TLS_SERVER_NAME` and
`ORABBIT_WORKER_POLL` to those flags.

</details>

<details>
<summary><b>Connector and CLI environment</b></summary>

| Variable | Purpose |
|---|---|
| `ORABBIT_DEFAULT_S3_ENDPOINT`, `ORABBIT_DEFAULT_S3_REGION` | Default S3 endpoint and region |
| `ORABBIT_DEFAULT_S3_ACCESS_KEY_ID`, `ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY` | Default S3 keys |
| `ORABBIT_S3_FORCE_PATH_STYLE` | Force path-style S3 addressing |
| `ORABBIT_AUTO_MAX_IN_FLIGHT` | Override CLI automatic concurrency |
| `ORABBIT_{POSTGRES,MYSQL,MARIADB,MSSQL,TRINO}_ORDERED_RANGE_READS` | Enable engine-specific ordered range reads |

`orabbit-client` sends `ORABBIT_HTTP_AUTH_TOKEN` as its bearer token. API
callers may set `X-Orabbit-Actor` to name themselves in audit records.

</details>

<details>
<summary><b>Admission and upload limits</b></summary>

Limits are layered. `ORABBIT_MAX_ACTIVE_RUNS` admits runs in creation order;
queued runs stay `PLANNING` with `MAX_ACTIVE_RUNS` events.
`ORABBIT_MAX_ACTIVE_TASKS` caps attempts across admitted runs, and each job's
`max_in_flight_tasks` caps its own run. Before uploading, a worker takes an
upload capacity lease from the master; waiting for one never fails the task,
and a lost worker's slot is reclaimed after `ORABBIT_UPLOAD_CAPACITY_LEASE_TTL`.
Use `orabbit-client run diagnose <run-id>` to see which limit a run waits on.

</details>

## Deployment

**Docker images** are published on every push to `main` by the
[Release images](.github/workflows/release.yml) workflow, after all CI checks
pass:

```text
levonghukas/orabbit:latest-master
levonghukas/orabbit:latest-worker
```

Both run as the unprivileged user `orabbit` (uid/gid `10001`). Because the tag
never changes, run `docker compose pull` to update a server.

**Split deployment.** Master, workers and MinIO run as separate Compose
projects, configured by environment files:

```sh
cp .env.master.example .env.master
cp .env.worker.example .env.worker
cp .env.minio.example .env.minio
# Fill in every placeholder and secret.

./deploy-minio.sh
./deploy-master.sh
WORKER_SCALE=2 ./deploy-worker.sh
```

| Compose file | Purpose |
|---|---|
| `docker-compose.yaml` | Local development stack (see [Quick start](#quick-start)) |
| `docker-compose.master.yml` | Master for a server |
| `docker-compose.master.grpc-publish.yml` | Optional override that publishes gRPC (loopback by default, for a tunnel) |
| `docker-compose.master.backup.yml` | Optional Litestream backup of the master database |
| `docker-compose.worker.yml` | Workers for a server |
| `docker-compose.minio.yml` | MinIO for a server |
| `docker-compose.ex-db.yml` | Source databases for development and integration tests |

Master and workers join the external Docker network `orabbit-control-plane`,
which the scripts create. On one host, set
`ORABBIT_MASTER_GRPC_ADDR=orabbit-master:9102`. For workers on other hosts,
prefer a private VPN or tunnel. Publish gRPC publicly
(`ORABBIT_GRPC_PUBLISH_ADDR=0.0.0.0`) only with TLS, worker tokens and
network-level restrictions.

<details>
<summary><b>Production checklist</b></summary>

- Distinct `ORABBIT_HTTP_AUTH_TOKEN`, `ORABBIT_WORKER_AUTH_TOKEN` and `ORABBIT_MASTER_KEY`
- gRPC TLS with enrolled worker identities for any cross-host worker
- Ports 9100/9102, source databases and object storage restricted at the network layer
- Read-only users for every source connection
- Master SQLite backed up (Litestream or `sqlite3 .backup`), verified regularly with `scripts/verify-master-backup.sh`, and `ORABBIT_MASTER_KEY` stored separately
- All example MinIO and catalog credentials replaced
- Cleanup dry-run and retention settings reviewed before enabling deletion
- Mounted TLS files readable by uid `10001`

</details>

> [!NOTE]
> **The master is a single point of failure**: there is no automatic failover.
> Run it under a supervisor that restarts it, and back up its database. With
> the Litestream backup the targets are an RPO of 1 minute and an RTO of
> 30 minutes, checked by a weekly restore drill; restore with
> `scripts/master-restore.sh`. See
> [docs/master-backup-restore.md](docs/master-backup-restore.md).

> [!NOTE]
> **MinIO.** Upstream MinIO no longer publishes images and its repository is
> archived. The Compose files use `levonghukas/minio` and `levonghukas/mc`,
> built from pinned upstream releases with patched dependencies by
> [docker/minio/Dockerfile](docker/minio/Dockerfile). Prefer managed S3 or
> another maintained S3-compatible store for production data.

<details>
<summary><b>Upgrading volumes from root-running images</b></summary>

Volumes created by older images that ran as root must be handed over once,
with the service stopped:

```sh
docker run --rm -v orabbit-master_orabbit_master_data:/d debian:bookworm-slim chown -R 10001:10001 /d
docker run --rm -v orabbit-worker_orabbit_worker_identity:/d debian:bookworm-slim chown -R 10001:10001 /d
```

</details>

## Observability

- **Metrics:** `GET /metrics` (Prometheus format, behind the API token) exposes
  run, task, lease, registration and leadership gauges, gRPC and HTTP request
  counts and latencies (`orabbit_grpc_server_*`, `orabbit_http_*`), task
  throughput (`orabbit_task_*`), commit latency
  (`orabbit_run_commit_duration_seconds`) and Go runtime metrics. Labels are
  bounded (statuses, phases); no metric names a run, task or worker.
  - Queue health: `orabbit_oldest_registration_age_seconds{status}`
    (PENDING from creation, other statuses from their last change),
    `orabbit_oldest_registration_attempt_age_seconds` (the oldest active
    catalog call) and `orabbit_registration_attempt_leases_expired`.
  - Catalog capacity: `orabbit_catalog_work_slots_in_use`,
    `orabbit_catalog_work_slots_capacity` and
    `orabbit_catalog_work_rejected_total`.
  - `orabbit_runs_operator_action_required` counts runs that wait for an
    operator.
- **Alerts and dashboard:** [`monitoring/prometheus/orabbit-alerts.yml`](monitoring/prometheus/orabbit-alerts.yml)
  holds Prometheus alert rules; every alert links to its section in
  [docs/runbooks.md](docs/runbooks.md). `severity: critical` is meant to page,
  `warning` to open a ticket. Durations wait out normal retries and lease
  scans, so brief retries do not alert. The rules' unit tests
  (`orabbit-alerts.test.yml`) run in CI with `promtool`. Import
  [`monitoring/grafana/orabbit-lifecycle.json`](monitoring/grafana/orabbit-lifecycle.json)
  into Grafana and pick the Prometheus data source. Only the leader serves
  `/metrics`, so scrape every master; the rules aggregate with `max()`.

  ```yaml
  scrape_configs:
    - job_name: orabbit-master
      authorization: { credentials_file: /etc/prometheus/orabbit-token }  # ORABBIT_HTTP_AUTH_TOKEN
      static_configs: [{ targets: ["master:9100"] }]
  rule_files: [orabbit-alerts.yml]
  ```
- **Tracing:** off by default. Set the standard OpenTelemetry variables on the
  master and workers, for example
  `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317` (OTLP/gRPC). Trace
  context propagates from worker to master.
- **Events:** every run has a durable event log (`/runs/{id}/events`) and a live
  SSE stream.

## Development

**Requirements:** Go 1.26 (see [go.mod](go.mod)); Docker for integration tests;
`golangci-lint` for `make lint`.

```sh
make build         # go build ./...
make build-client  # ./orabbit-client (also build-master, build-worker)
make test          # go test ./...
make vet           # go vet ./...
make lint          # golangci-lint run
gofmt -w path/to/changed_file.go
```

<details>
<summary><b>Repository layout</b></summary>

```text
cmd/
  master/              Master executable, configuration and key rotation
  worker/              Worker executable and rolling Parquet writer
  orabbit-client/      CLI executable
internal/
  arrowio/             SQL, document and file rows to Arrow; type planning
  artifact/            Parquet artifact metadata and verification
  connectors/          Source drivers, cursors and query capabilities
  crypto/              Secret encryption with ORABBIT_MASTER_KEY
  dataset/             Dataset identity and layout
  db/                  SQLite store, migrations, leases, leadership and audit
  failure/             Failure classes shared by API, planner and workers
  grpc/                Worker control-plane service and run commit
  grpcpb/              Code generated from proto/controlplane.proto
  http/                REST/SSE API and remote operations
  httperr/             The HTTP error envelope
  icebergreg/          Iceberg registration, receipts and reconciliation
  jobopts/             Job options
  ops/                 SSH, Docker, configuration and deployment services
  orabbitcli/          CLI commands and local process supervision
  parquetio/           Parquet writer
  planner/             Run planning and auto-tuning
  s3io/                S3 uploads and multipart handling
  telemetry/           Metrics and tracing
  typesystem/          Logical types, conversion and type warnings
  workeridentity/      Worker certificates and enrollment
  workerworkspace/     Worker temporary workspace and scavenging
  envutil/, sysinfo/   Small helpers
proto/                 Control-plane protobuf source
docs/                  OpenAPI spec, backup runbook, design review
docker/minio/          Patched MinIO image build
scripts/               CI helpers (vulnerability check, end-to-end test), master restore and backup verification
test/e2e/              End-to-end stack and tests
```

</details>

### Tests

Unit tests use temporary SQLite databases and test servers. Integration tests
run only when their environment variables are set:

```sh
docker compose -f docker-compose.ex-db.yml up -d ex-postgres ex-mariadb ex-clickhouse ex-mongodb

ORABBIT_IT_POSTGRES_DSN='postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable' \
ORABBIT_IT_MARIADB_DSN='root:root@tcp(localhost:3307)/test' \
ORABBIT_IT_CLICKHOUSE_DSN='clickhouse://default:clickhouse@localhost:9003/default' \
ORABBIT_IT_MONGODB_DSN='mongodb://root:root@localhost:27017/orabbit_it?authSource=admin' \
go test -count=1 -run 'Integration|ReadOnly' ./internal/connectors/
```

<details>
<summary><b>More integration variables</b></summary>

- `ORABBIT_IT_MYSQL_DSN`
- `ORABBIT_IT_ORACLE_DSN`, for example `oracle://app:app@localhost:1521/FREEPDB1`
- `ORABBIT_IT_TRINO_DSN`, for example `http://orabbit@localhost:8080?catalog=memory&schema=default`
- `ORABBIT_IT_CASSANDRA_DSN`, for example `cassandra://localhost:9042/orabbit_it`
- `ORABBIT_IT_MSSQL_DSN`, for example `sqlserver://sa:YourStrong!Passw0rd@localhost:1433?database=master`
  (on Apple Silicon use the `mcr.microsoft.com/azure-sql-edge` image)

Planner and worker integration tests also need an S3 bucket, for example a
local MinIO:

```sh
ORABBIT_IT_POSTGRES_DSN=... ORABBIT_IT_MONGODB_DSN=... \
ORABBIT_IT_S3_ENDPOINT=http://127.0.0.1:9010 ORABBIT_IT_S3_BUCKET=orabbit-it \
ORABBIT_IT_S3_ACCESS_KEY_ID=... ORABBIT_IT_S3_SECRET_ACCESS_KEY=... \
go test -count=1 -run Integration ./internal/planner ./cmd/worker
```

</details>

The end-to-end test runs the real master and worker images against
PostgreSQL, MinIO and an Iceberg REST catalog, then reads the result back
through the catalog: full and incremental exports, snapshot history, data
files against the accepted worker files, and two catalog faults on the commit
(a timeout before the catalog applies it, and a lost reply after it did). It
needs Docker, Go and `openssl`, and uses ports 15001, 15432, 18181, 19000 and
19100 on the host:

```sh
./scripts/e2e.sh
```

The script builds the images, starts the isolated `orabbit-e2e` stack from
[`test/e2e/docker-compose.yml`](test/e2e/docker-compose.yml), runs
`go test ./test/e2e`, saves container logs to `e2e-artifacts/` on failure and
removes the stack. `ORABBIT_E2E_KEEP=1` leaves the stack running for
debugging. Without `ORABBIT_E2E=1`, `go test ./...` skips these tests.
`ORABBIT_E2E_DR=1 ./scripts/e2e.sh` adds the master's Litestream backup and
runs the restore drill described in
[docs/master-backup-restore.md](docs/master-backup-restore.md#restore-drill).

### CI

| Workflow | Runs on | Does |
|---|---|---|
| [CI](.github/workflows/ci.yml) | Pull requests; reused by Release images | Unit tests (race detector), integration tests against PostgreSQL, MySQL, MariaDB and ClickHouse, the end-to-end test (`scripts/e2e.sh`), `golangci-lint`, `shellcheck`, Compose validation, vulnerability check, and an image build, smoke test and Trivy scan |
| [Release images](.github/workflows/release.yml) | Push to `main` | Runs CI, then pushes `latest-master` and `latest-worker` |
| [Restore drill](.github/workflows/dr-drill.yml) | Every Monday, and on demand | Loses and restores the master in the end-to-end stack (`ORABBIT_E2E_DR=1 scripts/e2e.sh`) and keeps the measured RTO/RPO report |
| [Vulnerability check](.github/workflows/vulncheck.yml) | Within CI, and every Monday | `scripts/vulncheck.sh`: fails on reachable Go vulnerabilities not reviewed in `.govulncheck-allow` |
| [MinIO images](.github/workflows/minio-images.yml) | Changes under `docker/minio/` | Builds, tests, scans and publishes the MinIO images |

### Contracts to keep in sync

- **Worker protocol:** `WorkerProtocolVersion` in `internal/grpc/server.go` is an
  exact-match contract. Bump it whenever the worker RPC contract changes, and
  deploy master and workers together.
- **Protobuf:** `internal/grpcpb` is generated from `proto/controlplane.proto`.
  There is no generator target in the repository, so never edit the generated files by hand.
  Regenerate with the versions in the generated file headers (protoc 35.1,
  protoc-gen-go v1.36.11, protoc-gen-go-grpc 1.6.2):
  `protoc --go_out=. --go_opt=module=github.com/LevonGhukas/O_Rabbit --go-grpc_out=. --go-grpc_opt=module=github.com/LevonGhukas/O_Rabbit proto/controlplane.proto`.
- **Database:** migrations in `internal/db/migrate.go` run automatically at
  startup. Never edit a migration that may already have run.
- **API:** add every new `/api/v1` route to `docs/openapi.yaml`; a test checks it.

### Deprecations

Deprecated options still work. When a run uses one, the planner records a
`deprecated job options used` run event, so usage can be measured before removal.

| Deprecated | Use instead |
|---|---|
| Unversioned and `/api/...` HTTP paths | `/api/v1/...` |
| `frontend_submit_supported` in `/source-engines` | `oneshot_submit_supported` |
| Job option `id_column` | `cursor_column` |
| `partition_strategy: int_range` | `ordered_cursor` |
| Job option `chunk_size` | `planned_tasks` or `target_rows_per_task` |
| S3 sources without `format` | Set `source.format` |
| `options_json` keys `iceberg_enabled`, `iceberg_engine`, `iceberg_table`, `iceberg_partition_keys` (never read) | `iceberg_registration` |
| `options_json` key `fetch_limit_rows` (never read) | `fetch_size` |

## Troubleshooting

<details>
<summary><b>A worker registers but cannot complete tasks</b></summary>

Check the worker's connectivity to the source and the S3 target, not only to
the master. DSNs are delivered to workers, so `localhost` in a DSN means the
worker itself.

</details>

<details>
<summary><b>API calls return 401</b></summary>

Use the same `ORABBIT_HTTP_AUTH_TOKEN` as the master. `/healthz` and `/ready`
never need it.

</details>

<details>
<summary><b>The client cannot find daemon binaries</b></summary>

Put `orabbit-master` and `orabbit-worker` next to `orabbit-client`, or pass
`--master-bin` and `--worker-bin`.

</details>

<details>
<summary><b>TLS startup or worker connection fails</b></summary>

With `-insecure=false` the master needs both `-tls-cert` and `-tls-key`;
workers need `-tls-ca` and, if the name differs, `-tls-server-name`.

- *"worker certificate required; enroll the worker first"*: start the worker once
  with `ORABBIT_WORKER_ENROLLMENT_TOKEN`.
- *"worker identity is unknown or revoked"*: enroll again with a new token and an
  empty `ORABBIT_WORKER_IDENTITY_DIR`.

</details>

<details>
<summary><b>Starting a run returns <code>dataset_busy</code> (409)</b></summary>

A dataset (target bucket and prefix) has at most one active run. The error
details name the active run (`active_run_id`, `active_job_id`,
`active_run_status`). Wait for it, or cancel it with
`orabbit-client run cancel <run-id>`. A run left in `PLANNING` by a master
crash is marked `FAILED` at the next start.

</details>

<details>
<summary><b>The master cannot decrypt stored secrets</b></summary>

Restore the exact `ORABBIT_MASTER_KEY` that wrote them. A new key cannot read
old values; use key rotation to change it. `orabbit-master check-db -db
<path>` checks a database against the key in `ORABBIT_MASTER_KEY` without
starting a master or changing the file (it also runs SQLite's integrity check).

</details>

<details>
<summary><b>Rotating the master key</b></summary>

`orabbit-master rotate-master-key` re-encrypts every stored secret in one
transaction. If any value does not open with the current key, nothing changes.

1. Back up the database ([docs/master-backup-restore.md](docs/master-backup-restore.md)).
2. Stop the master; the command refuses to run while a master holds the lock.
3. Run it with both keys:

   ```sh
   ORABBIT_MASTER_KEY=<current key> ORABBIT_NEW_MASTER_KEY="$(openssl rand -base64 32)" \
     orabbit-master rotate-master-key -db /var/lib/orabbit/master.sqlite

   # or with Docker Compose
   docker compose -f docker-compose.master.yml run --rm -e ORABBIT_NEW_MASTER_KEY=<new key> orabbit-master rotate-master-key
   ```

4. Replace `ORABBIT_MASTER_KEY` everywhere it is stored and start the master.
   Worker certificates stay valid.
5. Keep the old key until backups made with it expire.

</details>

<details>
<summary><b>A canceled or failed run leaves objects behind</b></summary>

Uploads that no published dataset will reference (canceled attempts, artifacts
of failed or canceled runs that never began a commit, and multipart uploads of
failed attempts) are quarantined for `ORABBIT_CANCELED_OBJECT_RETENTION`
(7 days), re-checked, and verified by size, SHA-256 and metadata before any
delete. Runs whose commit started and then failed are left under
`_runs/run-<id>/` for manual review.

**Cleanup is dry-run by default.** With
`ORABBIT_CANCELED_OBJECT_CLEANUP_DRY_RUN=true` nothing is deleted; candidates
are only marked `WOULD_DELETE`. To enable deletion, review the candidates in
`canceled_object_cleanup` of `GET /api/v1/runs/{id}`, ideally enable bucket
versioning, then set the variable to `false` and restart the master.

</details>

<details>
<summary><b>Iceberg registration fails</b></summary>

For `engine: ice`, check that the master can run the `ice` binary and read its
configuration. For REST, check catalog reachability, warehouse and S3
credentials, namespace and table names, and partition sources. The run's events
and `GET /api/v1/runs/{id}/diagnosis` show the failing step;
`POST /api/v1/runs/{id}/registration/retry` retries it.

</details>

<details>
<summary><b>Master exit codes</b></summary>

| Code | Meaning |
|---|---|
| `0` | Requested shutdown (SIGINT/SIGTERM) |
| `1` | Runtime failure: startup recovery failed, leadership was lost, or a server failed |
| `2` | Invalid configuration, including a missing or malformed `ORABBIT_MASTER_KEY` |

On exit the master releases its leadership lease and waits up to 30 seconds
for in-flight requests; a run interrupted mid-commit is resumed by the next
leader.

</details>

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md)
first; in short: keep changes focused, add tests, run `gofmt`, `go vet`,
`go test ./...` and `golangci-lint run`, and never commit real credentials.

## License

Copyright 2026 MindWise Levon Ghukasyan. Licensed under the
[Apache License 2.0](LICENSE); see [NOTICE](NOTICE).
