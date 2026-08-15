# O_Rabbit

O_Rabbit is a distributed data-export system that reads database tables or
queries, writes the results as Parquet, and uploads the files to S3-compatible
object storage. A central master plans and tracks work; one or more workers pull
leased tasks and perform extraction, Parquet generation, upload, and integrity
reporting.

The project is aimed at data engineers and operators who need repeatable,
parallel exports with explicit run state, incremental cursors, cancellation,
retry/recovery behavior, and optional registration of completed files in an
Iceberg catalog.

## Features

- Pull-based master/worker execution over gRPC
- REST API and CLI for connections, jobs, runs, progress, artifacts, and
  cancellation
- SSE event replay and live run watching
- Parquet output with rolling files and artifact integrity metadata
- S3-compatible targets, including MinIO
- Full and incremental ordered-cursor exports
- Automatic task planning and concurrency tuning, or manual planning controls
- Task leases, retries, fencing tokens, master leadership fencing, recovery,
  multipart cleanup, and quarantined cleanup of canceled-run objects
- Optional post-run Iceberg registration through Ice REST or the Altinity
  `ice` CLI
- Remote server, Docker container, configuration, and deployment operations
  over SSH through the master HTTP API

### Source engines

The connector registry currently contains:

| Engine | Common aliases | Ordered-cursor extraction | Query mode |
| --- | --- | ---: | ---: |
| Microsoft SQL Server | `sqlserver`, `ms-sql` | Yes | Yes |
| PostgreSQL | `postgresql`, `pg` | Yes | Yes |
| ClickHouse | `click-house`, `ch` | Yes | Yes |
| Oracle | `ora` | Yes | Yes |
| MySQL | — | Yes | Yes |
| MariaDB | `mariadb-server` | Yes | Yes |
| Trino | — | Yes | Yes |
| Cassandra | `cql`, `cassandra-db` | Yes | CQL passthrough |
| MongoDB | `mongo` | Yes | MQL filter passthrough |
| FlightSQL | `flight-sql`, `adbc`, others | Dedicated full-query path | No ordered query mode |
| S3 | `file`, `minio` | No | No |

The guided HTTP submission API supports table and query modes according to the
capabilities returned by `GET /api/source-engines`. The file-based
`orabbit-client run submit` path is narrower: ordered-cursor engines use table
mode, while FlightSQL uses a full SQL query.

## Architecture

```mermaid
flowchart LR
    CLI[orabbit-client] -->|REST and SSE| M[orabbit-master]
    UI[Other HTTP clients] -->|REST and SSE| M
    W1[orabbit-worker] -->|gRPC pull, lease, progress, result| M
    W2[orabbit-worker] -->|gRPC pull, lease, progress, result| M
    M -->|state and migrations| DB[(SQLite)]
    W1 --> SRC[(Source systems)]
    W2 --> SRC
    W1 --> S3[(S3 or MinIO)]
    W2 --> S3
    M -->|optional registration| ICE[Iceberg catalog]
    M -->|optional operations| SSH[Remote hosts over SSH]
```

The three binaries are:

- `orabbit-master`: owns SQLite state, planning, REST/SSE, worker gRPC,
  leadership, reconciliation, cleanup, Iceberg registration, and remote
  operations.
- `orabbit-worker`: polls for tasks, reads a source, converts Arrow data to
  Parquet, uploads parts, renews leases, and reports progress and results.
- `orabbit-client`: the user-facing CLI. It can manage local daemon processes,
  run a guided export, submit a strict YAML/JSON spec, watch events, and cancel
  runs.

Connections contain source or target metadata plus a secret payload. Jobs
reference connections and describe extraction/planning options. A run snapshots
one job execution and contains logical tasks; task attempts carry leases and
fencing credentials. Events, artifact records, high-water marks, registration
attempts, and cleanup records provide the durable execution history.

## Tech stack

- Go 1.25
- SQLite via the pure-Go `modernc.org/sqlite` driver
- gRPC and Protocol Buffers for master/worker communication
- `net/http` JSON APIs and Server-Sent Events
- Apache Arrow and ADBC
- Apache Parquet
- AWS SDK for Go v2 for S3-compatible storage
- Apache Iceberg Go and the Altinity `ice` CLI
- Docker and Docker Compose for local and distributed deployments

Database-specific Go drivers are listed in [go.mod](go.mod).

## Repository layout

```text
cmd/
  master/             Master executable and configuration
  worker/             Worker executable and rolling-Parquet execution
  orabbit-client/     CLI executable
internal/
  arrowio/            SQL/MongoDB-to-Arrow conversion
  connectors/         Source drivers, cursor handling, and query capabilities
  db/                 SQLite store, migrations, leases, leadership, and audit
  grpc/               Worker control-plane service
  http/               REST/SSE API and control-panel operations
  icebergreg/         Iceberg registration, receipts, and reconciliation
  ops/                SSH, Docker, config, and deployment services
  orabbitcli/         CLI parsing, local process supervision, and run flows
  parquetio/          Parquet writer
  planner/            Task planning and automatic tuning
  s3io/               S3 upload and multipart handling
  workerworkspace/    Managed worker temporary workspace and scavenging
proto/                Control-plane protobuf source and generated Go files
docs/                 Design reviews, query-mode notes, and resilience roadmap
Dockerfile.orabbit    Multi-stage master and worker image
docker-compose*.yml   Local, supporting-service, and split-host deployments
```

## Requirements

- Go 1.25.x, matching the `go 1.25.0` directive in `go.mod`
- Network access from every worker to:
  - the master gRPC endpoint
  - the selected source
  - the S3-compatible target
- Docker with Compose v2 for container workflows
- `golangci-lint` only when running `make lint`
- A Java 21-capable master image is already provided when using
  `Dockerfile.orabbit`; it is needed there for the bundled `ice` CLI
- Optional local tools:
  - `mc` for manual MinIO administration
  - `curl` or `wget`, plus `nc` or `telnet`, for `healthcheck.sh`
  - `psql` for its optional SQL check

No external database server is required for master state: the master creates
and migrates its SQLite database at startup.

## Installation

Clone the repository and build all packages:

```sh
git clone https://github.com/LevonGhukas/O_Rabbit.git
cd O_Rabbit
go mod download
make build
```

Build named binaries in the repository root:

```sh
make build-client
make build-master
make build-worker
```

For an installed local CLI stack, place the three binaries in the same
directory. The client resolves sibling `orabbit-master` and `orabbit-worker`
binaries by default:

```sh
go install ./cmd/orabbit-client
go build -o "$(go env GOPATH)/bin/orabbit-master" ./cmd/master
go build -o "$(go env GOPATH)/bin/orabbit-worker" ./cmd/worker
export PATH="$(go env GOPATH)/bin:$PATH"

orabbit-client help
```

Explicit daemon paths can instead be supplied with `--master-bin` and
`--worker-bin`.

## Configuration

Command-line flags override the master defaults loaded from environment
variables. `ORABBIT_WORKER_AUTH_TOKEN` is read directly by the worker; the
split-worker Compose file translates its other connection settings into flags.

### Master environment

| Variable | Default | Purpose |
| --- | --- | --- |
| `ORABBIT_DB_PATH` | `./master.sqlite` | SQLite database path |
| `ORABBIT_HTTP_ADDR` | `127.0.0.1:9100` | HTTP listen address |
| `ORABBIT_GRPC_ADDR` | `127.0.0.1:9102` | gRPC listen address |
| `ORABBIT_HTTP_AUTH_TOKEN` | empty | Bearer token for known API and SSE routes |
| `ORABBIT_WORKER_AUTH_TOKEN` | empty | Shared bearer token for worker control-plane RPCs; required for non-loopback gRPC |
| `ORABBIT_MASTER_KEY` | empty | Base64 or hex encoded 32-byte AES-256 key |
| `ORABBIT_ICE_BIN` | `ice` | `ice` CLI executable used by `engine=ice` registration |
| `ORABBIT_GRPC_INSECURE` | `true` | Disable master gRPC TLS |
| `ORABBIT_TLS_CERT_FILE` | empty | Master gRPC certificate |
| `ORABBIT_TLS_KEY_FILE` | empty | Master gRPC private key |
| `ORABBIT_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `ORABBIT_LOG_FORMAT` | `json` | `json` or `text` |
| `ORABBIT_TASK_LEASE_DURATION` | `30s` | Attempt lease duration |
| `ORABBIT_TASK_LEASE_SCAN_INTERVAL` | `5s` | Expired-lease scan cadence |
| `ORABBIT_TASK_MAX_ATTEMPTS` | `3` | Attempts per logical task |
| `ORABBIT_MAX_ACTIVE_RUNS` | `16` | Durable global `RUNNING`/`COMMITTING` run limit |
| `ORABBIT_MAX_ACTIVE_TASKS` | `64` | Global active task-attempt admission limit |
| `ORABBIT_CATALOG_WORK_LIMIT` | `2` | Shared concurrent registration/reconciliation limit |
| `ORABBIT_UPLOAD_CAPACITY_LIMIT` | `8` | Global concurrent object-upload task leases |
| `ORABBIT_UPLOAD_CAPACITY_LEASE_TTL` | `2m` | Upload capacity lease lifetime and crash-recovery bound |
| `ORABBIT_TASK_RETRY_BACKOFF` | `1s` | Initial retry delay |
| `ORABBIT_TASK_RETRY_BACKOFF_MAX` | `30s` | Maximum retry delay |
| `ORABBIT_LEADERSHIP_LEASE_DURATION` | `15s` | Durable leadership lease |
| `ORABBIT_LEADERSHIP_RENEW_INTERVAL` | `5s` | Leadership renewal cadence |
| `ORABBIT_MULTIPART_CLEANUP_SCAN_INTERVAL` | `1m` | Abandoned-upload scan cadence |
| `ORABBIT_MULTIPART_ABANDONMENT_GRACE` | `15m` | Delay before multipart cleanup |
| `ORABBIT_MULTIPART_CLEANUP_MAX_ATTEMPTS` | `5` | Multipart cleanup retry limit |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_SCAN_INTERVAL` | `5m` | Canceled-object scan cadence |
| `ORABBIT_CANCELED_OBJECT_RETENTION` | `168h` | Quarantine before object removal |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_MAX_ATTEMPTS` | `5` | Object cleanup retry limit |
| `ORABBIT_CANCELED_OBJECT_CLEANUP_DRY_RUN` | `true` | Report candidates without deletion |
| `ORABBIT_FULL_RUN_RETAIN_COUNT` | `1` | Successful full-refresh datasets retained after Iceberg publication |

Admission limits are layered. `ORABBIT_MAX_ACTIVE_RUNS` counts durable
`RUNNING` and `COMMITTING` runs; excess planned runs keep their tasks
`PENDING`, remain `PLANNING`, and are admitted in creation order when a
terminal run frees capacity. `ORABBIT_MAX_ACTIVE_TASKS` caps active attempts
across admitted runs, while each job's `max_in_flight_tasks` remains the
per-run task cap. `ORABBIT_CATALOG_WORK_LIMIT` bounds master-owned registration
and reconciliation calls.

Before uploading Parquet objects, each worker waits for a master-issued,
DB-backed upload capacity lease. One lease covers the task's upload fan-out,
works across worker processes, is renewed during upload, and is released when
the upload phase ends. Temporary capacity exhaustion does not fail the task or
consume another attempt; a lost worker's slot is reclaimed after
`ORABBIT_UPLOAD_CAPACITY_LEASE_TTL`.

When work appears admission-limited, inspect the run status and bounded events
with `orabbit-client run diagnose <run-id>`. A `PLANNING` run with
`MAX_ACTIVE_RUNS` admission events is queued; also inspect active run/task
counts, connected workers, and master logs before raising a limit. Repeated
upload waiting with healthy task-lease renewal indicates the global upload
limit is full.

If `ORABBIT_MASTER_KEY` is absent, connection secrets use plaintext
compatibility storage. Remote SSH credentials and saved remote configuration
versions require a master key. Keep the key stable: replacing or losing it makes
previously encrypted values unreadable.

Generate suitable secrets without placing their values in source control:

```sh
openssl rand -hex 32       # HTTP bearer token
openssl rand -hex 32       # separate worker gRPC bearer token
openssl rand -base64 32    # master encryption key
```

### Worker environment

These values configure logging and managed temporary storage directly:

| Variable | Default |
| --- | --- |
| `ORABBIT_WORKER_AUTH_TOKEN` | empty; must match the master when configured |
| `ORABBIT_LOG_LEVEL` | `INFO` |
| `ORABBIT_LOG_FORMAT` | `json` |
| `ORABBIT_WORKER_TEMP_ROOT` | OS temp directory plus `orabbit-worker` |
| `ORABBIT_TEMP_SCAN_INTERVAL` | `5m` |
| `ORABBIT_TEMP_UNLOCKED_GRACE` | `30m` |
| `ORABBIT_TEMP_OFFLINE_RETENTION` | `168h` |
| `ORABBIT_TEMP_MAX_ENTRIES` | `100` |
| `ORABBIT_TEMP_MAX_BYTES_PER_SCAN` | `10737418240` (10 GiB) |
| `ORABBIT_TEMP_MIN_FREE_BYTES` | `1073741824` (1 GiB) |
| `ORABBIT_TEMP_MAX_MANAGED_BYTES` | `107374182400` (100 GiB) |
| `ORABBIT_TEMP_DRY_RUN` | `false` |

The worker flags `-master`, `-worker-id`, `-worker-addr`, `-insecure`,
`-tls-ca`, `-tls-server-name`, `-worker-auth-token`, and `-poll` configure
control-plane access. Prefer the environment variable over the token flag so
the credential is not exposed in process arguments. In
`docker-compose.worker.yml`, `ORABBIT_MASTER_GRPC_ADDR`,
`ORABBIT_GRPC_INSECURE`, `ORABBIT_TLS_CA_FILE`,
`ORABBIT_TLS_SERVER_NAME`, and `ORABBIT_WORKER_POLL` are translated to those
flags.

### Connector and CLI environment

| Variable | Purpose |
| --- | --- |
| `ORABBIT_DEFAULT_S3_ENDPOINT` | Default S3 endpoint |
| `ORABBIT_DEFAULT_S3_REGION` | Default S3 region |
| `ORABBIT_DEFAULT_S3_ACCESS_KEY_ID` | Default access key |
| `ORABBIT_DEFAULT_S3_SECRET_ACCESS_KEY` | Default secret key |
| `ORABBIT_S3_FORCE_PATH_STYLE` | Force path-style S3 addressing |
| `ORABBIT_AUTO_MAX_IN_FLIGHT` | Override CLI automatic concurrency |
| `ORABBIT_{POSTGRES,MYSQL,MARIADB,MSSQL,TRINO}_ORDERED_RANGE_READS` | Enable engine-specific ordered range reads |

`orabbit-client` reads `ORABBIT_HTTP_AUTH_TOKEN` and sends it as a bearer
token. API callers may also send `X-Orabbit-Actor`; otherwise audit records use
the authenticated-token identity or a default actor.

Example deployment files are provided as `.env.master.example`,
`.env.worker.example`, `.env.worker.on.master.example`, and
`.env.minio.example`. Do not commit the copied files.

## Running locally

### Native processes

Start a master:

```sh
./orabbit-master \
  -db ./master.sqlite \
  -http-addr 127.0.0.1:9100 \
  -grpc-addr 127.0.0.1:9102 \
  -insecure=true
```

Start one or more workers in separate terminals:

```sh
./orabbit-worker \
  -master localhost:9102 \
  -worker-id local-1 \
  -worker-addr localhost \
  -insecure=true
```

Or let the CLI supervise a foreground local stack:

```sh
orabbit-client stack start master worker --count 4
```

The client records managed process state under
`/tmp/orabbit-client-gocache` by default. Use another `--gocache` when multiple
independent local stacks are needed.

Check and stop managed processes:

```sh
orabbit-client stack status
orabbit-client stack status --json
orabbit-client stack stop --dry-run
orabbit-client stack stop
```

### Docker Compose

Build the daemon images:

```sh
make docker-build-master
make docker-build-worker
```

The root `docker-compose.yaml` defines MinIO, PostgreSQL, an Ice REST catalog,
ClickHouse, one master, and two workers:

```sh
export ORABBIT_HTTP_AUTH_TOKEN="$(openssl rand -hex 32)"
export ORABBIT_WORKER_AUTH_TOKEN="$(openssl rand -hex 32)"
docker compose up -d --build
docker compose ps
docker compose down
```

At the current revision it mounts `./docker/postgres/initdb`, but that directory
is not present in the repository. Create it (it may be empty) or remove that
mount before using this compose file.

Supporting compose files include:

- `docker-compose.ex-db.yml`: development source databases
- `docker-compose.ice-rest-catalog.yml`: standalone Ice REST catalog
- `docker-compose.clickhouse-altinity.yml`: standalone Altinity ClickHouse
  integration example
- `docker-compose.master.yml`, `.worker.yml`, and `.minio.yml`: split-host
  deployments driven by environment files

The split master and worker projects join an external Docker network named
`orabbit-control-plane`; the deployment scripts create it when missing. When
both projects run on the same Docker host, set
`ORABBIT_MASTER_GRPC_ADDR=orabbit-master:9102`. The base master Compose file
publishes HTTP but does not publish gRPC port 9102.

For cross-host workers, prefer a private VPN or trusted tunnel and retain the
same worker token on both sides. The optional
`docker-compose.master.grpc-publish.yml` override publishes gRPC on host
loopback by default for a tunnel:

```sh
ORABBIT_ENV_FILE=.env.master docker compose \
  --env-file .env.master \
  -f docker-compose.master.yml \
  -f docker-compose.master.grpc-publish.yml \
  up -d --build
```

Setting `ORABBIT_GRPC_PUBLISH_ADDR=0.0.0.0` is an explicit public/private-host
publication. Do that only with `ORABBIT_WORKER_AUTH_TOKEN`,
`ORABBIT_GRPC_INSECURE=false`, a certificate/key mounted into the container,
and network-layer source restrictions.

## Using the CLI

Run `orabbit-client help <command> <subcommand>` for the authoritative flag
list.

### Guided run

```sh
orabbit-client run interactive
```

This TTY-only flow can start a local master and workers, prompt for source,
target, performance, and Iceberg settings, submit a run, and stream its events.
Use `--advanced` for lower-level tuning or target an existing master:

```sh
orabbit-client run interactive \
  --master-http http://master.example:9100 \
  --master-grpc master.example:9102 \
  --local-workers=false
```

### File-based run

`run submit` accepts strict YAML or JSON; unknown fields are rejected. Example
for an automatically planned PostgreSQL table export:

```yaml
master:
  http: http://127.0.0.1:9100

source:
  name: app-postgres
  engine: postgres
  dsn: postgres://user:password@localhost:5432/app?sslmode=disable

target:
  name: local-minio
  endpoint: http://localhost:9000
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
  id_column: id
  auto_tune: true
```

Submit and follow the run:

```sh
orabbit-client run submit --file ./run.yaml
orabbit-client run watch <run-id>
```

When `auto_tune` is `false`, set `max_in_flight_tasks`, `fetch_limit`, and at
least one of `planned_tasks` or `chunk_size`. FlightSQL requires `source.sql`,
`incremental: false`, `auto_tune: false`, no `id_column`, and no manual planning
fields.

### Iceberg table options

The Iceberg YAML snapshot accepts table metadata and write policy options in
addition to `uri`, `bearerToken`, and `s3`:

```yaml
uri: http://catalog:8181
bearerToken: token

partition_spec:
  - source: created_at
    name: created_day
    transform: day

sort_order:
  - source: created_at
    direction: desc
    null_order: nulls_last

schema_evolution: additive
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
  mode: merge-on-read

credential_vending:
  enabled: true
  required: true
```

Partition transforms support `identity`, `year`, `month`, `day`, `hour`,
`bucket[N]`, and `truncate[N]`. Schema evolution is `strict` by default;
`additive` adds optional columns and permits Iceberg-compatible type promotion.
Upsert requires Iceberg format v2 and non-null, unique keys in every incoming
run. When credential vending is required, static S3 credentials are not passed
to the catalog-backed table filesystem. Partitioned registration rewrites the
committed source Parquet stream into Iceberg-managed partitioned files; the
unpartitioned path remains zero-copy.

Cancel a run explicitly:

```sh
orabbit-client run cancel <run-id>
```

Pressing Ctrl+C while watching only stops the client; it does not cancel the
run.

## HTTP API

The API listens on port 9100 by default and returns JSON except for health text
and SSE streams. There is no generated OpenAPI document in this repository.

When `ORABBIT_HTTP_AUTH_TOKEN` is set, send:

```http
Authorization: Bearer <token>
```

`/healthz` and `/ready` remain unauthenticated. The current HTTP model has two
access classes: those public probe routes, and one bearer-authenticated class
covering both read-only API/SSE access and privileged administration. There is
no separate read-only token or per-user identity yet. Authentication failures
return only a generic `unauthorized` response.

The default HTTP and gRPC listeners bind to `127.0.0.1`. A non-loopback HTTP
listener is accepted only when `ORABBIT_HTTP_AUTH_TOKEN` is set and must sit
behind a trusted TLS-terminating proxy or tunnel; the built-in HTTP listener
does not terminate TLS. A non-loopback gRPC listener is accepted only when
`ORABBIT_WORKER_AUTH_TOKEN` is set. The master validates the bearer credential
on every worker-facing RPC; health checks remain unauthenticated.
`ORABBIT_GRPC_INSECURE=true` disables encryption, not worker authentication,
and is suitable only for loopback or a trusted private container/VPN network.
Any public gRPC endpoint must use both worker authentication and TLS.

### Core routes

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/healthz`, `/ready`, `/status` | Liveness, durable-leader readiness, and master/leadership status |
| `GET` | `/metrics` | Bounded-label Prometheus lifecycle metrics |
| `GET` | `/workers` or `/api/workers` | Active workers; use `?all=true` for all |
| `GET`, `POST` | `/connections` | List or create connections |
| `GET`, `PUT`, `DELETE` | `/connections/{id}` | Read, replace, or delete a connection |
| `GET`, `POST` | `/jobs` | List or create jobs |
| `GET`, `PUT`, `DELETE` | `/jobs/{id}` | Read, replace, or delete a job |
| `POST` | `/jobs/{id}/runs` | Start a stored job |
| `GET` | `/runs`, `/runs/{id}` | List runs or read one run |
| `POST` | `/runs/{id}/cancel` | Cancel a run |
| `GET` | `/runs/{id}/progress` | Aggregated run progress |
| `GET` | `/runs/{id}/events` | Persisted run events |
| `GET` | `/runs/{id}/events/stream` | Run-scoped SSE |
| `GET` | `/runs/{id}/artifacts` | Committed artifact metadata |
| `GET` | `/api/runs/{id}/diagnosis` | Redacted lifecycle diagnosis and suggested next action |
| `POST` | `/api/runs/{id}/recover` | Narrow audited recovery request with `action` and `reason` |
| `POST` | `/runs/{id}/registration/cancel` | Cancel Iceberg registration |
| `GET` | `/sse?run_id={id}` | General replay/live event stream |
| `GET` | `/api/source-engines` | Connector capabilities |
| `POST` | `/api/runs/validate` | Validate guided submission input |
| `POST` | `/api/runs/submit` | Validate, upsert, and start a guided run |
| `GET` | `/api/runs` | Alias for run listing |
| `POST` | `/api/jobs/{id}/runs` | Start a job with optional mode/Iceberg overrides |
| `POST` | `/api/maintenance/submit` | Submit a maintenance operation |

Example:

```sh
curl -sS \
  -H "Authorization: Bearer $ORABBIT_HTTP_AUTH_TOKEN" \
  http://localhost:9100/runs
```

`/healthz` means only that the HTTP process is alive. `/ready` additionally
requires a queryable database and a fresh durable leadership assertion against
SQLite. A cached `LEADER` status cannot authorize mutating work after its lease
expires.

```sh
orabbit-client run diagnose <run-id> --master-http http://localhost:9100
orabbit-client run recover <run-id> \
  --master-http http://localhost:9100 \
  --action reconcile_commit \
  --reason "live reconciliation is overdue"
```

When `ORABBIT_HTTP_AUTH_TOKEN` is configured, the CLI sends it automatically.
Recovery actions are state-checked, idempotent, and recorded in the audit log
and run events. Registration replay is refused after a durable catalog receipt.

### Remote operations routes

The master also exposes a control-panel API for registered SSH servers:

- `GET|POST /servers`
- `GET|PATCH|DELETE /servers/{id}`
- `POST /servers/{id}/ssh/test`
- `POST /servers/{id}/project/validate`
- `GET /servers/{id}/system`
- `GET /servers/{id}/docker`
- `GET /servers/{id}/containers`
- `POST /servers/{id}/containers/{container}/actions/{start|stop|restart}`
- `GET /servers/{id}/containers/{container}/logs`
- `GET /servers/{id}/containers/{container}/logs/stream`
- `GET /servers/{id}/configs`
- `GET|PUT /servers/{id}/configs/{config}`
- `POST /servers/{id}/configs/{config}/validate`
- `GET|POST /deployments`
- `GET /deployments/{id}` and `/deployments/{id}/stream`
- `GET /executions/{id}` and `/executions/{id}/stream`

These operations execute allowlisted actions on remote hosts. SSH credentials
and stored configuration versions require `ORABBIT_MASTER_KEY`.

### gRPC service

Workers use the `orabbit.v1.ControlPlane` service on port 9102:
`RegisterWorker`, `Heartbeat`, `RequestTask`, `RenewTaskLease`,
`AcquireUploadCapacity`, `ReleaseUploadCapacity`, `ReportTaskProgress`, and
`ReportTaskResult`. When configured, the worker sends
`ORABBIT_WORKER_AUTH_TOKEN` as gRPC `authorization: Bearer ...` metadata on
every call. See
[proto/controlplane.proto](proto/controlplane.proto) for the wire contract.

## Database and migrations

The master uses one SQLite database. `internal/db/migrate.go` automatically
applies versioned migrations when `db.Open` runs; there is no separate migration
CLI or seed command.

Major table groups include:

- connections, jobs, runs, tasks, task attempts, events, artifacts, high-water
  marks, and workers
- audit log
- Iceberg registrations, attempts, receipts, and reconciliation attempts
- multipart uploads and canceled-object cleanup candidates/attempts
- master leadership and leadership history
- remote servers, encrypted credentials, command executions, deployments, and
  configuration versions

Only one master process may use a given local database identity. The database
uses durable leadership records and mutation fencing in addition to a local
process lock.

Back up the SQLite database together with its `-wal` and `-shm` files using a
SQLite-aware backup procedure. Preserve `ORABBIT_MASTER_KEY` separately.

## Development

```sh
make build       # go build ./...
make test        # go test ./...
make vet         # go vet ./...
make lint        # golangci-lint run
```

Formatting is standard `gofmt`; the lint configuration also checks `gofmt`,
`ineffassign`, and `unused`:

```sh
gofmt -w path/to/changed_file.go
golangci-lint run
```

Run a single package or test:

```sh
go test ./internal/planner
go test ./internal/grpc -run TestName -v
```

Tests are package-level unit and integration-style tests using temporary SQLite
databases and test servers. There is no committed CI workflow and no enforced
coverage threshold.

The generated files in `internal/grpcpb` correspond to
`proto/controlplane.proto`; no protobuf generation target or pinned generator
tooling is currently provided.

Worker/master compatibility is an exact protocol-version contract. See
[Worker protocol compatibility](docs/WORKER_PROTOCOL_COMPATIBILITY.md) for the
accepted version, fail-closed matrix, rolling-upgrade order, deprecation
policy, and protobuf reservation rules.

For connector fixtures:

```sh
docker compose -f docker-compose.ex-db.yml up -d
./seed-databases.sh
```

The seed script covers PostgreSQL, MySQL, MariaDB, Oracle, MongoDB, SQL Server,
Cassandra, and ClickHouse containers. It does not seed Trino.

## Deployment

The repository supports split Docker deployments:

```sh
cp .env.master.example .env.master
cp .env.worker.example .env.worker
cp .env.minio.example .env.minio
# Edit every placeholder and secret.

# The master and worker scripts create the shared external Docker network.
./deploy-minio.sh
./deploy-master.sh
WORKER_SCALE=2 ./deploy-worker.sh
```

Set `ORABBIT_ENV_FILE` to use a different environment file. The deployment
scripts run Docker Compose from the repository checkout and build daemon images
locally.

For production:

- set distinct `ORABBIT_HTTP_AUTH_TOKEN`, `ORABBIT_WORKER_AUTH_TOKEN`, and
  `ORABBIT_MASTER_KEY` values
- leave port 9102 unpublished for workers on the shared Docker network
- for cross-host gRPC, use a private VPN/tunnel; if the endpoint is public,
  enable gRPC TLS with a master certificate/key and worker CA settings
- restrict port 9100 and any intentionally published 9102, source databases,
  and object storage at the network layer
- use unique worker IDs
- persist and back up master SQLite and object-storage data
- replace all example MinIO and catalog credentials
- review cleanup dry-run and retention settings before enabling deletion

The repository does not contain a CI/CD pipeline, Kubernetes manifests,
Terraform, or an automated release process.

## Troubleshooting

### The client cannot find daemon binaries

Build/install `orabbit-master` and `orabbit-worker` next to `orabbit-client`, or
pass explicit `--master-bin` and `--worker-bin` paths.

### A worker registers but cannot complete tasks

Verify worker connectivity to the source and S3 target, not only to the master.
DSNs stored on the master are delivered to workers, so `localhost` usually
refers to the worker itself.

### API calls return 401

Export the same `ORABBIT_HTTP_AUTH_TOKEN` used by the master or send the bearer
header manually. Health and readiness endpoints intentionally do not require
the token.

### The master cannot decrypt stored secrets

Restore the exact `ORABBIT_MASTER_KEY` used when the values were written. A new
key cannot decrypt existing AES-GCM blobs.

### TLS startup or worker connection fails

When master `-insecure=false`, both `-tls-cert` and `-tls-key` are required.
Workers must use `-insecure=false`, a trusted `-tls-ca`, and, when needed,
`-tls-server-name`.

### A canceled run leaves objects temporarily

Canceled objects are quarantined and cleanup defaults to dry-run for seven
days. Review the cleanup records and explicitly change the dry-run setting only
after validating the target and retention policy.

### Root Docker Compose fails on the PostgreSQL mount

The committed compose file references the absent `docker/postgres/initdb`
directory. Create the directory or remove the mount.

### Iceberg registration fails

For `engine=ice`, ensure the master can execute the configured `ice` binary and
read its configuration. For REST registration, verify catalog reachability,
warehouse/S3 credentials, namespace/table naming, and partition keys. Consult
[docs/query-mode.md](docs/query-mode.md) and the resilience documents under
`docs/` for implementation details and current recovery semantics.

## Contributing

Before submitting a change:

1. Keep changes focused and add tests for behavior changes.
2. Run `gofmt` on modified Go files.
3. Run `go test ./...`, `go vet ./...`, and `golangci-lint run`.
4. Update this README, protobuf source/generated files, examples, and design
   notes when their contracts change.
5. Never commit real DSNs, access keys, bearer tokens, TLS keys, or environment
   files.

No issue template, pull-request template, or separate contribution policy is
currently included.

## License

Copyright 2026 MindWise Levon Ghukasyan.

Licensed under the [Apache License 2.0](LICENSE). See [NOTICE](NOTICE) for the
project attribution notice.
