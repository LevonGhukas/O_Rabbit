# Codebase Review & Implementation Roadmap

## 1. Executive Summary

O_Rabbit is distributed database-export control plane: master persists run state in SQLite, workers extract cursor-partitioned source data to Parquet, upload verified artifacts to S3, commit dataset state, optionally register Iceberg, and expose HTTP/CLI/SSH operations.

Health: substantial production-minded work exists: fencing, durable attempts, artifact verification, recovery paths, SQLite constraints, workspace hygiene, and broad unit coverage. `go test ./...` and `go vet ./...` pass. Main risks are data correctness claims not met, security-safe defaults missing, worker boot identity unused, and migrations not crash-atomic.

Priority: protect stored/in-transit credentials; repair strong-snapshot path; remove MSSQL dirty reads; make migrations restart-safe; then close API, observability, and documentation gaps.

## 2. System Architecture

```text
HTTP API / CLI
       |
master: HTTP + gRPC + planner + recovery + Iceberg/SSH operations
       |
SQLite: jobs, runs, tasks, attempts, artifacts, audit, leadership
       |
workers: source connector -> Arrow conversion -> rolling Parquet -> S3
       |
dataset manifest/state -> optional Iceberg registration
```

- `cmd/master`: configuration, singleton/leadership, HTTP/gRPC servers, recovery loops.
- `internal/db`: SQLite schema, migrations, state machine, fencing, audit, cleanup records.
- `internal/grpc`: worker control plane, task result processing, durable commit/reconciliation.
- `cmd/worker`: polling, lease renewal, extraction, Parquet rolling, upload/reporting.
- `internal/connectors`, `arrowio`, `planner`: source validation, cursor range planning, Arrow conversion.
- `internal/s3io`, `artifact`, `icebergreg`: object integrity, upload lifecycle, publication/catalog work.
- `internal/http`, `internal/orabbitcli`, `internal/ops`: user API, local CLI, remote SSH/Docker/config operations.

## 3. Critical Findings

### ISSUE-001 — Connection secrets default to plaintext at rest

Severity: P1  
Category: Security / Configuration  
Location: `internal/crypto/plain.go:17,52`; `internal/http/server.go:381`; `cmd/master/main.go:35`; `docker-compose.yaml:105-108`
Status: ✅ Completed

Problem

Empty `ORABBIT_MASTER_KEY` is accepted. `crypto.Encrypt` writes `[version 0][plaintext]`; connection create/update uses it for source DSNs and S3 credentials. Default Compose master does not set key. SSH credentials correctly require key, but connection credentials do not.

Why it matters

SQLite volume/database backup/host read exposes database passwords, S3 keys, session tokens, and DSNs. This conflicts with API comment “encrypted at rest.”

Recommended fix

Require master key before create/update of any connection with secret material; refuse remote/non-development startup without it. Keep explicit, documented migration command for legacy version-0 blobs, then reject new version-0 writes. Do not silently re-encrypt with a new key.

Validation

Regression tests: empty-key connection POST/PUT returns actionable 4xx; non-empty key produces version-1 AES-GCM blob; legacy migration preserves decryptability; Compose startup requires injected key.

Implementation

- Connection create/update and API run submission now reject secret writes without `ORABBIT_MASTER_KEY`; legacy version-0 blobs remain decryptable.
- Default Compose now requires a stable master key; example labels it required for stored connection secrets.
- Added HTTP regression coverage for rejection, AES-GCM/version-1 storage, plaintext absence, and legacy read compatibility.

### ISSUE-002 — Strong snapshot never reaches worker extraction; PostgreSQL snapshot leases leak

Severity: P1  
Status: ✅ Completed  
Category: Functional correctness / Data integrity / Reliability  
Location: `internal/planner/planner.go:655-671,795,823`; `cmd/worker/main.go:45-64,1036-1058`; `internal/connectors/postgres.go:59-105,208-239`

Problem

Planner writes `snapshot_context` into task JSON. Worker `partitionSpec` has no `SnapshotContext`, and its `CursorQuery` omits it. `STRONG_SNAPSHOT` therefore executes ordinary reads. Separately, PostgreSQL `ExportSnapshot` retains transaction/connection through unconditional `time.Sleep(24*time.Hour)` with no run completion, cancellation, or process lifecycle release. Connector comments acknowledge imported query transaction is not committed/rolled back or connection-closed after rows close.

Why it matters

Advertised strong-consistency exports can contain cross-partition gaps/duplicates under source writes. Each planned PostgreSQL strong snapshot can hold MVCC history and server resources for 24 hours; enough runs can cause vacuum bloat or connection exhaustion.

Recommended fix

First either disable/reject `STRONG_SNAPSHOT` until fully implemented, or implement end-to-end: add snapshot field to worker spec, propagate into `CursorQuery`, own imported transaction+connection in a closable reader, and register a run-scoped snapshot lease released on terminal run/cancel/failure. Bound duration; make lease expiration fail/reconcile run rather than silently downgrade consistency. Test PostgreSQL with concurrent writes.

Validation

Integration test proves all partitions see one snapshot and `SET TRANSACTION SNAPSHOT` occurs. Cancellation/completion test proves exporting and importing sessions close. Test missing/expired snapshot fails run, never falls back to eventual read.

### ISSUE-003 — MSSQL export and planning use `NOLOCK`

Severity: P1  
Status: ✅ Completed  
Category: Data correctness  
Location: `internal/connectors/mssql.go:162,251,337`

Problem

Table extraction always appends `WITH (NOLOCK)`; count/bounds fallback also use it. `NOLOCK` permits dirty reads, rows twice, skipped rows, and inconsistent cursor bounds. No job option documents or accepts this semantics.

Why it matters

Completed Parquet can be internally incorrect despite valid hashes and successful durable commit. Incremental high-water marks can then advance past rows never exported.

Recommended fix

Remove unconditional `NOLOCK`. Default to database normal isolation. If operators need dirty reads, expose explicit `read_consistency=dirty` option, show warning in API/CLI, and prohibit it with incremental/high-water-mark or strong-consistency modes. Prefer SQL Server snapshot isolation when source supports/configures it.

Validation

Connector SQL tests assert no hint by default. Integration test with concurrent update/insert proves default export no dirty/uncommitted rows; validation rejects unsafe mode combinations.

### ISSUE-004 — Worker process boot identity omitted from every control-plane RPC

Severity: P1  
Status: ✅ Completed  
Category: Reliability / Observability / Fencing  
Location: `proto/controlplane.proto:27-190`; `cmd/worker/main.go:219,256,314,368,391,480,558,692,772,790,895,967`; `internal/db/store.go:892-1044`; `internal/db/attempts.go:91-184`

Problem

Protocol and DB fence attempts by `worker_id` plus `boot_id`. Worker generates `workerInstanceID`, but sends no `BootId` in register, heartbeat, request, lease, capacity, progress, multipart, or result RPCs. All active worker instances are stored under empty boot ID; `worker_instances.boot_id` primary-key upsert overwrites prior instance state.

Why it matters

Per-process observability and restart identity are false. Same configured worker ID across restart/replica cannot be distinguished at protocol boundary; intended boot-level fencing has no effect.

Recommended fix

Thread one generated immutable boot ID through every request and helper signature. Include hostname, PID, and version on registration. Reject empty boot ID for protocol version 5 after compatible rollout; migrate old workers through protocol bump or temporary dual acceptance.

Validation

End-to-end two workers/restart test: distinct `worker_instances` rows, attempts retain correct boot ID, stale pre-restart request rejected, all worker RPCs contain same non-empty boot ID.

### ISSUE-005 — SSH host verification accepts any key when fingerprint absent

Severity: P1  
Category: Security  
Location: `internal/ops/ssh/ssh.go:145-149,236-250`; `internal/db/controlpanel.go:716-718`
Status: ✅ Completed

Problem

`matchesFingerprint` returns true for empty expected fingerprint. Credential validation permits empty fingerprint. Master can then send SSH password/private key and run deployment/config commands to first network responder.

Why it matters

Man-in-the-middle can steal SSH credentials and receive remote deployment authority.

Recommended fix

Require SHA-256 host key fingerprint for persisted credentials. Provide explicit enrollment/test endpoint which displays observed fingerprint but does not persist/use credential until caller confirms it; reject blank/legacy MD5 for new writes. Existing blank records need operator re-enrollment.

Validation

Tests: blank fingerprint rejected for save/use; wrong key rejected; enrollment requires explicit confirmation; existing records are diagnosed as insecure, not silently trusted.

Implementation

- Persisted SSH credentials require valid `SHA256:<base64 digest>` fingerprint; runtime matching rejects blank pins.
- Legacy unpinned records now fail with explicit operator-action diagnostic. Enrollment UX intentionally deferred; operator must obtain and save fingerprint out of band.
- Added credential validation, legacy-record, blank-pin, mismatch, and correct-pin regression coverage.

### ISSUE-006 — Migration schema change and version recording are separate operations

Severity: P1  
Category: Reliability / Database  
Location: `internal/db/migrate.go:680-727`
Status: ✅ Completed

Problem

Each migration SQL/apply runs first; `schema_migrations` insert runs afterward outside transaction. Crash or I/O failure between them leaves schema changed but version absent. On restart `ALTER TABLE ... ADD COLUMN` or table/index migration can rerun and fail, blocking master startup.

Why it matters

Power loss/kill during upgrade can turn a recoverable deployment interruption into manual SQLite repair and control-plane outage.

Recommended fix

Apply each migration and version insert in one SQLite transaction. For inherently idempotent/legacy migration, inspect exact schema and make apply idempotent before recording. Add migration lock/transaction semantics appropriate to SQLite single writer.

Validation

Failure-injection tests after DDL-before-version and after version-write; rerun `Migrate` must converge with exactly one version row and expected schema/data.

Implementation

- Each migration now executes schema work and version insertion inside one SQLite transaction.
- Existing partial version-5 column state remains recoverable through its schema-aware migration function.
- Added partial-version-5 restart/convergence regression coverage; repeat migration remains safe.

## 4. Other Findings

### ISSUE-007 — HTTP JSON bodies are generally unbounded

Severity: P2  
Status: ✅ Completed  
Category: Reliability / Security  
Location: `internal/http/server.go:318-342`; direct decoders in `internal/http/operability.go:44`, `internal/http/api_maintenance.go:25`

Problem

`readJSON` and `readOptionalJSON` call `io.ReadAll`; several handlers use unrestricted `json.Decoder`. One run/config/connection request can allocate arbitrary memory.

Why it matters

Authenticated client or leaked bearer token can memory-exhaust master; reverse proxy limits may not exist in local/Docker deployment.

Recommended fix

Apply route-specific `http.MaxBytesReader`, decoder `DisallowUnknownFields` where contracts stable, and reject trailing JSON. Use larger bounded limit only for config content.

Validation

Oversize/trailing/unknown-field API tests return 413/400 without handler allocation beyond limit.

### ISSUE-008 — Metrics, status, readiness remain public when HTTP auth enabled

Severity: P2  
Status: ✅ Completed  
Category: Security / Observability  
Location: `internal/http/server.go:116-143`; `internal/http/middleware.go:57-84`

Problem

Auth intentionally bypasses `/healthz`, `/ready`, and any path `!isKnownAPIPath`; `isKnownAPIPath` includes `/metrics` and `/status`, so these are public. `/status` exposes process ID, addresses, DB identity, and leadership data; metrics expose workload state.

Why it matters

Published master port leaks topology and operational metadata. Health checks need public access; metrics/status usually do not.

Recommended fix

Keep `/healthz` public. Make `/ready`, `/status`, `/metrics` policy-configurable, secure by default for non-loopback bind, and document probe authentication/network policy.

Validation

Auth-enabled handler tests: health allowed; status/metrics/ready follow selected policy; no sensitive DB path in public endpoint.

### ISSUE-009 — Request-level SQL fragments are concatenated without a safe contract

Severity: P2  
Status: ✅ Completed  
Category: Security / Data correctness  
Location: `internal/http/api_runs.go:26-36`; `internal/connectors/{mssql,postgres,mysql,mariadb,clickhouse,trino}.go` `QueryCursor`; `internal/connectors/query_mode.go:421-459`

Problem

`where_clause` is client text interpolated into SQL. Table-mode code does not validate it; read-only scanner only protects query-mode source SQL. `select_columns` also becomes SQL projection through connector builders. Current bearer token grants unrestricted control, but API contract does not label inputs as trusted SQL nor enforce read-only source users.

Why it matters

If token reaches lower-trust client, it can issue source-side unintended SQL where driver supports multi-statements; even read-only fragments can bypass intended extraction filter.

Recommended fix

Choose contract explicitly: safest is parameterized structured filters plus identifier-only projections. If arbitrary SQL remains product requirement, require source DB read-only account, reject multi-statement/comments in every interpolated fragment, document trusted-admin-only semantics, and audit query hash plus actor. Do not claim generic injection prevention from current query-mode scanner.

Validation

Cross-driver tests reject semicolons/comments/DML fragments; integration tests prove least-privilege source account cannot mutate data.

### ISSUE-010 — `STRONG_SNAPSHOT` validation lacks end-to-end acceptance test; several source semantics need real-engine coverage

Severity: P2  
Status: ✅ Completed  
Category: Testing / Data engineering  
Location: `internal/planner/planner_test.go`; `internal/connectors/*_test.go`; `cmd/worker/main_test.go`

Problem

Unit suite is broad but mostly mocks SQL/S3/gRPC. No real PostgreSQL snapshot export/import lifecycle, no MSSQL concurrent-read isolation test, no full master-worker-S3 commit test across crash/retry, and no protocol test that populates boot ID.

Why it matters

Most severe bugs above compile and unit tests pass. Distributed data correctness depends on real database/driver transaction behavior.

Recommended fix

Add tagged Docker integration suite: PostgreSQL/MinIO mandatory; MSSQL optional CI lane; direct worker/master protocol tests; fault injection at upload/result/commit boundaries. Keep fast unit suite unchanged.

Validation

CI reports integration matrix separately and exercises every P1 regression scenario.

### ISSUE-011 — Documentation links point to absent operational documents

Severity: P3  
Status: ✅ Completed  
Category: Documentation  
Location: `README.md:747,844`; `docs/`

Problem

README links `docs/WORKER_PROTOCOL_COMPATIBILITY.md` and `docs/query-mode.md`; neither exists. `docs/technical_system_review.md` references absent `docs/parquet_file_rolling_validation.md`.

Why it matters

Operators cannot verify protocol rollout rules or query semantics; new contributors hit dead links.

Recommended fix

Restore referenced docs or remove links. Document protocol version rollout, source consistency semantics, security-required environment variables, and measured/manual Parquet validation status.

Validation

Markdown link checker in CI; README procedures run against current Compose files.

## 5. Functional Gaps

- `STRONG_SNAPSHOT` is accepted/planned but extraction omits snapshot context: ISSUE-002.
- MSSQL default exports advertise repeatable/parallel behavior while `NOLOCK` makes dataset result non-repeatable: ISSUE-003.
- Boot-aware worker lifecycle exists in proto/schema but production worker never activates it: ISSUE-004.
- Maintenance endpoint returns `submitted` while comment states it only simulates dispatch: `internal/http/api_maintenance.go:18-45`. Needs verification: endpoint may be intentionally preview-only; if public API promises execution, return `not implemented`/501 until durable executor exists. Track as part of ISSUE-007 contract hardening, not separate severity escalation.

## 6. Data Engineering Assessment

Good: ordered cursor bounds are parameterized; attempts are fenced; artifacts carry size/SHA/schema identity; S3 verification reads object bytes; run-scoped object keys and durable manifest/state commit reduce overwrite risk; cleanup/reconciliation tables provide recovery records; SQLite foreign keys and uniqueness constraints are extensive.

Risks: source read consistency is unsafe for MSSQL; strong PostgreSQL consistency is broken; incremental watermark correctness therefore needs source-specific integration proof. Planner/state preserves query hash and source mode, which is good lineage. Schema/type conversion tests exist, but no runtime source schema-drift policy is evident; incompatible drift fails worker rather than producing explicit run diagnosis. Needs verification: define expected policy (fail-fast likely correct) and record source schema fingerprint in run metadata.

Pipeline reruns: task artifacts use unique object keys and durable commit identity, good. Failed/canceled object cleanup is conservative. Migration reruns are not safe after mid-migration interruption: ISSUE-006.

## 7. Testing Gaps

- ISSUE-001: encryption-required API and legacy-blob upgrade tests.
- ISSUE-002: real PostgreSQL snapshot/cancel/resource-release integration test.
- ISSUE-003: SQL assertion plus concurrent MSSQL transaction integration test.
- ISSUE-004: actual worker RPC request capture/end-to-end boot-ID fencing test.
- ISSUE-005: blank fingerprint forbidden/enrollment test.
- ISSUE-006: DDL/version-record crash-window migration convergence test.
- ISSUE-007/008/009: HTTP body limits, auth policy, and SQL-fragment contract tests.
- Full E2E: master + worker + MinIO extract, worker crash after upload, result-report retry, master restart during commit, and exact final manifest/state assertion.

## 8. Performance Opportunities

- Potential future concern — `internal/s3io/uploader.go:119-169`: each uploaded artifact performs `HeadObject` and then full `GetObject` SHA-256 read, including when provider checksum exists. This is deliberate integrity tradeoff, not change now. Benchmark real object sizes/cost first; retain full read unless provider checksum guarantees meet product integrity requirement.
- Likely bottleneck — `internal/db/store.go:34-35`: SQLite uses one connection, intentional to prevent write locks. Under many workers, progress/event writes serialize and may delay leases. Measure `SQLITE_BUSY`, RPC latency, event rate before considering batching/coalescing progress updates. Do not replace SQLite or add queue without evidence.
- Potential future concern — `cmd/worker/main.go:850-914`: all rolled files for one task upload concurrently. File count can be large for tiny target file size. Bound per-task fan-out or reuse uploader `maxConcurrency` after workload measurement; global capacity alone limits tasks, not goroutines/files.

## 9. Security & Reliability

P1: plaintext connection secrets (ISSUE-001), unverified SSH host keys (ISSUE-005), insecure strong snapshot/data correctness (ISSUE-002/003), and non-atomic migrations (ISSUE-006).

P2: unbounded request memory, public operational metadata, and trusted SQL fragment ambiguity (ISSUE-007/008/009).

Good controls to retain: constant-time bearer comparisons, loopback auth checks, gRPC worker auth, random fencing tokens, AES-GCM when configured, S3 SHA verification, managed temp-root validation, durable cleanup quarantine, and audit tables. Authentication is one bearer token with no authorization roles; acceptable for single trusted operator only. Needs verification: intended multi-user/control-panel deployment. If multi-user, add identity/roles before exposing operations.

## 10. Cleanup Opportunities

- Fix documentation drift first (ISSUE-011); do not refactor package layout.
- `cmd/worker/main.go` is large and mixes process lifecycle, RPC helpers, extraction, upload, and telemetry. After P1 fixes, extract narrow request-builder and task-executor units only where tests benefit.
- Repeated best-effort JSON marshal errors are ignored in telemetry paths. Low risk because payloads are simple; use typed event structs only if future fields become non-serializable.
- Generated protobuf files should remain generated; do not hand-edit.

## 11. Implementation Roadmap

### Phase 0 — Safety / Critical Fixes

No confirmed P0 findings.

1. ✅ ISSUE-001 — Require encrypted connection-secret writes and Compose key configuration. Completed. Validation: HTTP secret-write and legacy-compatibility regressions.
2. ✅ ISSUE-005 — Require pinned SHA-256 SSH host fingerprints. Completed. Validation: credential and live SSH pinning regressions.

### Phase 1 — Correctness & Data Integrity

1. ✅ ISSUE-002 — Disable broken `STRONG_SNAPSHOT` or complete propagation/lifecycle. Reason: current advertised mode silently violates consistency. Dependencies: none. Risk: high; touches planner/worker/connector transaction ownership. Validation: PostgreSQL concurrent-write integration regression.
2. ✅ ISSUE-003 — Remove default MSSQL `NOLOCK`; validate any explicit dirty-read option. Reason: prevents silent missing/duplicate/dirty data. Dependencies: source-consistency contract from ISSUE-002. Risk: medium; possible source blocking/performance change. Validation: SQL and concurrent-source tests.
3. ✅ ISSUE-004 — Send/enforce boot ID everywhere. Reason: make instance fencing/observability real. Dependencies: protocol compatibility decision. Risk: medium; rolling worker upgrade. Validation: mixed-version and stale-instance tests.

### Phase 2 — Reliability

1. ✅ ISSUE-006 — Transactional, restart-safe migrations. Completed. Validation: partial-version restart and repeated migration tests.
2. ✅ ISSUE-007 — Bound/validate HTTP bodies. Reason: master availability under malformed/large requests. Dependencies: route payload limits. Risk: low. Validation: 400/413 route tests.
3. ✅ ISSUE-008 — Harden operational endpoint auth policy. Reason: minimize public metadata. Dependencies: deployment probe requirements. Risk: medium; health monitoring configuration. Validation: auth policy integration tests.

### Phase 3 — Architecture & Maintainability

1. ✅ ISSUE-009 — Define structured filters or explicit trusted-SQL contract. Reason: safe source boundary and clear ownership. Dependencies: API compatibility decision. Risk: medium/high; client payload contract. Validation: cross-driver safety suite and docs.
2. ✅ ISSUE-011 — Repair missing docs and protocol rollout guide. Reason: operations depend on these contracts. Dependencies: decisions from ISSUE-004/008/009. Risk: low. Validation: link checker.

### Phase 4 — Testing

1. ✅ ISSUE-010 — Build tagged integration/fault suite alongside every preceding fix. Reason: distributed correctness cannot be proven by mocks alone. Dependencies: Docker CI service availability. Risk: medium, CI time. Validation: repeatable PostgreSQL/MinIO matrix; optional MSSQL lane.

### Phase 5 — Performance

1. Measure opportunities in Section 8 before implementation. Reason: current integrity/SQLite choices are deliberate. Dependencies: production-like telemetry. Risk: low. Validation: benchmark and SLO comparison.

### Phase 6 — Cleanup

1. Split worker orchestration only after behavior tests exist. Reason: improve change isolation, not aesthetics. Dependencies: ISSUE-002/004/010. Risk: medium regression. Validation: unchanged integration results and race suite.

## 12. Recommended Implementation Order

1. ✅ ISSUE-001 — Encrypted connection secrets (Completed)
2. ✅ ISSUE-005 — Pinned SSH host key fingerprints (Completed)
3. ✅ ISSUE-006 — Transactional restart-safe migrations (Completed)
4. ✅ ISSUE-002 — Strong snapshot propagation & session lifecycle (Completed)
5. ✅ ISSUE-003 — Safe MSSQL isolation without NOLOCK (Completed)
6. ✅ ISSUE-004 — Boot ID identity in all control-plane RPCs (Completed)
7. ✅ ISSUE-007 — Bounded HTTP JSON bodies & strict decoding (Completed)
8. ✅ ISSUE-008 — Protected operational endpoints & DB path sanitization (Completed)
9. ✅ ISSUE-009 — SQL fragment validation & injection rejection (Completed)
10. ✅ ISSUE-010 — Boot fencing & snapshot integration testing (Completed)
11. ✅ ISSUE-011 — Operational documentation & protocol specifications (Completed)

## 13. Things That Should NOT Be Changed

- Keep master/worker pull model, SQLite durable state, and single SQLite writer. Fits current deployment and existing fencing/recovery design.
- Keep run-scoped object names, artifact SHA-256 verification, manifest/state commit identity, multipart/canceled-object quarantine, and Iceberg reconciliation. These directly reduce data-loss ambiguity.
- Keep connector registry and Arrow/Parquet boundaries. They are coherent and well-tested enough for current source set.
- Keep existing small service boundaries in `internal/ops`; shell quoting/path containment are concrete protections.
- Do not add Kafka, Airflow, Spark, Redis, microservices, extra databases, or broad cache layer. No evidence current workload requires them.
