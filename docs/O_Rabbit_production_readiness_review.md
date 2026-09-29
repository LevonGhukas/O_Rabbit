
I reviewed O_Rabbit as a production-readiness check. It's more mature than most systems at this stage. It already has fencing tokens, durable commit intents, idempotent manifests, artifact SHA-256 checks, leadership epochs and quarantined cleanup. `go build`, `go vet` and `go test -race ./...` all pass.

The system is ~75k lines, so I went deep on the control plane: master bootstrap, auth, crypto, HTTP, gRPC, commit, planner, SQLite store, worker loop, SSH ops and deployment. I only spot-checked the connectors, the type system, Iceberg internals and the CLI. I didn't change any code.

## 🔴 Blockers (fix before production)

**1. HTTP auth fails open.** In [server.go:215](internal/http/server.go:215), `withAuth` skips the token check for any path that isn't in the hand-kept allowlist `isKnownAPIPath` ([middleware.go:58](internal/http/middleware.go:58)).
- `/api/maintenance/submit` is registered but not on the list, so it is unauthenticated today.
- Any route added later is public unless someone remembers to update the list.
- Fix: authenticate everything by default and exempt only `/healthz` and `/ready`.
**Status:** DONE

**2. `/api/maintenance/submit` is a stub that claims success.** In [api_maintenance.go](internal/http/api_maintenance.go) it logs the request and returns `"status":"submitted"` without doing anything. Remove it or return 501 until compact/vacuum really exist.
**Status:** DONE

**3. Secrets are stored and sent in plaintext by default.**
- If `ORABBIT_MASTER_KEY` is unset, connection secrets are stored as plaintext ([plain.go](internal/crypto/plain.go)). Production should refuse to start without a key.
- `Decrypt` also accepts unversioned and plaintext blobs even when a key is set, and plaintext secrets are never migrated to encrypted form.
- `runs.registration_config_json` stores the Iceberg `RunConfig` as plain JSON. That includes the S3 `secret_access_key` and the catalog `bearer_token` ([icebergreg.go:78](internal/icebergreg/icebergreg.go:78)), and it bypasses the encryption entirely.
- Every task assignment sends the source DSN and S3 keys to the worker ([server.go:958](internal/grpc/server.go:958)). `ORABBIT_GRPC_INSECURE` defaults to `true`, and every `.env.*.example` binds `0.0.0.0` with insecure gRPC. So in the documented deployment, credentials cross the network in cleartext.
- Fix: default to TLS, and require it (ideally mTLS) whenever the listener is not loopback.
**Status:** DONE

**4. Worker identity and task authorization are not strongly bound.**
Workers currently present a caller-chosen worker_id, while authentication is based on shared credentials and generic mTLS trust. That means identity, authorization, and task ownership are not cleanly separated, and a compromised worker may impersonate another worker or receive credentials outside its intended scope.
Fix:
Introduce a durable, server-issued worker identity and bind it to the worker’s mTLS certificate. The master must derive the authenticated worker identity from the certificate and must not trust a request-provided worker ID for authorization. Use that identity for worker registration, heartbeats, leases, task ownership, and audit logs.
Then add a second part:
Reduce credential blast radius.
Do not treat all workers as equally trusted for all credentials. Prefer task-scoped, short-lived credentials where supported:
- S3: STS credentials scoped to the assigned bucket/prefix, or presigned operations
- Databases: read-only or temporary credentials where supported
- Fallback: long-lived credentials may be delivered only to the authenticated worker assigned that task, over mTLS
So the task becomes two concrete phases:
1. Worker identity redesign
   - server-issued immutable UUID
   - UUID embedded in certificate SAN
   - master extracts identity from TLS context
   - request worker_id is ignored or validated only as metadata
   - leases / heartbeats / assignments use authenticated identity
2. Task-scoped authorization
   - only the assigned worker receives that task’s secrets
   - S3 credentials should be scoped and temporary where possible
   - no worker should receive credentials unrelated to its task
**Status:** DONE

**5. A panic in any gRPC handler crashes the master.** [server.go:1673](internal/grpc/server.go:1673) installs only the auth interceptor, and grpc-go does not recover panics. Add a recovery interceptor like the HTTP one, and set keepalive and max-message-size options.
**Status:** DONE

**6. The master exits 0 on fatal errors.** In [cmd/master/main.go](cmd/master/main.go), recovery failures (committing runs, leases, registrations) and loss of leadership all `return` from `main`, which exits with code 0. `restart: on-failure` and Kubernetes treat that as a clean exit. Use `os.Exit(1)`.
**Status:** DONE

**7. Starting a run kills the job's in-flight run.** In [planner.go](internal/planner/planner.go), `CreateRunAndTasks` calls `FailRunningRunsForJob(... "superseded by new run")` before the active-dataset guard. A double-click, a CLI retry or a second scheduler tick silently fails a healthy run. The direct SQL in `failRunIDs` ([store.go:168](internal/db/store.go:168)) also:
- skips `task_attempts` and upload-capacity leases;
- emits no events;
- skips the leadership fence;
- registers no cleanup for objects already uploaded.

Fix: reject the new run with `DatasetBusyError` (that logic already exists) and require an explicit cancel.
**Status:** DONE

**8. Commit runs synchronously inside the last worker's `ReportTaskResult` call.** See [server.go:~610](internal/grpc/server.go).
- The commit re-downloads and re-hashes every artifact through the master, with a 30-minute budget.
- Its context comes from the RPC context, and the worker gives up after 5s per attempt and 15s overall ([result_reporting.go](cmd/worker/result_reporting.go)). Any real-sized run therefore has its commit cancelled mid-flight and relies on the 2-second reconciliation loop.
- That loop also picks up the same run immediately (`next_eligible_at IS NULL`), so two commits run at once with no claim or lease. The intent CAS stops corruption, but the losing commit can record false failures such as "state for run exists without durable intent" and use up retry attempts.
- Fix: move the run to COMMITTING and return; let a single leased committer do the work.
**Status:** DONE

## 🟠 High

9. **Commit integrity check doesn't scale.** It streams every Parquet object back through the master to re-hash it, so exports in the TB range bottleneck on one process's bandwidth. Use S3 checksums (`x-amz-checksum-sha256`) at upload time and a HEAD at commit.
   **Status:** DONE
10. **Runs don't snapshot their configuration.** `buildAssignment` and `commitRun` read the live job, connection and options each time. If someone edits a job or connection mid-run, workers can write under a different prefix, bucket or options than the commit expects. The README says runs snapshot their configuration; they don't.
   **Status:** DONE
11. **Parse errors are silently ignored.** Examples: `_ = json.Unmarshal(meta)`, `opts, _ := jobopts.Parse(...)`, and a missing endpoint silently becoming `http://localhost:9000` / `us-east-1`. In production these should be hard errors.
   **Status:** DONE
12. **Incremental cursors can lose rows.** Using an exclusive lower bound on the last high-water mark ([postgres.go:171](internal/connectors/postgres.go:171)) permanently skips late-committing rows and ties at the boundary (for example `updated_at` set at transaction start, or out-of-order sequences). Add a lookback window with dedupe or upsert, or at least document the limitation.
   **Status:** DONE
13. **Query mode is only "read-only" by denylist.** Its safety is a keyword denylist ([query_mode.go:518](internal/connectors/query_mode.go:518)). Side-effecting functions pass it (`pg_terminate_backend`, `dblink_exec`, `lo_import`), and so does the raw `WhereClause` interpolation. Enforce it at the database: open read-only transactions and document that a read-only database user is required.
   **Status:** DONE
14. **SSH host-key checking is off by default.** An empty `HostKeyFingerprint` accepts any host key ([ssh.go:236](internal/ops/ssh/ssh.go:236)), which allows a man-in-the-middle to capture SSH credentials. More broadly, remote Docker/deploy over SSH makes the ETL master a remote-execution service behind one bearer token. Consider moving it out of the core binary or putting it behind a separate role.
   **Status:** DONE
15. **The HTTP server lacks basic hardening.**
    - No `ReadHeaderTimeout`, `ReadTimeout` or `IdleTimeout`, which leaves it open to slow-client (slowloris) attacks.
    - `readJSON` uses `io.ReadAll` with no size limit.
    - `/runs` and `/connections` lists are not paginated.
    - `Shutdown(context.Background())` can hang forever on open SSE streams.
   **Status:** DONE

## 🟡 Medium: scalability and operations

16. **SQLite with `MaxOpenConns(1)` serializes everything.** Every worker RPC, SSE query, `/metrics` scan and list endpoint shares one connection. On top of that, `RequestTask` runs `AdmitPendingRuns` (a write transaction) on every poll from every worker. Expect contention past a handful of workers.
   **Status:** DONE

17. **The master is a single point of failure with no backup plan.** The leadership lease only protects against two processes on the same file. Document the single-master model, add SQLite backups (litestream or similar), and write a 
restore runbook.
   **Status:** DONE

18. **Nothing is ever cleaned out of the database.** There is no retention for `events`, `task_attempts`, artifacts or registration history, so the DB grows without bound.
   **Status:** DONE

19. **Timestamp comparisons are inconsistent.** Task leases compare with `julianday()`. Registrations, multipart uploads, canceled objects and commit reconciliation compare RFC3339Nano strings lexically. Those strings have variable-length fractions, so ordering within the same second is wrong. The impact is small, but standardize on one fixed-width format.
   **Status:** DONE

20. **Hard-coded limits.** The source query timeout is 2 hours ([worker main.go:1066](cmd/worker/main.go:1066)), so big partitions fail. Commit and registration timeouts and retry policies (5 attempts, 30s leases) are also hard-coded rather than configurable.
   **Status:** DONE

21. **Protocol quirks.**
    - Worker failure and cancel results are sent once, with no timeout or retry, so the task waits out its lease.
    - Multipart lifecycle messages are multiplexed through `ReportTaskProgress` via the magic string `Message == "MULTIPART_LIFECYCLE"`. They should be a real RPC.
    - Worker-supplied `Message`/`FieldsJson` is stored as events with no size cap.
    - The worker hard-codes `ProtocolVersion: 5` instead of using the shared constant.
   **Status:** DONE
22. **Failed runs leave orphaned objects.** Uploaded objects from failed or superseded runs don't appear to enter the canceled-object cleanup path.
   **Status:** DONE

23. **Cleanup deletion is off by default.** `CANCELED_OBJECT_CLEANUP_DRY_RUN=true` means it never deletes unless someone changes it. Keep the safe default, but document it clearly.
   **Status:** DONE

## 🔵 Hygiene

- **`fix.go` at the repo root** is a `package main` codemod that rewrites files under `internal/`. `go run .` would run it. Delete it, along with the committed `.DS_Store`.
  **Status:** DONE
- **Containers run as root**; add a `USER`.
  **Status:** DONE
- **CI** runs neither `-race` nor golangci-lint (the config already exists), and has no `govulncheck` or image scanning. Images should be tagged by SHA or semver, not just pushed from `main`.
  **Status:** DONE
- **Example `.env` files** ship `minioadmin/minioadmin` and `0.0.0.0` + insecure defaults. Examples tend to get copied into production.
  **Status:** DONE
- **Observability:** `/metrics` covers lifecycle state counts only. There are no latency or throughput histograms, no RPC metrics and no tracing.
  **Status:** DONE
- **Master key rotation:** there is no way to rotate the key or re-encrypt stored secrets.
  **Status:** DONE

## Suggested order
1. **Security (blockers 1–5, plus 14–15):** fail-closed auth, required TLS and master key, encrypt the registration config, gRPC panic recovery.
2. **Correctness (6–8, 10):** exit codes, no silent superseding of runs, async single-owner commits, per-run config snapshots.
3. **Scale (9, 16, 18):** S3 checksums instead of re-hashing, cut per-poll write transactions, add retention.
