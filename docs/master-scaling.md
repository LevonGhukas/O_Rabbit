# Master scaling limits

One master handles every worker call and holds all state in SQLite with a
single writer connection. This page records how far that was measured to go,
what limits it first, and how to configure within the tested range. The
numbers come from the load harness below and are reproducible.

## Supported operating envelope

Tested on one machine (Apple M4 Pro, 12 cores, 24 GB, Go 1.26.10, local SSD),
with the production defaults unless noted. A deployment inside every bound
below stays correct and responsive. Outside them it stays correct (no work is
lost in any tested profile) but slows down, as described in
[Saturation behavior](#saturation-behavior).

| Dimension | Tested, responsive | Limit observed |
| --- | --- | --- |
| Concurrently running tasks (`ORABBIT_MAX_ACTIVE_TASKS`) | up to 128 (`RequestTask` p99 ≤ 0.3 s) | 256: p99 0.77 s, SQLite writer queue of 6 |
| Workers connected | 256 | not reached; extra workers above the active-task cap only poll |
| Task length | 2 s tasks at up to 128 running (writer queue ≤ 1.7) | 0.1 s tasks at 64 running (~80 completions per second): writer queue 6.4, commit p95 1.2 s |
| Pending tasks queued on one master | up to ~10,000 | assignment cost grows with the backlog: 7 ms each at 10,000, 34 ms at 50,000 |
| Runs registered in the catalog | ~2 per second with a 0.2 s catalog | about 4 registrations every 2 s, slower when the catalog is slow; independent of `ORABBIT_CATALOG_WORK_LIMIT` |
| Progress reports | every 1 s (default) | every 0.1 s at 64 workers: writer queue 0.57 → 0.85, still fine |
| Catalog outage | 60 s of 45 s responses | registrations queue and drain afterwards; nothing fails |

Memory stayed under 140 MB and CPU under one core in every profile, for the
master and all simulated workers together. CPU and memory are not the
bottleneck; the single SQLite writer and the catalog registration loop are.

## What limits the master, in order

1. **Pending-task backlog: task assignment cost grows with it.** Each
   assignment picks the next task across all runs, ordered by how many tasks
   of each run are already running (fair sharing between runs). That order is
   computed on every call, so SQLite reads and sorts every eligible pending
   task (`AssignNextPendingTaskInPool`, `internal/db/attempts.go`), and the
   single writer is held meanwhile. Measured per assignment: 1.1 ms with
   1,000 pending tasks, 6.9 ms with 10,000, 34 ms with 50,000. At 50,000 the
   whole master can hand out about 29 tasks per second, and every heartbeat,
   progress report and result waits behind those assignments. This is the
   first limit for large exports planned into many tasks.
2. **Catalog registration throughput: about 2 runs per second.** The master's
   catalog loop (`cmd/master/main.go`) claims up to four registrations one
   after another every 2 seconds, plus one immediate attempt after each
   commit. With a 0.2 s catalog that is about 2 registrations per second
   (measured 2.1); with a 2 s catalog, about 0.5 per second (measured 0.5).
   Raising `ORABBIT_CATALOG_WORK_LIMIT` from 2 to 8 shortened the slow-catalog
   profile by only 14%, because the loop does not run claims in parallel. Data
   is committed long before it is registered when runs finish faster than
   that: in `many-small-runs`, 512 runs finished their tasks in seconds but
   took 4 minutes to register.
3. **SQLite writer queue: grows with running tasks and completion rate.**
   Every heartbeat, poll, progress report, lease renewal and result takes the
   one writer connection. The writer queue (average callers waiting) was 0.07
   at 8 workers, 0.57 at 64, 1.1 at 128 and 6.1 at 256 running tasks, where
   `RequestTask` p99 reached 0.77 s. Short tasks (0.1 s each, ~80 completions
   per second) reached the same queue of 6.4 with only 64 workers.
4. **Idle polling costs writes.** A `RequestTask` that finds nothing still
   writes the worker heartbeat, runs run admission and tries an assignment
   (0.12 ms on the writer). Workers above `ORABBIT_MAX_ACTIVE_TASKS`, and all
   workers between runs, pay this on every poll interval.

## Recommended configuration

- **Plan tasks that run for seconds, not milliseconds.** Size
  `target_rows_per_task` so a task takes at least a few seconds. With 0.1 s
  tasks, 64 workers already saturated the writer.
- **Keep pending tasks per master under ~10,000.** Split very large exports
  into several runs over time rather than one run with tens of thousands of
  tasks, until assignment no longer scans the backlog (proposal 1 below).
- **`ORABBIT_MAX_ACTIVE_TASKS`:** the default 64 is safe. Up to 128 was
  measured responsive. 256 works but every call is slower; raise it only with
  workers to match, and watch `RequestTask` latency.
- **`ORABBIT_CATALOG_WORK_LIMIT`:** leave it at 2 for now; raising it does not
  raise registration throughput with the current loop (limit 2 above).
- **Expect registration to trail commits** when more than about two runs
  finish per second, or when the catalog is slow. Alert on queue age with
  `orabbit_oldest_registration_age_seconds` (see the shipped Prometheus
  rules), not on the number of pending registrations.
- **Worker poll interval:** 1–2 s. Shorter adds idle-poll writes without
  making assignment faster.

## Saturation behavior

Every profile, including the saturated ones, finished with all runs
committed and registered, no task left running or pending, and no task
attempt retried. Saturation shows up as:

- higher latency of worker calls (writer queue), never rejected calls: no
  RPC returned an error and SQLite never reported `SQLITE_BUSY` in any
  profile, because callers queue for the one writer connection instead;
- registration lagging behind commits, visible as catalog queue age;
- after a catalog outage, the queue drains at the normal registration rate
  once the catalog answers again (60 s for 64 queued runs).

Work is not silently stranded because task leases (30 s) and registration
leases are renewed only while the call path is alive, and expired ones are
retried by the master's lease scans.

## Results

`elapsed` runs from the first worker request until every run is registered,
so it includes the catalog queue at the end; `tasks/s` is that end-to-end
rate. `writer queue` is the time callers waited for the SQLite writer per
second of wall time, which equals the average number of callers waiting.

| profile | workers | max active | tasks | runs | task work | elapsed s | tasks/s | peak active | RequestTask p50/p99 ms | progress p99 ms | result p99 ms | commit p95 ms | catalog queue age p95/max s | writer queue | CPU cores | heap MB | all published |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| smoke | 2 | 64 | 4 | 2 | 0.2s | 1 | 5 | 2 | 2 / 3 | 0 | 3 | 178 | 0.2 / 0.2 | 0.03 | 0.08 | 17 | yes |
| baseline | 8 | 64 | 128 | 16 | 2s | 36 | 4 | 8 | 5 / 12 | 9 | 13 | 364 | 4.1 / 4.1 | 0.07 | 0.07 | 26 | yes |
| workers-32 | 32 | 64 | 512 | 32 | 2s | 46 | 11 | 32 | 4 / 32 | 9 | 14 | 444 | 10.6 / 10.8 | 0.25 | 0.15 | 38 | yes |
| workers-64 | 64 | 64 | 1024 | 64 | 2s | 65 | 16 | 64 | 4 / 90 | 16 | 25 | 415 | 28.9 / 30.7 | 0.57 | 0.23 | 50 | yes |
| workers-128 | 128 | 64 | 2048 | 64 | 2s | 91 | 23 | 64 | 4 / 160 | 14 | 21 | 604 | 21.9 / 23.4 | 1.09 | 0.34 | 78 | yes |
| workers-128-active-256 | 128 | 256 | 2048 | 64 | 2s | 63 | 33 | 128 | 3 / 304 | 28 | 40 | 599 | 25.3 / 26.9 | 1.73 | 0.36 | 77 | yes |
| workers-256-active-512 | 256 | 512 | 4096 | 128 | 2s | 90 | 45 | 256 | 2 / 766 | 51 | 85 | 616 | 49.4 / 52.9 | 6.12 | 0.63 | 131 | yes |
| progress-100ms | 64 | 64 | 1024 | 64 | 2s | 61 | 17 | 64 | 3 / 92 | 10 | 17 | 414 | 23.9 / 25.8 | 0.85 | 0.42 | 123 | yes |
| short-tasks | 64 | 64 | 4096 | 128 | 0.1s | 50 | 81 | 64 | 9 / 140 | 0 | 55 | 1205 | 33.0 / 38.9 | 6.42 | 0.67 | 91 | yes |
| many-small-runs | 64 | 64 | 1024 | 512 | 0.5s | 243 | 4 | 64 | 2 / 20 | 0 | 18 | 362 | 223.7 / 239.4 | 0.26 | 0.37 | 54 | yes |
| catalog-2s | 32 | 64 | 256 | 128 | 0.5s | 253 | 1 | 32 | 3 / 13 | 0 | 12 | 269 | 237.0 / 250.7 | 0.11 | 0.16 | 51 | yes |
| catalog-2s-limit-8 | 32 | 64 | 256 | 128 | 0.5s | 217 | 1 | 32 | 2 / 15 | 0 | 9 | 264 | 199.6 / 211.6 | 0.11 | 0.15 | 37 | yes |
| catalog-outage-60s | 32 | 64 | 256 | 64 | 0.5s | 121 | 2 | 32 | 3 / 17 | 0 | 11 | 286 | 115.5 / 117.1 | 0.12 | 0.17 | 38 | yes |

All profiles: 0.2 s catalog and 5 ms object storage unless the name says
otherwise, progress every 1 s, idle poll every 1 s, catalog work limit 2.
`catalog-outage-60s` answers in 45 s for the first 60 s.

Cost of the busiest SQLite writes, per operation on the single writer
(`go test ./internal/db -run '^$' -bench Hotpath`):

| Operation | Worker call | Cost |
| --- | --- | --- |
| Assign a task, 1,000 / 10,000 / 50,000 pending | `RequestTask` | 1.07 / 6.9 / 34 ms |
| Idle poll, nothing pending | `RequestTask` | 0.12 ms |
| Complete a task with one artifact | `ReportTaskResult` | 0.20 ms |
| Renew a task lease | `RenewTaskLease` | 0.035 ms |
| Record progress | `ReportTaskProgress` | 0.026 ms |

## How the harness works

`internal/grpc/loadtest_test.go` starts a real master in the test process:
a SQLite file with the durable leadership lease and write fence, the
production lease, catalog and upload-capacity policies, the background loops
of `cmd/master` that load reaches (commit reconciliation, catalog
registration and reconciliation, lease expiry), and the gRPC server with
worker authentication on loopback.

Simulated workers, each with its own gRPC connection, follow the real worker
protocol: register, heartbeat every 5 s, request a task, fetch its
credentials, report progress and renew the lease while "working", take upload
capacity, upload one object and report the result with a verified artifact.
Runs are seeded as the planner leaves them, one incremental job and dataset
per run. Object storage and the Iceberg catalog are in-memory fakes with a
configurable latency, so the results describe the master and not MinIO, the
catalog or source databases. Process CPU and memory include the simulated
workers.

After each profile the harness checks that every run is committed and
registered and every task succeeded, and fails otherwise.

### Running it

```bash
# All profiles (about 25 minutes), with a JSON report
ORABBIT_LOAD=1 ORABBIT_LOAD_REPORT=load-report.json go test ./internal/grpc -run TestMasterLoad -timeout 60m -v

# Selected profiles
ORABBIT_LOAD=1 ORABBIT_LOAD_PROFILES=smoke,workers-64 go test ./internal/grpc -run TestMasterLoad -v

# SQLite hot paths
go test ./internal/db -run '^$' -bench Hotpath -benchtime 2000x
```

The [Load test workflow](../.github/workflows/load.yml) runs the same on
demand on a GitHub runner and keeps the report. Compare results only from the
same machine type: runner speed changes the absolute numbers, not the order of
the limits.

## Proposals

Measured, not yet implemented. Each needs a before/after run of this harness
and must keep leases, fencing and commit ordering as they are.

1. **Assignment without scanning the backlog.** Choose the run first (fewest
   running tasks, oldest), then take its lowest pending task through an index
   on `(run_id, status, task_index)`. The fair-share order stays the same, and
   the cost depends on the number of runs, not tasks.
2. **Parallel catalog registration.** Let the catalog loop claim up to the
   free slots at once instead of four in sequence, so
   `ORABBIT_CATALOG_WORK_LIMIT` controls throughput as its name suggests.
3. **Cheaper idle polls.** Skip run admission and the assignment query when
   nothing is pending (one cheap read), and refresh the heartbeat from
   `Heartbeat` only.
