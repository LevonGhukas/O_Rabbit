# Master deployment model, backup and restore

## Single-master model

O_Rabbit runs exactly one master. All control-plane state (jobs, runs, tasks,
attempts, registrations, encrypted credentials) lives in one SQLite database
(`ORABBIT_DB_PATH`, WAL mode).

- There is **no automatic failover**. The durable leadership lease and the local
  process lock only stop a second process from using the *same* database file;
  they do not replicate state to another host, and they cannot stop two masters
  that run on two *copies* of the database.
- While the master is down, workers cannot receive tasks, renew leases or
  report results. Running attempts expire and are retried once a master is back.
  Workers reconnect on their own.
- Recovery is therefore: restore the database from a backup onto a host, start
  one master with the same `ORABBIT_MASTER_KEY`, and let the lease and commit
  reconcilers repair in-flight work.

What is lost with the database is lost for good: `ORABBIT_MASTER_KEY` alone
cannot rebuild it, and without the key the database cannot decrypt stored
secrets. Back up both, **separately**.

## Recovery targets

| Target | Value | Measured in the restore drill |
| --- | --- | --- |
| **RPO** (data-loss window) | **1 minute** | 1.8–3.9 s |
| **RTO** (outage, from losing the master to it serving again) | **30 minutes** | 12–15 s of automated work, plus operator time |

These targets hold under these assumptions:

- Litestream runs beside the master (`docker-compose.master.backup.yml`) with
  the shipped `litestream.yml` (1 s sync interval), and its bucket is reachable.
- `scripts/verify-master-backup.sh` runs at least every 15 minutes and alerts
  on failure, so a stopped replication is noticed well within the RPO.
- An operator is on call, can fence the old host, and can provision a
  replacement host or volume with Docker and the same master image version.
- `ORABBIT_MASTER_KEY` is retrievable from its separate secret store within
  minutes.

The RTO budget is mostly people: about 10 minutes to notice and decide,
5 minutes to fence the old host, 10 minutes to provision a host and fetch the
key, and 5 minutes for `scripts/master-restore.sh`, which restores, checks and
starts the master. The drill measures only that last part: the restore and
checks take seconds for a small database, and the start waits out the lost
master's leadership lease (`ORABBIT_LEADERSHIP_LEASE_DURATION`, 15 s).
Restore time grows with the database size and the WAL written since the last
snapshot (`snapshot-interval: 24h`).

The RPO is the age of the newest change in the backup when the master was
lost. The drill measures it as the time between the disaster and the last
leadership renewal in the restored copy. A running master renews every
5 seconds, so this is an upper bound with 5-second resolution: replication
itself trails by Litestream's 1 s sync plus upload time. The drill measured
1.8–3.9 s. Only the window matters, not the database size.

### Restore drill

`ORABBIT_E2E_DR=1 scripts/e2e.sh` runs the drill against the isolated
end-to-end stack (`test/e2e/docker-compose.dr.yml`), and the
[Restore drill workflow](../.github/workflows/dr-drill.yml) runs it every week.
Each drill writes `dr-drill-report.json` with the disaster time, the restore
time, the measured RTO and RPO, and every check that passed. The workflow keeps
these reports for 90 days.

The drill:

1. exports an incremental table and registers it;
2. starts a second run whose catalog commit is held open;
3. kills the master and Litestream and deletes the master's volume (the host
   and its disk are lost);
4. restores onto a fresh volume with `scripts/master-restore.sh`, which also
   moves replication to a new path;
5. checks the API, the worker reconnecting, the in-flight registration
   finishing exactly once, the rows, files and snapshots in the catalog, a new
   incremental run, and `scripts/verify-master-backup.sh` on the new backup;
6. restores again to a point before the last run, and checks that the run stays
   published and that the next incremental run neither replays nor loses rows.

## Continuous backup with Litestream

[Litestream](https://litestream.io) streams WAL changes to S3-compatible
storage (AWS S3, MinIO) with about one second of lag and periodic snapshots.

1. Create a bucket that is not the dataset bucket, ideally with versioning.
2. `cp .env.backup.example .env.backup` and fill in the bucket and credentials.
3. Start the master with the backup overlay:

   ```bash
   docker compose -f docker-compose.master.yml -f docker-compose.master.backup.yml up -d
   ```

4. Check that it replicates:

   ```bash
   docker compose -f docker-compose.master.yml -f docker-compose.master.backup.yml exec litestream litestream snapshots -config /etc/litestream.yml /var/lib/orabbit/master.sqlite
   ```

`litestream.yml` keeps 7 days of snapshots and WAL (`retention: 168h`). For a
native (non-Docker) master, run `litestream replicate -config litestream.yml`
next to it with `path` pointing at `ORABBIT_DB_PATH`.

The master never forces WAL checkpoints (`wal_checkpoint(TRUNCATE)`) or
`VACUUM`, so it is compatible with Litestream's checkpoint handling.

### Verify backups regularly

A backup that has never been restored is not known to work.
`scripts/verify-master-backup.sh` restores the latest backup into a temporary
Docker volume, without touching the running master, and fails when:

- `orabbit-master check-db` finds a problem: SQLite integrity, foreign keys, a
  schema newer than the image, or stored secrets that `ORABBIT_MASTER_KEY`
  cannot decrypt;
- the restored copy is older than `--max-age` (default `60s`, the RPO). A
  running master writes every few seconds, so an old copy means replication
  stopped;
- more than one backup generation was updated within `--max-age`, which means
  two masters are replicating to the same path.

Run it from cron on the master host, from the repository checkout that holds
the Compose and env files, and alert on a non-zero exit:

```cron
*/15 * * * * cd /opt/orabbit && ./scripts/verify-master-backup.sh >>/var/log/orabbit-backup-verify.log 2>&1 || <your alert command>
```

### Without Litestream

Take a consistent online copy with SQLite's backup API, never with `cp` of the
live file:

```bash
sqlite3 /var/lib/orabbit/master.sqlite ".backup '/backups/master-$(date +%Y%m%dT%H%M%S).sqlite'"
```

Run it from cron and ship the file off the host. The RPO is then the cron
interval, not one minute. Check a copy with
`orabbit-master check-db -db <copy>` before relying on it.

## Restore runbook

`scripts/master-restore.sh` automates every step that is safe to automate.
Fencing the old host stays a human decision.

1. **Fence the old master.** Stop it and make sure the host cannot come back:
   power it off, detach its disk, or block it from the workers and object
   storage. Two masters on two copies of the database would both hand out work
   and commit to the same datasets. Nothing in the database can prevent this,
   because each copy has its own leadership lease.
2. **Prepare the new host** with the same master image version as the lost
   one, the Compose files, `.env.master` with the **original**
   `ORABBIT_MASTER_KEY` from its separate store, and `.env.backup`.
3. **Point replication at a new path.** Set `LITESTREAM_PATH` in `.env.backup`
   to an unused value, for example `master-2026-10-26`. If the old host does
   come back by mistake, its Litestream then writes to the old path and cannot
   mix its changes into the backup that this host now relies on.
4. **Restore, check and start:**

   ```bash
   scripts/master-restore.sh --from-path <old LITESTREAM_PATH> --old-master-fenced --start
   ```

   The script stops before changing anything when:
   - `--old-master-fenced` is missing;
   - a master or Litestream is already running on this host;
   - the database file already exists (it restores onto an empty volume only);
   - `LITESTREAM_PATH` still equals `--from-path`, or already holds a backup.

   It then restores with `litestream restore`, hands the database and its
   directory to the master's user (uid `10001`; Litestream runs as root), and
   runs `orabbit-master check-db`. Integrity, schema version and the master key
   must all pass. It waits out the lost master's leadership lease, starts the
   master and Litestream, and waits for `GET /ready`.

   Add `--timestamp 2026-01-01T00:00:00Z` to restore to a point in time, and
   `--report <file>` to keep the check report. Without `--start`, it stops after
   the checks and prints the start command.
5. **Check recovery.**
   - `GET /ready` returns ready (the script waits for it).
   - Workers show up in `GET /api/v1/workers` with a fresh `last_heartbeat`.
     Workers with TLS identities keep working; see the exceptions below.
   - Runs that were in flight continue or reach a terminal state: expired
     attempts are retried, the commit reconciler finishes `COMMITTING` runs, and
     registrations in `REGISTERING` or `RECONCILING` are reconciled against the
     catalog. Wait until none is left in those states, then check
     `orabbit-client run diagnose` for any run that needs an operator.
   - Run `scripts/verify-master-backup.sh` once the new backup has a snapshot.
6. **Go through what the restore lost** (next section).

### Restoring a native (non-Docker) master

Run the same steps by hand: restore with `litestream restore -config
litestream.yml -o <ORABBIT_DB_PATH> <ORABBIT_DB_PATH>`, make the file and its
directory owned by the master's user, run `ORABBIT_MASTER_KEY=<key>
orabbit-master check-db -db <ORABBIT_DB_PATH>` (it must report `"ok": true`),
switch `LITESTREAM_PATH` to a new path, and start one master.

## What a restore loses or replays

The database restore rolls back only the master's own records. The dataset
bucket (`_state.json`, the per-run commit manifests and the data files) and the
Iceberg catalog are **not** rolled back. Incremental runs read their
high-water mark from the dataset's `_state.json`, not from the database, so
what has been committed is never exported twice.

| Work in the window between the backup point and the disaster | After the restore |
| --- | --- |
| Run committed **and registered** in the catalog | Rows stay published. The run, its events and its artifacts are missing from the API. The next incremental run continues after it, without replaying or losing rows (checked by the drill). |
| Run committed in S3 but **not yet registered** in the catalog, unknown to the backup or shown there as `RUNNING` | **Its rows never reach the catalog.** A run shown as `RUNNING` re-runs its tasks, hits `EXISTING_OBJECT_CONFLICT` on the already committed files and fails. A run the backup never saw is never registered. Because `_state.json` already includes the rows, later incremental runs skip them. See "Runs committed after the backup point" below. |
| Run committed, shown as `COMMITTING` in the backup | The commit reconciler finishes it from the persisted commit intent. |
| Registration in flight (`REGISTERING`, `RECONCILING`) | Reconciled against the catalog: committed once, or retried (checked by the drill). |
| Run submitted but not yet committed | Unknown to the restored master. Workers' reports for it are rejected, and its uploads are left under the run's prefix: they are never cleaned up because the database does not know them. Submit the run again. Incremental runs start from the committed high-water mark, full runs overwrite. |
| Job, connection or target created or changed | Lost. Create or change it again. |
| Worker enrolled | The worker's certificate is valid but its identity is unknown: re-enroll it. |
| Worker identity **revoked** | **Active again.** Revoke it again immediately. |
| Master key rotated | The backup still holds secrets under the old key: restore with the old key, then rotate again. |

### Runs committed after the backup point

This is the one case that loses data, and there is no automatic repair yet.
With a working backup the window is a few seconds, so it needs a run that
committed in the last seconds before the master was lost. After a restore,
check every dataset that was written around the disaster:

1. Read `<dataset prefix>/_state.json` from the dataset bucket and note
   `last_committed_run_id`.
2. If that run is unknown to the restored master or `FAILED`, look for it in
   the Iceberg table's snapshot summaries (`orabbit.run_id`).
3. If no snapshot has it, its rows are committed but not in the table.
   Re-export that cursor range into the table, or rebuild the table with a full
   export.

## Database retention

The master prunes old operational history every
`ORABBIT_HISTORY_PRUNE_INTERVAL` (default `1h`). Rows are removed once the run
finished more than `ORABBIT_HISTORY_RETENTION` ago (default `720h`, `0`
disables pruning):

| Removed | Kept |
| --- | --- |
| Events of `SUCCEEDED`, `FAILED` and `CANCELED` runs | Run, task, job, HWM and connection rows |
| Task attempts and their artifacts of `FAILED`/`CANCELED` runs, once no canceled-object candidate or multipart upload of that run is still open | Attempts and artifacts of `SUCCEEDED` runs |
| Finished registration/reconciliation attempts except the latest per registration | Registrations |
| Master leadership history | Audit log |

Deletes run in batches of 500 rows so they do not hold the writer for long.
SQLite reuses freed pages; the file does not shrink on its own.
