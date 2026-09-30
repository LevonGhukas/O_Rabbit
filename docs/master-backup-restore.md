# Master deployment model, backup and restore

## Single-master model

O_Rabbit runs exactly one master. All control-plane state (jobs, runs, tasks,
attempts, registrations, encrypted credentials) lives in one SQLite database
(`ORABBIT_DB_PATH`, WAL mode).

- There is **no automatic failover**. The durable leadership lease and the local
  process lock only stop a second process from using the *same* database file;
  they do not replicate state to another host.
- While the master is down, workers cannot receive tasks, renew leases or
  report results. Running attempts expire and are retried once a master is back.
  Workers reconnect on their own.
- Recovery is therefore: restore the database from a backup onto a host, start
  one master with the same `ORABBIT_MASTER_KEY`, and let the lease and commit
  reconcilers repair in-flight work.

What is lost with the database is lost for good: `ORABBIT_MASTER_KEY` alone
cannot rebuild it, and without the key the database cannot decrypt stored
secrets. Back up both, **separately**.

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

### Without Litestream

Take a consistent online copy with SQLite's backup API, never with `cp` of the
live file:

```bash
sqlite3 /var/lib/orabbit/master.sqlite ".backup '/backups/master-$(date +%Y%m%dT%H%M%S).sqlite'"
```

Run it from cron and ship the file off the host.

## Restore runbook

1. **Stop every master** that might use the database, and make sure the old
   host cannot come back (stop the container, or fence the host). Two masters
   on two restored copies would both accept work.
2. **Restore** onto the volume that will hold `ORABBIT_DB_PATH`. Remove any
   stale `master.sqlite`, `-wal` and `-shm` files first.

   ```bash
   litestream restore -config litestream.yml -o /var/lib/orabbit/master.sqlite /var/lib/orabbit/master.sqlite
   ```

   Add `-timestamp 2026-01-01T00:00:00Z` to restore to a point in time. With a
   `.backup` file, copy it to `ORABBIT_DB_PATH` instead.
3. **Verify** the copy:

   ```bash
   sqlite3 /var/lib/orabbit/master.sqlite "PRAGMA integrity_check;"
   ```

   It must print `ok`.
4. **Start one master** with the original `ORABBIT_MASTER_KEY`. On startup it
   takes a new leadership epoch, applies pending migrations, and fences writes
   from any older epoch.
5. **Check recovery**: `GET /ready` returns ready, workers show up in
   `GET /workers`, and runs that were in flight either continue (expired
   attempts are retried) or reach a terminal state. Runs that were
   `COMMITTING` are finished by the commit reconciler.
6. **Restart the Litestream sidecar** so replication continues from the
   restored database.

Work that happened after the last replicated change is gone. Jobs that ran in
that window may run again; incremental jobs resume from the high-water mark
stored in the restored database.

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
