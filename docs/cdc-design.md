# Change data capture: proposed design

Status: **proposed**, not implemented. Today O_Rabbit refuses the CDC options
(`partition_strategy: cdc_stream`, `consistency_mode: STRONG_CDC`), and
incremental runs do not propagate source deletes. See
[Deleted source rows](../README.md#deleted-source-rows) for current behavior
and workarounds.

## 1. Problem

An ordered-cursor incremental run reads rows whose cursor is above the
committed high-water mark (`internal/planner/planner.go`, resume from the
stored mark; `internal/grpc/commit_run.go`, the mark only moves forward). It
captures inserts, and updates that move the cursor. A deleted row produces
nothing to read, so its copy stays in the dataset and the Iceberg table until
a full refresh. A look-back window only re-reads rows that still exist, and
upsert only replaces keys that appear in a run.

Deletes can only be observed from the source's change log. That needs a
different read path: an ordered stream of change events with durable,
resumable positions.

## 2. Goals and non-goals

Goals:

- Propagate inserts, updates **and deletes** of a source table to the Iceberg
  table, so that after a run the target equals the source at a known source
  position.
- Exactly-once *effect* on the target despite worker retries, master
  restarts and ambiguous catalog commits (at-least-once delivery plus an
  idempotent apply).
- An explicit, per-engine support matrix. Engines without a change log are
  refused, not emulated.

Non-goals (first version):

- Sub-minute latency. CDC runs are batches, scheduled like other runs.
- DDL replication. A schema change stops the stream with an operator error.
- Multi-table transactional consistency across jobs. Each job is one table.
- Plain Parquet consumers reconstructing state themselves (see section 10).

## 3. Connector support

| Engine | Change source | Position | Decision |
|---|---|---|---|
| PostgreSQL | Logical replication (`pgoutput`) through a replication slot; `wal_level=logical` | LSN of the transaction's commit | **Phase 1** |
| MySQL, MariaDB | Row-based binlog (`binlog_format=ROW`, `binlog_row_image=FULL`) | GTID set (file:pos only as a fallback) | **Phase 1** |
| SQL Server | Native CDC change tables | `__$start_lsn` + `__$seqval` | Phase 2 |
| MongoDB | Change streams | Resume token | Phase 2 |
| Oracle | LogMiner / XStream (licensing, privileges) | SCN | Not planned |
| ClickHouse, Trino, FlightSQL, Cassandra, files on S3 | No usable row change log | — | **Refused** |

The existing readers in `internal/connectors/mysql_cdc.go` and
`mssql_cdc.go` are unused prototypes and not a basis as they are. The MySQL
reader tracks `file:pos` without GTIDs or transaction boundaries. The SQL
Server reader orders by LSN but ignores `__$seqval`, so changes within one
transaction are unordered. Neither persists an offset.

## 4. Change-event model

Each change is one record in a batch file (Parquet, one schema per table):

| Column | Meaning |
|---|---|
| `_op` | `I` insert, `U` update (after image), `D` delete (tombstone) |
| `_pos` | Source position of the event's **transaction commit**, as a sortable string (zero-padded LSN, GTID sequence, SCN) |
| `_seq` | Order of the event within its transaction (0, 1, …) |
| `_txid` | Source transaction ID, for diagnostics |
| `_commit_ts` | Source commit timestamp (UTC), for diagnostics and freshness metrics |
| key columns | Always set, including for deletes |
| other columns | After image for `I`/`U`; null for `D` |

**Delete identity.** A delete identifies its row by the table's key, which
must be declared:

- The job sets `cdc.key_columns`. They must be the source primary key (or a
  unique, non-null key) and become the Iceberg identifier fields. The upsert
  path already sets those (`schemaWithIdentifierFields`).
- PostgreSQL needs `REPLICA IDENTITY DEFAULT` with a primary key, or `FULL`.
  MySQL needs `binlog_row_image=FULL` or a primary key. A table without a key
  is refused.
- A key update (the key value changes) is emitted as `D` of the old key
  followed by `I` of the new key, in the same transaction.

## 5. Initial snapshot and handoff

A CDC job starts with a consistent snapshot, then streams from the position
the snapshot represents:

1. Create the slot (PostgreSQL) or record the current GTID set (MySQL)
   **before** reading. The slot's exported snapshot or a consistent-snapshot
   read gives the table at exactly position `P0`.
2. Export the snapshot with the existing ordered-cursor full path.
3. Commit it with `P0` as the dataset's change offset (section 6).
4. Every later run streams from the committed offset.

Without step 1 a change committed between the snapshot and the first stream
read is lost or applied twice, so the snapshot and the offset are produced by
one planner step.

## 6. Batches, offsets and checkpointing

- A run reads one **batch**: changes with position in `(from, to]`. The
  planner fixes `from` (the committed offset) and `to` (the source's current
  position, capped by `cdc.max_batch_events`). It stores both in the task's
  partition spec **before** any worker starts, so every retry reads exactly
  the same batch.
- `to` is always a transaction boundary. A transaction is never split across
  batches, so a batch applies whole transactions.
- The committed offset lives next to the high-water mark in the dataset state
  (`_state.json`, field `cdc_offset`) and in the run's commit intent and
  manifest. It advances only when the commit that applies the batch is
  verified, exactly like the high-water mark today.
- The source must keep the log until the offset advances. PostgreSQL slots do
  this, and O_Rabbit confirms the slot's flush position only after the commit.
  MySQL relies on binlog retention: if the committed offset is no longer
  available, the run fails with `CDC_POSITION_LOST` and the operator must run
  a new snapshot (section 5). It never silently skips changes.

## 7. Ordering semantics

- Events are totally ordered by `(_pos, _seq)` within a table.
- Within a batch, the apply step collapses each key to its **last** event in
  that order:
  - last event `D` → the key is deleted in the target;
  - last event `I`/`U` → the target row becomes that image.

  So "insert, update, delete" in one batch deletes the row, and "delete,
  insert" leaves the inserted row.
- Batches apply strictly in offset order. One active run per dataset (an
  existing index) prevents two batches of a table from applying
  concurrently, and the planner refuses a batch whose `from` is not the
  committed offset.

## 8. Retries and failures

| Failure | Effect | Recovery |
|---|---|---|
| Worker fails mid-batch | Task retried; same `(from, to]` | Re-read the same events; output is identical |
| Master restarts before commit | Run stays `COMMITTING` | Existing commit reconciliation resumes from the persisted intent |
| Source position lost | Run fails `CDC_POSITION_LOST` (terminal) | Operator runs a new snapshot |
| Schema change in batch | Run fails `CDC_SCHEMA_CHANGED` (terminal) | Operator evolves the table and re-snapshots or resumes |
| Catalog commit ambiguous | See section 9 | Reconciliation by snapshot stamp |

Delivery is at-least-once; the apply is idempotent because the collapsed
result of a batch depends only on its events, and a batch is applied at most
once per offset range (section 9).

## 9. Applying a batch to Iceberg

One Iceberg transaction per batch:

1. Delete the target rows of every key in the batch (deleted, updated or
   inserted keys alike).
2. Append the surviving after-images.
3. Stamp the snapshot with `orabbit.cdc_offset_from`, `orabbit.cdc_offset_to`
   and the run's `orabbit.commit_id`.

Engine choice:

- **iceberg-go v0.5 (today):** a copy-on-write delete plus the append, in one
  transaction, which is the upsert path as fixed in TID-953 (not yet merged).
  Merge-on-read is not usable on v0.5, because it writes no position deletes
  for files added without field IDs. Copy-on-write rewrites every data file
  that holds a changed key, so batches must stay bounded, like TID-953's
  `upsert.max_keys`.
- **iceberg-go v0.6+ (target):** equality-delete files on the identifier
  fields plus the data files, committed together with the `RowDelta` API.
  Cost is proportional to the batch, not the table. This needs the
  dependency upgrade, periodic compaction (the maintenance operation), and a
  check that ClickHouse/Altinity reads equality deletes.

**Reconciliation after an ambiguous catalog commit** (timeout, 5xx,
`CommitStateUnknown`):

- Reload the table and look for a snapshot stamped with this run's
  `commit_id`. If found, the batch is applied: advance the offset. If not
  found, and the table's current snapshot still descends from the expected
  parent, retry the commit.
- Otherwise, if another commit intervened, rebuild the transaction on the new
  current snapshot. Applying a collapsed batch is a pure function of its
  events, so re-applying is safe only while its stamp is absent.
- The existing catalog-observation flow (`InspectCatalog`, receipts) already
  records snapshot evidence; CDC extends its stamp with the offset range.

## 10. Plain Parquet output

Without Iceberg, a CDC run writes the change batch as-is (with `_op`, `_pos`,
`_seq`), not a state table. Consumers must apply it in `(_pos, _seq)` order,
or use Iceberg registration. This is documented, not hidden.

## 11. Configuration

```yaml
job:
  incremental: true
  partition_strategy: cdc_stream
  cdc:
    key_columns: [id]          # required; becomes the Iceberg identifier
    slot: orabbit_orders       # PostgreSQL replication slot (created if absent)
    max_batch_events: 1000000  # bound on one run's batch
```

`consistency_mode: STRONG_CDC` becomes an alias that selects `cdc_stream`.
Validation refuses engines outside the support matrix, tables without a key,
and sources whose log settings (`wal_level`, `binlog_format`,
`binlog_row_image`) cannot produce row images.

## 12. Test plan

The acceptance test, run against real PostgreSQL and MySQL (the
`ORABBIT_IT_*_DSN` integration setup) with MinIO and the REST catalog:

1. Create the job and run the initial snapshot of an empty table.
2. Insert `id=1, v='a'` and run. The target has exactly `1|a`.
3. Update `v='b'` and run. The target has exactly `1|b` (one row).
4. Delete `id=1` and run. The target has no row `1`.
5. In one transaction insert `id=2`, update it, delete it, then run. Nothing
   appears.
6. Insert `id=3`, kill the worker mid-task and let it retry. Exactly one row
   `3`.
7. Insert `id=4`, make the catalog commit time out after it lands, and
   restart the master. Exactly one snapshot for the batch, and the offset
   advanced once.
8. Purge the source log past the committed offset. The run fails with
   `CDC_POSITION_LOST` and the target is unchanged.

Unit tests cover collapsing (`I U D`, `D I`, key changes), batch-boundary
selection at transaction ends, offset persistence in the commit intent, and
stamp-based reconciliation.

## 13. Rollout

1. Documentation and refusal of the unimplemented options (done with this
   proposal).
2. PostgreSQL CDC on the v0.5 copy-on-write apply, behind the explicit
   `cdc_stream` strategy.
3. MySQL/MariaDB.
4. iceberg-go upgrade and equality deletes; compaction schedule.
5. SQL Server and MongoDB.

Open questions:

- Should the master or a worker own the replication connection? A slot
  allows one consumer at a time, so it fits the existing one-active-run rule
  with the slot held per run.
- How should slot lag be monitored and alerted? An idle job holding a slot
  makes PostgreSQL keep WAL.
