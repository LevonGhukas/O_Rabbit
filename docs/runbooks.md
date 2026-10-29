# Alert runbooks

One section per alert in
[`monitoring/prometheus/orabbit-alerts.yml`](../monitoring/prometheus/orabbit-alerts.yml).
Alerts carry no run IDs (metric labels stay bounded), so each runbook starts
by finding the affected runs.

## Finding the affected runs

`GET /api/v1/runs` returns the newest runs with their lifecycle projection:
`catalog_status`, `readiness`, `operator_action_required`,
`registration_blocked_by` and `commit_reconciliation_status`. For example:

```sh
MASTER=http://master:9100
runs() { curl -fsS -H "Authorization: Bearer $ORABBIT_HTTP_AUTH_TOKEN" "$MASTER/api/v1/runs?limit=1000"; }

runs | jq '.[] | select(.catalog_status=="REGISTERING") | {id, readiness, registration_attempt}'
runs | jq '.[] | select(.operator_action_required) | {id, status, readiness}'
```

For one run, `orabbit-client run diagnose <run-id>` (or
`GET /api/v1/runs/{id}/diagnosis`) prints its state and the suggested next
action. Recovery actions are audited and need a reason:

```sh
orabbit-client run recover <run-id> --action registration_retry --reason "catalog back"
```

Actions: `registration_retry`, `reconciliation_retry`,
`acknowledge_quarantine`, `reconcile_commit`. Registration can also be
retried or canceled with `POST /api/v1/runs/{id}/registration/retry` and
`/registration/cancel`.

## OrabbitCatalogAttemptStuck

**Meaning.** A catalog registration attempt has been active for more than 45
minutes, longer than `ORABBIT_REGISTRATION_TIMEOUT` (30m). Registrations of a
dataset are ordered, so every later registration of that dataset waits.

**Check.**
- Find it: `runs | jq '.[] | select(.catalog_status=="REGISTERING")'`.
- Master logs for `iceberg registration` and the catalog URI: a hanging
  REST catalog or object store is the usual cause.
- Is the catalog reachable from the master host (`curl <uri>/v1/config`)?

**Act.**
- Fix catalog or network reachability. The attempt's lease then expires and
  the next attempt runs.
- If the master process itself hangs, restart it. Leadership fencing makes the
  restart safe, and the attempt is reconciled against the catalog before any
  replay.
- Do not cancel a registration whose catalog commit may have landed;
  `run diagnose` shows whether a catalog receipt exists.

## OrabbitRegistrationQueueStalled

**Meaning.** A registration has been PENDING for more than 30 minutes. Normal
registrations start within seconds.

**Check.**
- `runs | jq '.[] | select(.catalog_status=="PENDING") | {id, registration_blocked_by}'`.
- An earlier registration of the same dataset that is REGISTERING,
  RECONCILING or BLOCKED holds the queue (see `registration_blocked_by`).
- `orabbit_catalog_work_slots_in_use` at capacity means every catalog slot is
  busy (see [OrabbitCatalogCapacityFull](#orabbitcatalogcapacityfull)).

**Act.** Resolve the registration it waits for (the other runbooks in this
file), or raise `ORABBIT_CATALOG_WORK_LIMIT` if the catalog has headroom.

## OrabbitRegistrationQueueBlocked

**Meaning.** A registration has been PENDING for more than 2 hours. New data
is not reaching the Iceberg tables of that dataset.

**Check and act.** As for
[OrabbitRegistrationQueueStalled](#orabbitregistrationqueuestalled). This
pages because consumers now read stale tables.

## OrabbitRegistrationReconcilingSustained

**Meaning.** A registration has been RECONCILING for more than 30 minutes. An
earlier attempt's catalog outcome is unknown (for example, a timeout after
the commit was sent), and O_Rabbit is inspecting the catalog before it
decides whether to replay.

**Check.**
- `runs | jq '.[] | select(.catalog_status=="RECONCILING") | {id, reconciliation}'`.
- The `reconciliation` projection's classification:
  `CATALOG_OBSERVATION_UNAVAILABLE` means the catalog cannot be read;
  `CATALOG_HISTORY_INCOMPLETE` means snapshots were expired or the table
  changed during inspection.

**Act.**
- Restore catalog access. If reconciliation is idle in RETRY_REQUIRED, run
  `run recover <run-id> --action reconciliation_retry`.
- Never replay by hand: O_Rabbit replays only after it proves the commit did
  not land.

## OrabbitRegistrationRetrySustained

**Meaning.** A registration has waited to retry for more than an hour.
Retries back off for minutes at most, so they keep failing.

**Check.** `runs | jq '.[] | select(.catalog_status=="RETRY_REQUIRED") | {id, registration_last_error_class, registration_next_retry_at}'`.
The error class names the cause (authentication, schema, catalog
unavailable, …).

**Act.** Fix the cause, then `run recover <run-id> --action
registration_retry` to retry now instead of waiting for the backoff.

## OrabbitRegistrationBlocked

**Meaning.** A registration is BLOCKED behind an earlier registration of the
same dataset that failed or is quarantined and needs a decision.

**Check.** `runs | jq '.[] | select(.registration_blocked_by != null and .registration_blocked_by != "") | {id, registration_blocked_by}'`,
then `run diagnose` the blocking run.

**Act.** Retry the blocking registration once its cause is fixed, or cancel
it (`POST /api/v1/runs/{id}/registration/cancel`) if its data must not be
published. The queue then continues in order.

## OrabbitRegistrationLeaseExpired

**Meaning.** An active registration attempt has had an expired lease for more
than 5 minutes. The expiry scan normally clears it within seconds.

**Check.** Master logs for `registration lease expiration scan failed`, and
the master's database (disk full, a locked SQLite file, slow storage).

**Act.** Fix the master's storage or restart the master. On startup it
expires stale leases before it serves.

## OrabbitOperatorActionRequired

**Meaning.** At least one run is flagged `operator_action_required`: O_Rabbit
stopped because continuing automatically could be unsafe.

**Check.** `runs | jq '.[] | select(.operator_action_required) | {id, status, readiness}'`,
then `run diagnose <run-id>`, which names the next action.

**Act.** Follow the diagnosis: acknowledge quarantined tasks, reconcile a
commit, or retry or cancel a registration. Every action is audited.

## OrabbitRegistrationFailures

**Meaning.** A registration became FAILED or QUARANTINED in the last 30
minutes. The alert resolves by itself after 30 minutes; the failure does not.

**Check.** `runs | jq '.[] | select(.catalog_status=="FAILED" or .catalog_status=="QUARANTINED") | {id, registration_last_error_class}'`.

**Act.** Fix the cause, then retry the registration, or acknowledge and cancel
it. A failed registration blocks later registrations of its dataset.

## OrabbitReconciliationRetryExhausted

**Meaning.** A catalog reconciliation used all
`ORABBIT_RECONCILIATION_MAX_ATTEMPTS` attempts without proving the catalog
outcome.

**Check.** As for
[OrabbitRegistrationReconcilingSustained](#orabbitregistrationreconcilingsustained).

**Act.** Restore catalog access, then `run recover <run-id> --action
reconciliation_retry`. If the catalog history was expired, inspect the table
by hand before deciding; never assume the commit failed.

## OrabbitCatalogCapacityFull

**Meaning.** Every catalog work slot (`ORABBIT_CATALOG_WORK_LIMIT`) has been
busy for 15 minutes. Further registrations and reconciliations wait;
`orabbit_catalog_work_rejected_total` counts the deferrals.

**Check.**
- Is one slot held by a stuck attempt
  ([OrabbitCatalogAttemptStuck](#orabbitcatalogattemptstuck))?
- Or is the queue simply long (many datasets committing at once)?

**Act.** Clear the stuck attempt, or raise `ORABBIT_CATALOG_WORK_LIMIT` when
the catalog can take more concurrent commits.

## OrabbitCommittingRunTooOld

**Meaning.** The oldest COMMITTING run started more than 6 hours ago and has
not finished publishing. The age counts from the run's start, so it includes
extraction time.

**Check.** `runs | jq '.[] | select(.status=="COMMITTING") | {id, started_at, commit_phase, commit_reconciliation_status}'`
and `run diagnose`. Object store errors in the master log are the usual
cause.

**Act.** Fix object store access. If the run is in RETRY_REQUIRED, run
`run recover <run-id> --action reconcile_commit`.

## OrabbitCommitRetrySustained

**Meaning.** At least one run has needed a commit retry for 30 minutes.

**Check and act.** As for
[OrabbitCommittingRunTooOld](#orabbitcommittingruntooold).

## OrabbitTaskLeaseExpired

**Meaning.** An active task attempt has had an expired lease for more than 5
minutes. The lease scan (`ORABBIT_TASK_LEASE_SCAN_INTERVAL`, 5s) normally
reassigns it within seconds.

**Check.** Master logs for `task lease expiration scan failed`, and master
database health.

**Act.** Fix the master's storage or restart the master.

## OrabbitNoActiveLeader

**Meaning.** No scraped master reports active leadership, or no master can
be scraped. Only the leader serves `/metrics`. Runs, commits and
registrations are stopped.

**Check.**
- Are master processes running? Can Prometheus reach their HTTP port?
- `GET /status` on each master shows its leadership view.
- Master logs for `MASTER_LEADERSHIP_LOST` or `MASTER_RECOVERY_FAILED`.

**Act.** Start a master, or fix its database or network. A master that failed
recovery exits; its log names the failing phase.

## OrabbitLeadershipFlapping

**Meaning.** Leadership changed more than twice in 30 minutes. Each change
pauses work while the new leader recovers.

**Check.** Master logs for lease renewal failures. Common causes: a slow or
shared disk under the SQLite database, CPU starvation, or two masters
fighting over one database.

**Act.** Fix the master host's resources. Make sure
`ORABBIT_LEADERSHIP_LEASE_DURATION` comfortably exceeds
`ORABBIT_LEADERSHIP_RENEW_INTERVAL` (defaults 15s and 5s).
