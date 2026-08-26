# Worker Protocol Compatibility & Fencing Specification

## Overview

O_Rabbit coordinates distributed extraction workers through a gRPC control-plane protocol with strict versioning and fencing guarantees. This document defines the protocol contract (Version 5), boot identity tracking, and task attempt fencing.

## Protocol Versioning

- **Current Accepted Version**: `5`
- **Negotiation Model**: Exact match fail-closed.
- Any worker presenting a mismatched `protocol_version` in `RequestTask` is rejected with `codes.FailedPrecondition`.

## Boot Identity & Instance Lifecycle

Every worker process generates a persistent unique `workerInstanceID` (UUID) upon startup:

1. **Worker Registration (`RegisterWorker`)**:
   - Sends `worker_id`, `boot_id`, `hostname`, `pid`, and `version`.
   - Master registers an active entry in `worker_instances` keyed by `(boot_id)`.
2. **Heartbeats (`Heartbeat`)**:
   - Worker sends periodic heartbeats with `worker_id` and `boot_id`.
   - Master touches `worker_instances.last_heartbeat` and `workers.last_heartbeat`.
3. **Task Assignment (`RequestTask`)**:
   - Worker includes its `boot_id`.
   - Master assigns pending tasks and creates a new `task_attempts` record recording `attempt_id`, `task_id`, `worker_id`, `boot_id`, `fencing_token`, and lease expiration.
4. **Task Lease Renewal (`RenewTaskLease`)**:
   - Requires valid `worker_id`, `boot_id`, `task_id`, `attempt_id`, and `fencing_token`.
   - Stale workers or attempts superseded by retries are rejected immediately.
5. **Upload Capacity & Result Reporting**:
   - `AcquireUploadCapacity`, `ReleaseUploadCapacity`, `ReportTaskProgress`, and `ReportTaskResult` all validate the caller boot identity.

## Rolling Upgrades

When deploying new worker versions or restarting worker instances:
- New worker instances boot with fresh `boot_id` values.
- Stale tasks leased to prior boots will timeout or be cleanly reassigned.
- The master guarantees no two distinct boots can commit results for the same task attempt.
