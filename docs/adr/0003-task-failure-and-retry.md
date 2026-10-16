# ADR 0003: Bounded Task Attempts and Retry Backoff

- Status: Accepted
- Date: 2026-10-16
- Extends: [ADR 0002](0002-task-lease.md)

## Context

Durable leases recover stopped workers, but immediately returning every expired
Task to `PENDING` allows unlimited execution attempts. The fake worker also has
no durable path for execution failures.

## Decision

Both reported execution failures and lease expiry consume the current attempt.
`attempt` increases only on lease acquisition. `max_attempts` includes the first
attempt: the existing default of 3 permits at most two retries.

An unsuccessful `LEASED` or `EXECUTING` attempt follows one of two paths:

- When `attempt < max_attempts`, clear the lease, enter `RETRYING`, and set
  `next_attempt_at` from the database clock. Emit `TaskRetried` when this retry
  is scheduled, including the failed attempt, reason, delay, and next eligible
  time. Claiming a ready retry transitions directly from `RETRYING` to `LEASED`.
- Otherwise clear the lease, enter Task `FAILED`, and set its Run to `FAILED`.
  Emit `TaskFailed` followed by `RunFailed` in the same transaction.

The delay after attempt n is `min(initial_delay * 2^(n-1), max_delay)`, using
overflow-safe saturation. Defaults are 1 second initially and a 30 second cap.
The Run stays `RUNNING` throughout retries; `RunStarted` is emitted only once.

The watchdog applies the same budget and backoff. It first records
`TaskLeaseExpired` as an observation (including previous ownership), followed by
`TaskRetried` or `TaskFailed`. Only the latter events describe state transitions.
This replaces ADR 0002's immediate `PENDING` requeue behavior.

Worker failure reports require a matching owner, status, version, and a live
lease. Watchdog recovery requires an expired lease and locks rows with
`FOR UPDATE SKIP LOCKED`. Lease validity uses PostgreSQL `clock_timestamp()`:
transaction-start time must not let an expired worker submit a late result.
Duplicate and stale reports produce neither updates nor events.

`TaskExecutor` separates the fake result from scheduling. The default executor
still succeeds with the fixed summary. Exceptions from execution enter the
failure path; database/event-write exceptions propagate and roll back the
transaction rather than being classified as task failures.

## Consequences

Current state and the event timeline commit atomically. Worker restarts preserve
retry eligibility because it is persisted, and a successful Task is never
selected for another lease. This bounds attempts; it does not guarantee
exactly-once external side effects, which require their own idempotency keys.

The current one-Task-per-Run schema makes final failure propagation immediate.
Multiple-Task/DAG failure policy, cancellation, lease heartbeats, jitter, and
failure classification remain separate milestones.
