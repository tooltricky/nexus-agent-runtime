# ADR 0002: Durable Task Lease and Expiry Recovery

- Status: Accepted
- Date: 2026-10-04
- Supersedes: None
- Extends: [ADR 0001](0001-mvp-vertical-slice.md)

## Context

ADR 0001 established PostgreSQL as the durable source of truth and used
`FOR UPDATE SKIP LOCKED` for the in-process fake worker. That protects the queue
claim transaction, but a worker can still stop after claiming a Task. Without a
durable lease, another worker cannot safely determine when it may take over, and
a stale worker could attempt to submit a late state transition.

The second-week reliability work therefore needs a real Task lease while keeping
the existing modular-monolith and PostgreSQL transaction boundaries.

## Decision

Add the following durable fields to `tasks`:

- `lease_owner`: worker identity holding the lease;
- `lease_expires_at`: database timestamp after which the lease is invalid;
- `attempt`: number of lease claims;
- `max_attempts`: retry ceiling reserved for the retry milestone;
- `next_attempt_at`: earliest time at which a pending Task may be claimed.

Task claiming is one atomic database operation. It selects a ready `PENDING`
Task with `FOR UPDATE SKIP LOCKED`, changes it to `LEASED`, writes the worker
identity and expiry timestamp, and increments `attempt`.

Only the worker whose identity matches `lease_owner`, with a lease that is still
valid and whose optimistic version is current, may transition a Task from
`LEASED` to `EXECUTING` or from `EXECUTING` to `SUCCEEDED`.

A watchdog scans `LEASED` and `EXECUTING` Tasks whose lease has expired. It
clears the lease, returns the Task to `PENDING`, updates its version, and appends
a `TaskLeaseExpired` event. The next worker can then claim it normally.

## Consequences

- Worker crashes after claiming or while executing are recoverable without a
  manual database repair.
- Concurrent workers cannot hold the same live lease because claiming is
  serialized by PostgreSQL row locking and `SKIP LOCKED`.
- A delayed worker cannot commit a result after lease expiry or takeover.
- Lease expiry and requeue are visible in the append-only event timeline.
- Lease renewal/heartbeat, retry backoff, terminal failure, and cancellation
  remain separate follow-up decisions and are intentionally not defined here.
