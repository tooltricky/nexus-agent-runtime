# ADR 0006: Lease Heartbeats Without State Version Changes

- Status: Accepted
- Date: 2026-11-02
- Extends: [ADR 0002](0002-task-lease.md), [ADR 0003](0003-task-failure-and-retry.md), [ADR 0005](0005-cancellation-foundation.md)

## Context

B06 moved execution outside database transactions and fences results by owner,
attempt, version, and a live lease. A healthy call lasting longer than the initial
lease would still lose its result. Heartbeats must extend that lease without
invalidating the executor's version or reviving cancelled or recovered work.

## Decision

The local worker registers each committed claim with `LeaseHeartbeatService`.
Claiming, entering execution, and invoking the executor remain separate polls.
The execution transition returns the committed Task snapshot so the registration
can advance to its new version. Executing Tasks recovered by the same live worker
are registered before calling the executor. A separate scheduler renews registered
leases; it shares neither the execution pool nor the worker/watchdog scheduling
threads. Manual test workers without a heartbeat service remain under explicit
test control and do not start background renewals.

Each renewal uses its own short transaction. Lock the Task row with
`FOR UPDATE NOWAIT` first, then check
owner, attempt, version, `LEASED` / `EXECUTING` status, and
`lease_expires_at > clock_timestamp()`. Checking after acquiring the lock prevents
a late heartbeat from renewing an expired lease.
Use PostgreSQL time and set expiry to the greater of its existing value and the
current time plus the configured lease duration. Renewals never shorten a lease.

If another transaction owns the row lock, fail the renewal immediately, roll back
its short transaction, and continue the heartbeat sweep. PostgreSQL SQL state
`55P03` is normalized to `CannotAcquireLockException`; contention retains the
registration for the next tick and is not treated as a stale Task or executor
failure. Do not wait for a row lock on the single heartbeat thread: one locked
Task must not prevent unrelated Tasks from renewing. After unlock, renewal must
still pass all fence and expiry checks; contention does not grant extra lease time.

Renewal updates only lease expiry and `updated_at`. It does not change status,
`attempt`, or `version`, and emits no domain event. The original `TaskLeased`
event retains the original acquisition expiry; current expiry is read from Task.
State transitions continue to increment versions and commit their events
atomically. Successful results retain the existing live-lease and fence checks.
No HTTP/schema changes or database migration are required; V1–V4 are unchanged.

Registrations are scoped to Task, attempt, and owner. Replacing a registration
with a new version cannot be undone by a rejected older heartbeat or late cleanup.
Completion, executor failure, and result-submission failure clean up execution's
registration in `finally`. Cancellation, expiry, takeover, and other stale fences
are removed when their next heartbeat is rejected. Application shutdown clears
registrations, refuses new ones, and destroys the heartbeat scheduler. Already
in-flight renewal transactions remain subject to the same database checks.

Database/transaction errors in renewal are logged using only Task identity and
attempt. They do not enter the executor failure path or consume attempts. The
registration remains eligible for a later heartbeat, which must still pass the
live-lease check; a missed expiry can never be repaired by a late heartbeat.
One failed renewal does not prevent the remaining registrations in that sweep
from being attempted. Expected row-lock contention is handled without a warning
on every heartbeat tick.

The default lease is 30 seconds; heartbeat interval is 10 seconds. Startup rejects
nonpositive values or an interval greater than or equal to lease duration, with
the offending configuration names. Validation also applies when the local worker
is disabled. The interval must leave operational margin for scheduling and database
latency; positive values alone do not guarantee timely delivery.

Renewal and terminal changes serialize on the Task row. If renewal wins first,
completion/failure/cancellation can still finish the Task normally. If a terminal
transition or watchdog recovery holds the lock first, renewal skips that tick;
after its commit, the next renewal rejects the stale fence.
Watchdog retains ADR 0003's attempt budget and retry backoff: it skips locked rows,
and cannot recover a lease that is still live after renewal. Expired leases cannot
be renewed, even before watchdog has processed them.

## Consequences

Healthy long-running work can finish past the initial acquisition expiry. When
heartbeats stop, the existing independent watchdog eventually retries or fails
the Task; an old attempt cannot overwrite its replacement, even with the same
owner string. Database integration tests cover renewal eligibility, nonblocking lock contention,
transaction order, rollback, stale cleanup, real heartbeat scheduling, takeover,
and cancellation during a blocked downstream call.

The lock-contention regression holds Task A's row in an independent transaction,
observes A's first renewal attempt, and then registers executing Task B. On the
single heartbeat scheduler B stays live past its initial lease, watchdog does not
recover it, and A renews again after unlock. A separate regression keeps a lock
until expiry and verifies the next attempt cannot revive the expired lease.

A heartbeat proves worker ownership, not downstream progress. A stuck executor
can retain its lease while its worker remains healthy. Persisted execution
deadlines/TIMEOUT belong to B08; immediate interruption and exactly-once external
effects are not provided by renewal. This decision adds no remote worker protocol.
