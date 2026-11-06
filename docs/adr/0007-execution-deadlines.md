# ADR 0007: Persisted execution deadlines and TIMEOUT

- Status: Accepted
- Date: 2026-11-06
- Scope: B08; extends ADR 0003, 0005 and 0006.

## Deadline and request identity

`POST /v1/runs` accepts optional `deadline_at`: an RFC 3339 offset timestamp
(years 0001–9999, seconds required, at most nine fractional digits). Numeric offsets
range from `-23:59` through `+23:59`, with minutes 00–59, independently of Java's
`ZoneOffset` limit of ±18 hours. Missing or null means no deadline, including migrated rows.
Normalize offsets to UTC and truncate fractional seconds to PostgreSQL microseconds
before hashing and storage. The year range applies to the input's local date,
not the resulting UTC year: valid offsets may normalize into UTC year 0000 or
10000. Responses and event deadline strings normally use UTC; for these boundary
instants use `+23:59` or `-23:59` respectively to retain a four-digit local year
and a valid RFC 3339 representation. This changes presentation only, not the
stored instant or idempotency identity. Numeric epochs and timestamps without an offset are
invalid. A Run and its sole Task share the same persisted absolute deadline;
deferrable constraint triggers reject mismatches at commit. No existing deadline
is recalculated at restart, retry, or a future approval wait. Approval itself is
outside B08; waiting for approval will continue consuming this deadline.

Requests without a deadline retain the legacy hash bytes and therefore existing
idempotency keys. Deadline requests hash a separate `deadline-v1\0` prefix followed
by a JSON array of session, input, and normalized deadline, avoiding delimiter
ambiguity. An insertion with `ON CONFLICT DO NOTHING` arbitrates concurrent requests.
Compare the hash on replay before any time validation. Only a newly inserted Run
must have a deadline strictly after database `clock_timestamp()`; an invalid
creation rolls back. Replaying the same request after expiry returns the same Run;
a different deadline conflicts. Ordinary syntax/field and Session checks still
apply. HTTP null and omission are equivalent.

## Terminal decisions and transactions

TIMEOUT is an irreversible Run and Task terminal state. PENDING/LEASED/EXECUTING/
RETRYING Tasks and CREATED/QUEUED/RUNNING Runs may time out. Existing terminal
states remain unchanged. Every competing state writer locks Task before Run.
Worker success, final failure, retry, cancellation, and timeout evaluate database
time after acquiring their required locks; a blocked operation rechecks after
waiting. The decision point is this locked eligibility check, not the wall-clock
instant of COMMIT. A valid terminal decision made before the deadline may commit
later and still wins; a rolled-back transaction never wins. At or after the
deadline (`now >= deadline_at`), unfinished work can only terminate as TIMEOUT.

Claim, owned selection, execution admission, heartbeat, success, and failure also
check persisted deadlines even if the scanner has not caught up. Lease expiry is
capped at deadline. Heartbeat retains NOWAIT and never changes status version.
A deadline rejection of a worker result is a no-op; the scanner records timeout.
Failures and watchdog recovery invoke the same timeout transaction when due.
Cancellation of a timed-out Run returns 409 `cancellation_conflict`; cancellation
which first discovers expiry commits TIMEOUT and its events before returning the
same 409. Throwing a conflict inside that transaction must not roll back timeout.
A cancelled Run's repeat cancellation remains 200.

If attempts remain but the retry backoff would reach or exceed deadline, no
executable retry is scheduled. Reuse RETRYING as a waiting state with
`next_attempt_at = deadline_at`, release the lease, and emit versioned
`TaskRetrySkipped` (reason `DEADLINE_BEFORE_RETRY`), rather than `TaskRetried`.
The deadline and readiness predicates together make this Task unclaimable.
It becomes TIMEOUT only when actually due; no extra attempt is consumed.
Before deadline, an exhausted attempt can still fail normally.

## Timeout scanning and execution stop

A separate, validated scheduler scans bounded batches with SKIP LOCKED, independently
of the worker, heartbeat, lease watchdog, and execution pool. It remains enabled
when the local worker is disabled. Each batch uses short transactions; it never
calls an executor while holding a row lock. Locked Tasks may be processed on the
next scan. Timeout clears lease and output, increments state versions, and appends
`TaskTimedOut` then `RunTimedOut` atomically. Either event-write failure rolls back
all changes. Events include deadline, reason `DEADLINE_EXCEEDED`, prior/next state,
and attempt context or associated Task ID. Repeated scans emit no duplicate events.

Local executions register a stop signal by Task/attempt/version before the final
execution admission check. After timeout commits, the registry requests stop,
interrupts the executing threads, and clears heartbeats for every local registration
of that Task whose attempt is at most the timed-out attempt. This includes older
executors still running after lease loss and takeover. The upper bound prevents
a stale notification from stopping a newer attempt; other Tasks are unaffected. Executors
can check the signal between operations or respond to thread interruption. The
worker does not classify a requested stop as an ordinary retryable executor error.
Registration and cleanup cannot affect a newer execution. Shutdown requests stop
and clears registrations. Result fences remain authoritative if a signal races
with execution or if an executor ignores interruption. A noncooperative executor
can retain its thread until it exits; do not pretend that capacity has been freed.
Database unavailability may delay scanning/signalling but cannot authorize expired
results after recovery. Signals cannot undo external effects or guarantee their
exactly-once delivery.

Each instance also periodically reconciles its locally registered executions
against committed TIMEOUT rows. Queries use only the local Task IDs, in bounded
batches with no row locks; they never scan all historical terminal Tasks. This
delivers a stop when another instance won the timeout transaction, or the local
after-commit notification was missed. Reconciliation runs at the configured
deadline poll interval even when this instance's timeout writer is disabled or
heartbeats were removed after lease loss. An uncommitted or rolled-back timeout
cannot trigger a stop. A failed database read leaves executions registered for
the next tick. Stop requests are idempotent, without repeatedly interrupting an
executor that has already received the signal. Disabled writers everywhere can
still delay the TIMEOUT transition itself.

## Contracts and compatibility

HTTP representations identify revision 0.3 with `Nexus-API-Contract`. Preserve frozen
`openapi-v0.2.yaml`, 0.1, and both previous event schemas/samples. Only new timeout
and skipped-retry events use explicit `schema_version: "0.3"`; existing writers
keep their previous event versions. Consumers must support TIMEOUT and 0.3 events
before deploying this writer. Unknown versions fail validation and are preserved
for reporting rather than reinterpreted. Deadline response fields are omitted
when absent; TIMEOUT requires a non-null deadline and has no output or lease.
V5 adds columns/constraints/indexes without editing V1–V4 or historical events.
