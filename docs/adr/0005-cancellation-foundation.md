# ADR 0005: Cancellation foundation and versioned contracts

- Status: Accepted
- Date: 2026-10-25
- Scope: B04; B05 implements the cancellation transaction and HTTP entry point.

## Decision

Run may move from CREATED, QUEUED, or RUNNING to CANCELLED. Task may move from
PENDING, LEASED, EXECUTING, or RETRYING to CANCELLED. SUCCEEDED, FAILED, and
CANCELLED are terminal. CANCELLED cannot be changed again, including to itself.
During retry, Run remains RUNNING. CREATED is an internal creation phase and
does not create a public cancellation window.

B05 must atomically cancel the Run and unfinished Task, clear the Task lease,
preserve attempts, retry timestamps, and history, and append TaskCancelled then
RunCancelled. Cancellation does not consume another attempt. Cancelled objects
have no output, and cancelled Tasks have no owner or lease expiry. An event-write
failure must roll back both state changes and events.
Repeated cancellation is a successful no-op without another terminal event;
completed Runs are rejected, and missing Runs return 404. The first valid
terminal transaction to commit wins races with success or final failure.
Retry and watchdog recovery must serialize with cancellation and recheck the
latest state: a committed retry can still be cancelled; an already cancelled
Task cannot be recovered or retried. B05 must use consistent row locking and
version fences for Run and Task, rather than an unlocked read followed by a
write. A cancelled Task cannot be
claimed, renewed, completed, failed, or retried by a late worker. Cancellation
does not undo external side effects or promise immediate interruption.

## Contract versioning

The old event schema and HTTP definition remain frozen. V4 adds nullable
domain_events.schema_version; NULL means the historical 0.1 event and is not
backfilled. New cancellation events use schema_version string 0.2 and the
independent schema ID https://nexus.local/schemas/v0.2/domain-event.schema.json.
Unknown, null, or numeric versions fail validation rather than falling back.
TaskCancelled requires from, to, reason, attempt, and max_attempts; RunCancelled
requires from, to, reason, and task_id. The reason is USER_REQUESTED and to is
CANCELLED. Existing writers continue producing legacy events until B05 adds the
versioned cancellation writer.

The current GET Run and timeline responses identify HTTP contract revision 0.2
with Nexus-API-Contract. This is separate from the /v1 route and OpenAPI
info.version. B04 deliberately adds no cancel operation or route. Consumers
must understand the new status and mixed event versions before B05 enables it.
The frozen 0.1 HTTP definition is for historical validation, not response
negotiation. Deploy upgraded consumers, then the B04 migration and query code,
then enable cancellation in B05; never relabel CANCELLED as FAILED for old clients.
At the event boundary, no schema_version selects 0.1 and any explicit value must
select a supported version. Consumers must preserve and report unknown versions
without applying them to task state; the query API preserves stored version
strings rather than silently discarding them.

## Migration and verification

V4 only extends the status checks, adds cancellation field checks, and adds the
nullable event version column. It does not rewrite V1-V3 or disable append-only
events. Tests cover every source and terminal transition, old and new contract
samples, unknown versions, V3-to-V4 data preservation, fresh migration,
cancellation constraints, and mixed-version reads. B04 verified the absence of
a cancel route; B05 replaces that assertion with live cancellation HTTP tests.

## B05 implementation (2026-10-27)

`POST /v1/runs/{run_id}/cancel` requires no body or Idempotency-Key; a supplied
body is ignored. Success and repeat cancellation return 200 with the current
cancelled Run and `Nexus-API-Contract: 0.2`. Completed Runs return
409 with code `cancellation_conflict`, distinct from creation's
`idempotency_conflict`; missing Runs return 404 with code `not_found`.

Cancellation locks the Task before the Run, matching worker completion and
failure handling. It reads the locked current state, clears the lease, increments
each state version, and writes both versioned events in the same transaction.
Repeated cancellation returns without another update. Task writes also fence on
attempt, version, status, and, for worker results, owner and lease validity.
The database still enforces one Task per Run; no new migration is needed.

Independent-connection tests hold the first operation's locks until the second
operation is observed waiting or finishing via SKIP LOCKED. Both orders of
cancellation versus claim, success, final failure, retry, and watchdog recovery
are covered. Database triggers reject either cancellation event to verify full
rollback, including leases and the first event. A rolled-back cancellation does
not defeat a subsequently committed terminal operation.

Executor calls still run inside the worker transaction: cancellation may wait
for its Task lock and return 409 if completion commits first. B06 will address
long transactions. There is no heartbeat renewal endpoint yet; B07 will enforce
cancellation checks there as well. Neither immediate interruption nor reversal
of external side effects is promised.
