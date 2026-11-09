# ADR 0008: Persisted pause, resume and Task-boundary checkpoints

- Status: Accepted
- Date: 2026-11-07
- Scope: B09; extends ADR 0003, 0005–0007. One Task per Run remains enforced.

## State and checkpoint

PAUSED is nonterminal. QUEUED/RUNNING Runs and PENDING/LEASED/EXECUTING/RETRYING
Tasks may pause. A pause locks Task then Run, rechecks database time, and writes
both PAUSED states, increments their versions, clears lease/output, and upserts
one version-1 checkpoint per Task. The checkpoint identifies its pause cycle
(UUID), Task/Run, prior states and versions, consumed attempt, next_attempt_at and
pause time. Input and successful output stay in their existing records; no thread,
stack, partial output, model call or external side effect is snapshotted.
Deferred database constraints require a matching unconsumed checkpoint exactly
when both Run and Task are PAUSED. Old rows need no checkpoint. V6 is additive;
V1–V5 and historical events are not modified. A new pause replaces the last
consumed checkpoint; immutable events retain prior cycle IDs and decisions.

Resume consumes that checkpoint atomically and increments current versions;
it never restores old versions or refunds attempts. PENDING resumes PENDING;
RETRYING resumes RETRYING with the original absolute next_attempt_at (including
backoff clamped to deadline). Interrupted LEASED/EXECUTING resumes PENDING,
ready immediately, and the next claim consumes a new attempt. Run returns to
its saved QUEUED/RUNNING state; RunStarted is not emitted again for RUNNING.
If an interrupted attempt has already exhausted max_attempts, resume consumes
the checkpoint with ATTEMPTS_EXHAUSTED, transitions directly from PAUSED to
FAILED, and emits TaskFailed then RunFailed with reason
PAUSED_ATTEMPTS_EXHAUSTED, with no Resumed event or additional execution.
Success is terminal and cannot be paused/resumed/reexecuted. This checkpoint
supports only a whole unfinished Task restarting; DAG and step-level snapshots
are outside B09.

## HTTP and competing decisions

POST /v1/runs/{run_id}/pause and /resume require no body or Idempotency-Key;
bodies are ignored. Return 200 with the current Run. Repeating pause while
PAUSED or resume while active is a no-op, including versions and events. Missing
Run is 404; terminal Runs return 409 pause_conflict, preserving results. Resume
which exhausts attempts returns 200 with FAILED (an effective resume decision).
An optional Pause-Checkpoint header on resume must equal the latest checkpoint
ID, otherwise 409. It binds a retry to one pause cycle, even after that checkpoint
has been consumed; repeated resume of that cycle is a no-op while active. Without
the header the request targets the current cycle: a delayed request can resume a
later pause, so clients needing cross-cycle retry isolation must send the header.
The header is an opaque equality token, not authentication. Pause retries after
an intervening resume are new pause requests, not globally deduplicated commands.

Pause does not freeze or extend deadline_at. PAUSED may cancel or time out;
those decisions also consume its checkpoint (CANCELLED/TIMEOUT). Both endpoints
check expiry before a repeated-operation no-op or a checkpoint conflict. They
commit newly discovered TIMEOUT and then return 409 without rolling it back.
Existing terminal states are checked first. All competing writers retain Task →
Run lock order and the locked eligibility decision point in ADR 0007: a valid
pre-deadline decision may commit later, rolled-back decisions never win. After
pause commits no new execution, heartbeat, lease recovery or late success/failure
can pass the status/owner/attempt/version predicates. Before resume commits
workers cannot claim the Task. Independent callers serialize; only one effective
resume occurs per cycle, even if the first subsequently starts executing.

## Stop delivery and transactions

Pause stores its prior attempt/version as a persistent invalidation bound.
Only after commit, send stop and remove heartbeats for that Task through that
fence (older attempts, or equal attempt with version <= saved version). The
pause monitor reads latest checkpoints for only locally executing Task IDs in
bounded, lock-free batches, independent of worker/timeout-writer enablement and
heartbeat registration. It reads consumed checkpoints too: quick pause/resume
or another instance's pause cannot hide invalidation. A newer checkpoint's bound
also covers all earlier pauses. This catches missed after-commit notifications
without ever stopping a newer attempt/version. Failed reads retry next tick;
uncommitted/rolled-back checkpoints are invisible. Existing timeout monitoring
continues separately. Registration precedes final admission, closing the gap
between pause and an executor starting. Signals are idempotent and scoped to
registrations, with cleanup before pooled-thread reuse.

Stop is cooperative; a noncooperative executor retains its slot until it exits.
Its result remains fenced. A requested stop is not an ordinary retryable executor
error. No external side effect is undone and no exactly-once guarantee is made.
No executor is called inside a row-locking transaction. Database failures are
propagated as database failures. State, checkpoint, leases, versions and all
events commit or roll back together; an event/checkpoint failure sends no stop.

## Events and compatibility

HTTP revision 0.4 uses Nexus-API-Contract: 0.4; freeze openapi-v0.3.yaml. A Task
with pause history exposes checkpoint, including consumed_at/disposition once
resolved. PAUSED requires an unconsumed checkpoint and no lease/output.
TaskPaused → RunPaused and TaskResumed → RunResumed use schema_version 0.4
with cycle ID, prior/next states and attempt context. Exhausted resume's failure
events use 0.4 too. Cancel/timeout originating at PAUSED use 0.4 schemas accepting
that new origin; existing origins keep their current 0.2/0.3 versions. Do not
widen frozen schemas or rewrite old events. Unknown versions are rejected.
Readers must support PAUSED and mixed 0.1/0.2/0.3/0.4 events before deployment.
