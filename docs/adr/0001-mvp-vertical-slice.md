# ADR 0001: MVP Run lifecycle as a modular monolith

- Status: Accepted
- Date: 2026-09-24

## Context

The target platform design contains separate control-plane, orchestration, runtime, worker, event-bus, and policy components. Building those boundaries before proving the Run lifecycle would create deployment and failure-handling complexity without validating the central domain model.

The first milestone must create a Session, accept an idempotent Run, create a fixed Task, execute it, persist the result, and expose an ordered event timeline.

## Decision

Build one Spring Boot control-plane service backed by PostgreSQL. Keep API, application services, state machines, persistence, and the fake worker as separate Java packages inside the same deployable process.

The database owns durable truth:

- `sessions`, `runs`, and `tasks` store current state.
- `domain_events` stores the append-only history, ordered by a database-generated sequence.
- Aggregate changes and their events are committed in the same database transaction.
- A database trigger rejects updates and deletes from `domain_events`.
- Optimistic versions and expected-current-state predicates protect transitions.
- The fake worker uses `FOR UPDATE SKIP LOCKED` and advances one Task state per transaction. A restart can therefore resume Tasks left in `PENDING`, `LEASED`, or `EXECUTING`.

`Idempotency-Key` is globally unique for this MVP. Repeating the same key and request returns the original `run_id`; repeating it with a different Session or input returns `409 Conflict`.

The worker creates one fixed Task named `analyze-order-service-error-rate` and returns the deterministic result:

```json
{
  "summary": "发现错误率上升，需要进一步检查最近发布"
}
```

## Consequences

The vertical slice has one process and one transactional boundary, so it is simple to run and its event timeline cannot drift from aggregate state. PostgreSQL also provides sufficient queue semantics for this milestone.

This is not an event-sourced system: current aggregate rows remain authoritative, while events are an immutable audit and integration log. A later milestone can publish this table through an outbox relay to Kafka without changing API semantics. The in-process scheduler can similarly move behind a runtime gateway once independent scaling and isolation are needed.

This milestone intentionally excludes Kubernetes, Kafka, Redis, model calls, multi-Agent orchestration, retries, approvals, authentication, and tool execution.

