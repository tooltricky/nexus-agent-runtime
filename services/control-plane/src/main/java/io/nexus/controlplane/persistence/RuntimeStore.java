package io.nexus.controlplane.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.PauseCheckpointRecord;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.SessionRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Optional;

@Repository
public class RuntimeStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;

    public RuntimeStore(JdbcTemplate jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public SessionRecord insertSession(String id) {
        return jdbc.queryForObject(
                "INSERT INTO sessions (id) VALUES (?) RETURNING id, created_at",
                this::mapSession,
                id
        );
    }

    public boolean sessionExists(String id) {
        Boolean exists = jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM sessions WHERE id = ?)", Boolean.class, id);
        return Boolean.TRUE.equals(exists);
    }

    public boolean tryInsertRun(String id, String sessionId, String idempotencyKey, String requestHash, String input) {
        return tryInsertRun(id, sessionId, idempotencyKey, requestHash, input, null);
    }

    public boolean tryInsertRun(String id, String sessionId, String idempotencyKey, String requestHash, String input,
                                Instant deadlineAt) {
        return jdbc.update("""
                INSERT INTO runs (id, session_id, idempotency_key, request_hash, input, status, deadline_at)
                VALUES (?, ?, ?, ?, ?, 'CREATED', ?)
                ON CONFLICT (idempotency_key) DO NOTHING
                """, id, sessionId, idempotencyKey, requestHash, input, timestamp(deadlineAt)) == 1;
    }

    public Optional<RunRecord> findRun(String id) {
        return queryOne("SELECT * FROM runs WHERE id = ?", this::mapRun, id);
    }

    /** Call only after locking this Run's Task, matching worker/failure lock order. */
    public Optional<RunRecord> findRunForUpdate(String id) {
        return queryOne("SELECT * FROM runs WHERE id = ? FOR UPDATE", this::mapRun, id);
    }

    public Optional<RunRecord> findRunByIdempotencyKey(String idempotencyKey) {
        return queryOne("SELECT * FROM runs WHERE idempotency_key = ?", this::mapRun, idempotencyKey);
    }

    public void insertTask(String id, String runId, String name, JsonNode input) {
        insertTask(id, runId, name, input, null);
    }

    public void insertTask(String id, String runId, String name, JsonNode input, Instant deadlineAt) {
        jdbc.update("""
                INSERT INTO tasks (id, run_id, name, status, input, deadline_at)
                VALUES (?, ?, ?, 'PENDING', CAST(? AS jsonb), ?)
                """, id, runId, name, json(input), timestamp(deadlineAt));
    }

    public List<TaskRecord> findTasks(String runId) {
        return jdbc.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY created_at, id", this::mapTask, runId);
    }

    public List<TaskRecord> findTasksForUpdate(String runId) {
        return jdbc.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY id FOR UPDATE", this::mapTask, runId);
    }

    public Instant currentTime() {
        return jdbc.queryForObject("SELECT clock_timestamp()", Timestamp.class).toInstant();
    }

    public Optional<TaskRecord> findTaskForUpdate(String taskId) {
        return queryOne("SELECT * FROM tasks WHERE id = ? FOR UPDATE", this::mapTask, taskId);
    }

    public List<TaskRecord> findDueTasksForUpdate(int batchSize) {
        return jdbc.query("""
                SELECT * FROM tasks WHERE status IN ('PENDING','LEASED','EXECUTING','RETRYING','PAUSED')
                  AND deadline_at <= clock_timestamp()
                ORDER BY deadline_at, id LIMIT ? FOR UPDATE SKIP LOCKED
                """, this::mapTask, batchSize);
    }

    /** Read committed terminal state without waiting for row locks or scanning historical Tasks. */
    public List<TaskRecord> findTimedOutTasks(Set<String> taskIds) {
        if (taskIds.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(taskIds.size(), "?"));
        return jdbc.query("SELECT * FROM tasks WHERE status = 'TIMEOUT' AND id IN (" + placeholders + ")",
                this::mapTask, taskIds.toArray());
    }

    public void timeoutTask(TaskRecord task) {
        task.status().requireTransitionTo(TaskStatus.TIMEOUT);
        int updated = jdbc.update("""
                UPDATE tasks SET status = 'TIMEOUT', output = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND status = ? AND version = ? AND attempt = ?
                """, task.id(), task.status().name(), task.version(), task.attempt());
        requireSingleUpdate(updated, "Task", task.id());
    }

    public void cancelTask(TaskRecord task) {
        task.status().requireTransitionTo(TaskStatus.CANCELLED);
        int updated = jdbc.update("""
                UPDATE tasks
                SET status = 'CANCELLED', output = NULL,
                    lease_owner = NULL, lease_expires_at = NULL,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND status = ? AND version = ? AND attempt = ?
                """, task.id(), task.status().name(), task.version(), task.attempt());
        requireSingleUpdate(updated, "Task", task.id());
    }

    public Optional<PauseCheckpointRecord> findPauseCheckpoint(String taskId) {
        return queryOne("SELECT * FROM task_pause_checkpoints WHERE task_id = ?", this::mapCheckpoint, taskId);
    }

    /** Include consumed checkpoints so a rapid resume cannot conceal a committed pause. */
    public List<PauseCheckpointRecord> findPauseCheckpoints(Set<String> taskIds) {
        if (taskIds.isEmpty()) return List.of();
        String placeholders = String.join(",", java.util.Collections.nCopies(taskIds.size(), "?"));
        return jdbc.query("SELECT * FROM task_pause_checkpoints WHERE task_id IN (" + placeholders + ")",
                this::mapCheckpoint, taskIds.toArray());
    }

    public PauseCheckpointRecord savePauseCheckpoint(TaskRecord task, RunRecord run, Instant now) {
        return jdbc.queryForObject("""
                INSERT INTO task_pause_checkpoints (task_id, run_id, checkpoint_id, schema_version,
                    run_status, task_status, attempt, task_version, run_version, next_attempt_at, paused_at)
                VALUES (?, ?, ?::uuid, 1, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (task_id) DO UPDATE SET checkpoint_id = EXCLUDED.checkpoint_id,
                    run_status = EXCLUDED.run_status, task_status = EXCLUDED.task_status,
                    attempt = EXCLUDED.attempt, task_version = EXCLUDED.task_version,
                    run_version = EXCLUDED.run_version, next_attempt_at = EXCLUDED.next_attempt_at,
                    paused_at = EXCLUDED.paused_at, consumed_at = NULL, disposition = NULL
                RETURNING *
                """, this::mapCheckpoint, task.id(), run.id(), java.util.UUID.randomUUID().toString(),
                run.status().name(), task.status().name(), task.attempt(), task.version(), run.version(),
                timestamp(task.nextAttemptAt()), timestamp(now));
    }

    public void consumePauseCheckpoint(TaskRecord task, String disposition) {
        if (task.status() != TaskStatus.PAUSED) return;
        requireSingleUpdate(jdbc.update("""
                UPDATE task_pause_checkpoints SET consumed_at = clock_timestamp(), disposition = ?
                WHERE task_id = ? AND consumed_at IS NULL
                """, disposition, task.id()), "Checkpoint", task.id());
    }

    public void pauseTask(TaskRecord task) {
        task.status().requireTransitionTo(TaskStatus.PAUSED);
        requireSingleUpdate(jdbc.update("""
                UPDATE tasks SET status = 'PAUSED', output = NULL, lease_owner = NULL, lease_expires_at = NULL,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND status = ? AND version = ? AND attempt = ?
                """, task.id(), task.status().name(), task.version(), task.attempt()), "Task", task.id());
    }

    public void resumeTask(TaskRecord task, TaskStatus next, Instant readyAt) {
        task.status().requireTransitionTo(next);
        requireSingleUpdate(jdbc.update("""
                UPDATE tasks SET status = ?, next_attempt_at = ?, version = version + 1,
                    updated_at = clock_timestamp()
                WHERE id = ? AND status = 'PAUSED' AND version = ? AND attempt = ?
                """, next.name(), timestamp(readyAt), task.id(), task.version(), task.attempt()), "Task", task.id());
    }

    public record TaskClaim(TaskRecord task, TaskStatus previousStatus) {
    }

    public record TaskFence(String taskId, int attempt, long version) {
    }

    /** Atomically claims the oldest task ready to run and writes its lease. */
    public Optional<TaskClaim> claimNextTask(String leaseOwner, Duration leaseDuration) {
        long leaseMillis = leaseDuration.toMillis();
        return jdbc.query("""
                        WITH candidate AS (
                            SELECT id, status AS previous_status
                            FROM tasks
                            WHERE status IN ('PENDING', 'RETRYING')
                              AND attempt < max_attempts
                              AND next_attempt_at <= clock_timestamp()
                              AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                            ORDER BY created_at, id
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        UPDATE tasks AS task
                        SET status = 'LEASED',
                            lease_owner = ?,
                            lease_expires_at = LEAST(deadline_at, clock_timestamp() + (? * INTERVAL '1 millisecond')),
                            attempt = attempt + 1,
                            version = version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        FROM candidate
                        WHERE task.id = candidate.id
                          AND (task.deadline_at IS NULL OR task.deadline_at > clock_timestamp())
                        RETURNING task.*, candidate.previous_status
                        """, (rs, rowNum) -> new TaskClaim(mapTask(rs, rowNum),
                                TaskStatus.valueOf(rs.getString("previous_status"))), leaseOwner, leaseMillis)
                .stream()
                .findFirst();
    }

    public Optional<TaskRecord> findOwnedActiveTaskForUpdate(String leaseOwner) {
        return findOwnedActiveTaskForUpdate(leaseOwner, Set.of());
    }

    public Optional<TaskRecord> findOwnedActiveTaskForUpdate(String leaseOwner, Set<TaskFence> inFlight) {
        List<TaskFence> fences = new ArrayList<>(inFlight);
        String excluded = fences.isEmpty() ? "" : " AND NOT (status = 'EXECUTING' AND (id, attempt, version) IN ("
                + fences.stream().map(fence -> "(?, ?, ?)").reduce((left, right) -> left + ", " + right).orElseThrow()
                + "))";
        List<Object> parameters = new java.util.ArrayList<>();
        parameters.add(leaseOwner);
        fences.forEach(fence -> {
            parameters.add(fence.taskId());
            parameters.add(fence.attempt());
            parameters.add(fence.version());
        });
        return jdbc.query("""
                        SELECT * FROM tasks
                        WHERE status IN ('LEASED', 'EXECUTING')
                          AND lease_owner = ?
                          AND lease_expires_at > clock_timestamp()
                          AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                          %s
                        ORDER BY created_at, id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                        """.formatted(excluded), this::mapTask, parameters.toArray())
                .stream()
                .findFirst();
    }

    public boolean hasLiveExecutingLease(TaskRecord task, String leaseOwner) {
        Boolean exists = jdbc.queryForObject("""
                SELECT EXISTS(
                    SELECT 1 FROM tasks
                    WHERE id = ?
                      AND status = 'EXECUTING'
                      AND version = ?
                      AND attempt = ?
                      AND lease_owner = ?
                      AND lease_expires_at > clock_timestamp()
                      AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                )
                """, Boolean.class, task.id(), task.version(), task.attempt(), leaseOwner);
        return Boolean.TRUE.equals(exists);
    }

    /** Never wait for a Task row: lock contention must not stall the shared heartbeat sweep. */
    public boolean renewTaskLease(TaskRecord task, String leaseOwner, Duration leaseDuration) {
        long millis = leaseDuration.toMillis();
        if (millis <= 0) throw new IllegalArgumentException("Lease duration must be positive");
        // NOWAIT raises a transient lock exception, distinct from an absent/stale Task (false).
        // The caller retains the heartbeat registration and retries in the next sweep.
        try {
            if (jdbc.queryForList("SELECT id FROM tasks WHERE id = ? FOR UPDATE NOWAIT", String.class, task.id()).isEmpty()) {
                return false;
            }
        } catch (UncategorizedSQLException failure) {
            // Spring's default SQL-state translator does not classify PostgreSQL lock_not_available.
            if ("55P03".equals(failure.getSQLException().getSQLState())) {
                throw new CannotAcquireLockException("Task lease row is busy", failure);
            }
            throw failure;
        }
        return jdbc.update("""
                UPDATE tasks
                SET lease_expires_at = LEAST(deadline_at, GREATEST(lease_expires_at,
                        clock_timestamp() + (? * INTERVAL '1 millisecond'))),
                    updated_at = clock_timestamp()
                WHERE id = ? AND status IN ('LEASED', 'EXECUTING')
                  AND version = ? AND attempt = ? AND lease_owner = ?
                  AND lease_expires_at > clock_timestamp()
                  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                """, millis, task.id(), task.version(), task.attempt(), leaseOwner) == 1;
    }

    public void transitionRun(RunRecord run, RunStatus next, JsonNode output) {
        run.status().requireTransitionTo(next);
        int updated = jdbc.update("""
                UPDATE runs
                SET status = ?, output = CAST(? AS jsonb), version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = ? AND version = ?
                """, next.name(), jsonOrNull(output), run.id(), run.status().name(), run.version());
        requireSingleUpdate(updated, "Run", run.id());
    }

    public void transitionTask(TaskRecord task, TaskStatus next, JsonNode output) {
        task.status().requireTransitionTo(next);
        int updated = jdbc.update("""
                UPDATE tasks
                SET status = ?, output = CAST(? AS jsonb), version = version + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = ? AND status = ? AND version = ?
                """, next.name(), jsonOrNull(output), task.id(), task.status().name(), task.version());
        requireSingleUpdate(updated, "Task", task.id());
    }

    /** Transitions a task only while the supplied worker still owns a live lease. */
    public boolean tryTransitionTaskOwned(
            TaskRecord task,
            TaskStatus next,
            JsonNode output,
            String leaseOwner
    ) {
        if (task.status() == next) {
            return false;
        }
        task.status().requireTransitionTo(next);
        if (findTaskForUpdate(task.id()).isEmpty()) return false;
        int updated = jdbc.update("""
                UPDATE tasks
                SET status = ?,
                    output = CAST(? AS jsonb),
                    lease_owner = CASE WHEN ? IN ('SUCCEEDED', 'FAILED') THEN NULL ELSE lease_owner END,
                    lease_expires_at = CASE WHEN ? IN ('SUCCEEDED', 'FAILED') THEN NULL ELSE lease_expires_at END,
                    version = version + 1,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                  AND status = ?
                  AND version = ?
                  AND attempt = ?
                  AND lease_owner = ?
                  AND lease_expires_at > clock_timestamp()
                  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                """, next.name(), jsonOrNull(output), next.name(), next.name(), task.id(),
                task.status().name(), task.version(), task.attempt(), leaseOwner);
        return updated == 1;
    }

    /** Locks expired attempts for the watchdog; state and events are committed by the caller. */
    public List<TaskRecord> findExpiredLeasesForUpdate(int batchSize) {
        return jdbc.query("""
                SELECT * FROM tasks
                WHERE status IN ('LEASED', 'EXECUTING')
                  AND lease_expires_at <= clock_timestamp()
                ORDER BY lease_expires_at, id
                LIMIT ?
                FOR UPDATE SKIP LOCKED
                """, this::mapTask, batchSize);
    }

    public Optional<TaskRecord> finishFailedAttempt(
            TaskRecord task, String leaseOwner, Duration retryDelay, boolean expired
    ) {
        if (findTaskForUpdate(task.id()).isEmpty()) return Optional.empty();
        TaskStatus next = task.attempt() < task.maxAttempts() ? TaskStatus.RETRYING : TaskStatus.FAILED;
        task.status().requireTransitionTo(next);
        // Expiry recovery and worker failure reporting use mutually exclusive lease predicates.
        String leasePredicate = expired
                ? "lease_expires_at <= clock_timestamp()"
                : "lease_expires_at > clock_timestamp()";
        return jdbc.query("""
                UPDATE tasks
                SET status = ?, output = NULL,
                    lease_owner = NULL, lease_expires_at = NULL,
                    next_attempt_at = CASE WHEN ? = 'RETRYING' THEN
                        LEAST(deadline_at, clock_timestamp() + (? * INTERVAL '1 millisecond'))
                        ELSE clock_timestamp() END,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND status = ? AND version = ? AND attempt = ? AND lease_owner = ?
                  AND %s
                  AND (deadline_at IS NULL OR deadline_at > clock_timestamp())
                RETURNING *
                """.formatted(leasePredicate), this::mapTask, next.name(), next.name(), retryDelay.toMillis(),
                task.id(), task.status().name(), task.version(), task.attempt(), leaseOwner).stream().findFirst();
    }

    public void appendEvent(
            String eventId,
            String aggregateType,
            String aggregateId,
            String runId,
            String taskId,
            String eventType,
            JsonNode payload
    ) {
        appendVersionedEvent(eventId, aggregateType, aggregateId, runId, taskId, eventType, payload, null);
    }

    public void appendVersionedEvent(
            String eventId,
            String aggregateType,
            String aggregateId,
            String runId,
            String taskId,
            String eventType,
            JsonNode payload,
            String schemaVersion
    ) {
        jdbc.update("""
                INSERT INTO domain_events
                    (event_id, aggregate_type, aggregate_id, run_id, task_id, event_type, payload, schema_version)
                VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), ?)
                """, eventId, aggregateType, aggregateId, runId, taskId, eventType, json(payload), schemaVersion);
    }

    public List<EventRecord> findTimeline(String runId) {
        return jdbc.query(
                "SELECT * FROM domain_events WHERE run_id = ? ORDER BY sequence",
                this::mapEvent,
                runId
        );
    }

    private SessionRecord mapSession(ResultSet rs, int rowNum) throws SQLException {
        return new SessionRecord(rs.getString("id"), instant(rs, "created_at"));
    }

    private RunRecord mapRun(ResultSet rs, int rowNum) throws SQLException {
        return new RunRecord(
                rs.getString("id"),
                rs.getString("session_id"),
                rs.getString("idempotency_key"),
                rs.getString("request_hash"),
                rs.getString("input"),
                instantOrNull(rs, "deadline_at"),
                RunStatus.valueOf(rs.getString("status")),
                jsonNode(rs.getString("output")),
                rs.getLong("version"),
                instant(rs, "created_at"),
                instant(rs, "updated_at")
        );
    }

    private TaskRecord mapTask(ResultSet rs, int rowNum) throws SQLException {
        return new TaskRecord(
                rs.getString("id"),
                rs.getString("run_id"),
                rs.getString("name"),
                instantOrNull(rs, "deadline_at"),
                TaskStatus.valueOf(rs.getString("status")),
                jsonNode(rs.getString("input")),
                jsonNode(rs.getString("output")),
                rs.getString("lease_owner"),
                instantOrNull(rs, "lease_expires_at"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                instant(rs, "next_attempt_at"),
                rs.getLong("version"),
                instant(rs, "created_at"),
                instant(rs, "updated_at")
        );
    }

    private PauseCheckpointRecord mapCheckpoint(ResultSet rs, int rowNum) throws SQLException {
        return new PauseCheckpointRecord(rs.getInt("schema_version"), rs.getString("checkpoint_id"),
                rs.getString("task_id"), rs.getString("run_id"), RunStatus.valueOf(rs.getString("run_status")),
                TaskStatus.valueOf(rs.getString("task_status")), rs.getInt("attempt"), rs.getLong("task_version"),
                rs.getLong("run_version"), instant(rs, "next_attempt_at"), instant(rs, "paused_at"),
                instantOrNull(rs, "consumed_at"), rs.getString("disposition"));
    }

    private EventRecord mapEvent(ResultSet rs, int rowNum) throws SQLException {
        return new EventRecord(
                rs.getLong("sequence"),
                rs.getString("event_id"),
                rs.getString("aggregate_type"),
                rs.getString("aggregate_id"),
                rs.getString("run_id"),
                rs.getString("task_id"),
                rs.getString("event_type"),
                jsonNode(rs.getString("payload")),
                instant(rs, "occurred_at"),
                rs.getString("schema_version")
        );
    }

    private JsonNode jsonNode(String value) throws SQLException {
        if (value == null) {
            return null;
        }
        try {
            return objectMapper.readTree(value);
        } catch (JsonProcessingException e) {
            throw new SQLException("Invalid JSON stored in database", e);
        }
    }

    private String json(JsonNode value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize JSON", e);
        }
    }

    private String jsonOrNull(JsonNode value) {
        return value == null ? null : json(value);
    }

    private Timestamp timestamp(Instant value) { return value == null ? null : Timestamp.from(value); }

    private Instant instant(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value.toInstant();
    }

    private Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        Timestamp value = rs.getTimestamp(column);
        return value == null ? null : value.toInstant();
    }

    private <T> Optional<T> queryOne(String sql, org.springframework.jdbc.core.RowMapper<T> mapper, Object... args) {
        return jdbc.query(sql, mapper, args).stream().findFirst();
    }

    private void requireSingleUpdate(int updated, String aggregate, String id) {
        if (updated != 1) {
            throw new IllegalStateException(aggregate + " " + id + " changed concurrently");
        }
    }
}
