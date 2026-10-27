package io.nexus.controlplane.persistence;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.SessionRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
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
        return jdbc.update("""
                INSERT INTO runs (id, session_id, idempotency_key, request_hash, input, status)
                VALUES (?, ?, ?, ?, ?, 'CREATED')
                ON CONFLICT (idempotency_key) DO NOTHING
                """, id, sessionId, idempotencyKey, requestHash, input) == 1;
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
        jdbc.update("""
                INSERT INTO tasks (id, run_id, name, status, input)
                VALUES (?, ?, ?, 'PENDING', CAST(? AS jsonb))
                """, id, runId, name, json(input));
    }

    public List<TaskRecord> findTasks(String runId) {
        return jdbc.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY created_at, id", this::mapTask, runId);
    }

    public List<TaskRecord> findTasksForUpdate(String runId) {
        return jdbc.query("SELECT * FROM tasks WHERE run_id = ? ORDER BY id FOR UPDATE", this::mapTask, runId);
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

    public record TaskClaim(TaskRecord task, TaskStatus previousStatus) {
    }

    /** Atomically claims the oldest task ready to run and writes its lease. */
    public Optional<TaskClaim> claimNextTask(String leaseOwner, Duration leaseDuration) {
        long leaseMillis = leaseDuration.toMillis();
        return jdbc.query("""
                        WITH candidate AS (
                            SELECT id, status AS previous_status
                            FROM tasks
                            WHERE status IN ('PENDING', 'RETRYING')
                              AND next_attempt_at <= clock_timestamp()
                            ORDER BY created_at, id
                            FOR UPDATE SKIP LOCKED
                            LIMIT 1
                        )
                        UPDATE tasks AS task
                        SET status = 'LEASED',
                            lease_owner = ?,
                            lease_expires_at = clock_timestamp() + (? * INTERVAL '1 millisecond'),
                            attempt = attempt + 1,
                            version = version + 1,
                            updated_at = CURRENT_TIMESTAMP
                        FROM candidate
                        WHERE task.id = candidate.id
                        RETURNING task.*, candidate.previous_status
                        """, (rs, rowNum) -> new TaskClaim(mapTask(rs, rowNum),
                                TaskStatus.valueOf(rs.getString("previous_status"))), leaseOwner, leaseMillis)
                .stream()
                .findFirst();
    }

    public Optional<TaskRecord> findOwnedActiveTaskForUpdate(String leaseOwner) {
        return jdbc.query("""
                        SELECT * FROM tasks
                        WHERE status IN ('LEASED', 'EXECUTING')
                          AND lease_owner = ?
                        ORDER BY created_at, id
                        FOR UPDATE SKIP LOCKED
                        LIMIT 1
                        """, this::mapTask, leaseOwner)
                .stream()
                .findFirst();
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
        task.status().requireTransitionTo(next);
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
                """, next.name(), jsonOrNull(output), next.name(), next.name(), task.id(),
                task.status().name(), task.version(), task.attempt(), leaseOwner);
        return updated == 1;
    }

    /** Locks expired attempts for the watchdog; state and events are committed by the caller. */
    public List<TaskRecord> findExpiredLeasesForUpdate() {
        return jdbc.query("""
                SELECT * FROM tasks
                WHERE status IN ('LEASED', 'EXECUTING')
                  AND lease_expires_at <= clock_timestamp()
                ORDER BY lease_expires_at, id
                FOR UPDATE SKIP LOCKED
                """, this::mapTask);
    }

    public Optional<TaskRecord> finishFailedAttempt(
            TaskRecord task, String leaseOwner, Duration retryDelay, boolean expired
    ) {
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
                    next_attempt_at = clock_timestamp() + (? * INTERVAL '1 millisecond'),
                    version = version + 1, updated_at = clock_timestamp()
                WHERE id = ? AND status = ? AND version = ? AND attempt = ? AND lease_owner = ?
                  AND %s
                RETURNING *
                """.formatted(leasePredicate), this::mapTask, next.name(), retryDelay.toMillis(),
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
