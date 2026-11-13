package io.nexus.controlplane.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public final class Records {
    private Records() {
    }

    public record SessionRecord(String id, Instant createdAt, String tenantId, String projectId) {
    }

    public record RunRecord(
            String id,
            String sessionId,
            String idempotencyKey,
            String requestHash,
            String input,
            Instant deadlineAt,
            RunStatus status,
            JsonNode output,
            long version,
            Instant createdAt,
            Instant updatedAt,
            String tenantId, String projectId
    ) {
    }

    public record TaskRecord(
            String id,
            String runId,
            String name,
            Instant deadlineAt,
            TaskStatus status,
            JsonNode input,
            JsonNode output,
            String leaseOwner,
            Instant leaseExpiresAt,
            int attempt,
            int maxAttempts,
            Instant nextAttemptAt,
            long version,
            Instant createdAt,
            Instant updatedAt,
            String tenantId, String projectId
    ) {

    }

    public record PauseCheckpointRecord(
            int schemaVersion, String checkpointId, String taskId, String runId,
            RunStatus runStatus, TaskStatus taskStatus, int attempt, long taskVersion, long runVersion,
            Instant nextAttemptAt, Instant pausedAt, Instant consumedAt, String disposition
    ) { }

    public record EventRecord(
            long sequence,
            String eventId,
            String aggregateType,
            String aggregateId,
            String runId,
            String taskId,
            String eventType,
            JsonNode payload,
            Instant occurredAt,
            String schemaVersion,
            String tenantId, String projectId
    ) {
    }
}
