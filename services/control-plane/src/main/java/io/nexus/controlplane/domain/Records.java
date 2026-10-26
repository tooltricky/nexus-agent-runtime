package io.nexus.controlplane.domain;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

public final class Records {
    private Records() {
    }

    public record SessionRecord(String id, Instant createdAt) {
    }

    public record RunRecord(
            String id,
            String sessionId,
            String idempotencyKey,
            String requestHash,
            String input,
            RunStatus status,
            JsonNode output,
            long version,
            Instant createdAt,
            Instant updatedAt
    ) {
    }

    public record TaskRecord(
            String id,
            String runId,
            String name,
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
            Instant updatedAt
    ) {
    }

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
            String schemaVersion
    ) {
    }
}
