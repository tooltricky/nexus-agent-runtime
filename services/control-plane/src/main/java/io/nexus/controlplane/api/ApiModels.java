package io.nexus.controlplane.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import jakarta.validation.constraints.NotBlank;

import java.time.Instant;
import java.util.List;

public final class ApiModels {
    private ApiModels() {
    }

    public record CreateRunRequest(@NotBlank String sessionId, @NotBlank String input,
            @com.fasterxml.jackson.databind.annotation.JsonDeserialize(using = DeadlineDeserializer.class)
            Instant deadlineAt) {
        public CreateRunRequest(String sessionId, String input) { this(sessionId, input, null); }
    }

    public record SessionResponse(String sessionId, Instant createdAt) {
    }

    public record AcceptedRunResponse(String runId) {
    }

    public record TaskResponse(
            String taskId,
            String name,
            @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = DeadlineSerializer.class) Instant deadlineAt,
            TaskStatus status,
            JsonNode input,
            JsonNode output,
            String leaseOwner,
            Instant leaseExpiresAt,
            int attempt,
            int maxAttempts,
            Instant nextAttemptAt,
            Instant createdAt,
            Instant updatedAt
    ) {
    }

    public record RunResponse(
            String runId,
            String sessionId,
            String input,
            @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = DeadlineSerializer.class) Instant deadlineAt,
            RunStatus status,
            JsonNode output,
            List<TaskResponse> tasks,
            Instant createdAt,
            Instant updatedAt
    ) {
    }

    public record TimelineEventResponse(
            long sequence,
            String eventId,
            String aggregateType,
            String aggregateId,
            String runId,
            String taskId,
            String eventType,
            JsonNode payload,
            Instant occurredAt,
            @JsonInclude(JsonInclude.Include.NON_NULL) String schemaVersion
    ) {
    }

    public record TimelineResponse(String runId, List<TimelineEventResponse> events) {
    }

    public record ErrorResponse(String code, String message) {
    }
}
