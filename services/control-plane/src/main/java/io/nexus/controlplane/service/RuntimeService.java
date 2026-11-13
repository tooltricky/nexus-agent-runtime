package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.api.ApiModels.AcceptedRunResponse;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.api.ApiModels.RunResponse;
import io.nexus.controlplane.api.ApiModels.SessionResponse;
import io.nexus.controlplane.api.ApiModels.TaskResponse;
import io.nexus.controlplane.api.ApiModels.TimelineEventResponse;
import io.nexus.controlplane.api.ApiModels.TimelineResponse;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.SessionRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.identity.TenantContextSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Service
public class RuntimeService {
    private static final String TASK_NAME = "analyze-order-service-error-rate";

    private final RuntimeStore store;
    private final ObjectMapper objectMapper;
    private final TenantContextSource identities;

    public RuntimeService(RuntimeStore store, ObjectMapper objectMapper, TenantContextSource identities) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.identities = identities;
    }

    @Transactional
    public SessionResponse createSession() {
        var scope = identities.requireContext();
        SessionRecord session = store.insertSession(Ids.sessionId(), scope.tenantId(), scope.projectId());
        return new SessionResponse(session.id(), session.createdAt(), session.tenantId(), session.projectId());
    }

    @Transactional
    public AcceptedRunResponse createRun(String idempotencyKey, CreateRunRequest request) {
        var scope = identities.requireContext();
        if (!store.sessionExists(request.sessionId(), scope.tenantId(), scope.projectId())) {
            throw new NotFoundException("Session not found: " + request.sessionId());
        }

        if (request.deadlineAt() != null) {
            request = new CreateRunRequest(request.sessionId(), request.input(),
                    request.deadlineAt().truncatedTo(java.time.temporal.ChronoUnit.MICROS));
        }
        String requestHash = requestHash(request);
        String runId = Ids.runId();
        boolean created = store.tryInsertRun(
                runId,
                request.sessionId(),
                idempotencyKey,
                requestHash,
                request.input(),
                request.deadlineAt()
        );

        if (!created) {
            RunRecord existing = store.findRunByIdempotencyKey(scope.tenantId(), idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("Idempotent Run disappeared"));
            if (!existing.projectId().equals(scope.projectId()) || !MessageDigest.isEqual(
                    requestHash.getBytes(StandardCharsets.UTF_8),
                    existing.requestHash().getBytes(StandardCharsets.UTF_8))) {
                throw new ConflictException("Idempotency-Key was already used with a different request");
            }
            return new AcceptedRunResponse(existing.id());
        }

        // Validate time only after winning insertion: an expired idempotent replay remains valid.
        if (request.deadlineAt() != null && !request.deadlineAt().isAfter(store.currentTime())) {
            throw new InvalidRequestException("deadline_at must be in the future");
        }

        ObjectNode runCreated = objectMapper.createObjectNode()
                .put("status", RunStatus.CREATED.name())
                .put("input", request.input());
        store.appendEvent(Ids.eventId(), "RUN", runId, runId, null, "RunCreated", runCreated);

        String taskId = Ids.taskId();
        ObjectNode taskInput = objectMapper.createObjectNode().put("input", request.input());
        store.insertTask(taskId, runId, TASK_NAME, taskInput, request.deadlineAt());
        ObjectNode taskCreated = objectMapper.createObjectNode()
                .put("status", "PENDING")
                .put("name", TASK_NAME);
        store.appendEvent(Ids.eventId(), "TASK", taskId, runId, taskId, "TaskCreated", taskCreated);

        RunRecord createdRun = store.findRun(runId).orElseThrow();
        store.transitionRun(createdRun, RunStatus.QUEUED, null);
        store.appendEvent(
                Ids.eventId(),
                "RUN",
                runId,
                runId,
                null,
                "RunQueued",
                transitionPayload(RunStatus.CREATED.name(), RunStatus.QUEUED.name())
        );

        return new AcceptedRunResponse(runId);
    }

    @Transactional(readOnly = true)
    public RunResponse getRun(String runId) {
        RunRecord run = findRun(runId);
        List<TaskResponse> tasks = store.findTasks(runId).stream().map(this::toTaskResponse).toList();
        return new RunResponse(
                run.id(),
                run.sessionId(),
                run.input(),
                run.deadlineAt(),
                run.status(),
                run.output(),
                tasks,
                run.createdAt(),
                run.updatedAt(), run.tenantId(), run.projectId()
        );
    }

    @Transactional(readOnly = true)
    public TimelineResponse getTimeline(String runId) {
        findRun(runId);
        List<TimelineEventResponse> events = store.findTimeline(runId).stream()
                .map(this::toTimelineEvent)
                .toList();
        return new TimelineResponse(runId, events);
    }

    public RunRecord requireRunScope(String runId) {
        var scope = identities.requireContext();
        return store.findRun(runId)
                .filter(run -> run.tenantId().equals(scope.tenantId()) && run.projectId().equals(scope.projectId()))
                .orElseThrow(() -> new NotFoundException("Run not found: " + runId));
    }

    private RunRecord findRun(String runId) { return requireRunScope(runId); }

    private TaskResponse toTaskResponse(TaskRecord task) {
        return new TaskResponse(
                task.id(),
                task.name(),
                task.deadlineAt(),
                task.status(),
                task.input(),
                task.output(),
                task.leaseOwner(),
                task.leaseExpiresAt(),
                task.attempt(),
                task.maxAttempts(),
                task.nextAttemptAt(),
                store.findPauseCheckpoint(task.id()).orElse(null),
                task.createdAt(),
                task.updatedAt(), task.tenantId(), task.projectId()
        );
    }

    private TimelineEventResponse toTimelineEvent(EventRecord event) {
        return new TimelineEventResponse(
                event.sequence(),
                event.eventId(),
                event.aggregateType(),
                event.aggregateId(),
                event.runId(),
                event.taskId(),
                event.eventType(),
                event.payload(),
                event.occurredAt(),
                event.schemaVersion(), event.tenantId(), event.projectId()
        );
    }

    private String requestHash(CreateRunRequest request) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(
                    (request.deadlineAt() == null ? request.sessionId() + "\n" + request.input()
                            : "deadline-v1\0" + objectMapper.createArrayNode().add(request.sessionId())
                                    .add(request.input()).add(request.deadlineAt().toString()).toString())
                            .getBytes(StandardCharsets.UTF_8)
            );
            return HexFormat.of().formatHex(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private ObjectNode transitionPayload(String from, String to) {
        return objectMapper.createObjectNode().put("from", from).put("to", to);
    }
}
