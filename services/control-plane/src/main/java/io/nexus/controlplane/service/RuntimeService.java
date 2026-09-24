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

    public RuntimeService(RuntimeStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public SessionResponse createSession() {
        SessionRecord session = store.insertSession(Ids.sessionId());
        return new SessionResponse(session.id(), session.createdAt());
    }

    @Transactional
    public AcceptedRunResponse createRun(String idempotencyKey, CreateRunRequest request) {
        if (!store.sessionExists(request.sessionId())) {
            throw new NotFoundException("Session not found: " + request.sessionId());
        }

        String requestHash = requestHash(request);
        String runId = Ids.runId();
        boolean created = store.tryInsertRun(
                runId,
                request.sessionId(),
                idempotencyKey,
                requestHash,
                request.input()
        );

        if (!created) {
            RunRecord existing = store.findRunByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("Idempotent Run disappeared"));
            if (!MessageDigest.isEqual(
                    requestHash.getBytes(StandardCharsets.UTF_8),
                    existing.requestHash().getBytes(StandardCharsets.UTF_8))) {
                throw new ConflictException("Idempotency-Key was already used with a different request");
            }
            return new AcceptedRunResponse(existing.id());
        }

        ObjectNode runCreated = objectMapper.createObjectNode()
                .put("status", RunStatus.CREATED.name())
                .put("input", request.input());
        store.appendEvent(Ids.eventId(), "RUN", runId, runId, null, "RunCreated", runCreated);

        String taskId = Ids.taskId();
        ObjectNode taskInput = objectMapper.createObjectNode().put("input", request.input());
        store.insertTask(taskId, runId, TASK_NAME, taskInput);
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
                run.status(),
                run.output(),
                tasks,
                run.createdAt(),
                run.updatedAt()
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

    private RunRecord findRun(String runId) {
        return store.findRun(runId).orElseThrow(() -> new NotFoundException("Run not found: " + runId));
    }

    private TaskResponse toTaskResponse(TaskRecord task) {
        return new TaskResponse(
                task.id(),
                task.name(),
                task.status(),
                task.input(),
                task.output(),
                task.createdAt(),
                task.updatedAt()
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
                event.occurredAt()
        );
    }

    private String requestHash(CreateRunRequest request) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(
                    (request.sessionId() + "\n" + request.input()).getBytes(StandardCharsets.UTF_8)
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

