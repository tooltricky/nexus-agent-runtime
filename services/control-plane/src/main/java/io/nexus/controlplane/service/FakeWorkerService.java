package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@ConditionalOnProperty(name = "nexus.worker.enabled", havingValue = "true", matchIfMissing = true)
public class FakeWorkerService {
    private static final String SUMMARY = "发现错误率上升，需要进一步检查最近发布";

    private final RuntimeStore store;
    private final ObjectMapper objectMapper;

    public FakeWorkerService(RuntimeStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @Scheduled(fixedDelayString = "${nexus.worker.poll-delay-ms:200}")
    @Transactional
    public void advanceOneTask() {
        store.findNextActiveTaskForUpdate().ifPresent(this::advance);
    }

    private void advance(TaskRecord task) {
        switch (task.status()) {
            case PENDING -> lease(task);
            case LEASED -> execute(task);
            case EXECUTING -> succeed(task);
            default -> throw new IllegalStateException("Worker selected terminal Task " + task.id());
        }
    }

    private void lease(TaskRecord task) {
        store.transitionTask(task, TaskStatus.LEASED, null);
        appendTaskTransition(task, TaskStatus.LEASED, "TaskLeased");

        RunRecord run = requiredRun(task.runId());
        if (run.status() == RunStatus.QUEUED) {
            store.transitionRun(run, RunStatus.RUNNING, null);
            store.appendEvent(
                    Ids.eventId(),
                    "RUN",
                    run.id(),
                    run.id(),
                    null,
                    "RunStarted",
                    transitionPayload(RunStatus.QUEUED.name(), RunStatus.RUNNING.name())
            );
        }
    }

    private void execute(TaskRecord task) {
        store.transitionTask(task, TaskStatus.EXECUTING, null);
        appendTaskTransition(task, TaskStatus.EXECUTING, "TaskExecuting");
    }

    private void succeed(TaskRecord task) {
        ObjectNode output = objectMapper.createObjectNode().put("summary", SUMMARY);
        store.transitionTask(task, TaskStatus.SUCCEEDED, output);
        store.appendEvent(
                Ids.eventId(),
                "TASK",
                task.id(),
                task.runId(),
                task.id(),
                "TaskSucceeded",
                transitionPayload(TaskStatus.EXECUTING.name(), TaskStatus.SUCCEEDED.name()).set("output", output)
        );

        RunRecord run = requiredRun(task.runId());
        store.transitionRun(run, RunStatus.SUCCEEDED, output);
        store.appendEvent(
                Ids.eventId(),
                "RUN",
                run.id(),
                run.id(),
                null,
                "RunSucceeded",
                transitionPayload(RunStatus.RUNNING.name(), RunStatus.SUCCEEDED.name()).set("output", output)
        );
    }

    private void appendTaskTransition(TaskRecord task, TaskStatus next, String eventType) {
        store.appendEvent(
                Ids.eventId(),
                "TASK",
                task.id(),
                task.runId(),
                task.id(),
                eventType,
                transitionPayload(task.status().name(), next.name())
        );
    }

    private RunRecord requiredRun(String runId) {
        return store.findRun(runId).orElseThrow(() -> new IllegalStateException("Run not found: " + runId));
    }

    private ObjectNode transitionPayload(String from, String to) {
        return objectMapper.createObjectNode().put("from", from).put("to", to);
    }
}
