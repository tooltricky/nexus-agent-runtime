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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;

@Service
@ConditionalOnProperty(name = "nexus.worker.enabled", havingValue = "true", matchIfMissing = true)
public class FakeWorkerService {
    private static final String SUMMARY = "发现错误率上升，需要进一步检查最近发布";

    private final RuntimeStore store;
    private final ObjectMapper objectMapper;
    private final String workerId = Ids.workerId();
    private final long leaseDurationMs;

    @Autowired
    public FakeWorkerService(
            RuntimeStore store,
            ObjectMapper objectMapper,
            @Value("${nexus.worker.lease-duration-ms:30000}") long leaseDurationMs
    ) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.leaseDurationMs = leaseDurationMs;
    }

    public FakeWorkerService(RuntimeStore store, ObjectMapper objectMapper) {
        this(store, objectMapper, 30_000L);
    }

    @Scheduled(fixedDelayString = "${nexus.worker.poll-delay-ms:200}")
    @Transactional
    public void advanceOneTask() {
        recoverExpiredLeases();
        store.findOwnedActiveTaskForUpdate(workerId)
                .ifPresentOrElse(this::advance, this::claimAndAdvance);
    }

    private void claimAndAdvance() {
        store.claimNextTask(workerId, Duration.ofMillis(leaseDurationMs)).ifPresent(this::leased);
    }

    private void advance(TaskRecord task) {
        switch (task.status()) {
            case LEASED -> execute(task);
            case EXECUTING -> succeed(task);
            default -> throw new IllegalStateException("Worker selected non-active Task " + task.id());
        }
    }

    private void leased(TaskRecord task) {
        appendTaskLease(task);
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
        if (!store.tryTransitionTaskOwned(task, TaskStatus.EXECUTING, null, workerId)) {
            return;
        }
        appendTaskTransition(task, TaskStatus.EXECUTING, "TaskExecuting");
    }

    private void succeed(TaskRecord task) {
        ObjectNode output = objectMapper.createObjectNode().put("summary", SUMMARY);
        if (!store.tryTransitionTaskOwned(task, TaskStatus.SUCCEEDED, output, workerId)) {
            return;
        }
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

    private void recoverExpiredLeases() {
        List<TaskRecord> expired = store.requeueExpiredLeases();
        for (TaskRecord task : expired) {
            store.appendEvent(
                    Ids.eventId(),
                    "TASK",
                    task.id(),
                    task.runId(),
                    task.id(),
                    "TaskLeaseExpired",
                    transitionPayload(task.status().name(), TaskStatus.PENDING.name())
                            .put("previous_lease_owner", task.leaseOwner())
            );
        }
    }

    private void appendTaskLease(TaskRecord task) {
        store.appendEvent(
                Ids.eventId(),
                "TASK",
                task.id(),
                task.runId(),
                task.id(),
                "TaskLeased",
                transitionPayload(TaskStatus.PENDING.name(), TaskStatus.LEASED.name())
                        .put("lease_owner", task.leaseOwner())
                        .put("attempt", task.attempt())
                        .put("max_attempts", task.maxAttempts())
                        .put("lease_expires_at", task.leaseExpiresAt().toString())
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
