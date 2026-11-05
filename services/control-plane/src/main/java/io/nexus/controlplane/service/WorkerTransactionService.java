package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.persistence.RuntimeStore.TaskClaim;
import io.nexus.controlplane.persistence.RuntimeStore.TaskFence;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

@Service
public class WorkerTransactionService {
    private final RuntimeStore store;
    private final ObjectMapper objectMapper;

    public WorkerTransactionService(RuntimeStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<TaskSelection> nextTask(String workerId, Duration leaseDuration, Set<TaskFence> inFlight) {
        Optional<TaskRecord> owned = store.findOwnedActiveTaskForUpdate(workerId, inFlight);
        if (owned.isPresent()) {
            return owned.map(task -> new TaskSelection(task, false));
        }
        Optional<TaskClaim> claim = store.claimNextTask(workerId, leaseDuration);
        claim.ifPresent(this::recordLease);
        // Claiming, entering execution, and execution remain separate polls and short transactions.
        return claim.map(selected -> new TaskSelection(selected.task(), true));
    }

    public record TaskSelection(TaskRecord task, boolean newlyClaimed) { }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Optional<TaskRecord> markExecuting(TaskRecord task, String workerId) {
        if (!store.tryTransitionTaskOwned(task, TaskStatus.EXECUTING, null, workerId)) {
            return Optional.empty();
        }
        appendTaskTransition(task, TaskStatus.EXECUTING, "TaskExecuting");
        return store.findTasks(task.runId()).stream().filter(updated -> updated.id().equals(task.id())).findFirst();
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean renewLease(TaskRecord task, String workerId, Duration leaseDuration) {
        return store.renewTaskLease(task, workerId, leaseDuration);
    }

    /** Rechecks the lease immediately before an executor call starts. */
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean hasLiveExecutingLease(TaskRecord task, String workerId) {
        return store.hasLiveExecutingLease(task, workerId);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean completeSuccessfully(TaskRecord task, String workerId, JsonNode output) {
        if (!store.tryTransitionTaskOwned(task, TaskStatus.SUCCEEDED, output, workerId)) {
            return false;
        }
        store.appendEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), "TaskSucceeded",
                transitionPayload(TaskStatus.EXECUTING.name(), TaskStatus.SUCCEEDED.name()).set("output", output));

        RunRecord run = store.findRunForUpdate(task.runId())
                .orElseThrow(() -> new IllegalStateException("Run not found: " + task.runId()));
        store.transitionRun(run, RunStatus.SUCCEEDED, output);
        store.appendEvent(Ids.eventId(), "RUN", run.id(), run.id(), null, "RunSucceeded",
                transitionPayload(RunStatus.RUNNING.name(), RunStatus.SUCCEEDED.name()).set("output", output));
        return true;
    }

    private void recordLease(TaskClaim claim) {
        TaskRecord task = claim.task();
        store.appendEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), "TaskLeased",
                transitionPayload(claim.previousStatus().name(), TaskStatus.LEASED.name())
                        .put("lease_owner", task.leaseOwner())
                        .put("attempt", task.attempt())
                        .put("max_attempts", task.maxAttempts())
                        .put("lease_expires_at", task.leaseExpiresAt().toString()));

        RunRecord run = store.findRunForUpdate(task.runId())
                .orElseThrow(() -> new IllegalStateException("Run not found: " + task.runId()));
        if (run.status() == RunStatus.QUEUED) {
            store.transitionRun(run, RunStatus.RUNNING, null);
            store.appendEvent(Ids.eventId(), "RUN", run.id(), run.id(), null, "RunStarted",
                    transitionPayload(RunStatus.QUEUED.name(), RunStatus.RUNNING.name()));
        }
    }

    private void appendTaskTransition(TaskRecord task, TaskStatus next, String eventType) {
        store.appendEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), eventType,
                transitionPayload(task.status().name(), next.name()));
    }

    private ObjectNode transitionPayload(String from, String to) {
        return objectMapper.createObjectNode().put("from", from).put("to", to);
    }
}
