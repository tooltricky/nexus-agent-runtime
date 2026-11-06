package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.DeadlineTimestamp;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Service
public class TaskFailureService {
    private final RuntimeStore store;
    private final ObjectMapper objectMapper;
    private final RetryPolicy retryPolicy;
    private final TaskTimeoutService timeouts;

    public TaskFailureService(RuntimeStore store, ObjectMapper objectMapper, RetryPolicy retryPolicy, TaskTimeoutService timeouts) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.retryPolicy = retryPolicy;
        this.timeouts = timeouts;
    }

    /** Duplicate, stale, expired, and foreign-worker reports cannot change state or emit events. */
    @Transactional
    public boolean failOwnedTask(TaskRecord task, String leaseOwner, String error) {
        if (task.status() != TaskStatus.LEASED && task.status() != TaskStatus.EXECUTING) {
            return false;
        }
        return finishAttempt(task, leaseOwner, false, error);
    }

    @Transactional
    public void recoverExpiredLeases() {
        recoverExpiredLeasesInternal(100);
    }

    @Transactional
    public int recoverExpiredLeases(int batchSize) {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Watchdog batch size must be positive");
        }
        return recoverExpiredLeasesInternal(batchSize);
    }

    private int recoverExpiredLeasesInternal(int batchSize) {
        int recovered = 0;
        for (TaskRecord task : store.findExpiredLeasesForUpdate(batchSize)) {
            if (finishAttempt(task, task.leaseOwner(), true, null)) {
                recovered++;
            }
        }
        return recovered;
    }

    private boolean finishAttempt(TaskRecord task, String leaseOwner, boolean expired, String error) {
        TaskRecord current = store.findTaskForUpdate(task.id()).orElse(null);
        if (current == null || current.status() != task.status() || current.version() != task.version()
                || current.attempt() != task.attempt() || !java.util.Objects.equals(current.leaseOwner(), leaseOwner)) {
            return false;
        }
        RunRecord run = store.findRunForUpdate(task.runId()).orElseThrow();
        if (timeouts.expireLocked(current, run, store.currentTime())) return false;
        Duration delay = task.attempt() < task.maxAttempts()
                ? retryPolicy.delayAfter(task.attempt()) : Duration.ZERO;
        TaskRecord updated = store.finishFailedAttempt(task, leaseOwner, delay, expired).orElse(null);
        if (updated == null) {
            return false;
        }

        String reason = expired ? "LEASE_EXPIRED" : "EXECUTION_FAILED";
        if (expired) {
            appendTaskEvent(task, "TaskLeaseExpired", attemptPayload(task, reason)
                    .put("previous_lease_owner", task.leaseOwner())
                    .put("lease_expires_at", task.leaseExpiresAt().toString()));
        }

        ObjectNode payload = attemptPayload(task, reason)
                .put("from", task.status().name())
                .put("to", updated.status().name());
        if (error != null) {
            payload.put("error", error);
        }
        if (updated.status() == TaskStatus.RETRYING) {
            if (updated.deadlineAt() != null && !updated.nextAttemptAt().isBefore(updated.deadlineAt())) {
                store.appendVersionedEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(),
                        "TaskRetrySkipped", payload.put("reason", "DEADLINE_BEFORE_RETRY")
                                .put("deadline_at", DeadlineTimestamp.format(updated.deadlineAt()))
                                .put("next_attempt_at", DeadlineTimestamp.format(updated.nextAttemptAt())), "0.3");
            } else {
                appendTaskEvent(task, "TaskRetried", payload
                        .put("retry_delay_ms", delay.toMillis())
                        .put("next_attempt_at", updated.nextAttemptAt().toString()));
            }
        } else {
            appendTaskEvent(task, "TaskFailed", payload);
            // finishFailedAttempt already holds the Task lock, matching cancellation's lock order.
            store.transitionRun(run, RunStatus.FAILED, null);
            store.appendEvent(Ids.eventId(), "RUN", run.id(), run.id(), null, "RunFailed",
                    objectMapper.createObjectNode()
                            .put("from", run.status().name())
                            .put("to", RunStatus.FAILED.name())
                            .put("task_id", task.id())
                            .put("reason", reason));
        }
        return true;
    }

    private ObjectNode attemptPayload(TaskRecord task, String reason) {
        return objectMapper.createObjectNode()
                .put("attempt", task.attempt())
                .put("max_attempts", task.maxAttempts())
                .put("reason", reason);
    }

    private void appendTaskEvent(TaskRecord task, String eventType, ObjectNode payload) {
        store.appendEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), eventType, payload);
    }
}
