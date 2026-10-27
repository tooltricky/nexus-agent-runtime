package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
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

    public TaskFailureService(RuntimeStore store, ObjectMapper objectMapper, RetryPolicy retryPolicy) {
        this.store = store;
        this.objectMapper = objectMapper;
        this.retryPolicy = retryPolicy;
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
        for (TaskRecord task : store.findExpiredLeasesForUpdate()) {
            finishAttempt(task, task.leaseOwner(), true, null);
        }
    }

    private boolean finishAttempt(TaskRecord task, String leaseOwner, boolean expired, String error) {
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
            appendTaskEvent(task, "TaskRetried", payload
                    .put("retry_delay_ms", delay.toMillis())
                    .put("next_attempt_at", updated.nextAttemptAt().toString()));
        } else {
            appendTaskEvent(task, "TaskFailed", payload);
            // finishFailedAttempt already holds the Task lock, matching cancellation's lock order.
            RunRecord run = store.findRunForUpdate(task.runId())
                    .orElseThrow(() -> new IllegalStateException("Run not found: " + task.runId()));
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
