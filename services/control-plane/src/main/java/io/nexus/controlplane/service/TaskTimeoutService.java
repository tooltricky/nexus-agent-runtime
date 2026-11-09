package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.domain.DeadlineTimestamp;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import java.time.Instant;

@Service
public class TaskTimeoutService {
    private final RuntimeStore store;
    private final ObjectMapper mapper;
    private final ExecutionControlRegistry executions;

    public TaskTimeoutService(RuntimeStore store, ObjectMapper mapper, ExecutionControlRegistry executions) {
        this.store = store;
        this.mapper = mapper;
        this.executions = executions;
    }

    @Transactional
    public int expireDueTasks(int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("Timeout batch size must be positive");
        int expired = 0;
        for (TaskRecord task : store.findDueTasksForUpdate(batchSize)) {
            RunRecord run = store.findRunForUpdate(task.runId()).orElseThrow();
            if (expireLocked(task, run, store.currentTime())) expired++;
        }
        return expired;
    }

    /** Caller holds Task then Run locks in the same transaction. */
    boolean expireLocked(TaskRecord task, RunRecord run, Instant now) {
        if (task.status().isTerminal() || run.status().isTerminal()
                || task.deadlineAt() == null || task.deadlineAt().isAfter(now)) return false;
        String schemaVersion = task.status() == TaskStatus.PAUSED ? "0.4" : "0.3";
        store.consumePauseCheckpoint(task, "TIMEOUT");
        store.timeoutTask(task);
        store.transitionRun(run, RunStatus.TIMEOUT, null);
        store.appendVersionedEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), "TaskTimedOut",
                mapper.createObjectNode().put("from", task.status().name()).put("to", "TIMEOUT")
                        .put("reason", "DEADLINE_EXCEEDED").put("deadline_at", DeadlineTimestamp.format(task.deadlineAt()))
                        .put("attempt", task.attempt()).put("max_attempts", task.maxAttempts()), schemaVersion);
        store.appendVersionedEvent(Ids.eventId(), "RUN", run.id(), run.id(), null, "RunTimedOut",
                mapper.createObjectNode().put("from", run.status().name()).put("to", "TIMEOUT")
                        .put("reason", "DEADLINE_EXCEEDED").put("deadline_at", DeadlineTimestamp.format(run.deadlineAt()))
                        .put("task_id", task.id()), schemaVersion);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { executions.stop(task); }
        });
        return true;
    }
}
