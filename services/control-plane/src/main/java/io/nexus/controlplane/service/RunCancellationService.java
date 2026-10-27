package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.RunResponse;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class RunCancellationService {
    private final RuntimeStore store;
    private final ObjectMapper mapper;
    private final RuntimeService runtime;

    public RunCancellationService(RuntimeStore store, ObjectMapper mapper, RuntimeService runtime) {
        this.store = store;
        this.mapper = mapper;
        this.runtime = runtime;
    }

    @Transactional
    public RunResponse cancel(String runId) {
        // Every existing worker path locks/updates Task before Run. Never invert that order.
        var tasks = store.findTasksForUpdate(runId);
        var run = store.findRunForUpdate(runId)
                .orElseThrow(() -> new NotFoundException("Run not found: " + runId));
        if (run.status() == RunStatus.CANCELLED) {
            return runtime.getRun(runId);
        }
        if (run.status() == RunStatus.SUCCEEDED || run.status() == RunStatus.FAILED) {
            throw new CancellationConflictException("Completed Run cannot be cancelled: " + runId);
        }
        // B05 retains the database's one-Task-per-Run invariant; DAG aggregation belongs to B23.
        if (tasks.size() != 1) {
            throw new IllegalStateException("Expected one Task for Run: " + runId);
        }
        TaskRecord task = tasks.getFirst();
        if (task.status() == TaskStatus.SUCCEEDED || task.status() == TaskStatus.FAILED
                || task.status() == TaskStatus.CANCELLED) {
            throw new IllegalStateException("Active Run has terminal Task: " + runId);
        }
        store.cancelTask(task);
        store.transitionRun(run, RunStatus.CANCELLED, null);
        store.appendVersionedEvent(Ids.eventId(), "TASK", task.id(), runId, task.id(), "TaskCancelled",
                mapper.createObjectNode()
                        .put("from", task.status().name()).put("to", "CANCELLED")
                        .put("reason", "USER_REQUESTED")
                        .put("attempt", task.attempt()).put("max_attempts", task.maxAttempts()), "0.2");
        store.appendVersionedEvent(Ids.eventId(), "RUN", runId, runId, null, "RunCancelled",
                mapper.createObjectNode()
                        .put("from", run.status().name()).put("to", "CANCELLED")
                        .put("reason", "USER_REQUESTED").put("task_id", task.id()), "0.2");
        return runtime.getRun(runId);
    }
}
