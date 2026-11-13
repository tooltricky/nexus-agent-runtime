package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.api.ApiModels.RunResponse;
import io.nexus.controlplane.domain.Records.PauseCheckpointRecord;
import io.nexus.controlplane.domain.Records.RunRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class RunPauseService {
    private final RuntimeStore store;
    private final RuntimeService runtime;
    private final TaskTimeoutService timeouts;
    private final ExecutionControlRegistry executions;
    private final ObjectMapper mapper;

    public RunPauseService(RuntimeStore store, RuntimeService runtime, TaskTimeoutService timeouts,
                           ExecutionControlRegistry executions, ObjectMapper mapper) {
        this.store = store;
        this.runtime = runtime;
        this.timeouts = timeouts;
        this.executions = executions;
        this.mapper = mapper;
    }

    @Transactional(noRollbackFor = PauseConflictException.class)
    public RunResponse pause(String runId) {
        var locked = lockActiveRun(runId);
        var run = locked.run();
        var task = locked.task();
        var now = store.currentTime();
        if (timeouts.expireLocked(task, run, now)) throw conflict(runId);
        if (run.status() == RunStatus.PAUSED) return runtime.getRun(runId);
        var checkpoint = store.savePauseCheckpoint(task, run, now);
        store.pauseTask(task);
        store.transitionRun(run, RunStatus.PAUSED, null);
        append(task, "TaskPaused", taskPayload(checkpoint, task.status().name(), "PAUSED", task.maxAttempts()));
        appendRun(task, "RunPaused", runPayload(checkpoint, run.status().name(), "PAUSED"));
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override public void afterCommit() { executions.stopPaused(checkpoint); }
        });
        return runtime.getRun(runId);
    }

    @Transactional(noRollbackFor = PauseConflictException.class)
    public RunResponse resume(String runId, String checkpointId) {
        var locked = lockActiveRun(runId);
        var run = locked.run();
        var task = locked.task();
        var now = store.currentTime();
        if (timeouts.expireLocked(task, run, now)) throw conflict(runId);
        var checkpoint = store.findPauseCheckpoint(task.id()).orElse(null);
        if (checkpointId != null && (checkpoint == null || !checkpoint.checkpointId().equals(checkpointId))) {
            throw new PauseConflictException("Pause checkpoint does not match current cycle: " + runId);
        }
        if (run.status() != RunStatus.PAUSED) return runtime.getRun(runId);
        if (checkpoint == null || checkpoint.consumedAt() != null) {
            throw new IllegalStateException("Paused Run has no active checkpoint: " + runId);
        }
        boolean interrupted = checkpoint.taskStatus() == TaskStatus.LEASED || checkpoint.taskStatus() == TaskStatus.EXECUTING;
        boolean exhausted = interrupted && task.attempt() >= task.maxAttempts();
        TaskStatus nextTask = exhausted ? TaskStatus.FAILED
                : checkpoint.taskStatus() == TaskStatus.RETRYING ? TaskStatus.RETRYING : TaskStatus.PENDING;
        RunStatus nextRun = exhausted ? RunStatus.FAILED : checkpoint.runStatus();
        store.consumePauseCheckpoint(task, exhausted ? "ATTEMPTS_EXHAUSTED" : "RESUMED");
        store.resumeTask(task, nextTask, interrupted ? now : checkpoint.nextAttemptAt());
        store.transitionRun(run, nextRun, null);
        var taskPayload = taskPayload(checkpoint, "PAUSED", nextTask.name(), task.maxAttempts());
        var runPayload = runPayload(checkpoint, "PAUSED", nextRun.name());
        if (exhausted) {
            taskPayload.put("reason", "PAUSED_ATTEMPTS_EXHAUSTED");
            runPayload.put("reason", "PAUSED_ATTEMPTS_EXHAUSTED");
        }
        append(task, exhausted ? "TaskFailed" : "TaskResumed", taskPayload);
        appendRun(task, exhausted ? "RunFailed" : "RunResumed", runPayload);
        return runtime.getRun(runId);
    }

    private LockedRun lockActiveRun(String runId) {
        runtime.requireRunScope(runId);
        var tasks = store.findTasksForUpdate(runId);
        var run = store.findRunForUpdate(runId).orElseThrow(() -> new NotFoundException("Run not found: " + runId));
        if (run.status().isTerminal()) throw conflict(runId);
        if (tasks.size() != 1 || tasks.getFirst().status().isTerminal()) {
            throw new IllegalStateException("Expected one unfinished Task for Run: " + runId);
        }
        return new LockedRun(run, tasks.getFirst());
    }

    private PauseConflictException conflict(String runId) {
        return new PauseConflictException("Completed Run cannot be paused or resumed: " + runId);
    }

    private ObjectNode taskPayload(PauseCheckpointRecord checkpoint, String from, String to, int maxAttempts) {
        return transition(checkpoint, from, to).put("attempt", checkpoint.attempt())
                .put("max_attempts", maxAttempts);
    }
    private ObjectNode runPayload(PauseCheckpointRecord checkpoint, String from, String to) {
        return transition(checkpoint, from, to).put("task_id", checkpoint.taskId());
    }
    private ObjectNode transition(PauseCheckpointRecord checkpoint, String from, String to) {
        return mapper.createObjectNode().put("from", from).put("to", to)
                .put("checkpoint_id", checkpoint.checkpointId());
    }
    private void append(TaskRecord task, String type, ObjectNode payload) {
        store.appendVersionedEvent(Ids.eventId(), "TASK", task.id(), task.runId(), task.id(), type, payload, "0.4");
    }
    private void appendRun(TaskRecord task, String type, ObjectNode payload) {
        store.appendVersionedEvent(Ids.eventId(), "RUN", task.runId(), task.runId(), null, type, payload, "0.4");
    }
    private record LockedRun(RunRecord run, TaskRecord task) { }
}
