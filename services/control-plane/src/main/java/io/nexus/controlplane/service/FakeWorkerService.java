package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore.TaskFence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;

@Service
@ConditionalOnProperty(name = "nexus.worker.enabled", havingValue = "true", matchIfMissing = true)
public class FakeWorkerService {
    private final WorkerTransactionService workerTransactions;
    private final String workerId = Ids.workerId();
    private final long leaseDurationMs;
    private final TaskFailureService failures;
    private final TaskExecutor executor;
    private final Executor executionExecutor;
    private final long dispatchRetryDelayMs;
    private final LeaseHeartbeatService heartbeats;
    private final ExecutionControlRegistry executionControls;
    private volatile Instant dispatchRetryAt = Instant.MIN;
    private final Object selectionLock = new Object();
    private final Set<ExecutionKey> inFlight = new HashSet<>();

    public FakeWorkerService(
            WorkerTransactionService workerTransactions,
            TaskFailureService failures,
            TaskExecutor executor,
            @Qualifier("workerExecutionExecutor") Executor executionExecutor,
            long leaseDurationMs,
            long dispatchRetryDelayMs
    ) {
        this(workerTransactions, failures, executor, executionExecutor, leaseDurationMs, dispatchRetryDelayMs, null);
    }

    @Autowired
    public FakeWorkerService(
            WorkerTransactionService workerTransactions,
            TaskFailureService failures,
            TaskExecutor executor,
            @Qualifier("workerExecutionExecutor") Executor executionExecutor,
            WorkerLeaseConfiguration configuration,
            @Value("${nexus.worker.dispatch-retry-delay-ms:200}") long dispatchRetryDelayMs,
            LeaseHeartbeatService heartbeats,
            ExecutionControlRegistry executionControls
    ) {
        this(workerTransactions, failures, executor, executionExecutor, configuration.leaseDurationMs(),
                dispatchRetryDelayMs, heartbeats, executionControls);
    }

    public FakeWorkerService(
            WorkerTransactionService workerTransactions, TaskFailureService failures, TaskExecutor executor,
            Executor executionExecutor, long leaseDurationMs, long dispatchRetryDelayMs,
            LeaseHeartbeatService heartbeats
    ) {
        this(workerTransactions, failures, executor, executionExecutor, leaseDurationMs, dispatchRetryDelayMs, heartbeats, null);
    }

    public FakeWorkerService(
            WorkerTransactionService workerTransactions, TaskFailureService failures, TaskExecutor executor,
            Executor executionExecutor, long leaseDurationMs, long dispatchRetryDelayMs,
            LeaseHeartbeatService heartbeats, ExecutionControlRegistry executionControls
    ) {
        this.executionControls = executionControls;
        this.workerTransactions = workerTransactions;
        this.leaseDurationMs = leaseDurationMs;
        this.failures = failures;
        this.executor = executor;
        this.executionExecutor = executionExecutor;
        this.dispatchRetryDelayMs = dispatchRetryDelayMs;
        this.heartbeats = heartbeats;
    }

    /** Synchronous constructor retained for deterministic service-level tests. */
    public FakeWorkerService(
            WorkerTransactionService workerTransactions,
            TaskFailureService failures,
            TaskExecutor executor,
            long leaseDurationMs
    ) {
        this(workerTransactions, failures, executor, Runnable::run, leaseDurationMs, 200);
    }

    public FakeWorkerService(
            WorkerTransactionService workerTransactions,
            TaskFailureService failures,
            TaskExecutor executor,
            Executor executionExecutor,
            long leaseDurationMs
    ) {
        this(workerTransactions, failures, executor, executionExecutor, leaseDurationMs, 200);
    }

    @Scheduled(fixedDelayString = "${nexus.worker.poll-delay-ms:200}", scheduler = "workerTaskScheduler")
    public void advanceOneTask() {
        if (dispatchRetryAt.isAfter(Instant.now())) {
            return;
        }
        try {
            // Admission precedes every database operation. The production pool has no queue:
            // a full pool leaves pending tasks and their attempt budgets untouched.
            executionExecutor.execute(this::pollOneTask);
        } catch (RejectedExecutionException rejected) {
            dispatchRetryAt = Instant.now().plusMillis(dispatchRetryDelayMs);
        }
    }

    private void pollOneTask() {
        TaskRecord task;
        ExecutionKey key;
        // Serialize selection and registration only; never hold this lock during execution.
        synchronized (selectionLock) {
            Set<TaskFence> fences = inFlight.stream()
                    .map(active -> new TaskFence(active.taskId(), active.attempt(), active.version()))
                    .collect(Collectors.toUnmodifiableSet());
            var selected = workerTransactions.nextTask(workerId, Duration.ofMillis(leaseDurationMs), fences);
            if (selected.isEmpty()) {
                return;
            }
            task = selected.get().task();
            if (selected.get().newlyClaimed()) {
                track(task);
                return;
            }
            if (task.status() == TaskStatus.LEASED) {
                try {
                    workerTransactions.markExecuting(task, workerId).ifPresent(this::track);
                } finally {
                    stopHeartbeat(task);
                }
                return;
            }
            if (task.status() != TaskStatus.EXECUTING) {
                throw new IllegalStateException("Worker selected non-active Task " + task.id());
            }
            key = new ExecutionKey(task.id(), task.attempt(), task.version());
            if (!inFlight.add(key)) {
                return;
            }
            track(task);
        }
        ExecutionControlRegistry.Signal signal = executionControls == null ? null : executionControls.register(task);
        try {
            if (!workerTransactions.hasLiveExecutingLease(task, workerId) || (signal != null && signal.isStopRequested())) {
                return;
            }
            JsonNode output;
            try {
                output = signal == null ? executor.execute(task) : executor.execute(task, signal);
            } catch (RuntimeException failure) {
                if (signal != null && signal.isStopRequested()) return;
                failures.failOwnedTask(task, workerId, failure.toString());
                return;
            }
            if (signal == null || !signal.isStopRequested()) {
                workerTransactions.completeSuccessfully(task, workerId, output);
            }
        } finally {
            if (signal != null) signal.close();
            stopHeartbeat(task);
            synchronized (selectionLock) {
                inFlight.remove(key);
            }
        }
    }

    private void track(TaskRecord task) {
        if (heartbeats != null) heartbeats.track(task, workerId, Duration.ofMillis(leaseDurationMs));
    }

    private void stopHeartbeat(TaskRecord task) {
        if (heartbeats != null) heartbeats.stop(task, workerId);
    }

    private record ExecutionKey(String taskId, int attempt, long version) {
    }
}
