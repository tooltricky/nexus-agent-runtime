package io.nexus.controlplane.service;

import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.HashSet;

/** Check committed pause fences, including cycles already resumed on another instance. */
@Component
public class ExecutionPauseMonitor {
    private final RuntimeStore store;
    private final ExecutionControlRegistry executions;
    private final TimeoutConfiguration configuration;

    public ExecutionPauseMonitor(RuntimeStore store, ExecutionControlRegistry executions, TimeoutConfiguration configuration) {
        this.store = store;
        this.executions = executions;
        this.configuration = configuration;
    }

    @Scheduled(fixedDelayString = "${nexus.timeout.poll-delay-ms:1000}", scheduler = "deadlineTaskScheduler")
    public void reconcile() {
        var taskIds = new ArrayList<>(executions.activeTaskIds());
        for (int start = 0; start < taskIds.size(); start += configuration.batchSize()) {
            var batch = new HashSet<>(taskIds.subList(start, Math.min(start + configuration.batchSize(), taskIds.size())));
            store.findPauseCheckpoints(batch).forEach(executions::stopPaused);
        }
    }
}
