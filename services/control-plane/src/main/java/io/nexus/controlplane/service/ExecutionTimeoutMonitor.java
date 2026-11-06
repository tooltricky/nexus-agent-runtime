package io.nexus.controlplane.service;

import io.nexus.controlplane.persistence.RuntimeStore;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;

/** Deliver committed timeouts to local executors even when another instance wrote TIMEOUT. */
@Component
public class ExecutionTimeoutMonitor {
    private final RuntimeStore store;
    private final ExecutionControlRegistry executions;
    private final TimeoutConfiguration configuration;

    public ExecutionTimeoutMonitor(RuntimeStore store, ExecutionControlRegistry executions, TimeoutConfiguration configuration) {
        this.store = store;
        this.executions = executions;
        this.configuration = configuration;
    }

    // Remains enabled without the timeout writer or heartbeat registrations. Failed reads are
    // retried next tick: the local execution stays registered until it actually exits.
    @Scheduled(fixedDelayString = "${nexus.timeout.poll-delay-ms:1000}", scheduler = "deadlineTaskScheduler")
    public void reconcile() {
        var taskIds = new ArrayList<>(executions.activeTaskIds());
        for (int start = 0; start < taskIds.size(); start += configuration.batchSize()) {
            var batch = new HashSet<>(taskIds.subList(start, Math.min(start + configuration.batchSize(), taskIds.size())));
            store.findTimedOutTasks(batch).forEach(executions::stop);
        }
    }
}
