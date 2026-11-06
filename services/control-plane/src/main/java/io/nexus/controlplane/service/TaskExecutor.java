package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexus.controlplane.domain.Records.TaskRecord;

@FunctionalInterface
public interface TaskExecutor {
    JsonNode execute(TaskRecord task);

    /** Override for cooperative checks between downstream operations. Interruption also signals stop. */
    default JsonNode execute(TaskRecord task, ExecutionControlRegistry.Signal signal) {
        signal.throwIfStopRequested();
        return execute(task);
    }
}
