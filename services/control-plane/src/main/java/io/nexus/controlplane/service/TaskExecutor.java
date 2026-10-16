package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexus.controlplane.domain.Records.TaskRecord;

@FunctionalInterface
public interface TaskExecutor {
    JsonNode execute(TaskRecord task);
}
