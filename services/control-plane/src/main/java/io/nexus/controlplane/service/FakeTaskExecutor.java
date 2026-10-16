package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.domain.Records.TaskRecord;
import org.springframework.stereotype.Component;

@Component
public class FakeTaskExecutor implements TaskExecutor {
    private final ObjectMapper objectMapper;

    public FakeTaskExecutor(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public JsonNode execute(TaskRecord task) {
        return objectMapper.createObjectNode().put("summary", "发现错误率上升，需要进一步检查最近发布");
    }
}
