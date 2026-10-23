package io.nexus.controlplane.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ContractValidationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void requestSchemaRejectsMissingWrongTypeAndBlankFieldsButAllowsUnknownFields() throws Exception {
        for (String body : List.of("{}", "{\"session_id\":\"ses_test\"}",
                "{\"session_id\":\" \",\"input\":\"ok\"}",
                "{\"session_id\":\"ses_test\",\"input\":\"\\t\\n\"}",
                "{\"session_id\":\"ses_test\",\"input\":null}",
                "{\"session_id\":\"ses_test\",\"input\":{}}",
                "{\"session_id\":\"ses_test\",\"input\":123}",
                "{\"session_id\":\"ses_test\",\"input\":true}")) {
            assertThat(ContractAssertions.requestErrors(mapper.readTree(body))).as(body).isNotEmpty();
        }
        assertThat(ContractAssertions.requestErrors(mapper.readTree(
                "{\"session_id\":\"ses_test\",\"input\":\"分析错误\",\"future_option\":true}"))).isEmpty();
        for (String key : List.of("", "   ", "k".repeat(256))) {
            assertThat(ContractAssertions.keyErrors(key)).isNotEmpty();
        }
        assertThat(ContractAssertions.keyErrors("k".repeat(255))).isEmpty();
    }

    @Test
    void responseSchemasRejectMissingFieldsWrongTypesAndInvalidDateTime() throws Exception {
        JsonNode session = mapper.readTree("{\"session_id\":\"ses_test\",\"created_at\":\"2026-10-23T08:00:00Z\"}");
        assertThat(ContractAssertions.responseErrors("/v1/sessions", "post", 201, session)).isEmpty();
        for (String field : List.of("session_id", "created_at")) {
            ObjectNode missing = session.deepCopy();
            missing.remove(field);
            assertThat(ContractAssertions.responseErrors("/v1/sessions", "post", 201, missing)).isNotEmpty();
            ObjectNode wrongType = session.deepCopy();
            wrongType.put(field, 123);
            assertThat(ContractAssertions.responseErrors("/v1/sessions", "post", 201, wrongType)).isNotEmpty();
        }
        ((ObjectNode) session).put("created_at", "yesterday");
        assertThat(ContractAssertions.responseErrors("/v1/sessions", "post", 201, session)).isNotEmpty();
        assertThat(ContractAssertions.responseErrors("/v1/runs", "post", 202, mapper.readTree("{}"))).isNotEmpty();
        assertThat(ContractAssertions.responseErrors("/v1/runs", "post", 202, mapper.readTree("{\"run_id\":1}"))).isNotEmpty();
        assertThat(ContractAssertions.responseErrors("/v1/runs", "post", 400, mapper.readTree("{\"code\":\"invalid_request\"}"))).isNotEmpty();
    }

    @Test
    void optionalFieldsCanBeAddedAndRemovedWithoutBecomingRequired() throws Exception {
        ObjectNode run = (ObjectNode) mapper.readTree("""
                {
                  "run_id": "run_test", "session_id": "ses_test", "input": "test",
                  "status": "QUEUED", "tasks": [{
                    "task_id": "tsk_test", "name": "analyze", "status": "PENDING",
                    "input": {"input": "test"}, "attempt": 0, "max_attempts": 3,
                    "next_attempt_at": "2026-10-23T08:00:00Z",
                    "created_at": "2026-10-23T08:00:00Z",
                    "updated_at": "2026-10-23T08:00:00Z"
                  }],
                  "created_at": "2026-10-23T08:00:00Z",
                  "updated_at": "2026-10-23T08:00:00Z"
                }
                """);
        run.put("future_optional_field", "compatible");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty();
        run.remove("future_optional_field");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty();

        ObjectNode event = (ObjectNode) mapper.readTree("""
                {
                  "sequence": 1,
                  "event_id": "evt_00000000-0000-0000-0000-000000000001",
                  "aggregate_type": "TASK",
                  "aggregate_id": "tsk_00000000-0000-0000-0000-000000000001",
                  "run_id": "run_00000000-0000-0000-0000-000000000001",
                  "task_id": "tsk_00000000-0000-0000-0000-000000000001",
                  "event_type": "TaskRetried",
                  "payload": {
                    "from": "EXECUTING", "to": "RETRYING", "attempt": 1,
                    "max_attempts": 3, "reason": "EXECUTION_FAILED",
                    "retry_delay_ms": 1000, "next_attempt_at": "2026-10-23T08:00:01Z"
                  },
                  "occurred_at": "2026-10-23T08:00:00Z"
                }
                """);
        ((ObjectNode) event.path("payload")).put("future_optional_field", true);
        ContractAssertions.event(event);
        ((ObjectNode) event.path("payload")).remove("future_optional_field");
        ContractAssertions.event(event);
    }

    @Test
    void eventEvolutionAllowsOptionalFieldsButRejectsUnknownTypesAndInvalidRetryValues() throws Exception {
        ObjectNode retry = (ObjectNode) mapper.readTree("""
                {
                  "sequence": 1,
                  "event_id": "evt_00000000-0000-0000-0000-000000000001",
                  "aggregate_type": "TASK",
                  "aggregate_id": "tsk_00000000-0000-0000-0000-000000000001",
                  "run_id": "run_00000000-0000-0000-0000-000000000001",
                  "task_id": "tsk_00000000-0000-0000-0000-000000000001",
                  "event_type": "TaskRetried",
                  "payload": {
                    "from": "EXECUTING", "to": "RETRYING",
                    "attempt": 1, "max_attempts": 3, "reason": "EXECUTION_FAILED",
                    "retry_delay_ms": 1000, "next_attempt_at": "2026-10-23T08:00:01Z"
                  },
                  "occurred_at": "2026-10-23T08:00:00Z"
                }
                """);
        retry.put("future_envelope_field", true);
        ((ObjectNode) retry.path("payload")).put("future_payload_field", "compatible");
        ContractAssertions.event(retry);
        for (String field : List.of("attempt", "max_attempts", "retry_delay_ms")) {
            ObjectNode invalid = retry.deepCopy();
            ((ObjectNode) invalid.path("payload")).put(field, 0);
            assertThat(ContractAssertions.eventErrors(invalid)).as(field).isNotEmpty();
        }
        for (String field : List.of("reason", "next_attempt_at", "from", "to")) {
            ObjectNode invalid = retry.deepCopy();
            ((ObjectNode) invalid.path("payload")).put(field, "invalid");
            assertThat(ContractAssertions.eventErrors(invalid)).as(field).isNotEmpty();
        }
        for (String field : List.of("event_type", "aggregate_type", "occurred_at")) {
            ObjectNode invalid = retry.deepCopy();
            invalid.put(field, "unknown");
            assertThat(ContractAssertions.eventErrors(invalid)).as(field).isNotEmpty();
        }
        ObjectNode invalidTaskId = retry.deepCopy();
        invalidTaskId.remove("task_id");
        assertThat(ContractAssertions.eventErrors(invalidTaskId)).isNotEmpty();
        invalidTaskId.putNull("task_id");
        assertThat(ContractAssertions.eventErrors(invalidTaskId)).isNotEmpty();

        ObjectNode runEvent = retry.deepCopy();
        runEvent.put("aggregate_type", "RUN");
        runEvent.put("event_type", "RunStarted");
        runEvent.set("payload", mapper.readTree("{\"from\":\"QUEUED\",\"to\":\"RUNNING\"}"));
        runEvent.put("aggregate_id", runEvent.path("run_id").asText());
        runEvent.remove("task_id");
        ContractAssertions.event(runEvent);
        runEvent.putNull("task_id");
        ContractAssertions.event(runEvent);
    }
}
