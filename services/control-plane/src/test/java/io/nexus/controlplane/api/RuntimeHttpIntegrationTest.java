package io.nexus.controlplane.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.support.ContractAssertions;
import io.nexus.controlplane.service.FakeWorkerService;
import io.nexus.controlplane.service.TaskFailureService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import static io.nexus.controlplane.support.RuntimeTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexus.worker.enabled=false",
        "spring.datasource.url=${TEST_DB_URL}",
        "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class RuntimeHttpIntegrationTest {
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired RuntimeStore store;
    @Autowired ApplicationContext context;
    private String sessionId;

    @BeforeEach
    void reset() {
        clearDatabase(jdbc);
        sessionId = createSession();
    }

    @Test
    void httpLifecycleReturnsFixedOutputAndOrderedTimeline() throws Exception {
        String runId = accepted(create("success", body(sessionId, "分析订单服务错误率")));
        JsonNode queued = get("/v1/runs/" + runId);
        assertThat(queued.path("status").asText()).isEqualTo("QUEUED");
        assertThat(queued.path("tasks").size()).isEqualTo(1);
        assertThat(queued.path("tasks").get(0).path("status").asText()).isEqualTo("PENDING");
        assertThat(queued.has("output")).isFalse();
        assertThat(queued.path("tasks").get(0).has("lease_owner")).isFalse();
        var worker = worker(context);
        tick(context, worker);
        JsonNode running = get("/v1/runs/" + runId);
        assertThat(running.path("status").asText()).isEqualTo("RUNNING");
        assertThat(running.path("tasks").get(0).path("lease_owner").asText()).isNotBlank();
        tick(context, worker);
        assertThat(get("/v1/runs/" + runId).path("tasks").get(0).path("status").asText()).isEqualTo("EXECUTING");
        tick(context, worker);

        JsonNode result = get("/v1/runs/" + runId);
        JsonNode output = mapper.readTree("{\"summary\":\"发现错误率上升，需要进一步检查最近发布\"}");
        assertThat(result.path("run_id").asText()).isEqualTo(runId);
        assertThat(result.path("session_id").asText()).isEqualTo(sessionId);
        assertThat(result.path("input").asText()).isEqualTo("分析订单服务错误率");
        assertThat(result.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(result.path("output")).isEqualTo(output);
        assertThat(result.path("tasks").get(0).has("lease_owner")).isFalse();
        assertThat(result.path("tasks").get(0).has("lease_expires_at")).isFalse();
        assertThat(result.path("tasks").get(0).path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(result.path("tasks").get(0).path("output")).isEqualTo(output);
        for (String field : ContractAssertions.requiredRunFields()) {
            var missing = result.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) missing).remove(field);
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, missing))
                    .as("required success field %s", field).isNotEmpty();
            var wrongType = result.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) wrongType).putNull(field);
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, wrongType))
                    .as("non-null success field %s", field).isNotEmpty();
        }
        var missingOutput = result.deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) missingOutput).remove("output");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, missingOutput))
                .as("successful Run requires output").isNotEmpty();
        JsonNode timeline = get("/v1/runs/" + runId + "/timeline");
        assertThat(timeline.path("run_id").asText()).isEqualTo(runId);
        List<JsonNode> events = mapper.convertValue(timeline.path("events"),
                new com.fasterxml.jackson.core.type.TypeReference<List<JsonNode>>() { });
        assertThat(events).extracting(e -> e.path("event_type").asText()).containsExactly(
                "RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted",
                "TaskExecuting", "TaskSucceeded", "RunSucceeded");
        assertThat(events).extracting(e -> e.path("event_id").asText()).doesNotHaveDuplicates();
        assertThat(events).extracting(e -> e.path("sequence").asLong()).isSorted().doesNotHaveDuplicates();
        assertThat(events).allSatisfy(e -> assertThat(e.path("run_id").asText()).isEqualTo(runId));
        tick(context, worker);
        assertThat(get("/v1/runs/" + runId + "/timeline")).isEqualTo(timeline);
    }

    @Test
    void sequentialDuplicateAndConflictingRequestsLeaveOneAggregate() throws Exception {
        String request = body(sessionId, "original");
        String runId = accepted(create("same-key", request));
        assertThat(accepted(create("same-key", request))).isEqualTo(runId);
        assertError(create("same-key", body(sessionId, "different")), 409, "idempotency_conflict");
        assertError(create("same-key", body(createSession(), "original")), 409, "idempotency_conflict");
        assertOneAggregate(runId, sessionId, "original");
    }

    @Test
    void concurrentIdenticalRequestsReturnOneRun() throws Exception {
        String request = body(sessionId, "concurrent");
        List<Callable<ResponseEntity<JsonNode>>> requests = IntStream.range(0, 8)
                .mapToObj(i -> (Callable<ResponseEntity<JsonNode>>) () -> create("same-key", request)).toList();
        var responses = concurrently(requests);
        String runId = accepted(responses.getFirst());
        for (var response : responses) {
            assertThat(accepted(response)).isEqualTo(runId);
        }
        assertOneAggregate(runId, sessionId, "concurrent");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentDifferentRequestsHaveOneWinner(boolean differentSession) throws Exception {
        String otherSession = differentSession ? createSession() : sessionId;
        String otherInput = differentSession ? "original" : "different";
        List<ResponseEntity<JsonNode>> responses = concurrently(List.of(
                () -> create("race-key", body(sessionId, "original")),
                () -> create("race-key", body(otherSession, otherInput))));
        assertThat(responses).extracting(r -> r.getStatusCode().value()).containsExactlyInAnyOrder(202, 409);
        int winner = responses.get(0).getStatusCode().value() == 202 ? 0 : 1;
        assertError(responses.get(1 - winner), 409, "idempotency_conflict");
        assertOneAggregate(accepted(responses.get(winner)), winner == 0 ? sessionId : otherSession,
                winner == 0 ? "original" : otherInput);
    }

    @Test
    void invalidRequestsReturn400WithoutWritingBusinessRecords() throws Exception {
        String valid = body(sessionId, "valid");
        for (String key : new String[]{null, "", "   ", "k".repeat(256)}) {
            assertError(create(key, valid), 400, "invalid_request");
        }
        for (String request : List.of("{}", "{\"input\":\"test\"}",
                "{\"session_id\":\"\",\"input\":\"test\"}",
                "{\"session_id\":\"   \",\"input\":\"test\"}",
                "{\"session_id\":\"" + sessionId + "\"}", body(sessionId, ""), body(sessionId, "  "), body(sessionId, "\t\n"),
                "{\"session_id\":{},\"input\":\"test\"}",
                "{\"session_id\":\"" + sessionId + "\",\"input\":[]}",
                "{\"session_id\":null,\"input\":\"test\"}",
                "{\"session_id\":\"" + sessionId + "\",\"input\":null}", "{invalid", "")) {
            assertError(create(UUID.randomUUID().toString(), request), 400, "invalid_request");
        }
        assertCounts(0, 0, 0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sessions", Integer.class)).isEqualTo(1);
    }

    @Test
    void unknownRequestFieldsAreIgnoredAndDoNotAffectIdempotency() throws Exception {
        String request = body(sessionId, "valid");
        String runId = accepted(create("unknown-fields", request.substring(0, request.length() - 1)
                + ",\"future_option\":{\"enabled\":true}}"));
        assertThat(accepted(create("unknown-fields", request))).isEqualTo(runId);
        assertOneAggregate(runId, sessionId, "valid");
        assertThat(accepted(create("k".repeat(255), body(sessionId, "合法客户端请求")))).isNotBlank();
    }

    @Test
    void scalarValuesCannotBeCoercedIntoRequestStrings() {
        for (String scalar : List.of("123", "1.5", "true", "false")) {
            assertError(create("scalar-input", "{\"session_id\":\"" + sessionId + "\",\"input\":" + scalar + "}"),
                    400, "invalid_request");
            assertError(create("scalar-session", "{\"session_id\":" + scalar + ",\"input\":\"valid\"}"),
                    400, "invalid_request");
        }
        assertCounts(0, 0, 0);
    }

    @Test
    void missingResourcesReturn404WithoutWritingBusinessRecords() throws Exception {
        assertError(create("unknown", body("ses_missing", "test")), 404, "not_found");
        for (String path : List.of("/v1/runs/{run_id}", "/v1/runs/{run_id}/timeline")) {
            var response = http.getForEntity(path.replace("{run_id}", "run_missing"), JsonNode.class);
            ContractAssertions.response(path, "get", response);
            assertError(response, 404, "not_found");
        }
        assertCounts(0, 0, 0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sessions", Integer.class)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void executionFailuresHaveContractValidRetryAndFinalFailure(boolean exhaust) throws Exception {
        verifyFailedAttempts(exhaust, false);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leaseExpiryHasContractValidRetryAndFinalFailure(boolean exhaust) throws Exception {
        verifyFailedAttempts(exhaust, true);
    }

    private void verifyFailedAttempts(boolean exhaust, boolean expired) throws Exception {
        String runId = accepted(create("failure-contract", body(sessionId, "failure contract")));
        var failures = context.getBean(TaskFailureService.class);
        var failingWorker = new FakeWorkerService(store, mapper, failures, task -> {
            throw new IllegalStateException("controlled failure");
        }, 30_000);
        for (int attempt = 1; attempt <= (exhaust ? 3 : 1); attempt++) {
            tick(context, failingWorker);
            if (!expired) tick(context, failingWorker);
            if (expired) {
                jdbc.update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
                failures.recoverExpiredLeases();
            } else {
                tick(context, failingWorker);
            }
            JsonNode run = get("/v1/runs/" + runId);
            JsonNode task = run.path("tasks").get(0);
            assertThat(task.path("attempt").asInt()).isEqualTo(attempt);
            assertThat(task.path("status").asText()).isEqualTo(attempt == 3 ? "FAILED" : "RETRYING");
            assertThat(run.path("status").asText()).isEqualTo(attempt == 3 ? "FAILED" : "RUNNING");
            assertThat(run.has("output")).isFalse();
            assertThat(task.has("output")).isFalse();
            assertThat(task.has("lease_owner")).isFalse();
            assertThat(task.has("lease_expires_at")).isFalse();
            // HTTP timeline validation includes the external event schema and all retry payloads.
            get("/v1/runs/" + runId + "/timeline");
            if (attempt < 3) {
                jdbc.update("UPDATE tasks SET next_attempt_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
            }
        }
        if (!exhaust) {
            var successful = worker(context);
            tick(context, successful);
            tick(context, successful);
            tick(context, successful);
            assertThat(get("/v1/runs/" + runId).path("status").asText()).isEqualTo("SUCCEEDED");
        }
        JsonNode events = get("/v1/runs/" + runId + "/timeline").path("events");
        for (JsonNode event : events) {
            ContractAssertions.event(event);
            // Mutate real events rather than validating only hand-authored fixtures.
            var missingPayload = event.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) missingPayload).remove("payload");
            assertThat(ContractAssertions.eventErrors(missingPayload)).isNotEmpty();
            for (String field : ContractAssertions.requiredEventPayloadFields(event.path("event_type").asText())) {
                var missing = event.deepCopy();
                ((com.fasterxml.jackson.databind.node.ObjectNode) missing.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(missing)).as("required %s.%s", event.path("event_type"), field).isNotEmpty();
                var wrongType = event.deepCopy();
                ((com.fasterxml.jackson.databind.node.ObjectNode) wrongType.path("payload")).putArray(field);
                assertThat(ContractAssertions.eventErrors(wrongType)).as("type %s.%s", event.path("event_type"), field).isNotEmpty();
            }
        }
    }

    private String createSession() {
        var response = http.postForEntity("/v1/sessions", null, JsonNode.class);
        ContractAssertions.response("/v1/sessions", "post", response);
        assertThat(response.getStatusCode().value()).isEqualTo(201);
        assertThat(response.getBody().path("created_at").asText()).isNotBlank();
        String id = response.getBody().path("session_id").asText();
        assertThat(id).isNotBlank();
        return id;
    }

    private String body(String session, String input) throws Exception {
        return mapper.writeValueAsString(Map.of("session_id", session, "input", input));
    }

    private ResponseEntity<JsonNode> create(String key, String request) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (key != null) headers.set("Idempotency-Key", key);
        var response = http.exchange("/v1/runs", HttpMethod.POST, new HttpEntity<>(request, headers), JsonNode.class);
        ContractAssertions.response("/v1/runs", "post", response);
        return response;
    }

    private String accepted(ResponseEntity<JsonNode> response) {
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getBody().path("run_id").asText()).isNotBlank();
        return response.getBody().path("run_id").asText();
    }

    private JsonNode get(String path) {
        var response = http.getForEntity(path, JsonNode.class);
        ContractAssertions.response(path.endsWith("/timeline") ? "/v1/runs/{run_id}/timeline" : "/v1/runs/{run_id}", "get", response);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        return response.getBody();
    }

    private void assertError(ResponseEntity<JsonNode> response, int status, String code) {
        assertThat(response.getStatusCode().value()).isEqualTo(status);
        assertThat(response.getBody().path("code").asText()).isEqualTo(code);
        assertThat(response.getBody().path("message").asText()).isNotBlank();
    }

    private void assertOneAggregate(String runId, String session, String input) {
        assertCounts(1, 1, 3);
        var run = store.findRun(runId).orElseThrow();
        assertThat(run.sessionId()).isEqualTo(session);
        assertThat(run.input()).isEqualTo(input);
        assertThat(store.findTasks(runId)).hasSize(1);
        assertThat(store.findTasks(runId).getFirst().input().path("input").asText()).isEqualTo(input);
        assertThat(store.findTimeline(runId)).extracting(e -> e.eventType())
                .containsExactly("RunCreated", "TaskCreated", "RunQueued");
    }

    private void assertCounts(int runs, int tasks, int events) {
        assertThat(jdbc.queryForObject("SELECT count(*) FROM runs", Integer.class)).isEqualTo(runs);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tasks", Integer.class)).isEqualTo(tasks);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM domain_events", Integer.class)).isEqualTo(events);
    }
}
