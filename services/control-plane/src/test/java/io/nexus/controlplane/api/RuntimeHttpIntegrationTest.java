package io.nexus.controlplane.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.persistence.RuntimeStore;
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
        var worker = worker(context);
        tick(context, worker);
        tick(context, worker);
        tick(context, worker);

        JsonNode result = get("/v1/runs/" + runId);
        JsonNode output = mapper.readTree("{\"summary\":\"发现错误率上升，需要进一步检查最近发布\"}");
        assertThat(result.path("run_id").asText()).isEqualTo(runId);
        assertThat(result.path("session_id").asText()).isEqualTo(sessionId);
        assertThat(result.path("input").asText()).isEqualTo("分析订单服务错误率");
        assertThat(result.path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(result.path("output")).isEqualTo(output);
        assertThat(result.path("tasks").get(0).path("status").asText()).isEqualTo("SUCCEEDED");
        assertThat(result.path("tasks").get(0).path("output")).isEqualTo(output);
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
                "{\"session_id\":\"" + sessionId + "\"}", body(sessionId, ""), body(sessionId, "  "),
                "{\"session_id\":null,\"input\":\"test\"}",
                "{\"session_id\":\"" + sessionId + "\",\"input\":null}", "{invalid", "")) {
            assertError(create(UUID.randomUUID().toString(), request), 400, "invalid_request");
        }
        assertCounts(0, 0, 0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sessions", Integer.class)).isEqualTo(1);
    }

    @Test
    void missingResourcesReturn404WithoutWritingBusinessRecords() throws Exception {
        assertError(create("unknown", body("ses_missing", "test")), 404, "not_found");
        assertError(http.getForEntity("/v1/runs/run_missing", JsonNode.class), 404, "not_found");
        assertError(http.getForEntity("/v1/runs/run_missing/timeline", JsonNode.class), 404, "not_found");
        assertCounts(0, 0, 0);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sessions", Integer.class)).isEqualTo(1);
    }

    private String createSession() {
        var response = http.postForEntity("/v1/sessions", null, JsonNode.class);
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
        return http.exchange("/v1/runs", HttpMethod.POST, new HttpEntity<>(request, headers), JsonNode.class);
    }

    private String accepted(ResponseEntity<JsonNode> response) {
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        assertThat(response.getBody().path("run_id").asText()).isNotBlank();
        return response.getBody().path("run_id").asText();
    }

    private JsonNode get(String path) {
        var response = http.getForEntity(path, JsonNode.class);
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
