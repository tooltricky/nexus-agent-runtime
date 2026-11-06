package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nexus.controlplane.support.RuntimeTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexus.worker.enabled=false", "nexus.watchdog.enabled=false", "nexus.timeout.enabled=false",
        "nexus.worker.retry-initial-delay-ms=60000", "nexus.worker.retry-max-delay-ms=60000",
        "spring.datasource.url=${TEST_DB_URL}", "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class DeadlineIntegrationTest {
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired WorkerTransactionService workers;
    @Autowired TaskTimeoutService timeouts;
    @Autowired TaskFailureService failures;
    @Autowired RunCancellationService cancellations;
    @Autowired TestRestTemplate http;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired PlatformTransactionManager manager;
    @Autowired ExecutionControlRegistry controls;
    private TransactionTemplate tx;
    private String session;
    private String run;

    @BeforeEach
    void reset() {
        clearDatabase(jdbc);
        tx = new TransactionTemplate(manager);
        session = runtime.createSession().sessionId();
        run = create(Instant.now().plusSeconds(60));
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"PENDING","LEASED","EXECUTING","RETRYING"})
    void allStagesTimeoutAtomicallyAndRejectEveryLateOperation(TaskStatus stage) {
        prepare(stage);
        TaskRecord stale = task();
        due();
        var before = task();
        var history = store.findTimeline(run);
        assertThat(timeouts.expireDueTasks(10)).isEqualTo(1);
        assertTimedOut();
        assertThat(task().attempt()).isEqualTo(before.attempt());
        assertThat(task().version()).isEqualTo(before.version() + 1);
        var events = store.findTimeline(run);
        assertThat(events.subList(0, history.size())).isEqualTo(history);
        for (EventRecord event : events) {
            ContractAssertions.event(mapper.valueToTree(event));
        }
        var get = http.getForEntity("/v1/runs/" + run, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}", "get", get);
        assertThat(get.getBody().has("output")).isFalse();
        assertThat(get.getBody().path("tasks").get(0).has("lease_owner")).isFalse();
        for (EventRecord event : events.subList(history.size(), events.size())) {
            ObjectNode json = mapper.valueToTree(event);
            for (String field : ContractAssertions.requiredEventPayloadFields(event.eventType())) {
                ObjectNode bad = json.deepCopy();
                ((ObjectNode) bad.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
                ((ObjectNode) bad.path("payload")).putArray(field);
                assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            }
        }
        assertThat(timeouts.expireDueTasks(10)).isZero();
        assertThat(workers.nextTask("late", Duration.ofSeconds(30), Set.of())).isEmpty();
        if (stage == TaskStatus.LEASED || stage == TaskStatus.EXECUTING) {
            assertThat(workers.renewLease(stale, "worker", Duration.ofSeconds(30))).isFalse();
            assertThat(failures.failOwnedTask(stale, "worker", "late")).isFalse();
            if (stage == TaskStatus.EXECUTING) assertThat(workers.completeSuccessfully(stale, "worker", output())).isFalse();
        }
        failures.recoverExpiredLeases();
        assertThrows(CancellationConflictException.class, () -> cancellations.cancel(run));
        assertThat(store.findTimeline(run)).isEqualTo(events);
    }

    @Test
    void leaseAndRenewalAreCappedAndCannotReviveDeadlineBeforeScan() {
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        assertThat(workers.renewLease(stale, "worker", Duration.ofHours(2))).isTrue();
        assertThat(task().leaseExpiresAt()).isEqualTo(task().deadlineAt());
        assertThat(task().version()).isEqualTo(stale.version());
        due();
        assertThat(workers.renewLease(stale, "worker", Duration.ofSeconds(30))).isFalse();
        assertThat(workers.hasLiveExecutingLease(stale, "worker")).isFalse();
        assertThat(workers.completeSuccessfully(stale, "worker", output())).isFalse();
        assertThat(task().status()).isEqualTo(TaskStatus.EXECUTING);
        failures.recoverExpiredLeases();
        assertTimedOut();
        assertThat(types()).doesNotContain("TaskRetried","TaskFailed","TaskLeaseExpired");
    }

    @Test
    void overdueQueuedAndLeasedTasksCannotBeClaimedOrStartedBeforeScan() {
        due();
        assertThat(workers.nextTask("worker", Duration.ofSeconds(30), Set.of())).isEmpty();
        assertThat(task().attempt()).isZero();
        timeouts.expireDueTasks(10);
        run = create(Instant.now().plusSeconds(60));
        prepare(TaskStatus.LEASED);
        TaskRecord leased = task();
        due();
        assertThat(workers.markExecuting(leased, "worker")).isEmpty();
        assertThat(workers.nextTask("worker", Duration.ofSeconds(30), Set.of())).isEmpty();
    }

    @Test
    void backoffBeyondDeadlineWaitsWithoutClaimOrPrematureTimeout() {
        prepare(TaskStatus.EXECUTING);
        assertThat(failures.failOwnedTask(task(), "worker", "failure")).isTrue();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(task().nextAttemptAt()).isEqualTo(task().deadlineAt());
        assertThat(types()).endsWith("TaskRetrySkipped").doesNotContain("TaskRetried");
        ContractAssertions.event(mapper.valueToTree(store.findTimeline(run).getLast()));
        assertThat(timeouts.expireDueTasks(10)).isZero();
        assertThat(workers.nextTask("other", Duration.ofSeconds(30), Set.of())).isEmpty();
        due();
        timeouts.expireDueTasks(10);
        assertTimedOut();
        assertThat(task().attempt()).isEqualTo(1);
    }

    @Test
    void cancellationDiscoversExpiryCommitsTimeoutThenReturnsHttpConflict() {
        due();
        var response = http.postForEntity("/v1/runs/" + run + "/cancel", null, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/cancel", "post", response);
        assertThat(response.getStatusCode().value()).isEqualTo(409);
        assertThat(response.getBody().path("code").asText()).isEqualTo("cancellation_conflict");
        assertTimedOut();
    }

    @ParameterizedTest
    @ValueSource(strings = {"TaskTimedOut","RunTimedOut"})
    void eventFailureRollsBackStatesVersionsLeaseEventsAndStopSignal(String rejected) {
        prepare(TaskStatus.EXECUTING);
        due();
        var before = runtime.getRun(run);
        var history = store.findTimeline(run);
        try (var signal = controls.register(task())) {
            jdbc.execute("""
                    CREATE FUNCTION reject_test_timeout() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN
                      IF NEW.event_type = TG_ARGV[0] THEN RAISE EXCEPTION 'injected timeout event failure'; END IF;
                      RETURN NEW;
                    END $$
                    """);
            try {
                jdbc.execute("CREATE TRIGGER reject_test_timeout BEFORE INSERT ON domain_events "
                        + "FOR EACH ROW EXECUTE FUNCTION reject_test_timeout('" + rejected + "')");
                assertThrows(DataAccessException.class, () -> timeouts.expireDueTasks(10));
            } finally {
                jdbc.execute("DROP TRIGGER IF EXISTS reject_test_timeout ON domain_events");
                jdbc.execute("DROP FUNCTION reject_test_timeout()");
            }
            assertThat(signal.isStopRequested()).isFalse();
            assertThat(runtime.getRun(run)).isEqualTo(before);
            assertThat(store.findTimeline(run)).isEqualTo(history);
        }
        timeouts.expireDueTasks(10);
        assertTimedOut();
    }

    @Test
    void independentScannersProcessEachTaskOnlyOnce() throws Exception {
        due();
        concurrently(List.of(() -> timeouts.expireDueTasks(1), () -> timeouts.expireDueTasks(1)));
        assertTimedOut();
    }

    @Test
    void batchSkipsLockedTaskAndContinuesOtherRuns() throws Exception {
        due();
        String first = run;
        run = create(Instant.now().plusSeconds(60));
        due();
        var pool = Executors.newSingleThreadExecutor();
        CountDownLatch locked = new CountDownLatch(1), release = new CountDownLatch(1);
        var future = pool.submit(() -> tx.executeWithoutResult(s -> {
            store.findTasksForUpdate(first);
            locked.countDown();
            awaitLatch(release);
        }));
        try {
            assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(timeouts.expireDueTasks(1)).isEqualTo(1);
            assertTimedOut();
            assertThat(store.findTasks(first).getFirst().status()).isEqualTo(TaskStatus.PENDING);
        } finally {
            release.countDown(); future.get(5, TimeUnit.SECONDS); pool.shutdownNow();
        }
        assertThat(timeouts.expireDueTasks(1)).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"cancel","success","failure","retry","watchdog","renew"})
    void competingOperationsSerializeInBothOrdersAndNeverOverwriteTimeout(String operation) throws Exception {
        prepare(TaskStatus.EXECUTING);
        if (operation.equals("failure")) jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", run);
        if (operation.equals("watchdog")) jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", run);
        TaskRecord snapshot = task();
        Runnable action = action(operation, snapshot);
        // Operation wins while valid; scanner must observe its committed result after waiting.
        ordered(action, () -> timeouts.expireDueTasks(10));
        if (List.of("cancel","success","failure").contains(operation)) {
            due(); timeouts.expireDueTasks(10);
            assertThat(task().status().isTerminal()).isTrue();
            assertThat(types()).doesNotContain("TaskTimedOut");
        } else {
            due(); timeouts.expireDueTasks(10); assertTimedOut();
        }
        // Timeout holds its transaction while a different operation waits or skips.
        run = create(Instant.now().plusSeconds(60));
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        due();
        ordered(() -> timeouts.expireDueTasks(10), action(operation, stale));
        assertTimedOut();
        assertThat(types()).doesNotContain("TaskSucceeded","TaskFailed","TaskCancelled","TaskRetried");
    }

    @ParameterizedTest
    @ValueSource(strings = {"cancel","success","failure"})
    void validTerminalDecisionCanCommitAfterDeadline(String operation) throws Exception {
        prepare(TaskStatus.EXECUTING);
        if (operation.equals("failure")) jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", run);
        setDeadline(store.currentTime().plusMillis(500));
        ordered(action(operation, task()), () -> timeouts.expireDueTasks(10), true);
        timeouts.expireDueTasks(10);
        assertThat(task().status().isTerminal()).isTrue();
        assertThat(types()).doesNotContain("TaskTimedOut","RunTimedOut");
    }

    @Test
    void successWaitingForUnchangedRowLockRechecksTimeAfterDeadline() throws Exception {
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        // Use a real short future deadline, not a mocked clock, while holding the row unchanged.
        setDeadline(store.currentTime().plusMillis(500));
        ordered(() -> { }, () -> assertThat(new WorkerTransactionService(store, mapper).completeSuccessfully(stale, "worker", output())).isFalse(), true);
        timeouts.expireDueTasks(10);
        assertTimedOut();
    }

    @Test
    void httpNormalizesOffsetAndPrecisionAndReplaysExpiredRequestWithoutNewEvents() {
        Instant deadline = store.currentTime().plusSeconds(60);
        String key = UUID.randomUUID().toString();
        ObjectNode body = mapper.createObjectNode().put("session_id", session).put("input", "http")
                .put("deadline_at", deadline.toString());
        var accepted = post(key, body);
        assertThat(accepted.getStatusCode().value()).isEqualTo(202);
        String id = accepted.getBody().path("run_id").asText();
        var actual = store.findRun(id).orElseThrow();
        assertThat(store.findTasks(id).getFirst().deadlineAt()).isEqualTo(actual.deadlineAt());
        body.put("deadline_at", deadline.atOffset(java.time.ZoneOffset.ofHours(8)).toString());
        assertThat(post(key, body).getBody()).isEqualTo(accepted.getBody());
        body.put("deadline_at", deadline.plusSeconds(1).toString());
        assertThat(post(key, body).getStatusCode().value()).isEqualTo(409);

        // A real expiry proves request validation order; do not alter stored request identity.
        Instant shortly = store.currentTime().plusMillis(300);
        body.put("deadline_at", shortly.toString());
        String replayKey = UUID.randomUUID().toString();
        var original = post(replayKey, body);
        assertThat(original.getStatusCode().value()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(3)).until(() -> !store.currentTime().isBefore(shortly));
        assertThat(post(replayKey, body).getBody()).isEqualTo(original.getBody());
        assertThat(store.findTimeline(original.getBody().path("run_id").asText())).hasSize(3);
        assertThat(post(UUID.randomUUID().toString(), body).getStatusCode().value()).isEqualTo(400);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            "+19:00, 68400, T",
            "-19:00, -68400, T",
            "+23:59, 86340, T",
            "-23:59, -86340, t"
    })
    void httpAcceptsFullRfc3339OffsetRangeAndReplaysEquivalentUtc(String offset, int offsetSeconds, String separator) {
        // Use database time plus several days, so even the largest offset stays in the future.
        var local = java.time.LocalDateTime.ofInstant(store.currentTime().plus(Duration.ofDays(3)), java.time.ZoneOffset.UTC)
                .withNano(123456789);
        String input = local.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME).replace("T", separator) + offset;
        Instant expected = local.toInstant(java.time.ZoneOffset.UTC).minusSeconds(offsetSeconds)
                .truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        ObjectNode body = mapper.createObjectNode().put("session_id", session).put("input", "offset")
                .put("deadline_at", input);
        assertThat(ContractAssertions.requestErrors(body)).isEmpty();
        String key = UUID.randomUUID().toString();
        var response = post(key, body);
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        String id = response.getBody().path("run_id").asText();
        assertThat(store.findRun(id).orElseThrow().deadlineAt()).isEqualTo(expected);
        assertThat(store.findTasks(id).getFirst().deadlineAt()).isEqualTo(expected);
        var history = store.findTimeline(id);
        body.put("deadline_at", expected.toString());
        assertThat(post(key, body).getBody()).isEqualTo(response.getBody());
        assertThat(store.findTimeline(id)).isEqualTo(history);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            "9999-12-31T23:59:59-23:59, +10000-01-01T23:58:59Z",
            "9999-12-31T23:59:59.123456789-23:59, +10000-01-01T23:58:59.123456Z",
            "0001-01-01T00:00:00+23:59, 0000-12-31T00:01:00Z"
    })
    void deadlineYearRangeAppliesToInputRatherThanNormalizedUtc(String input, String utc) throws Exception {
        ObjectNode body = mapper.createObjectNode().put("session_id", session).put("input", "boundary")
                .put("deadline_at", input);
        assertThat(ContractAssertions.requestErrors(body)).isEmpty();
        Instant expected = Instant.parse(utc);
        assertThat(mapper.treeToValue(body, CreateRunRequest.class).deadlineAt()).isEqualTo(expected);
        ObjectNode canonical = body.deepCopy().put("deadline_at", io.nexus.controlplane.domain.DeadlineTimestamp.format(expected));
        assertThat(ContractAssertions.requestErrors(canonical)).isEmpty();
        assertThat(mapper.treeToValue(canonical, CreateRunRequest.class).deadlineAt()).isEqualTo(expected);
        String key = UUID.randomUUID().toString();
        var response = post(key, body);
        if (expected.isBefore(store.currentTime())) {
            // This valid timestamp is rejected only because a newly created deadline is past.
            assertThat(response.getStatusCode().value()).isEqualTo(400);
            return;
        }
        assertThat(response.getStatusCode().value()).isEqualTo(202);
        String id = response.getBody().path("run_id").asText();
        assertThat(store.findRun(id).orElseThrow().deadlineAt()).isEqualTo(expected);
        assertThat(store.findTasks(id).getFirst().deadlineAt()).isEqualTo(expected);
        var get = http.getForEntity("/v1/runs/" + id, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}", "get", get);
        String rendered = get.getBody().path("deadline_at").asText();
        assertThat(rendered).doesNotStartWith("+10000");
        assertThat(get.getBody().path("tasks").get(0).path("deadline_at").asText()).isEqualTo(rendered);
        body.put("deadline_at", rendered);
        assertThat(post(key, body).getBody()).isEqualTo(response.getBody());
        var timeline = http.getForEntity("/v1/runs/" + id + "/timeline", JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/timeline", "get", timeline);
    }

    @ParameterizedTest
    @ValueSource(strings = {"\"bad\"","42","true","{}","\"2026-11-06T00:00:00\"","\"2026-02-30T00:00:00Z\"","\"0000-01-01T00:00:00Z\"", "\"2026-12-01T12:00Z\"", "\"2026-12-01T12:00:00+08:00:01\"",
            "\"2026-12-01T12:00:00+24:00\"", "\"2026-12-01T12:00:00-24:00\"",
            "\"2026-12-01T12:00:00+19:60\"", "\"2026-12-01T12:00:00-00:60\""})
    void invalidDeadlineHttpInputIsRejected(String value) throws Exception {
        JsonNode body = mapper.readTree("{\"session_id\":\"" + session + "\",\"input\":\"x\",\"deadline_at\":" + value + "}");
        assertThat(post(UUID.randomUUID().toString(), body).getStatusCode().value()).isEqualTo(400);
    }

    private ResponseEntity<JsonNode> post(String key, JsonNode body) {
        var headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON); headers.set("Idempotency-Key", key);
        var response = http.postForEntity("/v1/runs", new HttpEntity<>(body, headers), JsonNode.class);
        ContractAssertions.response("/v1/runs", "post", response);
        return response;
    }
    private Runnable action(String name, TaskRecord snapshot) {
        // Invoke the transaction body inside the explicit test transaction, bypassing REQUIRES_NEW.
        // Otherwise the first operation would block on its own outer transaction's row lock.
        var body = new WorkerTransactionService(store, mapper);
        return switch (name) {
            case "cancel" -> () -> { try { cancellations.cancel(run); } catch (CancellationConflictException ignored) { } };
            case "success" -> () -> body.completeSuccessfully(snapshot, "worker", output());
            case "failure", "retry" -> () -> failures.failOwnedTask(snapshot, "worker", "test");
            case "renew" -> () -> { try { body.renewLease(snapshot, "worker", Duration.ofSeconds(30)); }
                                    catch (org.springframework.dao.CannotAcquireLockException ignored) { } };
            case "watchdog" -> () -> failures.recoverExpiredLeases();
            default -> throw new IllegalArgumentException(name);
        };
    }
    private void ordered(Runnable first, Runnable second) throws Exception { ordered(first, second, false); }
    private void ordered(Runnable first, Runnable second, boolean crossDeadline) throws Exception {
        CountDownLatch locked = new CountDownLatch(1), release = new CountDownLatch(1), started = new CountDownLatch(1);
        AtomicInteger firstPid = new AtomicInteger(), secondPid = new AtomicInteger();
        var pool = Executors.newFixedThreadPool(2);
        var a = pool.submit(() -> tx.executeWithoutResult(s -> {
            firstPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
            store.findTasksForUpdate(run);
            first.run(); locked.countDown(); awaitLatch(release);
        }));
        java.util.concurrent.Future<?> b = null;
        try {
            assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
            b = pool.submit(() -> tx.executeWithoutResult(s -> {
                secondPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
                started.countDown(); second.run();
            }));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            final var pending = b;
            await().atMost(Duration.ofSeconds(5)).until(() -> pending.isDone() || Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT ? = ANY(pg_blocking_pids(?))", Boolean.class, firstPid.get(), secondPid.get())));
            if (crossDeadline) await().atMost(Duration.ofSeconds(3)).until(() -> !store.currentTime().isBefore(task().deadlineAt()));
            release.countDown(); a.get(5, TimeUnit.SECONDS); b.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown(); a.cancel(true); if (b != null) b.cancel(true);
            pool.shutdownNow(); assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
    private static void awaitLatch(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("latch timeout"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
    private String create(Instant deadline) {
        return runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(session, "deadline", deadline)).runId();
    }
    private void prepare(TaskStatus state) {
        if (state != TaskStatus.PENDING) workers.nextTask("worker", Duration.ofMinutes(10), Set.of()).orElseThrow();
        if (state == TaskStatus.EXECUTING || state == TaskStatus.RETRYING) workers.markExecuting(task(), "worker").orElseThrow();
        if (state == TaskStatus.RETRYING) failures.failOwnedTask(task(), "worker", "retry");
    }
    private void setDeadline(Instant deadline) {
        tx.executeWithoutResult(s -> {
            jdbc.update("UPDATE tasks SET deadline_at=?::timestamptz, lease_expires_at=CASE WHEN lease_expires_at IS NULL THEN NULL ELSE LEAST(lease_expires_at,?::timestamptz) END WHERE run_id=?",
                    deadline.toString(), deadline.toString(), run);
            jdbc.update("UPDATE runs SET deadline_at=?::timestamptz WHERE id=?", deadline.toString(), run);
        });
    }
    private void due() { setDeadline(store.currentTime().minusSeconds(1)); }
    private TaskRecord task() { return store.findTasks(run).getFirst(); }
    private JsonNode output() { return mapper.createObjectNode().put("summary", "done"); }
    private List<String> types() { return store.findTimeline(run).stream().map(EventRecord::eventType).toList(); }
    private void assertTimedOut() {
        assertThat(task().status()).isEqualTo(TaskStatus.TIMEOUT);
        assertThat(runtime.getRun(run).status()).isEqualTo(RunStatus.TIMEOUT);
        assertThat(task().leaseOwner()).isNull(); assertThat(task().leaseExpiresAt()).isNull();
        assertThat(task().output()).isNull();
        assertThat(types()).containsOnlyOnce("TaskTimedOut","RunTimedOut").endsWith("TaskTimedOut","RunTimedOut");
    }
}
