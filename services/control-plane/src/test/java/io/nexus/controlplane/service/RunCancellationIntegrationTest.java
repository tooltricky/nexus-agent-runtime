package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.AfterEach;
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
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static io.nexus.controlplane.support.RuntimeTestSupport.concurrently;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** All destructive fixtures run only against the explicitly supplied disposable database. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexus.worker.enabled=false",
        "nexus.worker.retry-initial-delay-ms=60000",
        "nexus.worker.retry-max-delay-ms=240000",
        "spring.datasource.url=${TEST_DB_URL}",
        "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class RunCancellationIntegrationTest {
    private static final String CANCEL_PATH = "/v1/runs/{run_id}/cancel";
    @Autowired RuntimeService runtime;
    @Autowired RunCancellationService cancellations;
    @Autowired RuntimeStore store;
    @Autowired TaskFailureService failures;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired TestRestTemplate http;

    private TransactionTemplate tx;
    private String runId;
    private FakeWorkerService worker;

    @BeforeEach
    void createRun() {
        tx = new TransactionTemplate(transactionManager);
        clearDatabase(jdbc);
        runId = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "cancel test")).runId();
        worker = new FakeWorkerService(store, mapper, failures, new FakeTaskExecutor(mapper), 30_000);
    }

    @AfterEach
    void timelineMatchesStateAndContracts() {
        String runState = "CREATED";
        String taskState = "PENDING";
        var timeline = store.findTimeline(runId);
        assertThat(timeline).extracting(EventRecord::eventId).doesNotHaveDuplicates();
        assertThat(timeline).extracting(EventRecord::sequence).isSorted().doesNotHaveDuplicates();
        for (EventRecord event : timeline) {
            ContractAssertions.event(mapper.valueToTree(event));
            assertThat(event.schemaVersion()).isEqualTo(event.eventType().endsWith("Cancelled") ? "0.2" : null);
            if (event.payload().has("from")) {
                if (event.aggregateType().equals("TASK")) {
                    assertThat(event.payload().path("from").asText()).isEqualTo(taskState);
                    taskState = event.payload().path("to").asText();
                } else {
                    assertThat(event.payload().path("from").asText()).isEqualTo(runState);
                    runState = event.payload().path("to").asText();
                }
            }
        }
        assertThat(task().status().name()).isEqualTo(taskState);
        assertThat(store.findRun(runId).orElseThrow().status().name()).isEqualTo(runState);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"PENDING", "LEASED", "EXECUTING", "RETRYING"})
    void httpCancelsEveryStageAndRejectsLateResults(TaskStatus stage) {
        prepare(stage);
        TaskRecord before = task();
        var history = store.findTimeline(runId);
        var runBefore = store.findRun(runId).orElseThrow();
        var response = cancelHttp(runId);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(response.getBody().path("status").asText()).isEqualTo("CANCELLED");
        assertThat(response.getBody().path("tasks").get(0).path("status").asText()).isEqualTo("CANCELLED");
        TaskRecord cancelled = task();
        assertThat(cancelled.status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(cancelled.leaseOwner()).isNull();
        assertThat(cancelled.leaseExpiresAt()).isNull();
        assertThat(cancelled.output()).isNull();
        assertThat(cancelled.attempt()).isEqualTo(before.attempt());
        assertThat(cancelled.maxAttempts()).isEqualTo(before.maxAttempts());
        assertThat(cancelled.nextAttemptAt()).isEqualTo(before.nextAttemptAt());
        assertThat(cancelled.version()).isEqualTo(before.version() + 1);
        assertThat(store.findRun(runId).orElseThrow().version()).isEqualTo(runBefore.version() + 1);
        var events = store.findTimeline(runId);
        assertThat(events.subList(0, history.size())).isEqualTo(history);
        assertThat(eventTypes()).endsWith("TaskCancelled", "RunCancelled");
        EventRecord taskEvent = events.get(events.size() - 2);
        assertThat(taskEvent.payload().path("from").asText()).isEqualTo(stage.name());
        assertThat(taskEvent.payload().path("attempt").asInt()).isEqualTo(before.attempt());
        assertThat(taskEvent.payload().path("max_attempts").asInt()).isEqualTo(before.maxAttempts());
        assertThat(events.getLast().payload().path("from").asText()).isEqualTo(runBefore.status().name());
        assertThat(events.getLast().payload().path("task_id").asText()).isEqualTo(before.id());
        for (EventRecord event : events.subList(history.size(), events.size())) {
            ObjectNode json = mapper.valueToTree(event);
            for (String field : ContractAssertions.requiredEventPayloadFields(event.eventType())) {
                ObjectNode invalid = json.deepCopy();
                ((ObjectNode) invalid.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(invalid)).isNotEmpty();
                ((ObjectNode) invalid.path("payload")).putArray(field);
                assertThat(ContractAssertions.eventErrors(invalid)).isNotEmpty();
            }
        }
        assertThat(cancelHttp(runId).getBody()).isEqualTo(response.getBody());
        var get = http.getForEntity("/v1/runs/" + runId, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}", "get", get);
        assertThat(get.getBody()).isEqualTo(response.getBody());
        var timeline = http.getForEntity("/v1/runs/" + runId + "/timeline", JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/timeline", "get", timeline);
        if (stage == TaskStatus.LEASED || stage == TaskStatus.EXECUTING) {
            assertThat(failures.failOwnedTask(before, before.leaseOwner(), "late failure")).isFalse();
            assertThat(tx.<Boolean>execute(s -> store.tryTransitionTaskOwned(before,
                    stage == TaskStatus.LEASED ? TaskStatus.EXECUTING : TaskStatus.SUCCEEDED,
                    mapper.createObjectNode().put("summary", "late success"), before.leaseOwner()))).isFalse();
        }
        tx.executeWithoutResult(s -> assertThat(store.claimNextTask("late-worker", Duration.ofSeconds(30))).isEmpty());
        tick();
        failures.recoverExpiredLeases();
        assertThat(task()).isEqualTo(cancelled);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
    }

    @Test
    void concurrentHttpCancellationAppendsOnePairOfEvents() throws Exception {
        prepare(TaskStatus.EXECUTING);
        var responses = concurrently(IntStream.range(0, 6)
                .<java.util.concurrent.Callable<ResponseEntity<JsonNode>>>mapToObj(i -> () -> cancelHttp(runId)).toList());
        assertThat(responses).allSatisfy(r -> {
            assertThat(r.getStatusCode().value()).isEqualTo(200);
            assertThat(r.getBody()).isEqualTo(responses.getFirst().getBody());
        });
        assertThat(eventTypes()).containsOnlyOnce("TaskCancelled", "RunCancelled");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void completedRunsRejectCancellationWithoutChanges(boolean succeeded) {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        prepare(TaskStatus.EXECUTING);
        if (succeeded) tick();
        else assertThat(failures.failOwnedTask(task(), task().leaseOwner(), "final failure")).isTrue();
        var before = runtime.getRun(runId);
        var events = store.findTimeline(runId);
        var response = cancelHttp(runId);
        assertThat(response.getStatusCode().value()).isEqualTo(409);
        assertThat(response.getBody().path("code").asText()).isEqualTo("cancellation_conflict");
        assertThat(runtime.getRun(runId)).isEqualTo(before);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
    }

    @Test
    void missingRunReturns404() {
        var response = cancelHttp("run_missing");
        assertThat(response.getStatusCode().value()).isEqualTo(404);
        assertThat(response.getBody().path("code").asText()).isEqualTo("not_found");
        assertThat(eventTypes()).containsExactly("RunCreated", "TaskCreated", "RunQueued");
    }

    @ParameterizedTest
    @ValueSource(strings = {"TaskCancelled", "RunCancelled"})
    void eventInsertFailureRollsBackStatesLeasesAndBothEvents(String rejectedEvent) {
        prepare(TaskStatus.EXECUTING);
        var before = runtime.getRun(runId);
        var taskBefore = task();
        var runBefore = store.findRun(runId).orElseThrow();
        var events = store.findTimeline(runId);
        // A real database failure, including failure of the second event after the first was inserted.
        jdbc.execute("""
                CREATE FUNCTION reject_test_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.event_type = TG_ARGV[0] THEN RAISE EXCEPTION 'injected cancellation event failure'; END IF;
                  RETURN NEW;
                END $$
                """);
        try {
            jdbc.execute("CREATE TRIGGER reject_test_cancellation BEFORE INSERT ON domain_events "
                    + "FOR EACH ROW EXECUTE FUNCTION reject_test_cancellation('" + rejectedEvent + "')");
            assertThrows(DataAccessException.class, () -> cancellations.cancel(runId));
        } finally {
            jdbc.execute("DROP TRIGGER IF EXISTS reject_test_cancellation ON domain_events");
            jdbc.execute("DROP FUNCTION reject_test_cancellation()");
        }
        assertThat(runtime.getRun(runId)).isEqualTo(before);
        assertThat(task()).isEqualTo(taskBefore);
        assertThat(store.findRun(runId).orElseThrow()).isEqualTo(runBefore);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
        assertThat(cancellations.cancel(runId).status()).isEqualTo(RunStatus.CANCELLED);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndSuccessSerializeInBothOrders(boolean cancelFirst) throws Exception {
        prepare(TaskStatus.EXECUTING);
        if (cancelFirst) {
            ordered(() -> cancellations.cancel(runId), this::tick, false);
            assertCancelled();
        } else {
            ordered(this::tick, () -> assertThrows(CancellationConflictException.class,
                    () -> cancellations.cancel(runId)), false);
            assertThat(task().status()).isEqualTo(TaskStatus.SUCCEEDED);
            assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED);
            assertThat(eventTypes()).doesNotContain("TaskCancelled", "RunCancelled");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndFinalFailureSerializeInBothOrders(boolean cancelFirst) throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        Runnable fail = () -> failures.failOwnedTask(stale, stale.leaseOwner(), "final failure");
        if (cancelFirst) {
            ordered(() -> cancellations.cancel(runId), fail, false);
            assertCancelled();
        } else {
            ordered(fail, () -> assertThrows(CancellationConflictException.class,
                    () -> cancellations.cancel(runId)), false);
            assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
            assertThat(eventTypes()).endsWith("TaskFailed", "RunFailed");
            assertThat(eventTypes()).doesNotContain("TaskCancelled", "RunCancelled");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndRetrySerializeInBothOrders(boolean cancelFirst) throws Exception {
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        Runnable fail = () -> failures.failOwnedTask(stale, stale.leaseOwner(), "retryable failure");
        if (cancelFirst) ordered(() -> cancellations.cancel(runId), fail, false);
        else ordered(fail, () -> cancellations.cancel(runId), false);
        assertCancelled();
        assertThat(eventTypes().contains("TaskRetried")).isEqualTo(!cancelFirst);
        assertThat(store.findTimeline(runId).getLast().payload().path("from").asText()).isEqualTo("RUNNING");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndWatchdogSerializeInBothOrders(boolean cancelFirst) throws Exception {
        prepare(TaskStatus.EXECUTING);
        expireLease();
        if (cancelFirst) ordered(() -> cancellations.cancel(runId), failures::recoverExpiredLeases, false);
        else ordered(failures::recoverExpiredLeases, () -> cancellations.cancel(runId), false);
        assertCancelled();
        assertThat(eventTypes().contains("TaskLeaseExpired")).isEqualTo(!cancelFirst);
        assertThat(eventTypes().contains("TaskRetried")).isEqualTo(!cancelFirst);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndFinalWatchdogSerializeInBothOrders(boolean cancelFirst) throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        prepare(TaskStatus.LEASED);
        expireLease();
        if (cancelFirst) {
            ordered(() -> cancellations.cancel(runId), failures::recoverExpiredLeases, false);
            assertCancelled();
        } else {
            ordered(failures::recoverExpiredLeases, () -> assertThrows(CancellationConflictException.class,
                    () -> cancellations.cancel(runId)), false);
            assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
            assertThat(eventTypes()).endsWith("TaskLeaseExpired", "TaskFailed", "RunFailed");
            assertThat(eventTypes()).doesNotContain("TaskCancelled", "RunCancelled");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void cancellationAndClaimSerializeInBothOrders(boolean cancelFirst) throws Exception {
        if (cancelFirst) ordered(() -> cancellations.cancel(runId), this::tick, false);
        else ordered(this::tick, () -> cancellations.cancel(runId), false);
        assertCancelled();
        assertThat(task().attempt()).isEqualTo(cancelFirst ? 0 : 1);
    }

    @Test
    void rolledBackCancellationDoesNotWinAgainstFinalFailure() throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        prepare(TaskStatus.EXECUTING);
        TaskRecord stale = task();
        ordered(() -> cancellations.cancel(runId),
                () -> assertThat(failures.failOwnedTask(stale, stale.leaseOwner(), "final failure")).isTrue(), true);
        assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
        assertThat(eventTypes()).endsWith("TaskFailed", "RunFailed");
        assertThat(eventTypes()).doesNotContain("TaskCancelled", "RunCancelled");
    }

    @Test
    void wrongAttemptOrVersionCannotSubmitUsingTheSameOwner() {
        prepare(TaskStatus.EXECUTING);
        assertThat(failures.failOwnedTask(task(), task().leaseOwner(), "retryable failure")).isTrue();
        jdbc.update("UPDATE tasks SET next_attempt_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
        tick();
        tick();
        TaskRecord active = task();
        assertThat(active.attempt()).isEqualTo(2);
        for (TaskRecord stale : List.of(withFence(active, active.attempt() - 1, active.version()),
                withFence(active, active.attempt(), active.version() - 1))) {
            assertThat(failures.failOwnedTask(stale, active.leaseOwner(), "stale failure")).isFalse();
            assertThat(tx.<Boolean>execute(s -> store.tryTransitionTaskOwned(stale, TaskStatus.SUCCEEDED,
                    mapper.createObjectNode().put("summary", "stale"), active.leaseOwner()))).isFalse();
        }
        assertThat(task()).isEqualTo(active);
        cancellations.cancel(runId);
        assertThat(failures.failOwnedTask(active, active.leaseOwner(), "late failure")).isFalse();
        assertCancelled();
    }

    /** First operation holds its locks while the second uses a distinct connection to wait or skip. */
    private void ordered(Runnable first, Runnable second, boolean rollbackFirst) throws Exception {
        CountDownLatch firstReady = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch secondStarted = new CountDownLatch(1);
        CountDownLatch secondDone = new CountDownLatch(1);
        AtomicInteger firstPid = new AtomicInteger();
        AtomicInteger secondPid = new AtomicInteger();
        var threads = Executors.newFixedThreadPool(2);
        var a = threads.submit(() -> tx.executeWithoutResult(s -> {
            firstPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
            first.run();
            if (rollbackFirst) s.setRollbackOnly();
            firstReady.countDown();
            awaitLatch(release);
        }));
        java.util.concurrent.Future<?> b = null;
        try {
            awaitLatch(firstReady);
            b = threads.submit(() -> {
                try {
                    tx.executeWithoutResult(s -> {
                        secondPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
                        secondStarted.countDown();
                        second.run();
                        // Expected service conflicts mark this joined transaction rollback-only.
                        if (s.isRollbackOnly()) s.setRollbackOnly();
                    });
                } finally {
                    secondDone.countDown();
                }
            });
            awaitLatch(secondStarted);
            assertThat(secondPid.get()).isNotEqualTo(firstPid.get());
            // No timing guess: observe the second transaction either blocked by the first or finished via SKIP LOCKED.
            await().atMost(Duration.ofSeconds(5)).until(() -> secondDone.getCount() == 0
                    || Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT ? = ANY(pg_blocking_pids(?))", Boolean.class, firstPid.get(), secondPid.get())));
            release.countDown();
            a.get(10, TimeUnit.SECONDS);
            b.get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            a.cancel(true);
            if (b != null) b.cancel(true);
            threads.shutdownNow();
            assertThat(threads.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted concurrency test", e);
        }
    }

    private static TaskRecord withFence(TaskRecord t, int attempt, long version) {
        return new TaskRecord(t.id(), t.runId(), t.name(), t.status(), t.input(), t.output(), t.leaseOwner(),
                t.leaseExpiresAt(), attempt, t.maxAttempts(), t.nextAttemptAt(), version, t.createdAt(), t.updatedAt());
    }

    private void prepare(TaskStatus stage) {
        if (stage != TaskStatus.PENDING) tick();
        if (stage == TaskStatus.EXECUTING || stage == TaskStatus.RETRYING) tick();
        if (stage == TaskStatus.RETRYING) {
            assertThat(failures.failOwnedTask(task(), task().leaseOwner(), "retryable failure")).isTrue();
        }
        assertThat(task().status()).isEqualTo(stage);
    }

    private ResponseEntity<JsonNode> cancelHttp(String id) {
        var response = http.postForEntity("/v1/runs/" + id + "/cancel", null, JsonNode.class);
        ContractAssertions.response(CANCEL_PATH, "post", response);
        return response;
    }

    private void tick() {
        tx.executeWithoutResult(s -> worker.advanceOneTask());
    }

    private TaskRecord task() {
        return store.findTasks(runId).getFirst();
    }

    private void expireLease() {
        jdbc.update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
    }

    private List<String> eventTypes() {
        return store.findTimeline(runId).stream().map(EventRecord::eventType).toList();
    }

    private void assertCancelled() {
        assertThat(task().status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.CANCELLED);
        assertThat(eventTypes()).containsOnlyOnce("TaskCancelled", "RunCancelled");
        assertThat(eventTypes()).doesNotContain("TaskSucceeded", "RunSucceeded", "TaskFailed", "RunFailed");
    }
}
