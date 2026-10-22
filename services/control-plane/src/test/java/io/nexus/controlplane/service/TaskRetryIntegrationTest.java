package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static io.nexus.controlplane.support.RuntimeTestSupport.race;

/** Runs only against an explicitly supplied, disposable PostgreSQL database. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(properties = {
        "nexus.worker.enabled=false",
        "nexus.worker.retry-initial-delay-ms=60000",
        "nexus.worker.retry-max-delay-ms=240000",
        "spring.datasource.url=${TEST_DB_URL}",
        "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class TaskRetryIntegrationTest {
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired TaskFailureService failures;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;

    private TransactionTemplate tx;
    private String runId;

    @BeforeEach
    void createRun() {
        tx = new TransactionTemplate(transactionManager);
        clearDatabase(jdbc);
        String sessionId = runtime.createSession().sessionId();
        runId = runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(sessionId, "test")).runId();
    }

    @AfterEach
    void timelineTransitionsMatchPersistedState() {
        String taskStatus = TaskStatus.PENDING.name();
        String runStatus = RunStatus.CREATED.name();
        List<EventRecord> timeline = store.findTimeline(runId);
        assertThat(timeline).extracting(EventRecord::sequence).isSorted().doesNotHaveDuplicates();
        assertThat(timeline).extracting(EventRecord::eventId).doesNotHaveDuplicates();
        for (EventRecord event : timeline) {
            if (!event.payload().has("from")) {
                continue;
            }
            if (event.aggregateType().equals("TASK")) {
                assertThat(event.payload().path("from").asText()).isEqualTo(taskStatus);
                taskStatus = event.payload().path("to").asText();
            } else {
                assertThat(event.payload().path("from").asText()).isEqualTo(runStatus);
                runStatus = event.payload().path("to").asText();
            }
        }
        assertThat(taskStatus).isEqualTo(task().status().name());
        assertThat(runStatus).isEqualTo(store.findRun(runId).orElseThrow().status().name());
    }

    @Test
    void failedExecutionWaitsForBackoffThenSucceedsWithoutRepeatingSuccess() {
        AtomicInteger executions = new AtomicInteger();
        FakeWorkerService first = worker(task -> {
            executions.incrementAndGet();
            throw new IllegalStateException("temporary failure");
        });
        tick(first); // LEASED
        tick(first); // EXECUTING
        tick(first); // RETRYING

        TaskRecord retry = task();
        assertThat(retry.status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(retry.attempt()).isEqualTo(1);
        assertThat(retry.leaseOwner()).isNull();
        assertThat(retry.leaseExpiresAt()).isNull();
        EventRecord event = event("TaskRetried");
        assertThat(event.payload().path("retry_delay_ms").asLong()).isEqualTo(60_000);
        assertThat(Instant.parse(event.payload().path("next_attempt_at").asText())).isEqualTo(retry.nextAttemptAt());
        assertThat(Duration.between(event.occurredAt(), retry.nextAttemptAt()).toMillis()).isGreaterThanOrEqualTo(60_000);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.RUNNING);

        FakeWorkerService second = worker(task -> {
            executions.incrementAndGet();
            return mapper.createObjectNode().put("summary", "recovered");
        });
        tick(second);
        assertThat(task()).isEqualTo(retry); // No early claim or attempt increment.
        makeRetryReady();
        tick(second);
        tick(second);
        tick(second);
        tick(second);
        tick(first);

        assertThat(executions.get()).isEqualTo(2);
        assertThat(task().status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(task().attempt()).isEqualTo(2);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED);
        assertThat(eventTypes()).containsExactly("RunCreated", "TaskCreated", "RunQueued", "TaskLeased",
                "RunStarted", "TaskExecuting", "TaskRetried", "TaskLeased", "TaskExecuting",
                "TaskSucceeded", "RunSucceeded");
        assertThat(store.findTimeline(runId).get(7).payload().path("from").asText()).isEqualTo("RETRYING");
    }

    @Test
    void exhaustingAttemptsFailsTaskAndRunExactlyOnce() {
        AtomicInteger executions = new AtomicInteger();
        FakeWorkerService worker = worker(task -> {
            executions.incrementAndGet();
            throw new IllegalStateException("permanent failure");
        });
        for (int attempt = 1; attempt <= 3; attempt++) {
            tick(worker);
            tick(worker);
            tick(worker);
            if (attempt < 3) {
                assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
                makeRetryReady();
            }
        }
        List<EventRecord> timeline = store.findTimeline(runId);
        tick(worker);
        failures.recoverExpiredLeases();
        assertThat(executions.get()).isEqualTo(3);
        assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task().attempt()).isEqualTo(3);
        assertThat(task().output()).isNull();
        assertThat(task().leaseOwner()).isNull();
        assertThat(task().leaseExpiresAt()).isNull();
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.FAILED);
        assertThat(store.findTimeline(runId)).isEqualTo(timeline);
        assertThat(timeline.stream().filter(e -> e.eventType().equals("TaskRetried"))
                .map(e -> e.payload().path("retry_delay_ms").asLong())).containsExactly(60_000L, 120_000L);
        assertThat(eventTypes()).endsWith("TaskFailed", "RunFailed");
        assertThat(eventTypes()).doesNotContain("TaskLeaseExpired", "RunSucceeded", "TaskSucceeded");
        assertThat(event("TaskFailed").payload().path("attempt").asInt()).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"LEASED", "EXECUTING"})
    void expiryUsesRetryBudgetAndRejectsLateWorker(TaskStatus stage) {
        FakeWorkerService first = successfulWorker();
        tick(first);
        if (stage == TaskStatus.EXECUTING) {
            tick(first);
        }
        TaskRecord stale = task();
        expireLease();
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(eventTypes()).endsWith("TaskLeaseExpired", "TaskRetried");
        assertThat(event("TaskLeaseExpired").payload().path("attempt").asInt()).isEqualTo(1);
        assertThat(event("TaskRetried").payload().path("reason").asText()).isEqualTo("LEASE_EXPIRED");

        makeRetryReady();
        FakeWorkerService second = successfulWorker();
        tick(second);
        List<EventRecord> beforeLateReport = store.findTimeline(runId);
        assertThat(failures.failOwnedTask(stale, stale.leaseOwner(), "late failure")).isFalse();
        tx.executeWithoutResult(s -> assertThat(store.tryTransitionTaskOwned(stale,
                stage == TaskStatus.LEASED ? TaskStatus.EXECUTING : TaskStatus.SUCCEEDED,
                null, stale.leaseOwner())).isFalse());
        assertThat(store.findTimeline(runId)).isEqualTo(beforeLateReport);
        assertThat(task().attempt()).isEqualTo(2);
        assertThat(task().leaseOwner()).isNotEqualTo(stale.leaseOwner());
        tick(second);
        tick(second);
        assertThat(task().status()).isEqualTo(TaskStatus.SUCCEEDED);
    }

    @Test
    void expiredFinalAttemptFailsWithoutAnotherClaim() {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        tick(successfulWorker());
        expireLease();
        failures.recoverExpiredLeases();
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
        assertThat(task().attempt()).isEqualTo(1);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.FAILED);
        assertThat(eventTypes()).endsWith("TaskLeaseExpired", "TaskFailed", "RunFailed");
        assertThat(eventTypes()).doesNotContain("TaskRetried");
        tick(successfulWorker());
        assertThat(task().attempt()).isEqualTo(1);
    }

    @Test
    void foreignAndDuplicateFailureReportsDoNotEmitEvents() {
        tick(successfulWorker());
        TaskRecord leased = task();
        List<EventRecord> before = store.findTimeline(runId);
        assertThat(failures.failOwnedTask(leased, "foreign-worker", "failure")).isFalse();
        assertThat(store.findTimeline(runId)).isEqualTo(before);
        assertThat(failures.failOwnedTask(leased, leased.leaseOwner(), "failure")).isTrue();
        List<EventRecord> after = store.findTimeline(runId);
        assertThat(failures.failOwnedTask(leased, leased.leaseOwner(), "duplicate")).isFalse();
        assertThat(failures.failOwnedTask(task(), leased.leaseOwner(), "duplicate")).isFalse();
        assertThat(store.findTimeline(runId)).isEqualTo(after);
    }

    @Test
    void failureStateAndEventsRollBackTogether() {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        tick(successfulWorker());
        TaskRecord leased = task();
        List<EventRecord> before = store.findTimeline(runId);
        assertThrows(IllegalStateException.class, () -> tx.executeWithoutResult(s -> {
            assertThat(failures.failOwnedTask(leased, leased.leaseOwner(), "failure")).isTrue();
            throw new IllegalStateException("abort transaction");
        }));
        assertThat(task()).isEqualTo(leased);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.RUNNING);
        assertThat(store.findTimeline(runId)).isEqualTo(before);
    }

    @Test
    void leaseChecksUseWallClockEvenWithinOneTransaction() {
        FakeWorkerService worker = successfulWorker();
        tick(worker);
        tick(worker);
        tx.executeWithoutResult(s -> {
            jdbc.update("UPDATE tasks SET lease_expires_at = CURRENT_TIMESTAMP + INTERVAL '10 milliseconds' WHERE run_id = ?", runId);
            jdbc.execute("SELECT pg_sleep(0.05)");
            TaskRecord expired = task();
            assertThat(failures.failOwnedTask(expired, expired.leaseOwner(), "late failure")).isFalse();
            assertThat(store.tryTransitionTaskOwned(expired, TaskStatus.SUCCEEDED, null, expired.leaseOwner())).isFalse();
        });
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
    }

    @Test
    void successAndFinalFailureRaceHaveOneConsistentOutcome() throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts = 1 WHERE run_id = ?", runId);
        FakeWorkerService worker = successfulWorker();
        tick(worker);
        tick(worker);
        TaskRecord executing = task();
        race(() -> tick(worker), () -> failures.failOwnedTask(executing, executing.leaseOwner(), "failure"));

        if (task().status() == TaskStatus.SUCCEEDED) {
            assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED);
            assertThat(eventTypes()).endsWith("TaskSucceeded", "RunSucceeded");
            assertThat(eventTypes()).doesNotContain("TaskFailed", "RunFailed");
        } else {
            assertThat(task().status()).isEqualTo(TaskStatus.FAILED);
            assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.FAILED);
            assertThat(eventTypes()).endsWith("TaskFailed", "RunFailed");
            assertThat(eventTypes()).doesNotContain("TaskSucceeded", "RunSucceeded");
        }
    }

    @Test
    void competingWatchdogsRecoverAnAttemptOnce() throws Exception {
        tick(successfulWorker());
        expireLease();
        race(failures::recoverExpiredLeases, failures::recoverExpiredLeases);
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(eventTypes()).containsOnlyOnce("TaskLeaseExpired", "TaskRetried");
    }

    private FakeWorkerService successfulWorker() {
        return worker(new FakeTaskExecutor(mapper));
    }

    private FakeWorkerService worker(TaskExecutor executor) {
        return new FakeWorkerService(store, mapper, failures, executor, 30_000);
    }

    private void tick(FakeWorkerService worker) {
        tx.executeWithoutResult(s -> worker.advanceOneTask());
    }

    private TaskRecord task() {
        return store.findTasks(runId).getFirst();
    }

    private void expireLease() {
        jdbc.update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
    }

    private void makeRetryReady() {
        jdbc.update("UPDATE tasks SET next_attempt_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
    }

    private List<String> eventTypes() {
        return store.findTimeline(runId).stream().map(EventRecord::eventType).toList();
    }

    private EventRecord event(String type) {
        return store.findTimeline(runId).stream().filter(e -> e.eventType().equals(type)).findFirst().orElseThrow();
    }
}
