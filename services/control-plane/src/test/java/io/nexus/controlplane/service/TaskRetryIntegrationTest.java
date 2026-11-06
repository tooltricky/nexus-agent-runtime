package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static io.nexus.controlplane.support.RuntimeTestSupport.race;

/** Runs only against an explicitly supplied, disposable PostgreSQL database. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(properties = {
        "nexus.timeout.enabled=false", "nexus.worker.enabled=false",
        "nexus.watchdog.enabled=false",
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
    @Autowired WorkerTransactionService workerTransactions;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired @Qualifier("workerTaskScheduler") ThreadPoolTaskScheduler workerTaskScheduler;
    @Autowired @Qualifier("watchdogTaskScheduler") ThreadPoolTaskScheduler watchdogTaskScheduler;

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
            ContractAssertions.event(mapper.valueToTree(event));
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

    @ParameterizedTest
    @ValueSource(strings = {"TaskSucceeded", "RunSucceeded"})
    void resultEventDatabaseFailureRollsBackStateAndIsNotReportedAsExecutionFailure(String rejectedEvent) {
        FakeWorkerService worker = successfulWorker();
        tick(worker); // LEASED
        tick(worker); // EXECUTING
        TaskRecord executing = task();
        List<EventRecord> before = store.findTimeline(runId);

        installEventFailureTrigger(rejectedEvent);
        try {
            assertThrows(DataAccessException.class, () -> worker.advanceOneTask());
        } finally {
            removeEventFailureTrigger();
        }

        assertThat(task()).isEqualTo(executing);
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.RUNNING);
        assertThat(store.findTimeline(runId)).isEqualTo(before);
        assertThat(eventTypes()).doesNotContain("TaskSucceeded", "RunSucceeded", "TaskFailed", "RunFailed",
                "TaskRetried");

        worker.advanceOneTask();
        assertThat(task().status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(eventTypes()).endsWith("TaskSucceeded", "RunSucceeded");
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
    void expiredLeaseBetweenDispatchAndExecutorIsNotStarted() {
        AtomicInteger executions = new AtomicInteger();
        WorkerTransactionService leaseGate = org.mockito.Mockito.mock(WorkerTransactionService.class,
                org.mockito.AdditionalAnswers.delegatesTo(workerTransactions));
        org.mockito.Mockito.doAnswer(invocation -> {
            expireLease();
            return workerTransactions.hasLiveExecutingLease(invocation.getArgument(0), invocation.getArgument(1));
        }).when(leaseGate).hasLiveExecutingLease(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyString());
        FakeWorkerService worker = new FakeWorkerService(leaseGate, failures, task -> {
            executions.incrementAndGet();
            return mapper.createObjectNode().put("summary", "must not run");
        }, 30_000);

        tick(worker); // LEASED
        tick(worker); // EXECUTING
        tick(worker); // dispatch, then lease expires before the final check

        assertThat(executions).hasValue(0);
        assertThat(task().status()).isEqualTo(TaskStatus.EXECUTING);
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(eventTypes()).doesNotContain("TaskSucceeded", "RunSucceeded", "TaskFailed", "RunFailed");
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

    @Test
    void blockedExecutorHoldsNoDatabaseTransactionAndOtherRunAndWatchdogProgress() throws Exception {
        String blockedRun = runId;
        String otherRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "other run")).runId();
        CountDownLatch executorEntered = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        FakeWorkerService blockedWorker = worker(task -> {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            executorEntered.countDown();
            try {
                assertThat(releaseExecutor.await(10, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Executor interrupted", interrupted);
            }
            return mapper.createObjectNode().put("summary", "late result");
        });

        tick(blockedWorker);
        var executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(blockedWorker::advanceOneTask).get(5, TimeUnit.SECONDS); // mark EXECUTING
            var blocked = executor.submit(blockedWorker::advanceOneTask);
            assertThat(executorEntered.await(5, TimeUnit.SECONDS)).isTrue();

            expireLease(blockedRun);
            assertThat(failures.recoverExpiredLeases(1)).isEqualTo(1);
            assertThat(task(blockedRun).status()).isEqualTo(TaskStatus.RETRYING);

            FakeWorkerService otherWorker = successfulWorker();
            tick(otherWorker);
            tick(otherWorker);
            tick(otherWorker);
            assertThat(store.findRun(otherRun).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED);

            releaseExecutor.countDown();
            blocked.get(5, TimeUnit.SECONDS);
            assertThat(task(blockedRun).status()).isEqualTo(TaskStatus.RETRYING);
            assertThat(eventTypes(blockedRun)).doesNotContain("TaskSucceeded", "RunSucceeded", "TaskFailed");
        } finally {
            releaseExecutor.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void expiredExecutingTaskIsNotCalledAgainBeforeWatchdogRecovery() {
        AtomicInteger executions = new AtomicInteger();
        FakeWorkerService worker = worker(task -> {
            executions.incrementAndGet();
            return mapper.createObjectNode().put("summary", "result");
        });
        tick(worker); // LEASED
        tick(worker); // EXECUTING
        expireLease();

        worker.advanceOneTask();

        assertThat(executions).hasValue(0);
        assertThat(task().status()).isEqualTo(TaskStatus.EXECUTING);
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
    }

    @Test
    void independentSchedulersLetWatchdogRunWhileWorkerExecutorBlocks() throws Exception {
        CountDownLatch executorEntered = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        FakeWorkerService blockedWorker = worker(task -> {
            executorEntered.countDown();
            try {
                assertThat(releaseExecutor.await(5, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Executor interrupted", interrupted);
            }
            return mapper.createObjectNode().put("summary", "scheduled result");
        });
        tick(blockedWorker); // LEASED
        tick(blockedWorker); // EXECUTING
        CountDownLatch watchdogFinished = new CountDownLatch(1);
        var workerFuture = workerTaskScheduler.schedule(blockedWorker::advanceOneTask, Instant.now());
        try {
            assertThat(executorEntered.await(5, TimeUnit.SECONDS)).isTrue();
            expireLease();
            var watchdogFuture = watchdogTaskScheduler.schedule(() -> {
                try {
                    failures.recoverExpiredLeases(1);
                } finally {
                    watchdogFinished.countDown();
                }
            }, Instant.now());
            assertThat(watchdogFinished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
            watchdogFuture.cancel(false);
        } finally {
            releaseExecutor.countDown();
            if (workerFuture != null) workerFuture.cancel(false);
        }
    }

    @Test
    void rejectedPollDoesNotClaimTasksAndResumesAfterBackoff() {
        String secondRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "later run")).runId();
        AtomicInteger dispatches = new AtomicInteger();
        Executor executionExecutor = command -> {
            if (dispatches.getAndIncrement() == 0) {
                throw new RejectedExecutionException("execution pool is full");
            }
            command.run();
        };
        FakeWorkerService worker = new FakeWorkerService(workerTransactions, failures,
                new FakeTaskExecutor(mapper), executionExecutor, 30_000, 1000);

        assertDoesNotThrow(worker::advanceOneTask);
        for (int i = 0; i < 5; i++) tick(worker);
        assertThat(dispatches).hasValue(1);
        for (String id : List.of(runId, secondRun)) {
            assertThat(task(id).status()).isEqualTo(TaskStatus.PENDING);
            assertThat(task(id).attempt()).isZero();
        }
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            tick(worker);
            assertThat(task().status()).isEqualTo(TaskStatus.LEASED);
        });
        for (int i = 0; i < 5; i++) tick(worker);
        assertThat(task().status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(task(secondRun).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(task().attempt()).isEqualTo(1);
        assertThat(task(secondRun).attempt()).isEqualTo(1);
    }

    @Test
    void saturatedScheduledWorkerDoesNotSpendWaitingTasksLeaseOrRetryBudget() throws Exception {
        String blockedRun = runId;
        String waitingRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "wait for capacity")).runId();
        jdbc.update("UPDATE tasks SET max_attempts = 1");
        CountDownLatch executorEntered = new CountDownLatch(1);
        CountDownLatch releaseExecutor = new CountDownLatch(1);
        AtomicInteger waitingCalls = new AtomicInteger();
        var executionPool = (org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor)
                new io.nexus.controlplane.SchedulingConfiguration().workerExecutionExecutor(1);
        TaskExecutor blockingExecutor = task -> {
            if (task.runId().equals(blockedRun)) {
                executorEntered.countDown();
                try {
                    if (!releaseExecutor.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("executor was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("executor interrupted", interrupted);
                }
            } else {
                waitingCalls.incrementAndGet();
            }
            return mapper.createObjectNode().put("summary", "finished");
        };
        FakeWorkerService worker = new FakeWorkerService(workerTransactions, failures, blockingExecutor,
                executionPool, 500, 20);
        var schedule = workerTaskScheduler.scheduleWithFixedDelay(worker::advanceOneTask, Duration.ofMillis(20));
        try {
            assertThat(executorEntered.await(5, TimeUnit.SECONDS)).isTrue();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                failures.recoverExpiredLeases();
                assertThat(task(blockedRun).status()).isEqualTo(TaskStatus.FAILED);
            });
            // Recovery consumes the running task's final attempt, while the waiting task
            // remains untouched for longer than an entire lease duration.
            org.awaitility.Awaitility.await().during(Duration.ofMillis(600)).atMost(Duration.ofSeconds(3))
                    .untilAsserted(() -> {
                        assertThat(task(waitingRun).status()).isEqualTo(TaskStatus.PENDING);
                        assertThat(task(waitingRun).attempt()).isZero();
                        assertThat(task(waitingRun).leaseOwner()).isNull();
                        assertThat(task(waitingRun).leaseExpiresAt()).isNull();
                        assertThat(waitingCalls).hasValue(0);
                        assertThat(executionPool.getThreadPoolExecutor().getQueue()).isEmpty();
                    });
            releaseExecutor.countDown();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(task(waitingRun).status()).isEqualTo(TaskStatus.SUCCEEDED));
            assertThat(task(waitingRun).attempt()).isEqualTo(1);
            assertThat(waitingCalls).hasValue(1);
            assertThat(eventTypes(waitingRun)).doesNotContain("TaskRetried", "TaskLeaseExpired", "TaskFailed");
        } finally {
            schedule.cancel(false);
            releaseExecutor.countDown();
            executionPool.shutdown();
        }
    }

    @Test
    void watchdogRecoveryIsLimitedToConfiguredBatch() {
        String session = runtime.createSession().sessionId();
        String secondRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(session, "second expired task")).runId();
        FakeWorkerService firstWorker = successfulWorker();
        FakeWorkerService secondWorker = successfulWorker();
        tick(firstWorker);
        tick(secondWorker);
        jdbc.update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second'");

        assertThat(failures.recoverExpiredLeases(1)).isEqualTo(1);
        assertThat(List.of(task().status(), task(secondRun).status()))
                .contains(TaskStatus.RETRYING)
                .anyMatch(status -> status == TaskStatus.LEASED || status == TaskStatus.EXECUTING);
        assertThat(store.findTimeline(runId).stream().filter(e -> e.eventType().equals("TaskLeaseExpired"))).hasSize(1);
        assertThat(store.findTimeline(secondRun).stream().filter(e -> e.eventType().equals("TaskLeaseExpired"))).hasSize(0);
    }

    private FakeWorkerService successfulWorker() {
        return worker(new FakeTaskExecutor(mapper));
    }

    private FakeWorkerService worker(TaskExecutor executor) {
        return new FakeWorkerService(workerTransactions, failures, executor, 30_000);
    }

    private void tick(FakeWorkerService worker) {
        worker.advanceOneTask();
    }

    private TaskRecord task() {
        return task(runId);
    }

    private TaskRecord task(String id) {
        return store.findTasks(id).getFirst();
    }

    private void expireLease() {
        expireLease(runId);
    }

    private void expireLease(String id) {
        jdbc.update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", id);
    }

    private void makeRetryReady() {
        jdbc.update("UPDATE tasks SET next_attempt_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", runId);
    }

    private List<String> eventTypes() {
        return eventTypes(runId);
    }

    private List<String> eventTypes(String id) {
        return store.findTimeline(id).stream().map(EventRecord::eventType).toList();
    }

    private EventRecord event(String type) {
        return store.findTimeline(runId).stream().filter(e -> e.eventType().equals(type)).findFirst().orElseThrow();
    }

    private void installEventFailureTrigger(String eventType) {
        jdbc.execute("""
                CREATE FUNCTION reject_test_result_event() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF NEW.event_type = TG_ARGV[0] THEN RAISE EXCEPTION 'injected result event failure'; END IF;
                  RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER reject_test_result_event BEFORE INSERT ON domain_events "
                + "FOR EACH ROW EXECUTE FUNCTION reject_test_result_event('" + eventType + "')");
    }

    private void removeEventFailureTrigger() {
        jdbc.execute("DROP TRIGGER IF EXISTS reject_test_result_event ON domain_events");
        jdbc.execute("DROP FUNCTION IF EXISTS reject_test_result_event()");
    }
}
