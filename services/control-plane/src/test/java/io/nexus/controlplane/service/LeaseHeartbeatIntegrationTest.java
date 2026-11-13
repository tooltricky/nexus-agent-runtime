package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(properties = {
        "nexus.timeout.enabled=false", "nexus.worker.enabled=false", "nexus.watchdog.enabled=false",
        "nexus.worker.retry-initial-delay-ms=60000", "nexus.worker.retry-max-delay-ms=240000",
        "spring.datasource.url=${TEST_DB_URL}",
        "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class LeaseHeartbeatIntegrationTest {
    private static final Duration LEASE = Duration.ofSeconds(30);
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired WorkerTransactionService transactions;
    @Autowired TaskFailureService failures;
    @Autowired RunCancellationService cancellations;
    @Autowired JdbcTemplate jdbc;
    @Autowired ObjectMapper mapper;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired @Qualifier("heartbeatTaskScheduler") ThreadPoolTaskScheduler heartbeatScheduler;
    private TransactionTemplate tx;
    private String runId;

    @BeforeEach
    void prepare() {
        tx = new TransactionTemplate(transactionManager);
        clearDatabase(jdbc);
        runId = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "heartbeat")).runId();
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"LEASED", "EXECUTING"})
    void renewalPreservesFenceAndEventsAndAcceptsResult(TaskStatus state) {
        TaskRecord task = active(state);
        var events = store.findTimeline(runId);
        for (int i = 0; i < 3; i++) {
            assertThat(transactions.renewLease(task, "worker", LEASE)).isTrue();
        }
        assertThat(task().leaseExpiresAt()).isAfter(task.leaseExpiresAt());
        assertThat(task().version()).isEqualTo(task.version());
        assertThat(task().attempt()).isEqualTo(task.attempt());
        assertThat(task().status()).isEqualTo(state);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
        if (state == TaskStatus.LEASED) task = transactions.markExecuting(task, "worker").orElseThrow();
        assertThat(transactions.completeSuccessfully(task, "worker", mapper.createObjectNode())).isTrue();
        assertThat(store.findRun(runId).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED);
    }

    @Test
    void shorterRenewalNeverShortensAnExistingLease() {
        TaskRecord task = active(TaskStatus.LEASED);
        assertThat(transactions.renewLease(task, "worker", Duration.ofMillis(1))).isTrue();
        assertThat(task().leaseExpiresAt()).isEqualTo(task.leaseExpiresAt());
    }

    @Test
    void foreignOwnerOldAttemptOldVersionAndUnknownTaskCannotRenew() {
        TaskRecord task = active(TaskStatus.EXECUTING);
        var events = store.findTimeline(runId);
        assertThat(transactions.renewLease(task, "foreign", LEASE)).isFalse();
        assertThat(transactions.renewLease(withFence(task, task.attempt() - 1, task.version()), "worker", LEASE)).isFalse();
        assertThat(transactions.renewLease(withFence(task, task.attempt(), task.version() - 1), "worker", LEASE)).isFalse();
        assertThat(task()).isEqualTo(task);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
        assertThat(transactions.renewLease(new TaskRecord("absent", task.runId(), task.name(), task.deadlineAt(), task.status(),
                task.input(), task.output(), task.leaseOwner(), task.leaseExpiresAt(), task.attempt(), task.maxAttempts(),
                task.nextAttemptAt(), task.version(), task.createdAt(), task.updatedAt(), task.tenantId(), task.projectId()), "worker", LEASE)).isFalse();
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"PENDING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELLED"})
    void inactiveTasksCannotRenewEvenWithTheirCurrentFence(TaskStatus status) {
        TaskRecord original = task();
        if (status != TaskStatus.PENDING) {
            original = active(TaskStatus.EXECUTING);
            switch (status) {
                case SUCCEEDED -> transactions.completeSuccessfully(original, "worker", mapper.createObjectNode());
                case CANCELLED -> cancellations.cancel(runId);
                case FAILED, RETRYING -> {
                    if (status == TaskStatus.FAILED) jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", runId);
                    failures.failOwnedTask(task(), "worker", "test failure");
                }
                default -> throw new IllegalArgumentException();
            }
        }
        var before = task();
        var events = store.findTimeline(runId);
        assertThat(transactions.renewLease(original, "worker", LEASE)).isFalse();
        assertThat(transactions.renewLease(before, "worker", LEASE)).isFalse();
        assertThat(task()).isEqualTo(before);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
    }

    @Test
    void stoppedHeartbeatAllowsTakeoverAndFencesOldResultsEvenWithSameOwner() {
        TaskRecord stale = active(TaskStatus.EXECUTING);
        expire();
        assertThat(transactions.renewLease(stale, "worker", LEASE)).isFalse();
        failures.recoverExpiredLeases();
        jdbc.update("UPDATE tasks SET next_attempt_at=clock_timestamp() WHERE run_id=?", runId);
        transactions.nextTask("worker", LEASE, Set.of());
        TaskRecord next = task();
        assertThat(next.attempt()).isEqualTo(2);
        var events = store.findTimeline(runId);
        assertThat(transactions.renewLease(stale, "worker", LEASE)).isFalse();
        assertThat(transactions.completeSuccessfully(stale, "worker", mapper.createObjectNode())).isFalse();
        assertThat(failures.failOwnedTask(stale, "worker", "late failure")).isFalse();
        assertThat(task()).isEqualTo(next);
        assertThat(store.findTimeline(runId)).isEqualTo(events);
        var executing = transactions.markExecuting(next, "worker").orElseThrow();
        assertThat(transactions.completeSuccessfully(executing, "worker", mapper.createObjectNode())).isTrue();
        assertThat(types()).containsOnlyOnce("TaskLeaseExpired", "TaskRetried", "TaskSucceeded", "RunSucceeded");
    }

    @Test
    void heartbeatFailureRollsBackAndDoesNotConsumeAttemptOrBlockOtherRegistrations() {
        TaskRecord task = active(TaskStatus.EXECUTING);
        String otherRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "other")).runId();
        TaskRecord other = transactions.nextTask("other", LEASE, Set.of()).orElseThrow().task();
        var heartbeat = new LeaseHeartbeatService(transactions);
        heartbeat.track(task, "worker", LEASE);
        heartbeat.track(other, "other", LEASE);
        var events = store.findTimeline(runId);
        jdbc.execute("""
                CREATE FUNCTION reject_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.lease_owner = 'worker' AND NEW.version = OLD.version THEN
                        RAISE EXCEPTION 'injected renewal failure';
                    END IF;
                    RETURN NEW;
                END $$
                """);
        jdbc.execute("CREATE TRIGGER reject_heartbeat BEFORE UPDATE ON tasks FOR EACH ROW EXECUTE FUNCTION reject_heartbeat()");
        try {
            heartbeat.renewLeases();
            assertThat(task()).isEqualTo(task);
            assertThat(store.findTasks(otherRun).getFirst().leaseExpiresAt()).isAfter(other.leaseExpiresAt());
            assertThat(store.findTimeline(runId)).isEqualTo(events);
            assertThat(heartbeat.activeCount()).isEqualTo(2);
        } finally {
            jdbc.execute("DROP TRIGGER reject_heartbeat ON tasks");
            jdbc.execute("DROP FUNCTION reject_heartbeat()");
        }
        heartbeat.renewLeases();
        assertThat(task().leaseExpiresAt()).isAfter(task.leaseExpiresAt());
        expire();
        heartbeat.renewLeases();
        assertThat(heartbeat.activeCount()).isEqualTo(1);
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(task().attempt()).isEqualTo(1);
        heartbeat.close();
    }

    @Test
    void lockedTaskDoesNotBlockOtherHeartbeatsAndResumesAfterUnlock() throws Exception {
        TaskRecord locked = active(TaskStatus.EXECUTING);
        String otherRun = runtime.createRun(UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), "unlocked")).runId();
        var otherLease = Duration.ofMillis(1200);
        var lockedAttempted = new CountDownLatch(1);
        var sweepEnded = new CountDownLatch(1);
        // Observe entry without replacing the real database transaction or relying on map iteration order.
        var observed = org.mockito.Mockito.mock(WorkerTransactionService.class,
                org.mockito.AdditionalAnswers.delegatesTo(transactions));
        org.mockito.Mockito.doAnswer(invocation -> {
            lockedAttempted.countDown();
            return transactions.renewLease(invocation.getArgument(0), invocation.getArgument(1), invocation.getArgument(2));
        }).when(observed).renewLease(org.mockito.ArgumentMatchers.eq(locked),
                org.mockito.ArgumentMatchers.eq("worker"), org.mockito.ArgumentMatchers.eq(LEASE));
        var heartbeat = new LeaseHeartbeatService(observed);
        heartbeat.track(locked, "worker", LEASE);
        var lockReady = new CountDownLatch(1);
        var unlock = new CountDownLatch(1);
        var holder = Executors.newSingleThreadExecutor();
        var holding = holder.submit(() -> tx.executeWithoutResult(status -> {
            jdbc.queryForList("SELECT id FROM tasks WHERE id=? FOR UPDATE", locked.id());
            lockReady.countDown();
            awaitLatch(unlock);
        }));
        java.util.concurrent.ScheduledFuture<?> schedule = null;
        try {
            awaitLatch(lockReady);
            schedule = heartbeatScheduler.scheduleWithFixedDelay(() -> {
                try { heartbeat.renewLeases(); } finally { sweepEnded.countDown(); }
            }, Duration.ofMillis(50));
            awaitLatch(lockedAttempted);
            // A was attempted first while its lock is held. B joins subsequent sweeps.
            TaskRecord other = transactions.nextTask("other", otherLease, Set.of()).orElseThrow().task();
            TaskRecord executing = transactions.markExecuting(other, "other").orElseThrow();
            heartbeat.track(executing, "other", otherLease);
            assertThat(sweepEnded.await(1, TimeUnit.SECONDS)).as("A must not block the heartbeat thread").isTrue();
            await().during(Duration.ofMillis(1600)).atMost(Duration.ofSeconds(4)).untilAsserted(() -> {
                assertThat(failures.recoverExpiredLeases(10)).isZero();
                TaskRecord current = store.findTasks(otherRun).getFirst();
                assertThat(current.status()).isEqualTo(TaskStatus.EXECUTING);
                assertThat(current.attempt()).isEqualTo(1);
                assertThat(current.version()).isEqualTo(executing.version());
                assertThat(heartbeat.activeCount()).as("lock contention retains A for retry").isEqualTo(2);
            });
            assertThat(store.findTasks(otherRun).getFirst().leaseExpiresAt()).isAfter(executing.leaseExpiresAt());
            assertThat(store.findTimeline(otherRun)).extracting(EventRecord::eventType)
                    .doesNotContain("TaskLeaseExpired", "TaskRetried", "TaskFailed");
            unlock.countDown();
            holding.get(5, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(task().leaseExpiresAt()).isAfter(locked.leaseExpiresAt()));
            assertThat(transactions.completeSuccessfully(executing, "other", mapper.createObjectNode())).isTrue();
            assertThat(transactions.completeSuccessfully(locked, "worker", mapper.createObjectNode())).isTrue();
        } finally {
            heartbeat.close();
            if (schedule != null) schedule.cancel(false);
            unlock.countDown();
            holding.get(5, TimeUnit.SECONDS);
            awaitLatch(sweepEnded);
            holder.shutdownNow();
            assertThat(holder.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void replacingRegistrationAndLateCleanupCannotRemoveNewVersionOrAttempt() {
        TaskRecord leased = active(TaskStatus.LEASED);
        var heartbeat = new LeaseHeartbeatService(transactions);
        heartbeat.track(leased, "worker", LEASE);
        var executing = transactions.markExecuting(leased, "worker").orElseThrow();
        heartbeat.track(executing, "worker", LEASE);
        heartbeat.stop(leased, "worker");
        assertThat(heartbeat.activeCount()).isEqualTo(1);
        heartbeat.renewLeases();
        assertThat(task().leaseExpiresAt()).isAfter(executing.leaseExpiresAt());
        expire();
        failures.recoverExpiredLeases();
        jdbc.update("UPDATE tasks SET next_attempt_at=clock_timestamp() WHERE run_id=?", runId);
        var next = transactions.nextTask("worker", LEASE, Set.of()).orElseThrow().task();
        heartbeat.track(next, "worker", LEASE);
        heartbeat.stop(executing, "worker");
        heartbeat.renewLeases();
        assertThat(heartbeat.activeCount()).isEqualTo(1);
        cancellations.cancel(runId);
        heartbeat.renewLeases();
        assertThat(heartbeat.activeCount()).isZero();
        heartbeat.close();
        heartbeat.track(next, "worker", LEASE);
        heartbeat.renewLeases();
        assertThat(heartbeat.activeCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "failure", "cancel"})
    void renewalFirstThenTerminalTransactionProducesOneOutcome(String terminal) throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", runId);
        TaskRecord task = active(TaskStatus.EXECUTING);
        ordered(() -> assertThat(store.renewTaskLease(task, "worker", LEASE)).isTrue(),
                () -> finish(terminal, task));
        assertTerminal(terminal);
    }

    @ParameterizedTest
    @ValueSource(strings = {"success", "failure", "cancel"})
    void terminalLockFirstSkipsHeartbeatThenRejectsAfterCommit(String terminal) throws Exception {
        jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", runId);
        TaskRecord task = active(TaskStatus.EXECUTING);
        // The held terminal transaction owns the row; heartbeat must return immediately.
        ordered(() -> {
            switch (terminal) {
                case "success" -> {
                    // Invoke the real implementation inside the deliberately held test transaction.
                    assertThat(new WorkerTransactionService(store, mapper)
                            .completeSuccessfully(task, "worker", mapper.createObjectNode())).isTrue();
                }
                case "failure" -> failures.failOwnedTask(task, "worker", "failed");
                case "cancel" -> cancellations.cancel(runId);
                default -> throw new IllegalArgumentException();
            }
        }, () -> assertThrows(CannotAcquireLockException.class,
                () -> transactions.renewLease(task, "worker", LEASE)));
        assertThat(transactions.renewLease(task, "worker", LEASE)).isFalse();
        assertTerminal(terminal);
    }

    @Test
    void heartbeatSkipsLockedRowAndCannotRenewIfExpiredBeforeNextAttempt() throws Exception {
        TaskRecord task = active(TaskStatus.EXECUTING);
        // Set a short lease before the locking transaction, which never changes the row.
        jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()+INTERVAL '1500 milliseconds' WHERE run_id=?", runId);
        ordered(() -> {
            jdbc.queryForList("SELECT id FROM tasks WHERE run_id=? FOR UPDATE", runId);
        }, () -> assertThrows(CannotAcquireLockException.class,
                () -> transactions.renewLease(task, "worker", LEASE)), () -> {
            await().atMost(Duration.ofSeconds(3)).until(() -> Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT lease_expires_at <= clock_timestamp() FROM tasks WHERE run_id=?", Boolean.class, runId)));
        });
        assertThat(transactions.renewLease(task, "worker", LEASE)).isFalse();
        failures.recoverExpiredLeases();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
    }

    @Test
    void renewalAndWatchdogRespectLiveAndExpiredLeaseInBothOrders() throws Exception {
        TaskRecord task = active(TaskStatus.EXECUTING);
        ordered(() -> assertThat(store.renewTaskLease(task, "worker", LEASE)).isTrue(),
                () -> assertThat(failures.recoverExpiredLeases(1)).isZero());
        assertThat(task().status()).isEqualTo(TaskStatus.EXECUTING);
        expire();
        ordered(() -> assertThat(failures.recoverExpiredLeases(1)).isEqualTo(1),
                () -> assertThrows(CannotAcquireLockException.class,
                        () -> transactions.renewLease(task, "worker", LEASE)));
        assertThat(transactions.renewLease(task, "worker", LEASE)).isFalse();
        assertThat(task().status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(types()).containsOnlyOnce("TaskLeaseExpired", "TaskRetried");
    }

    /** Hold the first transaction until the second is observed waiting, skipping, or rejecting its lock. */
    private void ordered(Runnable first, Runnable second) throws Exception {
        ordered(first, second, () -> { });
    }

    private void ordered(Runnable first, Runnable second, Runnable beforeRelease) throws Exception {
        var ready = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var started = new CountDownLatch(1);
        var done = new CountDownLatch(1);
        var pid = new AtomicInteger();
        var threads = Executors.newFixedThreadPool(2);
        var a = threads.submit(() -> tx.executeWithoutResult(status -> {
            pid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
            first.run();
            ready.countDown();
            awaitLatch(release);
        }));
        java.util.concurrent.Future<?> b = null;
        try {
            awaitLatch(ready);
            b = threads.submit(() -> {
                started.countDown();
                try { second.run(); } finally { done.countDown(); }
            });
            awaitLatch(started);
            await().atMost(Duration.ofSeconds(5)).until(() -> done.getCount() == 0 || Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid)))",
                    Boolean.class, pid.get())));
            beforeRelease.run();
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

    private void finish(String terminal, TaskRecord task) {
        switch (terminal) {
            case "success" -> assertThat(transactions.completeSuccessfully(task, "worker", mapper.createObjectNode())).isTrue();
            case "failure" -> assertThat(failures.failOwnedTask(task, "worker", "failed")).isTrue();
            case "cancel" -> cancellations.cancel(runId);
            default -> throw new IllegalArgumentException();
        }
    }

    private void assertTerminal(String terminal) {
        TaskStatus expected = switch (terminal) {
            case "success" -> TaskStatus.SUCCEEDED;
            case "failure" -> TaskStatus.FAILED;
            case "cancel" -> TaskStatus.CANCELLED;
            default -> throw new IllegalArgumentException();
        };
        assertThat(task().status()).isEqualTo(expected);
        assertThat(store.findRun(runId).orElseThrow().status().name()).isEqualTo(expected.name());
        String event = switch (terminal) { case "success" -> "Succeeded"; case "failure" -> "Failed"; default -> "Cancelled"; };
        assertThat(types()).containsOnlyOnce("Task" + event, "Run" + event);
        assertThat(task().leaseOwner()).isNull();
        assertThat(task().leaseExpiresAt()).isNull();
    }

    private TaskRecord active(TaskStatus stage) {
        var task = transactions.nextTask("worker", LEASE, Set.of()).orElseThrow().task();
        return stage == TaskStatus.EXECUTING ? transactions.markExecuting(task, "worker").orElseThrow() : task;
    }
    private TaskRecord task() { return store.findTasks(runId).getFirst(); }
    private List<String> types() { return store.findTimeline(runId).stream().map(EventRecord::eventType).toList(); }
    private void expire() { jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", runId); }
    private static TaskRecord withFence(TaskRecord t, int attempt, long version) {
        return new TaskRecord(t.id(), t.runId(), t.name(), t.deadlineAt(), t.status(), t.input(), t.output(), t.leaseOwner(),
                t.leaseExpiresAt(), attempt, t.maxAttempts(), t.nextAttemptAt(), version, t.createdAt(), t.updatedAt(), t.tenantId(), t.projectId());
    }
    private static void awaitLatch(CountDownLatch latch) {
        try { assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue(); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IllegalStateException(interrupted); }
    }
}
