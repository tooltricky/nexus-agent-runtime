package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.nexus.controlplane.ControlPlaneApplication;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class DeadlineSchedulingIntegrationTest {
    private ConfigurableApplicationContext application;
    @AfterEach
    void close() {
        if (application != null) {
            var registry = application.getBean(ExecutionControlRegistry.class);
            var heartbeat = application.getBean(LeaseHeartbeatService.class);
            application.getBean(ControlledExecutor.class).release.countDown();
            application.close();
            assertThat(registry.activeCount()).isZero();
            assertThat(heartbeat.activeCount()).isZero();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void timeoutCommittedByAnotherInstanceStopsLocalExecutorEvenWithoutHeartbeat(boolean removeHeartbeat) throws Exception {
        start(true, false);
        var executor = application.getBean(ControlledExecutor.class);
        String run = create("blocked", application.getBean(RuntimeStore.class).currentTime().plusSeconds(30));
        assertThat(executor.entered.await(5, TimeUnit.SECONDS)).isTrue();
        try (var peer = openContext(false, false)) {
            assertThat(peer.getBean(ExecutionControlRegistry.class)).isNotSameAs(application.getBean(ExecutionControlRegistry.class));
            assertThat(peer.getBean(ExecutionControlRegistry.class).activeCount()).isZero();
            var store = peer.getBean(RuntimeStore.class);
            var jdbc = peer.getBean(JdbcTemplate.class);
            String due = store.currentTime().minusSeconds(1).toString();
            // The executing instance must never react to an uncommitted or rolled-back timeout.
            try (var observer = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                new TransactionTemplate(peer.getBean(PlatformTransactionManager.class)).executeWithoutResult(s -> {
                    jdbc.update("UPDATE tasks SET deadline_at=?::timestamptz, lease_expires_at=LEAST(lease_expires_at,?::timestamptz) WHERE run_id=?", due, due, run);
                    jdbc.update("UPDATE runs SET deadline_at=?::timestamptz WHERE id=?", due, run);
                    assertThat(peer.getBean(TaskTimeoutService.class).expireDueTasks(10)).isEqualTo(1);
                    try {
                        observer.submit(application.getBean(ExecutionTimeoutMonitor.class)::reconcile).get(5, TimeUnit.SECONDS);
                    } catch (Exception e) { throw new AssertionError("timeout observation must not block on row locks", e); }
                    assertThat(executor.signal.get().isStopRequested()).isFalse();
                    s.setRollbackOnly();
                });
            }
            assertThat(task(run).status()).isEqualTo(TaskStatus.EXECUTING);
            assertThat(executor.signal.get().isStopRequested()).isFalse();
            if (removeHeartbeat) {
                var task = executor.task.get();
                application.getBean(LeaseHeartbeatService.class).stop(task, task.leaseOwner());
                assertThat(application.getBean(LeaseHeartbeatService.class).activeCount()).isZero();
            }
            new TransactionTemplate(peer.getBean(PlatformTransactionManager.class)).executeWithoutResult(s -> {
                jdbc.update("UPDATE tasks SET deadline_at=?::timestamptz, lease_expires_at=LEAST(lease_expires_at,?::timestamptz) WHERE run_id=?", due, due, run);
                jdbc.update("UPDATE runs SET deadline_at=?::timestamptz WHERE id=?", due, run);
                assertThat(peer.getBean(TaskTimeoutService.class).expireDueTasks(10)).isEqualTo(1);
            });
            // No active rows remain for the executing instance's normal timeout scanner.
            assertThat(application.getBean(TaskTimeoutService.class).expireDueTasks(10)).isZero();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(executor.signal.get().isStopRequested()).isTrue();
                assertThat(executor.interrupted.getCount()).isZero();
                assertThat(application.getBean(ExecutionControlRegistry.class).activeCount()).isZero();
                assertThat(application.getBean(LeaseHeartbeatService.class).activeCount()).isZero();
            });
            assertThat(store.findTimeline(run).stream().map(EventRecord::eventType))
                    .containsOnlyOnce("TaskTimedOut", "RunTimedOut").doesNotContain("TaskSucceeded", "TaskFailed");
            String fast = create("fast", null);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(fast).status()).isEqualTo(TaskStatus.SUCCEEDED));
        }
    }

    @Test
    void timeoutAfterLeaseTakeoverInterruptsBothAttemptsAndReleasesBothSlots() throws Exception {
        start(false, false);
        var store = application.getBean(RuntimeStore.class);
        var controls = application.getBean(ExecutionControlRegistry.class);
        var heartbeats = application.getBean(LeaseHeartbeatService.class);
        var entered = new ConcurrentHashMap<Integer, TaskRecord>();
        var signals = new ConcurrentHashMap<Integer, ExecutionControlRegistry.Signal>();
        var interrupted = new java.util.concurrent.ConcurrentSkipListSet<Integer>();
        var release = new CountDownLatch(1);
        TaskExecutor executor = new TaskExecutor() {
            @Override public JsonNode execute(TaskRecord task) { throw new AssertionError("missing stop signal"); }
            @Override public JsonNode execute(TaskRecord task, ExecutionControlRegistry.Signal signal) {
                if (task.input().path("input").asText().equals("takeover")) {
                    signals.put(task.attempt(), signal);
                    entered.put(task.attempt(), task);
                    try { release.await(); }
                    catch (InterruptedException stopped) { interrupted.add(task.attempt()); }
                }
                // Even an executor returning a result on interruption cannot overwrite TIMEOUT.
                return JsonNodeFactory.instance.objectNode().put("summary", "late");
            }
        };
        var pool = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        var worker = new FakeWorkerService(application.getBean(WorkerTransactionService.class),
                application.getBean(TaskFailureService.class), executor, pool, 400, 10, heartbeats, controls);
        try {
            String run = create("takeover", store.currentTime().plusSeconds(30));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                worker.advanceOneTask();
                assertThat(entered).containsKey(1);
            });
            TaskRecord old = entered.get(1);
            // Simulate a failed renewal while its executor remains blocked in downstream work.
            heartbeats.stop(old, old.leaseOwner());
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                worker.advanceOneTask();
                assertThat(entered).containsKey(2);
            });
            assertThat(controls.activeCount()).isEqualTo(2);
            assertThat(signals.values()).allSatisfy(signal -> assertThat(signal.isStopRequested()).isFalse());
            var jdbc = application.getBean(JdbcTemplate.class);
            String due = store.currentTime().minusSeconds(1).toString();
            new TransactionTemplate(application.getBean(PlatformTransactionManager.class)).executeWithoutResult(s -> {
                jdbc.update("UPDATE tasks SET deadline_at=?::timestamptz, lease_expires_at=LEAST(lease_expires_at,?::timestamptz) WHERE run_id=?", due, due, run);
                jdbc.update("UPDATE runs SET deadline_at=?::timestamptz WHERE id=?", due, run);
            });
            application.getBean(TaskTimeoutService.class).expireDueTasks(10);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(signals.values()).allSatisfy(signal -> assertThat(signal.isStopRequested()).isTrue());
                assertThat(interrupted).containsExactly(1, 2);
                assertThat(controls.activeCount()).isZero();
                assertThat(heartbeats.activeCount()).isZero();
                assertThat(pool.getActiveCount()).isZero();
            });
            assertThat(task(run).status()).isEqualTo(TaskStatus.TIMEOUT);
            assertThat(task(run).output()).isNull();
            assertThat(store.findTimeline(run).stream().map(EventRecord::eventType))
                    .containsOnlyOnce("TaskTimedOut", "RunTimedOut").doesNotContain("TaskSucceeded", "TaskFailed");
            // Both slots can execute new work after the stopped attempts exit.
            var bothEntered = new CountDownLatch(2);
            var freeSlots = new CountDownLatch(1);
            try {
                for (int i = 0; i < 2; i++) pool.execute(() -> {
                    bothEntered.countDown();
                    try { freeSlots.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                });
                assertThat(bothEntered.await(5, TimeUnit.SECONDS)).isTrue();
            } finally { freeSlots.countDown(); }
        } finally {
            release.countDown();
            pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false,true})
    void independentDeadlineStopsExecutionWithHeartbeatsAndFullPool(boolean ignoresStop) throws Exception {
        start(true, true);
        var executor = application.getBean(ControlledExecutor.class);
        executor.ignoresStop = ignoresStop;
        Instant deadline = application.getBean(RuntimeStore.class).currentTime().plusMillis(1200);
        String blocked = create("blocked", deadline);
        assertThat(executor.entered.await(5, TimeUnit.SECONDS)).isTrue();
        TaskRecord initial = executor.task.get();
        // One execution slot is now occupied; the second Run still times out in its queue.
        String queued = create("queued", deadline);
        await().atMost(Duration.ofSeconds(5)).until(() -> application.getBean(RuntimeStore.class).currentTime()
                .isAfter(initial.leaseExpiresAt()));
        assertThat(task(blocked).status()).isEqualTo(TaskStatus.EXECUTING);
        assertThat(task(blocked).leaseExpiresAt()).isAfter(initial.leaseExpiresAt()).isBeforeOrEqualTo(task(blocked).deadlineAt());
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(task(blocked).status()).isEqualTo(TaskStatus.TIMEOUT);
            assertThat(task(queued).status()).isEqualTo(TaskStatus.TIMEOUT);
            assertThat(executor.signal.get().isStopRequested()).isTrue();
            assertThat(executor.interrupted.getCount()).isZero();
            assertThat(application.getBean(LeaseHeartbeatService.class).activeCount()).isZero();
        });
        assertThat(task(queued).attempt()).isZero();
        if (ignoresStop) {
            assertThat(application.getBean(ExecutionControlRegistry.class).activeCount()).isEqualTo(1);
            executor.release.countDown();
        }
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(application.getBean(ExecutionControlRegistry.class).activeCount()).isZero());
        assertThat(task(blocked).status()).isEqualTo(TaskStatus.TIMEOUT);
        assertThat(application.getBean(RuntimeStore.class).findTimeline(blocked).stream().map(EventRecord::eventType))
                .containsOnlyOnce("TaskTimedOut","RunTimedOut").doesNotContain("TaskFailed","TaskRetried","TaskSucceeded");
        String fast = create("fast", null);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(fast).status()).isEqualTo(TaskStatus.SUCCEEDED));
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"PENDING","LEASED","EXECUTING","RETRYING"})
    void workerDisabledStillExpiresQueuedTasksAndRestartKeepsDeadlineAndHistory(TaskStatus stage) {
        start(false, false);
        Instant deadline = application.getBean(RuntimeStore.class).currentTime().plusMillis(500);
        String run = create("restart", deadline);
        var transactions = application.getBean(WorkerTransactionService.class);
        if (stage != TaskStatus.PENDING) transactions.nextTask("stopped", Duration.ofSeconds(30), java.util.Set.of()).orElseThrow();
        if (stage == TaskStatus.EXECUTING || stage == TaskStatus.RETRYING) transactions.markExecuting(task(run), "stopped").orElseThrow();
        if (stage == TaskStatus.RETRYING) application.getBean(TaskFailureService.class).failOwnedTask(task(run), "stopped", "test");
        int attempts = task(run).attempt();
        Instant stored = task(run).deadlineAt();
        var history = application.getBean(RuntimeStore.class).findTimeline(run);
        application.close(); application = null;
        // The restarted pool and Spring context must use the persisted deadline, not a new duration.
        start(false, true, false);
        assertThat(task(run).deadlineAt()).isEqualTo(stored);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(run).status()).isEqualTo(TaskStatus.TIMEOUT));
        assertThat(task(run).attempt()).isEqualTo(attempts);
        assertThat(application.getBean(ControlledExecutor.class).calls).hasValue(0);
        assertThat(application.getBean(RuntimeStore.class).findTimeline(run).subList(0, history.size())).isEqualTo(history);
        String unlimited = create("unlimited", null);
        assertThat(task(unlimited).deadlineAt()).isNull();
        assertThat(task(unlimited).status()).isEqualTo(TaskStatus.PENDING);
    }

    private void start(boolean worker, boolean scanner) { start(worker, scanner, true); }
    private void start(boolean worker, boolean scanner, boolean clear) {
        // Clear before starting any state-writing scheduler: TRUNCATE otherwise races
        // the worker's Task -> event lock order and can deadlock during test setup.
        if (clear) {
            try (var cleaner = openContext(false, false, false)) {
                clearDatabase(cleaner.getBean(JdbcTemplate.class));
            }
        }
        application = openContext(worker, scanner);
    }
    private ConfigurableApplicationContext openContext(boolean worker, boolean scanner) {
        return openContext(worker, scanner, true);
    }
    private ConfigurableApplicationContext openContext(boolean worker, boolean scanner, boolean watchdog) {
        return new SpringApplicationBuilder(ControlPlaneApplication.class, ExecutorConfiguration.class)
                .web(WebApplicationType.NONE).run(
                        "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
                        "--spring.datasource.username=" + System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                        "--spring.datasource.password=" + System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"),
                        "--nexus.worker.enabled=" + worker, "--nexus.timeout.enabled=" + scanner,
                        "--nexus.watchdog.enabled=" + watchdog,
                        "--nexus.worker.execution-concurrency=1", "--nexus.worker.poll-delay-ms=20",
                        "--nexus.worker.dispatch-retry-delay-ms=10", "--nexus.worker.lease-duration-ms=400",
                        "--nexus.worker.heartbeat-interval-ms=50", "--nexus.worker.watchdog-poll-delay-ms=20",
                        "--nexus.timeout.poll-delay-ms=20");
    }
    private String create(String input, Instant deadline) {
        var runtime = application.getBean(RuntimeService.class);
        return runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(runtime.createSession().sessionId(), input, deadline)).runId();
    }
    private TaskRecord task(String run) { return application.getBean(RuntimeStore.class).findTasks(run).getFirst(); }
    @TestConfiguration
    static class ExecutorConfiguration {
        @Bean @Primary ControlledExecutor controlledExecutor() { return new ControlledExecutor(); }
    }
    static class ControlledExecutor implements TaskExecutor {
        final CountDownLatch entered = new CountDownLatch(1), interrupted = new CountDownLatch(1), release = new CountDownLatch(1);
        final AtomicReference<TaskRecord> task = new AtomicReference<>();
        final AtomicReference<ExecutionControlRegistry.Signal> signal = new AtomicReference<>();
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean ignoresStop;
        @Override public JsonNode execute(TaskRecord task) { throw new AssertionError("worker must supply signal"); }
        @Override public JsonNode execute(TaskRecord current, ExecutionControlRegistry.Signal stop) {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            calls.incrementAndGet();
            if (current.input().path("input").asText().equals("blocked")) {
                task.set(current); signal.set(stop); entered.countDown();
                boolean done = false;
                while (!done) {
                    try {
                        done = release.await(10, TimeUnit.SECONDS);
                        if (!done) throw new IllegalStateException("executor timed out");
                    } catch (InterruptedException e) {
                        interrupted.countDown();
                        if (!ignoresStop) throw new IllegalStateException("stopped", e);
                    }
                }
            }
            return JsonNodeFactory.instance.objectNode().put("summary", "completed");
        }
    }
}
