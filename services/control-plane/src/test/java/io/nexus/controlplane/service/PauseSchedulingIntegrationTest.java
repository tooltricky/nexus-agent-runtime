package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.nexus.controlplane.ControlPlaneApplication;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class PauseSchedulingIntegrationTest {
    @Test void realPauseStopsCooperativeExecutorAndFreesSlotForOtherRun() {
        clean();
        try (var app = open(true, false, 20)) {
            var executor = app.getBean(ControlledExecutor.class);
            String run = create(app, "blocked");
            await().atMost(Duration.ofSeconds(5)).until(() -> executor.signals.containsKey(1));
            app.getBean(RunPauseService.class).pause(run);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(executor.interrupted).contains(1);
                assertThat(app.getBean(ExecutionControlRegistry.class).activeCount()).isZero();
                assertThat(app.getBean(LeaseHeartbeatService.class).activeCount()).isZero();
            });
            assertThat(task(app, run).status()).isEqualTo(TaskStatus.PAUSED);
            String other = create(app, "fast");
            await().atMost(Duration.ofSeconds(5)).until(() -> task(app, other).status() == TaskStatus.SUCCEEDED);
            app.getBean(RunPauseService.class).resume(run, null);
            executor.release(2);
            await().atMost(Duration.ofSeconds(5)).until(() -> task(app, run).status() == TaskStatus.SUCCEEDED);
            assertThat(task(app, run).attempt()).isEqualTo(2);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void otherInstancePauseThenImmediateResumeStopsOnlyOldExecutionEvenWithoutHeartbeat(boolean removeHeartbeat) throws Exception {
        clean();
        try (var app = open(false, false, 60000); var peer = open(false, false, 60000)) {
            var executor = app.getBean(ControlledExecutor.class); executor.ignoreFirstStop = true;
            var controls = app.getBean(ExecutionControlRegistry.class);
            var heartbeats = app.getBean(LeaseHeartbeatService.class);
            var pool = new ThreadPoolExecutor(2, 2, 0, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
            var worker = new FakeWorkerService(app.getBean(WorkerTransactionService.class), app.getBean(TaskFailureService.class),
                    executor, pool, 30000, 10, heartbeats, controls);
            try {
                String run = create(app, "blocked");
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> { worker.advanceOneTask(); assertThat(executor.signals).containsKey(1); });
                var original = task(app, run);
                if (removeHeartbeat) heartbeats.stop(original, original.leaseOwner());
                var tx = new TransactionTemplate(peer.getBean(PlatformTransactionManager.class));
                try (var observer = Executors.newSingleThreadExecutor()) {
                    tx.executeWithoutResult(s -> {
                        peer.getBean(RunPauseService.class).pause(run);
                        try { observer.submit(app.getBean(ExecutionPauseMonitor.class)::reconcile).get(5, TimeUnit.SECONDS); }
                        catch (Exception e) { throw new AssertionError("pause reads must not wait on uncommitted row locks", e); }
                        assertThat(executor.signals.get(1).isStopRequested()).isFalse();
                        s.setRollbackOnly();
                    });
                }
                assertThat(task(app, run).status()).isEqualTo(TaskStatus.EXECUTING);
                assertThat(executor.signals.get(1).isStopRequested()).isFalse();
                assertThat(peer.getBean(RuntimeStore.class).findPauseCheckpoint(original.id())).isEmpty();
                peer.getBean(RunPauseService.class).pause(run);
                var cp = peer.getBean(RuntimeStore.class).findPauseCheckpoint(original.id()).orElseThrow();
                peer.getBean(RunPauseService.class).resume(run, cp.checkpointId());
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> { worker.advanceOneTask(); assertThat(executor.signals).containsKey(2); });
                assertThat(executor.signals.get(1).isStopRequested()).isFalse();
                // Checkpoint is already consumed, PAUSED is no longer visible, and no heartbeat is needed.
                app.getBean(ExecutionPauseMonitor.class).reconcile();
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(executor.interrupted).contains(1));
                assertThat(executor.signals.get(1).isStopRequested()).isTrue();
                assertThat(executor.signals.get(2).isStopRequested()).isFalse();
                assertThat(controls.activeCount()).isEqualTo(2); // ignored stop still owns its execution slot
                controls.stopPaused(cp); // delayed/duplicate callbacks must not stop attempt 2
                assertThat(executor.signals.get(2).isStopRequested()).isFalse();
                executor.release(2);
                await().atMost(Duration.ofSeconds(5)).until(() -> task(app, run).status() == TaskStatus.SUCCEEDED);
                var result = app.getBean(RuntimeService.class).getRun(run);
                executor.release(1);
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(controls.activeCount()).isZero());
                assertThat(app.getBean(RuntimeService.class).getRun(run)).isEqualTo(result);
                assertThat(result.output().path("summary").asText()).isEqualTo("attempt-2");
                assertThat(app.getBean(RuntimeStore.class).findTimeline(run).stream().map(e -> e.eventType()))
                        .containsOnlyOnce("TaskSucceeded", "RunSucceeded").doesNotContain("TaskRetried", "TaskFailed");
            } finally {
                executor.release(1); executor.release(2); pool.shutdownNow();
                assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            }
        }
    }

    @ParameterizedTest @EnumSource(value = TaskStatus.class, names = {"PENDING", "LEASED", "EXECUTING", "RETRYING"})
    void fullRestartKeepsPauseCheckpointDeadlineAndBackoffAndResumesOnce(TaskStatus stage) {
        clean(); String run; TaskRecord paused; Object checkpoint;
        try (var app = open(false, false, 20)) {
            run = create(app, "fast");
            var workers = app.getBean(WorkerTransactionService.class);
            if (stage != TaskStatus.PENDING) workers.nextTask("worker", Duration.ofSeconds(30), Set.of()).orElseThrow();
            if (stage == TaskStatus.EXECUTING || stage == TaskStatus.RETRYING) workers.markExecuting(task(app, run), "worker").orElseThrow();
            if (stage == TaskStatus.RETRYING) app.getBean(TaskFailureService.class).failOwnedTask(task(app, run), "worker", "retry");
            app.getBean(RunPauseService.class).pause(run);
            paused = task(app, run); checkpoint = app.getBean(RuntimeStore.class).findPauseCheckpoint(paused.id()).orElseThrow();
        }
        try (var restarted = open(true, false, 20)) {
            assertThat(task(restarted, run)).isEqualTo(paused);
            assertThat(restarted.getBean(RuntimeStore.class).findPauseCheckpoint(paused.id()).orElseThrow()).isEqualTo(checkpoint);
            assertThat(restarted.getBean(ControlledExecutor.class).calls.get()).isZero();
            restarted.getBean(RunPauseService.class).resume(run, null);
            if (stage == TaskStatus.RETRYING) {
                assertThat(task(restarted, run).nextAttemptAt()).isEqualTo(paused.nextAttemptAt());
                assertThat(restarted.getBean(ControlledExecutor.class).calls.get()).isZero();
                // Move only the independent test clock field after proving preserved backoff.
                restarted.getBean(JdbcTemplate.class).update("UPDATE tasks SET next_attempt_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", run);
            }
            await().atMost(Duration.ofSeconds(5)).until(() -> task(restarted, run).status() == TaskStatus.SUCCEEDED);
            assertThat(task(restarted, run).deadlineAt()).isEqualTo(paused.deadlineAt());
            assertThat(task(restarted, run).attempt()).isEqualTo(paused.attempt() + 1);
            assertThat(restarted.getBean(ControlledExecutor.class).calls.get()).isEqualTo(1);
            var before = restarted.getBean(RuntimeService.class).getRun(run);
            var tick = restarted.getBean(FakeWorkerService.class);
            for (int i = 0; i < 5; i++) tick.advanceOneTask();
            assertThat(restarted.getBean(RuntimeService.class).getRun(run)).isEqualTo(before);
            assertThat(restarted.getBean(RuntimeStore.class).findTimeline(run).stream().filter(e -> e.eventType().equals("RunStarted")).count()).isEqualTo(1);
        }
    }

    @Test void pausedWorkExpiredDuringShutdownTimesOutOnRestartWithoutExecution() {
        clean(); String run;
        try (var app = open(false, false, 20)) {
            run = create(app, "fast"); app.getBean(RunPauseService.class).pause(run);
            var jdbc = app.getBean(JdbcTemplate.class);
            new TransactionTemplate(app.getBean(PlatformTransactionManager.class)).executeWithoutResult(s -> {
                jdbc.update("UPDATE tasks SET deadline_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", run);
                jdbc.update("UPDATE runs SET deadline_at=(SELECT deadline_at FROM tasks WHERE run_id=?) WHERE id=?", run, run);
            });
        }
        try (var app = open(true, true, 20)) {
            await().atMost(Duration.ofSeconds(5)).until(() -> task(app, run).status() == TaskStatus.TIMEOUT);
            assertThat(app.getBean(ControlledExecutor.class).calls.get()).isZero();
            assertThat(app.getBean(RuntimeStore.class).findPauseCheckpoint(task(app, run).id()).orElseThrow().disposition()).isEqualTo("TIMEOUT");
        }
    }

    private void clean() { try (var app = open(false, false, 60000)) { clearDatabase(app.getBean(JdbcTemplate.class)); } }
    private ConfigurableApplicationContext open(boolean worker, boolean scanner, int interval) {
        return new SpringApplicationBuilder(ControlPlaneApplication.class, ExecutorConfiguration.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
                "--spring.datasource.username=" + System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                "--spring.datasource.password=" + System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"),
                "--nexus.worker.enabled=" + worker, "--nexus.timeout.enabled=" + scanner, "--nexus.watchdog.enabled=false",
                "--nexus.worker.execution-concurrency=1", "--nexus.worker.poll-delay-ms=20", "--nexus.worker.dispatch-retry-delay-ms=10",
                "--nexus.worker.lease-duration-ms=30000", "--nexus.worker.heartbeat-interval-ms=1000",
                "--nexus.worker.retry-initial-delay-ms=60000", "--nexus.worker.retry-max-delay-ms=60000",
                "--nexus.timeout.poll-delay-ms=" + interval);
    }
    private String create(ConfigurableApplicationContext app, String input) {
        var runtime = app.getBean(RuntimeService.class);
        return runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(runtime.createSession().sessionId(), input,
                app.getBean(RuntimeStore.class).currentTime().plusSeconds(120))).runId();
    }
    private TaskRecord task(ConfigurableApplicationContext app, String run) { return app.getBean(RuntimeStore.class).findTasks(run).getFirst(); }
    @TestConfiguration static class ExecutorConfiguration {
        @Bean @Primary ControlledExecutor controlledExecutor() { return new ControlledExecutor(); }
    }
    static class ControlledExecutor implements TaskExecutor {
        final ConcurrentHashMap<Integer, ExecutionControlRegistry.Signal> signals = new ConcurrentHashMap<>();
        final ConcurrentHashMap<Integer, CountDownLatch> releases = new ConcurrentHashMap<>();
        final Set<Integer> interrupted = ConcurrentHashMap.newKeySet();
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean ignoreFirstStop;
        void release(int attempt) { releases.computeIfAbsent(attempt, key -> new CountDownLatch(1)).countDown(); }
        @Override public JsonNode execute(TaskRecord task) { throw new AssertionError("missing signal"); }
        @Override public JsonNode execute(TaskRecord task, ExecutionControlRegistry.Signal signal) {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            calls.incrementAndGet();
            if (task.input().path("input").asText().equals("blocked")) {
                var latch = releases.computeIfAbsent(task.attempt(), key -> new CountDownLatch(1));
                signals.put(task.attempt(), signal);
                boolean released = false;
                while (!released) {
                    try { released = latch.await(10, TimeUnit.SECONDS); if (!released) throw new IllegalStateException("test executor timed out"); }
                    catch (InterruptedException stop) {
                        interrupted.add(task.attempt());
                        if (!ignoreFirstStop || task.attempt() != 1) throw new IllegalStateException("stopped", stop);
                    }
                }
            }
            return JsonNodeFactory.instance.objectNode().put("summary", "attempt-" + task.attempt());
        }
    }
}
