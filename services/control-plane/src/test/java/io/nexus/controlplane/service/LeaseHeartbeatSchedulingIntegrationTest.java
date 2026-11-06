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
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.nexus.controlplane.support.RuntimeTestSupport.clearDatabase;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Full Spring schedulers and execution pool, with a controlled downstream call. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class LeaseHeartbeatSchedulingIntegrationTest {
    private ConfigurableApplicationContext application;

    @AfterEach
    void close() {
        if (application != null) {
            var heartbeat = application.getBean(LeaseHeartbeatService.class);
            application.getBean(ControlledExecutor.class).release.countDown();
            application.close();
            assertThat(heartbeat.activeCount()).isZero();
        }
    }

    @Test
    void longExecutionRenewsWhileOtherRunAndWatchdogProgressThenCleansUp() throws Exception {
        start();
        String blocked = createRun("blocked");
        var executor = application.getBean(ControlledExecutor.class);
        assertThat(executor.entered.await(5, TimeUnit.SECONDS)).isTrue();
        TaskRecord initial = executor.task.get();
        String fast = createRun("fast");
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(fast).status()).isEqualTo(TaskStatus.SUCCEEDED));

        // Create and claim foreign work atomically, before this worker can see it.
        var tx = new TransactionTemplate(application.getBean(PlatformTransactionManager.class));
        String expired = tx.execute(status -> {
            String run = createRun("foreign");
            application.getBean(RuntimeStore.class).claimNextTask("stopped-worker", Duration.ofMillis(1)).orElseThrow();
            return run;
        });
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(expired).status()).isEqualTo(TaskStatus.RETRYING));
        // Observe a still-running task past its original expiry, and across several heartbeats.
        await().atMost(Duration.ofSeconds(5)).until(() -> Boolean.TRUE.equals(jdbc().queryForObject(
                "SELECT clock_timestamp() > ?::timestamptz", Boolean.class, initial.leaseExpiresAt().toString())));
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            TaskRecord current = task(blocked);
            assertThat(current.status()).isEqualTo(TaskStatus.EXECUTING);
            assertThat(current.version()).isEqualTo(initial.version());
            assertThat(current.attempt()).isEqualTo(1);
            assertThat(current.leaseExpiresAt()).isAfter(initial.leaseExpiresAt());
            assertThat(executor.calls).hasValue(1);
        });
        executor.release.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
            assertThat(task(blocked).status()).isEqualTo(TaskStatus.SUCCEEDED);
            assertThat(application.getBean(LeaseHeartbeatService.class).activeCount()).isZero();
        });
        assertThat(types(blocked)).doesNotContain("TaskLeaseExpired", "TaskRetried");
        assertThat(types(blocked)).containsOnlyOnce("TaskSucceeded", "RunSucceeded");
    }

    @Test
    void stoppingHeartbeatAllowsNewOwnerAndRejectsLateExecutorResult() throws Exception {
        start();
        String run = createRun("blocked");
        var executor = application.getBean(ControlledExecutor.class);
        assertThat(executor.entered.await(5, TimeUnit.SECONDS)).isTrue();
        TaskRecord stale = executor.task.get();
        application.getBean(LeaseHeartbeatService.class).close();
        application.getBean("workerTaskScheduler", org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler.class).shutdown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(run).status()).isEqualTo(TaskStatus.RETRYING));

        jdbc().update("UPDATE tasks SET next_attempt_at=clock_timestamp() WHERE run_id=?", run);
        // A second worker uses an independent identity and the same short transaction boundaries.
        var transactions = application.getBean(WorkerTransactionService.class);
        var next = transactions.nextTask("replacement", Duration.ofSeconds(30), Set.of()).orElseThrow().task();
        TaskRecord current = task(run);
        assertThat(current.attempt()).isEqualTo(2);
        assertThat(current.version()).isGreaterThan(stale.version());
        assertThat(current.leaseOwner()).isEqualTo("replacement");
        var executing = transactions.markExecuting(next, "replacement").orElseThrow();
        assertThat(transactions.completeSuccessfully(executing, "replacement",
                JsonNodeFactory.instance.objectNode().put("summary", "replacement"))).isTrue();
        assertThat(transactions.renewLease(stale, stale.leaseOwner(), Duration.ofSeconds(30))).isFalse();
        assertThat(application.getBean(TaskFailureService.class).failOwnedTask(stale, stale.leaseOwner(), "late")).isFalse();
        executor.release.countDown();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(task(run).status()).isEqualTo(TaskStatus.SUCCEEDED));
        assertThat(types(run)).containsOnlyOnce("TaskLeaseExpired", "TaskRetried", "TaskSucceeded", "RunSucceeded");
    }

    @Test
    void cancellationDuringBlockedExecutionStopsHeartbeatAndCannotRevive() throws Exception {
        start();
        String run = createRun("blocked");
        var executor = application.getBean(ControlledExecutor.class);
        assertThat(executor.entered.await(5, TimeUnit.SECONDS)).isTrue();
        application.getBean(RunCancellationService.class).cancel(run);
        var cancelled = task(run);
        var events = types(run);
        await().atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                assertThat(application.getBean(LeaseHeartbeatService.class).activeCount()).isZero());
        executor.release.countDown();
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(3)).untilAsserted(() -> {
            assertThat(task(run)).isEqualTo(cancelled);
            assertThat(types(run)).isEqualTo(events);
        });
    }

    private void start() {
        application = new SpringApplicationBuilder(ControlPlaneApplication.class, ExecutorConfiguration.class)
                .web(WebApplicationType.NONE).run("--nexus.timeout.enabled=false",
                        "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
                        "--spring.datasource.username=" + System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                        "--spring.datasource.password=" + System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"),
                        "--nexus.worker.lease-duration-ms=1200", "--nexus.worker.heartbeat-interval-ms=100",
                        "--nexus.worker.poll-delay-ms=20", "--nexus.worker.watchdog-poll-delay-ms=30",
                        "--nexus.worker.retry-initial-delay-ms=60000", "--nexus.worker.retry-max-delay-ms=60000");
        clearDatabase(jdbc());
    }
    private String createRun(String input) {
        var runtime = application.getBean(RuntimeService.class);
        return runtime.createRun(java.util.UUID.randomUUID().toString(),
                new CreateRunRequest(runtime.createSession().sessionId(), input)).runId();
    }
    private JdbcTemplate jdbc() { return application.getBean(JdbcTemplate.class); }
    private TaskRecord task(String run) { return application.getBean(RuntimeStore.class).findTasks(run).getFirst(); }
    private List<String> types(String run) {
        return application.getBean(RuntimeStore.class).findTimeline(run).stream().map(EventRecord::eventType).toList();
    }

    @TestConfiguration
    static class ExecutorConfiguration {
        @Bean @Primary ControlledExecutor controlledExecutor() { return new ControlledExecutor(); }
    }
    static class ControlledExecutor implements TaskExecutor {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicReference<TaskRecord> task = new AtomicReference<>();
        final AtomicInteger calls = new AtomicInteger();
        @Override public JsonNode execute(TaskRecord current) {
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            if (current.input().path("input").asText().equals("blocked")) {
                calls.incrementAndGet();
                task.set(current);
                entered.countDown();
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("test executor timed out");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            return JsonNodeFactory.instance.objectNode().put("summary", "completed");
        }
    }
}
