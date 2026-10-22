package io.nexus.controlplane.service;

import io.nexus.controlplane.ControlPlaneApplication;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.util.List;

import static io.nexus.controlplane.support.RuntimeTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Rebuilds the entire application context and connection pool against the same database. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class RuntimeRestartIntegrationTest {
    private ConfigurableApplicationContext application;

    @AfterEach
    void closeApplication() {
        if (application != null) application.close();
    }

    @Test
    void restartPreservesSuccessAndScheduledWorkerContinuesQueuedRun() {
        start(false);
        clearDatabase(jdbc());
        String succeeded = createRun("completed");
        var oldWorker = worker(application);
        for (int i = 0; i < 3; i++) tick(application, oldWorker);
        var result = runtime().getRun(succeeded);
        var timeline = store().findTimeline(succeeded);
        assertThat(result.status().name()).isEqualTo("SUCCEEDED");
        assertThat(result.output().path("summary").asText()).isEqualTo("发现错误率上升，需要进一步检查最近发布");
        String queued = createRun("queued");
        assertThat(task(queued).status()).isEqualTo(TaskStatus.PENDING);

        restart(true);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(task(queued).status()).isEqualTo(TaskStatus.SUCCEEDED));
        // The scheduled worker is live, and extra ticks must not replay the completed Run.
        for (int i = 0; i < 3; i++) application.getBean(FakeWorkerService.class).advanceOneTask();
        assertThat(runtime().getRun(succeeded)).isEqualTo(result);
        assertThat(store().findTimeline(succeeded)).isEqualTo(timeline);
        assertThat(task(queued).attempt()).isEqualTo(1);
        assertThat(runtime().getRun(queued).output()).isEqualTo(result.output());
        assertThat(store().findTimeline(queued)).extracting(EventRecord::eventType).containsExactly(
                "RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted",
                "TaskExecuting", "TaskSucceeded", "RunSucceeded");
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"LEASED", "EXECUTING"})
    void restartedWorkerHonorsOldLeaseBackoffAndFencesLateResults(TaskStatus stage) {
        start(false);
        clearDatabase(jdbc());
        String run = createRun("unfinished");
        var oldWorker = worker(application);
        tick(application, oldWorker);
        if (stage == TaskStatus.EXECUTING) tick(application, oldWorker);
        TaskRecord stale = task(run);
        List<EventRecord> oldTimeline = store().findTimeline(run);

        restart(false);
        assertThat(task(run)).isEqualTo(stale);
        var newWorker = worker(application);
        tick(application, newWorker);
        assertThat(task(run)).isEqualTo(stale);
        assertThat(store().findTimeline(run)).isEqualTo(oldTimeline);

        // Advance persisted test timestamps instead of sleeping through the production lease/backoff.
        jdbc().update("UPDATE tasks SET lease_expires_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", run);
        tick(application, newWorker);
        TaskRecord retry = task(run);
        assertThat(retry.status()).isEqualTo(TaskStatus.RETRYING);
        assertThat(retry.attempt()).isEqualTo(1);
        assertThat(retry.leaseOwner()).isNull();
        assertThat(retry.leaseExpiresAt()).isNull();
        assertThat(store().findTimeline(run)).extracting(EventRecord::eventType)
                .endsWith("TaskLeaseExpired", "TaskRetried");
        assertThat(jdbc().queryForObject("SELECT next_attempt_at > clock_timestamp() FROM tasks WHERE run_id = ?",
                Boolean.class, run)).isTrue();
        tick(application, newWorker);
        assertThat(task(run)).isEqualTo(retry);

        jdbc().update("UPDATE tasks SET next_attempt_at = clock_timestamp() - INTERVAL '1 second' WHERE run_id = ?", run);
        tick(application, newWorker);
        assertThat(task(run).status()).isEqualTo(TaskStatus.LEASED);
        assertThat(task(run).attempt()).isEqualTo(2);
        assertThat(task(run).leaseOwner()).isNotEqualTo(stale.leaseOwner());
        var beforeLateReport = store().findTimeline(run);
        assertThat(application.getBean(TaskFailureService.class)
                .failOwnedTask(stale, stale.leaseOwner(), "late failure after restart")).isFalse();
        new TransactionTemplate(application.getBean(PlatformTransactionManager.class)).executeWithoutResult(s ->
                assertThat(store().tryTransitionTaskOwned(stale,
                        stage == TaskStatus.LEASED ? TaskStatus.EXECUTING : TaskStatus.SUCCEEDED,
                        null, stale.leaseOwner())).isFalse());
        assertThat(store().findTimeline(run)).isEqualTo(beforeLateReport);
        tick(application, newWorker);
        tick(application, newWorker);
        assertThat(task(run).status()).isEqualTo(TaskStatus.SUCCEEDED);
        assertThat(task(run).attempt()).isEqualTo(2);
        assertThat(runtime().getRun(run).status().name()).isEqualTo("SUCCEEDED");
        assertThat(store().findTimeline(run)).extracting(EventRecord::eventType).containsExactly(
                stage == TaskStatus.LEASED
                        ? new String[]{"RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted",
                            "TaskLeaseExpired", "TaskRetried", "TaskLeased", "TaskExecuting", "TaskSucceeded", "RunSucceeded"}
                        : new String[]{"RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted", "TaskExecuting",
                            "TaskLeaseExpired", "TaskRetried", "TaskLeased", "TaskExecuting", "TaskSucceeded", "RunSucceeded"});
        var events = store().findTimeline(run);
        assertThat(events).extracting(EventRecord::eventId).doesNotHaveDuplicates();
        assertThat(events).extracting(EventRecord::sequence).isSorted().doesNotHaveDuplicates();
    }

    private void start(boolean workerEnabled) {
        application = new SpringApplicationBuilder(ControlPlaneApplication.class).web(WebApplicationType.NONE).run(
                "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
                "--spring.datasource.username=" + System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                "--spring.datasource.password=" + System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"),
                "--nexus.worker.enabled=" + workerEnabled,
                "--nexus.worker.poll-delay-ms=50",
                "--nexus.worker.retry-initial-delay-ms=60000",
                "--nexus.worker.retry-max-delay-ms=240000");
    }

    private void restart(boolean workerEnabled) {
        var previous = application;
        previous.close();
        assertThat(previous.isActive()).isFalse();
        start(workerEnabled);
        assertThat(application).isNotSameAs(previous);
    }

    private String createRun(String key) {
        String session = runtime().createSession().sessionId();
        return runtime().createRun(key, new CreateRunRequest(session, "test")).runId();
    }

    private RuntimeService runtime() { return application.getBean(RuntimeService.class); }
    private RuntimeStore store() { return application.getBean(RuntimeStore.class); }
    private JdbcTemplate jdbc() { return application.getBean(JdbcTemplate.class); }
    private TaskRecord task(String run) { return store().findTasks(run).getFirst(); }
}
