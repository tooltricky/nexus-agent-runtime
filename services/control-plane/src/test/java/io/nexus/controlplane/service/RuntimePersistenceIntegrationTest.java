package io.nexus.controlplane.service;

import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.EventRecord;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.ArrayList;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

import static io.nexus.controlplane.support.RuntimeTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.awaitility.Awaitility.await;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(properties = {
        "nexus.timeout.enabled=false", "nexus.worker.enabled=false",
        "nexus.watchdog.enabled=false",
        "spring.datasource.url=${TEST_DB_URL}",
        "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class RuntimePersistenceIntegrationTest {
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired JdbcTemplate jdbc;
    @Autowired ApplicationContext context;
    @Autowired WorkerTransactionService workerTransactions;
    @Autowired TaskFailureService failures;
    @Autowired @Qualifier("workerExecutionExecutor") Executor workerExecutionExecutor;

    @Autowired @Qualifier("workerTaskScheduler") ThreadPoolTaskScheduler workerTaskScheduler;
    @Autowired @Qualifier("watchdogTaskScheduler") ThreadPoolTaskScheduler watchdogTaskScheduler;

    @BeforeEach
    void reset() {
        clearDatabase(jdbc);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 5})
    void competingWorkersClaimEachTaskOnceWithIndependentTransactions(int taskCount) throws Exception {
        String session = runtime.createSession().sessionId();
        List<String> runs = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            runs.add(runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(session, "task-" + i)).runId());
        }
        List<FakeWorkerService> workers = IntStream.range(0, 8).mapToObj(i -> worker(context)).toList();
        List<Callable<Boolean>> claims = workers.stream()
                .map(w -> (Callable<Boolean>) () -> { tick(context, w); return true; }).toList();
        concurrently(claims);

        var tasks = runs.stream().flatMap(id -> store.findTasks(id).stream()).toList();
        assertThat(tasks).hasSize(taskCount);
        assertThat(tasks).allSatisfy(task -> {
            assertThat(task.status()).isEqualTo(TaskStatus.LEASED);
            assertThat(task.attempt()).isEqualTo(1);
            assertThat(task.leaseOwner()).isNotBlank();
            assertThat(jdbc.queryForObject("SELECT lease_expires_at > clock_timestamp() FROM tasks WHERE id = ?",
                    Boolean.class, task.id())).isTrue();
            assertThat(store.findTimeline(task.runId())).extracting(EventRecord::eventType)
                    .containsExactly("RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted");
            var event = store.findTimeline(task.runId()).get(3);
            assertThat(event.payload().path("lease_owner").asText()).isEqualTo(task.leaseOwner());
            assertThat(event.payload().path("attempt").asInt()).isEqualTo(1);
        });
        assertThat(tasks).extracting(t -> t.leaseOwner()).doesNotHaveDuplicates();

        concurrently(claims); // Existing owners enter EXECUTING; other owners cannot advance their tasks.
        concurrently(claims);
        for (String run : runs) {
            assertThat(store.findTasks(run).getFirst().status()).isEqualTo(TaskStatus.SUCCEEDED);
            assertThat(store.findTasks(run).getFirst().attempt()).isEqualTo(1);
            assertThat(store.findRun(run).orElseThrow().status().name()).isEqualTo("SUCCEEDED");
            assertThat(store.findTimeline(run)).extracting(EventRecord::eventType).containsExactly(
                    "RunCreated", "TaskCreated", "RunQueued", "TaskLeased", "RunStarted",
                    "TaskExecuting", "TaskSucceeded", "RunSucceeded");
        }
        var before = runs.stream().map(store::findTimeline).toList();
        concurrently(claims);
        assertThat(runs.stream().map(store::findTimeline).toList()).isEqualTo(before);
    }

    @Test
    void databaseRejectsEventUpdateAndDeleteWithoutChangingHistory() {
        String session = runtime.createSession().sessionId();
        String run = runtime.createRun("immutable", new CreateRunRequest(session, "test")).runId();
        var worker = worker(context);
        for (int i = 0; i < 3; i++) tick(context, worker);
        var before = store.findTimeline(run);
        for (String statement : List.of(
                "UPDATE domain_events SET payload = '{\"tampered\":true}'::jsonb WHERE run_id = ?",
                "DELETE FROM domain_events WHERE run_id = ?")) {
            DataAccessException error = assertThrows(DataAccessException.class, () -> jdbc.update(statement, run));
            assertThat(error.getMostSpecificCause().getMessage()).contains("domain_events is append-only");
            assertThat(store.findTimeline(run)).isEqualTo(before);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM domain_events", Integer.class)).isEqualTo(8);
        }
    }

    @Test
    void workerAndWatchdogUseIndependentSchedulers() {
        assertThat(workerTaskScheduler).isNotSameAs(watchdogTaskScheduler);
        assertThat(workerTaskScheduler.getThreadNamePrefix()).isEqualTo("nexus-worker-");
        assertThat(watchdogTaskScheduler.getThreadNamePrefix()).isEqualTo("nexus-watchdog-");
        assertThat(workerTaskScheduler.getScheduledThreadPoolExecutor().getCorePoolSize()).isEqualTo(1);
        assertThat(watchdogTaskScheduler.getScheduledThreadPoolExecutor().getCorePoolSize()).isEqualTo(1);
    }

    @Test
    void oneScheduledWorkerAdvancesAnotherRunWhileFirstExecutorBlocks() throws Exception {
        String session = runtime.createSession().sessionId();
        String blockedRun = runtime.createRun("blocked", new CreateRunRequest(session, "blocked" )).runId();
        String freeRun = runtime.createRun("free", new CreateRunRequest(session, "free" )).runId();
        CountDownLatch blockedStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocked = new CountDownLatch(1);
        TaskExecutor executor = task -> {
            if (task.input().path("input").asText().equals("blocked")) {
                blockedStarted.countDown();
                try {
                    if (!releaseBlocked.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("blocked executor was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("blocked executor interrupted", interrupted);
                }
            }
            return context.getBean(com.fasterxml.jackson.databind.ObjectMapper.class)
                    .createObjectNode().put("summary", task.input().path("input").asText());
        };
        FakeWorkerService worker = new FakeWorkerService(workerTransactions, failures, executor,
                workerExecutionExecutor, 30_000);
        ScheduledFuture<?> schedule = workerTaskScheduler.scheduleAtFixedRate(worker::advanceOneTask,
                Duration.ofMillis(20));
        try {
            assertThat(blockedStarted.await(5, TimeUnit.SECONDS)).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(store.findRun(freeRun).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED));
            assertThat(store.findRun(blockedRun).orElseThrow().status()).isEqualTo(RunStatus.RUNNING);
        } finally {
            releaseBlocked.countDown();
            schedule.cancel(false);
        }
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(store.findRun(blockedRun).orElseThrow().status()).isEqualTo(RunStatus.SUCCEEDED));
    }
}
