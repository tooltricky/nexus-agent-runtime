package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.domain.Records.TaskRecord;
import io.nexus.controlplane.domain.Records.EventRecord;
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
class RunPauseIntegrationTest {
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired WorkerTransactionService workers;
    @Autowired RunPauseService pauses;
    @Autowired RunCancellationService cancellations;
    @Autowired TaskFailureService failures;
    @Autowired TaskTimeoutService timeouts;
    @Autowired TestRestTemplate http;
    @Autowired ObjectMapper mapper;
    @Autowired JdbcTemplate jdbc;
    @Autowired PlatformTransactionManager manager;
    private TransactionTemplate tx;
    private String run;

    @BeforeEach void reset() {
        clearDatabase(jdbc); tx = new TransactionTemplate(manager);
        run = create();
    }
    @AfterEach void stateMatchesItsTimelineAndContract() {
        String taskState = "PENDING", runState = "CREATED";
        for (EventRecord event : store.findTimeline(run)) {
            ContractAssertions.event(mapper.valueToTree(event));
            if (!event.payload().has("from")) continue;
            if (event.aggregateType().equals("TASK")) {
                assertThat(event.payload().path("from").asText()).isEqualTo(taskState);
                taskState = event.payload().path("to").asText();
            } else {
                assertThat(event.payload().path("from").asText()).isEqualTo(runState);
                runState = event.payload().path("to").asText();
            }
        }
        assertThat(task().status().name()).isEqualTo(taskState);
        assertThat(runtime.getRun(run).status().name()).isEqualTo(runState);
        var get = http.getForEntity("/v1/runs/" + run, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}", "get", get);
        var timeline = http.getForEntity("/v1/runs/" + run + "/timeline", JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/timeline", "get", timeline);
    }

    @ParameterizedTest
    @EnumSource(value = TaskStatus.class, names = {"PENDING", "LEASED", "EXECUTING", "RETRYING"})
    void pauseAndResumeEachStageAndRejectStaleOperations(TaskStatus stage) {
        prepare(stage);
        TaskRecord before = task(); var runBefore = store.findRun(run).orElseThrow();
        var response = post("pause", null);
        assertThat(response.getStatusCode().value()).isEqualTo(200);
        assertThat(task().status()).isEqualTo(TaskStatus.PAUSED);
        assertThat(task().leaseOwner()).isNull(); assertThat(task().leaseExpiresAt()).isNull();
        assertThat(task().attempt()).isEqualTo(before.attempt());
        var checkpoint = store.findPauseCheckpoint(before.id()).orElseThrow();
        assertThat(checkpoint.taskVersion()).isEqualTo(before.version());
        assertThat(checkpoint.runVersion()).isEqualTo(runBefore.version());
        assertThat(checkpoint.taskStatus()).isEqualTo(stage);
        assertThat(checkpoint.consumedAt()).isNull();
        var paused = runtime.getRun(run); var events = store.findTimeline(run);
        assertThat(post("pause", null).getStatusCode().value()).isEqualTo(200);
        assertThat(runtime.getRun(run)).isEqualTo(paused); assertThat(store.findTimeline(run)).isEqualTo(events);
        assertThat(workers.nextTask("worker", Duration.ofSeconds(30), Set.of())).isEmpty();
        failures.recoverExpiredLeases(); assertThat(task().status()).isEqualTo(TaskStatus.PAUSED);
        if (stage == TaskStatus.LEASED || stage == TaskStatus.EXECUTING) {
            assertThat(workers.renewLease(before, "worker", Duration.ofSeconds(30))).isFalse();
            assertThat(workers.hasLiveExecutingLease(before, "worker")).isFalse();
            assertThat(failures.failOwnedTask(before, "worker", "late")).isFalse();
            if (stage == TaskStatus.LEASED) assertThat(workers.markExecuting(before, "worker")).isEmpty();
            else assertThat(workers.completeSuccessfully(before, "worker", output())).isFalse();
        }
        assertThat(post("resume", checkpoint.checkpointId()).getStatusCode().value()).isEqualTo(200);
        var resumed = task();
        assertThat(resumed.status()).isEqualTo(stage == TaskStatus.RETRYING ? TaskStatus.RETRYING : TaskStatus.PENDING);
        assertThat(resumed.attempt()).isEqualTo(before.attempt());
        assertThat(resumed.version()).isEqualTo(before.version() + 2);
        assertThat(runtime.getRun(run).status()).isEqualTo(runBefore.status());
        assertThat(store.findPauseCheckpoint(before.id()).orElseThrow().disposition()).isEqualTo("RESUMED");
        var after = runtime.getRun(run); events = store.findTimeline(run);
        assertThat(post("resume", checkpoint.checkpointId()).getStatusCode().value()).isEqualTo(200);
        assertThat(runtime.getRun(run)).isEqualTo(after); assertThat(store.findTimeline(run)).isEqualTo(events);
        if (stage == TaskStatus.RETRYING) {
            assertThat(resumed.nextAttemptAt()).isEqualTo(before.nextAttemptAt());
            assertThat(workers.nextTask("worker", Duration.ofSeconds(30), Set.of())).isEmpty();
        } else {
            workers.nextTask("new", Duration.ofSeconds(30), Set.of()).orElseThrow();
            assertThat(task().attempt()).isEqualTo(before.attempt() + 1);
            workers.markExecuting(task(), "new").orElseThrow();
            assertThat(workers.completeSuccessfully(task(), "new", output())).isTrue();
        }
        if (stage == TaskStatus.EXECUTING) {
            assertThat(workers.completeSuccessfully(before, "worker", output())).isFalse();
            assertThat(failures.failOwnedTask(before, "worker", "late")).isFalse();
        }
        assertThat(types().stream().filter("RunStarted"::equals).count()).isLessThanOrEqualTo(1);
    }

    @ParameterizedTest @EnumSource(value = TaskStatus.class, names = {"LEASED", "EXECUTING"})
    void interruptedFinalAttemptFailsOnResumeWithoutExtraClaim(TaskStatus stage) {
        prepare(stage); jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", run);
        pauses.pause(run); var cp = store.findPauseCheckpoint(task().id()).orElseThrow();
        assertThat(post("resume", cp.checkpointId()).getBody().path("status").asText()).isEqualTo("FAILED");
        assertThat(task().attempt()).isEqualTo(1);
        assertThat(store.findPauseCheckpoint(task().id()).orElseThrow().disposition()).isEqualTo("ATTEMPTS_EXHAUSTED");
        assertThat(types()).endsWith("TaskFailed", "RunFailed").doesNotContain("TaskResumed", "RunResumed");
        assertThat(workers.nextTask("new", Duration.ofSeconds(30), Set.of())).isEmpty();
    }

    @Test void concurrentRepeatsAndCycleTokenPreventExtraDispatch() throws Exception {
        concurrently(java.util.stream.IntStream.range(0, 6).mapToObj(i -> (java.util.concurrent.Callable<Boolean>) () -> {
            pauses.pause(run); return true;
        }).toList());
        var cp = store.findPauseCheckpoint(task().id()).orElseThrow();
        concurrently(java.util.stream.IntStream.range(0, 6).mapToObj(i -> (java.util.concurrent.Callable<Boolean>) () -> {
            pauses.resume(run, cp.checkpointId()); return true;
        }).toList());
        assertThat(types()).containsOnlyOnce("TaskPaused", "RunPaused", "TaskResumed", "RunResumed");
        assertThat(task().attempt()).isZero();
        workers.nextTask("worker", Duration.ofSeconds(30), Set.of()).orElseThrow();
        assertThat(task().attempt()).isEqualTo(1);
        pauses.pause(run);
        var second = store.findPauseCheckpoint(task().id()).orElseThrow();
        assertThat(second.checkpointId()).isNotEqualTo(cp.checkpointId());
        var before = runtime.getRun(run); var history = store.findTimeline(run);
        assertThat(post("resume", cp.checkpointId()).getStatusCode().value()).isEqualTo(409);
        assertThat(runtime.getRun(run)).isEqualTo(before); assertThat(store.findTimeline(run)).isEqualTo(history);
        assertThat(post("resume", second.checkpointId()).getStatusCode().value()).isEqualTo(200);
    }

    @Test void notFoundTerminalAndActiveNoopHttpBehaviors() {
        for (String op : List.of("pause", "resume")) {
            var missing = http.postForEntity("/v1/runs/missing/" + op, null, JsonNode.class);
            assertThat(missing.getStatusCode().value()).isEqualTo(404);
            ContractAssertions.response("/v1/runs/{run_id}/" + op, "post", missing);
        }
        var before = runtime.getRun(run); var events = store.findTimeline(run);
        assertThat(post("resume", null).getStatusCode().value()).isEqualTo(200);
        assertThat(post("resume", UUID.randomUUID().toString()).getStatusCode().value()).isEqualTo(409);
        assertThat(runtime.getRun(run)).isEqualTo(before); assertThat(store.findTimeline(run)).isEqualTo(events);
        prepare(TaskStatus.EXECUTING); workers.completeSuccessfully(task(), "worker", output());
        before = runtime.getRun(run); events = store.findTimeline(run);
        for (String op : List.of("pause", "resume")) assertThat(post(op, null).getStatusCode().value()).isEqualTo(409);
        assertThat(runtime.getRun(run)).isEqualTo(before); assertThat(store.findTimeline(run)).isEqualTo(events);
        assertThat(workers.nextTask("new", Duration.ofSeconds(30), Set.of())).isEmpty();
    }

    @Test void pausedCancellationConsumesCheckpointAndUsesNewEventOrigins() {
        pauses.pause(run); cancellations.cancel(run);
        assertThat(task().status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(store.findPauseCheckpoint(task().id()).orElseThrow().disposition()).isEqualTo("CANCELLED");
        assertThat(post("resume", null).getStatusCode().value()).isEqualTo(409);
        var events = store.findTimeline(run);
        assertThat(cancellations.cancel(run).status()).isEqualTo(RunStatus.CANCELLED);
        assertThat(store.findTimeline(run)).isEqualTo(events);
    }

    @ParameterizedTest @ValueSource(strings = {"scan", "pause", "resume", "cancel"})
    void deadlineKeepsRunningWhilePausedAndDiscoveredExpiryIsCommitted(String op) {
        pauses.pause(run); due();
        if (op.equals("scan")) assertThat(timeouts.expireDueTasks(10)).isEqualTo(1);
        else if (op.equals("cancel")) assertThrows(CancellationConflictException.class, () -> cancellations.cancel(run));
        else assertThat(post(op, null).getStatusCode().value()).isEqualTo(409);
        assertThat(task().status()).isEqualTo(TaskStatus.TIMEOUT);
        assertThat(store.findPauseCheckpoint(task().id()).orElseThrow().disposition()).isEqualTo("TIMEOUT");
        assertThat(types()).endsWith("TaskTimedOut", "RunTimedOut");
        assertThat(post("resume", null).getStatusCode().value()).isEqualTo(409);
    }

    @ParameterizedTest @ValueSource(strings = {"checkpoint", "TaskPaused", "RunPaused", "consume", "TaskResumed", "RunResumed"})
    void checkpointAndEitherEventFailureRollBackEverything(String failAt) {
        prepare(TaskStatus.EXECUTING);
        boolean resume = Set.of("consume", "TaskResumed", "RunResumed").contains(failAt);
        if (resume) pauses.pause(run);
        var before = task(); var beforeRun = store.findRun(run); var beforeCp = store.findPauseCheckpoint(before.id());
        var events = store.findTimeline(run);
        String table = failAt.equals("checkpoint") || failAt.equals("consume") ? "task_pause_checkpoints" : "domain_events";
        String predicate = table.equals("domain_events") ? "NEW.event_type = '" + failAt + "'" : "TRUE";
        jdbc.execute("CREATE FUNCTION reject_test_pause() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF " + predicate
                + " THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER reject_test_pause BEFORE INSERT OR UPDATE ON " + table + " FOR EACH ROW EXECUTE FUNCTION reject_test_pause()");
        try {
            assertThrows(DataAccessException.class, () -> { if (resume) pauses.resume(run, null); else pauses.pause(run); });
            assertThat(task()).isEqualTo(before); assertThat(store.findRun(run)).isEqualTo(beforeRun);
            assertThat(store.findPauseCheckpoint(before.id())).isEqualTo(beforeCp);
            assertThat(store.findTimeline(run)).isEqualTo(events);
        } finally {
            jdbc.execute("DROP TRIGGER IF EXISTS reject_test_pause ON " + table);
            jdbc.execute("DROP FUNCTION IF EXISTS reject_test_pause()");
        }
    }

    @ParameterizedTest @ValueSource(strings = {"claim", "execute", "renew", "success", "failure", "retry", "watchdog", "cancel", "timeout"})
    void pauseRacesWithEveryStateWriterInBothOrders(String operation) throws Exception {
        for (boolean pauseFirst : List.of(false, true)) {
            clearDatabase(jdbc); run = create();
            if (!operation.equals("claim")) prepare(operation.equals("execute") ? TaskStatus.LEASED : TaskStatus.EXECUTING);
            if (operation.equals("failure")) jdbc.update("UPDATE tasks SET max_attempts=1 WHERE run_id=?", run);
            if (operation.equals("watchdog")) jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", run);
            if (operation.equals("timeout")) due();
            TaskRecord snapshot = task();
            Runnable pause = () -> { try { pauses.pause(run); } catch (PauseConflictException ignored) { } };
            Runnable other = action(operation, snapshot);
            if (pauseFirst) ordered(pause, other, false); else ordered(other, pause, false);
            boolean terminalWins = operation.equals("success") || operation.equals("failure");
            TaskStatus expected = operation.equals("cancel") ? TaskStatus.CANCELLED
                    : operation.equals("timeout") ? TaskStatus.TIMEOUT
                    : !pauseFirst && terminalWins ? operation.equals("success") ? TaskStatus.SUCCEEDED : TaskStatus.FAILED
                    : TaskStatus.PAUSED;
            assertThat(task().status()).as("%s pauseFirst=%s", operation, pauseFirst).isEqualTo(expected);
            for (EventRecord event : store.findTimeline(run)) ContractAssertions.event(mapper.valueToTree(event));
        }
    }

    @ParameterizedTest @ValueSource(strings = {"cancel", "timeout", "pause", "resume", "claim"})
    void resumeRacesWithCancellationExpiryAndOtherCommandsInBothOrders(String operation) throws Exception {
        for (boolean resumeFirst : List.of(false, true)) {
            clearDatabase(jdbc); run = create(); pauses.pause(run);
            if (operation.equals("timeout")) due();
            var cp = store.findPauseCheckpoint(task().id()).orElseThrow();
            Runnable resume = () -> { try { pauses.resume(run, cp.checkpointId()); } catch (PauseConflictException ignored) { } };
            Runnable other = operation.equals("pause") ? () -> pauses.pause(run)
                    : operation.equals("resume") ? resume : action(operation, task());
            if (resumeFirst) ordered(resume, other, false); else ordered(other, resume, false);
            TaskStatus expected = operation.equals("cancel") ? TaskStatus.CANCELLED
                    : operation.equals("timeout") ? TaskStatus.TIMEOUT
                    : operation.equals("pause") && resumeFirst ? TaskStatus.PAUSED
                    : TaskStatus.PENDING;
            assertThat(task().status()).isEqualTo(expected);
            if (operation.equals("claim") && resumeFirst) {
                // SKIP LOCKED may skip the still-uncommitted resume; the next poll claims once.
                workers.nextTask("worker", Duration.ofSeconds(30), Set.of()).orElseThrow();
                assertThat(task().status()).isEqualTo(TaskStatus.LEASED);
                assertThat(task().attempt()).isEqualTo(1);
            }
            assertThat(types().stream().filter("TaskResumed"::equals).count()).isLessThanOrEqualTo(1);
            for (EventRecord event : store.findTimeline(run)) ContractAssertions.event(mapper.valueToTree(event));
        }
    }

    @ParameterizedTest @ValueSource(strings = {"execute", "renew", "success", "failure", "retry", "watchdog"})
    void resumeFencesOldAttemptWritersInBothOrders(String operation) throws Exception {
        for (boolean resumeFirst : List.of(false, true)) {
            clearDatabase(jdbc); run = create();
            prepare(operation.equals("execute") ? TaskStatus.LEASED : TaskStatus.EXECUTING);
            if (operation.equals("watchdog")) jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()-INTERVAL '1 second' WHERE run_id=?", run);
            TaskRecord old = task(); pauses.pause(run);
            Runnable resume = () -> pauses.resume(run, null);
            Runnable stale = action(operation, old);
            if (resumeFirst) ordered(resume, stale, false); else ordered(stale, resume, false);
            assertThat(task().status()).isEqualTo(TaskStatus.PENDING);
            assertThat(task().attempt()).isEqualTo(old.attempt());
            assertThat(task().version()).isEqualTo(old.version() + 2);
            assertThat(types()).doesNotContain("TaskSucceeded", "TaskFailed", "TaskRetried", "TaskLeaseExpired");
        }
    }

    @ParameterizedTest @ValueSource(strings = {"pause", "resume"})
    void validPauseOrResumeDecisionCanCommitAfterDeadline(String operation) throws Exception {
        if (operation.equals("resume")) pauses.pause(run);
        setDeadline(store.currentTime().plusMillis(500));
        ordered(() -> { if (operation.equals("pause")) pauses.pause(run); else pauses.resume(run, null); },
                () -> timeouts.expireDueTasks(10), true);
        assertThat(task().status()).isEqualTo(operation.equals("pause") ? TaskStatus.PAUSED : TaskStatus.PENDING);
        assertThat(timeouts.expireDueTasks(10)).isEqualTo(1);
        assertThat(task().status()).isEqualTo(TaskStatus.TIMEOUT);
    }

    @ParameterizedTest @ValueSource(strings = {"pause", "resume"})
    void waitingForRowLockRechecksExpiryBeforeMakingDecision(String op) throws Exception {
        if (op.equals("resume")) pauses.pause(run);
        setDeadline(store.currentTime().plusMillis(500));
        ordered(() -> { }, () -> assertThrows(PauseConflictException.class,
                () -> { if (op.equals("pause")) pauses.pause(run); else pauses.resume(run, null); }), true);
        assertThat(task().status()).isEqualTo(TaskStatus.TIMEOUT);
    }

    private Runnable action(String op, TaskRecord snapshot) {
        var body = new WorkerTransactionService(store, mapper);
        return switch (op) {
            case "claim" -> () -> body.nextTask("worker", Duration.ofSeconds(30), Set.of());
            case "execute" -> () -> body.markExecuting(snapshot, "worker");
            case "renew" -> () -> { try { body.renewLease(snapshot, "worker", Duration.ofSeconds(30)); }
                                    catch (org.springframework.dao.CannotAcquireLockException ignored) { } };
            case "success" -> () -> body.completeSuccessfully(snapshot, "worker", output());
            case "retry", "failure" -> () -> failures.failOwnedTask(snapshot, "worker", "test");
            case "watchdog" -> () -> failures.recoverExpiredLeases();
            case "cancel" -> () -> { try { cancellations.cancel(run); } catch (CancellationConflictException ignored) { } };
            case "timeout" -> () -> timeouts.expireDueTasks(10);
            default -> throw new IllegalArgumentException(op);
        };
    }
    private void ordered(Runnable first, Runnable second, boolean crossDeadline) throws Exception {
        CountDownLatch locked = new CountDownLatch(1), release = new CountDownLatch(1), started = new CountDownLatch(1);
        AtomicInteger aPid = new AtomicInteger(), bPid = new AtomicInteger();
        var pool = Executors.newFixedThreadPool(2);
        var a = pool.submit(() -> tx.executeWithoutResult(s -> {
            aPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class));
            store.findTasksForUpdate(run); first.run(); locked.countDown(); awaitLatch(release);
        }));
        java.util.concurrent.Future<?> b = null;
        try {
            assertThat(locked.await(5, TimeUnit.SECONDS)).isTrue();
            b = pool.submit(() -> tx.executeWithoutResult(s -> {
                bPid.set(jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class)); started.countDown(); second.run();
            }));
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue(); var pending = b;
            await().atMost(Duration.ofSeconds(5)).until(() -> pending.isDone() || Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT ? = ANY(pg_blocking_pids(?))", Boolean.class, aPid.get(), bPid.get())));
            if (crossDeadline) await().atMost(Duration.ofSeconds(3)).until(() -> !store.currentTime().isBefore(task().deadlineAt()));
            release.countDown(); a.get(5, TimeUnit.SECONDS); b.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown(); a.cancel(true); if (b != null) b.cancel(true); pool.shutdownNow();
            assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }
    private void prepare(TaskStatus stage) {
        if (stage != TaskStatus.PENDING) workers.nextTask("worker", Duration.ofSeconds(30), Set.of()).orElseThrow();
        if (stage == TaskStatus.EXECUTING || stage == TaskStatus.RETRYING) workers.markExecuting(task(), "worker").orElseThrow();
        if (stage == TaskStatus.RETRYING) failures.failOwnedTask(task(), "worker", "retry");
    }
    private ResponseEntity<JsonNode> post(String operation, String checkpoint) {
        var headers = new HttpHeaders(); if (checkpoint != null) headers.set("Pause-Checkpoint", checkpoint);
        var response = http.postForEntity("/v1/runs/" + run + "/" + operation, new HttpEntity<>(headers), JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/" + operation, "post", response); return response;
    }
    private String create() {
        return runtime.createRun(UUID.randomUUID().toString(), new CreateRunRequest(runtime.createSession().sessionId(), "pause", store.currentTime().plusSeconds(60))).runId();
    }
    private TaskRecord task() { return store.findTasks(run).getFirst(); }
    private List<String> types() { return store.findTimeline(run).stream().map(EventRecord::eventType).toList(); }
    private JsonNode output() { return mapper.createObjectNode().put("summary", "done"); }
    private void due() { setDeadline(store.currentTime().minusSeconds(1)); }
    private void setDeadline(Instant deadline) {
        tx.executeWithoutResult(s -> {
            jdbc.update("UPDATE tasks SET deadline_at=?::timestamptz, lease_expires_at=CASE WHEN lease_expires_at IS NULL THEN NULL ELSE LEAST(lease_expires_at, ?::timestamptz) END WHERE run_id=?", deadline.toString(), deadline.toString(), run);
            jdbc.update("UPDATE runs SET deadline_at=?::timestamptz WHERE id=?", deadline.toString(), run);
        });
    }
    private static void awaitLatch(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("latch timed out"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
    }
}
