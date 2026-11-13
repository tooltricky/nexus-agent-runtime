package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.identity.TenantContext;
import io.nexus.controlplane.identity.TenantContextSource;
import io.nexus.controlplane.persistence.RuntimeStore;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.dao.DataAccessException;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static io.nexus.controlplane.support.RuntimeTestSupport.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexus.timeout.enabled=false", "nexus.worker.enabled=false", "nexus.watchdog.enabled=false",
        "spring.datasource.url=${TEST_DB_URL}", "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class TenantOwnershipIntegrationTest {
    static final TenantContext A = new TenantContext("tenant-a", "project-a");
    static final TenantContext A2 = new TenantContext("tenant-a", "project-2");
    static final TenantContext B = new TenantContext("tenant-b", "project-a");
    @TestConfiguration
    static class IdentityConfiguration {
        @Bean @Primary ControlledIdentity controlledIdentity() { return new ControlledIdentity(); }
    }
    static class ControlledIdentity implements TenantContextSource {
        final AtomicReference<TenantContext> httpScope = new AtomicReference<>(A);
        final ThreadLocal<TenantContext> local = new ThreadLocal<>();
        public TenantContext requireContext() { return local.get() == null ? httpScope.get() : local.get(); }
        <T> T as(TenantContext scope, Callable<T> work) throws Exception {
            local.set(scope);
            try { return work.call(); } finally { local.remove(); }
        }
    }
    @Autowired ControlledIdentity identities;
    @Autowired RuntimeService runtime;
    @Autowired RuntimeStore store;
    @Autowired RunPauseService pauses;
    @Autowired RunCancellationService cancellations;
    @Autowired WorkerTransactionService workers;
    @Autowired TaskFailureService failures;
    @Autowired TaskTimeoutService timeouts;
    @Autowired JdbcTemplate jdbc;
    @Autowired TestRestTemplate http;
    @Autowired ApplicationContext context;

    @BeforeEach void reset() {
        clearDatabase(jdbc);
        identities.httpScope.set(A);
        for (var scope : List.of(A, A2, B)) {
            jdbc.update("INSERT INTO tenants(id) VALUES (?) ON CONFLICT DO NOTHING", scope.tenantId());
            jdbc.update("INSERT INTO projects(tenant_id,id) VALUES (?,?) ON CONFLICT DO NOTHING", scope.tenantId(), scope.projectId());
        }
    }
    String run(String key) { return runtime.createRun(key, new CreateRunRequest(runtime.createSession().sessionId(), "test")).runId(); }

    @Test void tenantScopedConcurrentKeysDoNotDuplicateOrLeakAcrossProjects() throws Exception {
        String a = runtime.createSession().sessionId();
        String a2 = identities.as(A2, () -> runtime.createSession().sessionId());
        String b = identities.as(B, () -> runtime.createSession().sessionId());
        var requests = IntStream.range(0, 12).<Callable<String>>mapToObj(i -> () -> identities.as(
                i % 2 == 0 ? A : B,
                () -> runtime.createRun("shared", new CreateRunRequest(i % 2 == 0 ? a : b, "test")).runId())).toList();
        var ids = concurrently(requests);
        assertThat(ids.stream().distinct()).hasSize(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM runs", Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM tasks", Integer.class)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM event_ownership", Integer.class)).isEqualTo(6);
        String aRun = runtime.createRun("shared", new CreateRunRequest(a, "test")).runId();
        assertThrows(ConflictException.class, () -> runtime.createRun("shared", new CreateRunRequest(a, "changed")));
        assertThrows(ConflictException.class, () -> identities.as(A2,
                () -> runtime.createRun("shared", new CreateRunRequest(a2, "test"))));
        assertThrows(NotFoundException.class, () -> identities.as(B, () -> runtime.getRun(aRun)));
        assertThrows(NotFoundException.class, () -> identities.as(B,
                () -> runtime.createRun("foreign-session", new CreateRunRequest(a, "test"))));
    }

    @Test void concurrentDifferentRequestsInOneTenantHaveOneWinner() throws Exception {
        String session = runtime.createSession().sessionId();
        var outcomes = concurrently(IntStream.range(0, 8).<Callable<String>>mapToObj(i -> () -> {
            try { return runtime.createRun("race", new CreateRunRequest(session, "input-" + i)).runId(); }
            catch (ConflictException conflict) { return "conflict"; }
        }).toList());
        assertThat(outcomes.stream().filter("conflict"::equals)).hasSize(7);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM event_ownership", Integer.class)).isEqualTo(3);
    }

    @Test void httpHeadersCannotSelectIdentityAndAllResourceOperationsRespectScope() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Tenant-Id", "tenant-b"); headers.set("X-Project-Id", "project-2");
        var session = http.exchange("/v1/sessions", HttpMethod.POST,
                new HttpEntity<>(Map.of("tenant_id", "tenant-b", "project_id", "project-2"), headers), JsonNode.class);
        ContractAssertions.response("/v1/sessions", "post", session);
        assertThat(session.getBody().path("tenant_id").asText()).isEqualTo(A.tenantId());
        assertThat(session.getBody().path("project_id").asText()).isEqualTo(A.projectId());
        headers.set("Idempotency-Key", "http-owned");
        var created = http.exchange("/v1/runs", HttpMethod.POST, new HttpEntity<>(Map.of(
                "session_id", session.getBody().path("session_id").asText(), "input", "test",
                "tenant_id", "tenant-b", "project_id", "project-2"), headers), JsonNode.class);
        ContractAssertions.response("/v1/runs", "post", created);
        String id = created.getBody().path("run_id").asText();
        var own = http.getForEntity("/v1/runs/" + id, JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}", "get", own);
        assertThat(own.getBody().path("tenant_id").asText()).isEqualTo(A.tenantId());
        identities.httpScope.set(A2);
        String otherSession = runtime.createSession().sessionId();
        var conflict = http.exchange("/v1/runs", HttpMethod.POST,
                new HttpEntity<>(Map.of("session_id", otherSession, "input", "test"), headers), JsonNode.class);
        ContractAssertions.response("/v1/runs", "post", conflict);
        assertThat(conflict.getStatusCode().value()).isEqualTo(409);
        assertThat(conflict.getBody().toString()).doesNotContain(id);
        for (var other : List.of(A2, B)) {
            identities.httpScope.set(other);
            for (String suffix : List.of("", "/timeline", "/cancel", "/pause", "/resume")) {
                var method = suffix.isEmpty() || suffix.equals("/timeline") ? HttpMethod.GET : HttpMethod.POST;
                var denied = http.exchange("/v1/runs/" + id + suffix, method, new HttpEntity<>(headers), JsonNode.class);
                assertThat(denied.getStatusCode().value()).as(other + suffix).isEqualTo(404);
                ContractAssertions.response("/v1/runs/{run_id}" + suffix, method.name().toLowerCase(), denied);
                assertThat(denied.getBody().has("input")).isFalse();
            }
        }
        identities.httpScope.set(A);
        var timeline = http.getForEntity("/v1/runs/" + id + "/timeline", JsonNode.class);
        ContractAssertions.response("/v1/runs/{run_id}/timeline", "get", timeline);
        assertThat(timeline.getBody().path("events")).allSatisfy(event -> {
            assertThat(event.path("tenant_id").asText()).isEqualTo(A.tenantId());
            assertThat(event.path("project_id").asText()).isEqualTo(A.projectId());
        });
        assertThat(store.findTimeline(id)).hasSize(3);
    }

    @Test void databaseRejectsWrongAssociationsAndOwnershipChanges() throws Exception {
        String a = run("a"); String task = store.findTasks(a).getFirst().id();
        String b = identities.as(B, () -> run("b"));
        String a2 = identities.as(A2, () -> run("a2"));
        String event = store.findTimeline(a).getFirst().eventId();
        for (String sql : List.of(
                "INSERT INTO sessions(id,tenant_id,project_id) VALUES ('bad','missing','missing')",
                "INSERT INTO runs(id,session_id,idempotency_key,request_hash,input,status,tenant_id,project_id) SELECT 'bad',session_id,'bad','hash','test','QUEUED','tenant-b','project-a' FROM runs WHERE id='" + a + "'",
                "INSERT INTO tasks(id,run_id,name,status,input,tenant_id,project_id) VALUES ('bad','" + a + "','bad','PENDING','{}','tenant-b','project-a')",
                "INSERT INTO domain_events(event_id,aggregate_type,aggregate_id,run_id,task_id,event_type,payload) VALUES ('bad','TASK','" + task + "','" + b + "','" + task + "','TaskCreated','{}')",
                "INSERT INTO runs(id,session_id,idempotency_key,request_hash,input,status,tenant_id,project_id) SELECT 'bad-project',session_id,'bad-project','hash','test','QUEUED','tenant-a','project-2' FROM runs WHERE id='" + a + "'",
                "INSERT INTO tasks(id,run_id,name,status,input,tenant_id,project_id) VALUES ('bad-project','" + a + "','bad','PENDING','{}','tenant-a','project-2')",
                "INSERT INTO domain_events(event_id,aggregate_type,aggregate_id,run_id,task_id,event_type,payload) VALUES ('bad-project','TASK','" + task + "','" + a2 + "','" + task + "','TaskCreated','{}')",
                "UPDATE sessions SET project_id='project-2' WHERE tenant_id='tenant-a'",
                "UPDATE runs SET project_id='project-2' WHERE id='" + a + "'",
                "UPDATE tasks SET tenant_id='tenant-b' WHERE id='" + task + "'",
                "UPDATE event_ownership SET tenant_id='tenant-b' WHERE event_id='" + event + "'",
                "DELETE FROM event_ownership WHERE event_id='" + event + "'")) {
            assertThrows(DataAccessException.class, () -> jdbc.update(sql), sql);
        }
        pauses.pause(a);
        assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE task_pause_checkpoints SET run_id=? WHERE task_id=?", b, task));
        assertThat(store.findTimeline(a)).hasSize(5);
    }

    @Test void ownershipInsertFailureRollsBackCreationAndStateOperations() {
        String id = run("existing");
        var before = runtime.getRun(id); var events = store.findTimeline(id);
        jdbc.execute("CREATE FUNCTION fail_b10_ownership() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ownership failure'; END $$");
        jdbc.execute("CREATE TRIGGER fail_b10_ownership BEFORE INSERT ON event_ownership FOR EACH ROW EXECUTE FUNCTION fail_b10_ownership()");
        try {
            String session = runtime.createSession().sessionId();
            assertThrows(DataAccessException.class, () -> runtime.createRun("rollback", new CreateRunRequest(session, "test")));
            assertThrows(DataAccessException.class, () -> pauses.pause(id));
            assertThrows(DataAccessException.class, () -> cancellations.cancel(id));
            assertThat(runtime.getRun(id)).isEqualTo(before);
            assertThat(store.findTimeline(id)).isEqualTo(events);
            assertThat(store.findPauseCheckpoint(before.tasks().getFirst().taskId())).isEmpty();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM runs", Integer.class)).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM event_ownership", Integer.class)).isEqualTo(3);
            jdbc.execute("DROP TRIGGER fail_b10_ownership ON event_ownership");
            var paused = pauses.pause(id);
            var pausedEvents = store.findTimeline(id);
            jdbc.execute("CREATE TRIGGER fail_b10_ownership BEFORE INSERT ON event_ownership FOR EACH ROW EXECUTE FUNCTION fail_b10_ownership()");
            assertThrows(DataAccessException.class, () -> pauses.resume(id, paused.tasks().getFirst().checkpoint().checkpointId()));
            assertThat(runtime.getRun(id)).isEqualTo(paused);
            assertThat(store.findTimeline(id)).isEqualTo(pausedEvents);
        } finally {
            jdbc.execute("DROP TRIGGER fail_b10_ownership ON event_ownership");
            jdbc.execute("DROP FUNCTION fail_b10_ownership()");
        }
    }

    @Test void retryLeaseRecoveryAndTimeoutKeepTheirOriginalTenant() throws Exception {
        String retried = identities.as(B, () -> run("retry"));
        var lease = java.time.Duration.ofSeconds(30);
        var first = workers.nextTask("b-worker", lease, java.util.Set.of()).orElseThrow().task();
        assertThat(workers.renewLease(first, "b-worker", lease)).isTrue();
        assertThat(failures.failOwnedTask(first, "b-worker", "controlled failure")).isTrue();
        jdbc.update("UPDATE tasks SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE run_id=?", retried);
        workers.nextTask("b-worker", lease, java.util.Set.of()).orElseThrow();
        jdbc.update("UPDATE tasks SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE run_id=?", retried);
        assertThat(failures.recoverExpiredLeases(10)).isEqualTo(1);
        identities.as(B, () -> cancellations.cancel(retried));
        String timeout = identities.as(B, () -> run("timeout"));
        var transaction = new org.springframework.transaction.support.TransactionTemplate(
                context.getBean(org.springframework.transaction.PlatformTransactionManager.class));
        transaction.executeWithoutResult(status -> {
            jdbc.update("UPDATE tasks SET deadline_at=clock_timestamp()-interval '1 second' WHERE run_id=?", timeout);
            jdbc.update("UPDATE runs SET deadline_at=(SELECT deadline_at FROM tasks WHERE run_id=?) WHERE id=?", timeout, timeout);
        });
        assertThat(timeouts.expireDueTasks(10)).isEqualTo(1);
        assertThat(store.findRun(timeout).orElseThrow().status().name()).isEqualTo("TIMEOUT");
        for (String id : List.of(retried, timeout)) {
            assertThat(store.findTimeline(id)).allSatisfy(event -> {
                assertThat(event.tenantId()).isEqualTo(B.tenantId()); assertThat(event.projectId()).isEqualTo(B.projectId());
            });
        }
        assertThat(store.findTimeline(retried)).extracting(event -> event.eventType()).contains("TaskRetried", "TaskLeaseExpired");
    }

    @Test void restartRetainsOwnershipAndResumesOnlyTheOwnedPausedWork() throws Exception {
        String id = identities.as(B, () -> run("restart"));
        identities.as(B, () -> pauses.pause(id));
        var old = store.findPauseCheckpoint(store.findTasks(id).getFirst().id()).orElseThrow();
        for (int cycle = 0; cycle < 2; cycle++) {
            try (var restarted = new org.springframework.boot.builder.SpringApplicationBuilder(
                    io.nexus.controlplane.ControlPlaneApplication.class).web(org.springframework.boot.WebApplicationType.NONE).run(
                    "--spring.datasource.url=" + System.getenv("TEST_DB_URL"),
                    "--spring.datasource.username=" + System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                    "--spring.datasource.password=" + System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"),
                    "--nexus.worker.enabled=false", "--nexus.watchdog.enabled=false", "--nexus.timeout.enabled=false",
                    "--nexus.identity.development-enabled=true", "--nexus.identity.tenant-id=" + B.tenantId(),
                    "--nexus.identity.project-id=" + B.projectId())) {
                var ownedRuntime = restarted.getBean(RuntimeService.class);
                assertThat(ownedRuntime.getRun(id).tenantId()).isEqualTo(B.tenantId());
                if (cycle == 0) {
                    assertThat(ownedRuntime.getRun(id).status().name()).isEqualTo("PAUSED");
                    assertThat(ownedRuntime.getRun(id).tasks().getFirst().checkpoint()).isEqualTo(old);
                    restarted.getBean(RunPauseService.class).resume(id, old.checkpointId());
                    var worker = worker(restarted);
                    for (int i = 0; i < 3; i++) tick(restarted, worker);
                }
                assertThat(ownedRuntime.getRun(id).status().name()).isEqualTo("SUCCEEDED");
                assertThat(ownedRuntime.getRun(id).tasks().getFirst().attempt()).isEqualTo(1);
            }
        }
        assertThrows(NotFoundException.class, () -> runtime.getRun(id));
    }

    @Test void backgroundLifecycleInheritsOwnershipRegardlessOfCurrentRequestContext() throws Exception {
        String success = identities.as(B, () -> run("success"));
        // Worker has no tenant context parameter and executes while the HTTP identity is A.
        var worker = worker(context);
        for (int i = 0; i < 3; i++) tick(context, worker);
        assertThat(store.findRun(success).orElseThrow().status().name()).isEqualTo("SUCCEEDED");
        String paused = identities.as(B, () -> run("paused"));
        identities.as(B, () -> pauses.pause(paused));
        identities.as(B, () -> pauses.resume(paused, null));
        identities.as(B, () -> cancellations.cancel(paused));
        for (String id : List.of(success, paused)) {
            assertThat(store.findTasks(id)).allSatisfy(task -> {
                assertThat(task.tenantId()).isEqualTo(B.tenantId()); assertThat(task.projectId()).isEqualTo(B.projectId());
            });
            assertThat(store.findTimeline(id)).allSatisfy(event -> {
                assertThat(event.tenantId()).isEqualTo(B.tenantId()); assertThat(event.projectId()).isEqualTo(B.projectId());
            });
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM domain_events e LEFT JOIN event_ownership o USING(event_id) WHERE o.event_id IS NULL", Integer.class)).isZero();
    }
}
