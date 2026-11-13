package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.api.ApiModels.CreateRunRequest;
import io.nexus.controlplane.identity.TenantContext;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class TenantMigrationIntegrationTest {
    @Test void v6UpgradePreservesHistoryResultsCheckpointAndOldIdempotentRequests() throws Exception {
        var source = new DriverManagerDataSource(System.getenv("TEST_DB_URL"),
                System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"), System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"));
        String schema = "b10_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        var admin = new JdbcTemplate(source);
        try {
            Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).target("6").load().migrate();
            try (var c = source.getConnection()) {
                c.setSchema(schema);
                var jdbc = new JdbcTemplate(new SingleConnectionDataSource(c, true));
                jdbc.update("INSERT INTO sessions(id) VALUES ('ses_old')");
                String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest("ses_old\ntest".getBytes(StandardCharsets.UTF_8)));
                for (String state : List.of("QUEUED", "SUCCEEDED", "TIMEOUT", "PAUSED")) {
                    boolean paused = state.equals("PAUSED");
                    c.setAutoCommit(false);
                    jdbc.update("INSERT INTO runs(id,session_id,idempotency_key,request_hash,input,status,output,deadline_at) VALUES (?,'ses_old',?,?,'test',?,?::jsonb,?::timestamptz)",
                            "run_"+state, "key_"+state, hash, paused ? "QUEUED" : state,
                            state.equals("SUCCEEDED") ? "{\"summary\":\"old\"}" : null, state.equals("TIMEOUT") ? "2026-11-06T00:00:00Z" : null);
                    jdbc.update("INSERT INTO tasks(id,run_id,name,status,input,output,deadline_at) SELECT ?,id,'old',?, '{\"input\":\"test\"}',output,deadline_at FROM runs WHERE id=?",
                            "tsk_"+state, state.equals("QUEUED") || paused ? "PENDING" : state, "run_"+state);
                    if (paused) {
                        jdbc.update("INSERT INTO task_pause_checkpoints(task_id,run_id,checkpoint_id,schema_version,run_status,task_status,attempt,task_version,run_version,next_attempt_at,paused_at) SELECT id,run_id,?::uuid,1,'QUEUED','PENDING',0,0,0,next_attempt_at,clock_timestamp() FROM tasks WHERE id='tsk_PAUSED'", UUID.randomUUID().toString());
                        jdbc.update("UPDATE tasks SET status='PAUSED',version=1 WHERE id='tsk_PAUSED'");
                        jdbc.update("UPDATE runs SET status='PAUSED',version=1 WHERE id='run_PAUSED'");
                    }
                    jdbc.update("INSERT INTO domain_events(event_id,aggregate_type,aggregate_id,run_id,event_type,payload) VALUES (?,'RUN',?,?,'RunCreated','{\"status\":\"CREATED\",\"input\":\"test\"}')", "evt_"+state,"run_"+state,"run_"+state);
                    c.commit(); c.setAutoCommit(true);
                }
                var sessions = jdbc.queryForList("SELECT to_jsonb(s)::text AS row FROM sessions s ORDER BY id");
                var runs = jdbc.queryForList("SELECT to_jsonb(r)::text AS row FROM runs r ORDER BY id");
                var tasks = jdbc.queryForList("SELECT to_jsonb(t)::text AS row FROM tasks t ORDER BY id");
                var checkpoints = jdbc.queryForList("SELECT to_jsonb(c)::text AS row FROM task_pause_checkpoints c");
                var events = jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence");
                var flyway = Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).load();
                assertThat(flyway.migrate().migrationsExecuted).isEqualTo(1); flyway.validate();
                assertThat(jdbc.queryForList("SELECT (to_jsonb(s)-'tenant_id'-'project_id')::text AS row FROM sessions s ORDER BY id")).isEqualTo(sessions);
                assertThat(jdbc.queryForList("SELECT (to_jsonb(r)-'tenant_id'-'project_id')::text AS row FROM runs r ORDER BY id")).isEqualTo(runs);
                assertThat(jdbc.queryForList("SELECT (to_jsonb(t)-'tenant_id'-'project_id')::text AS row FROM tasks t ORDER BY id")).isEqualTo(tasks);
                assertThat(jdbc.queryForList("SELECT to_jsonb(c)::text AS row FROM task_pause_checkpoints c")).isEqualTo(checkpoints);
                assertThat(jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence")).isEqualTo(events);
                assertThat(jdbc.queryForObject("SELECT count(*) FROM event_ownership WHERE tenant_id='legacy' AND project_id='legacy'", Integer.class)).isEqualTo(4);
                var store = new RuntimeStore(jdbc, new ObjectMapper());
                var runtime = new RuntimeService(store, new ObjectMapper(), () -> new TenantContext("legacy", "legacy"));
                for (String state : List.of("QUEUED", "SUCCEEDED", "TIMEOUT", "PAUSED")) {
                    assertThat(runtime.getRun("run_"+state).tenantId()).isEqualTo("legacy");
                    assertThat(runtime.getTimeline("run_"+state).events()).hasSize(1);
                }
                assertThat(runtime.createRun("key_QUEUED", new CreateRunRequest("ses_old", "test")).runId()).isEqualTo("run_QUEUED");
                for (String sql : List.of("UPDATE domain_events SET payload='{}'", "DELETE FROM domain_events", "DELETE FROM event_ownership", "UPDATE event_ownership SET project_id='other'")) {
                    assertThrows(DataAccessException.class, () -> jdbc.update(sql));
                }
                jdbc.execute("ALTER TABLE event_ownership DISABLE TRIGGER event_ownership_immutable");
                jdbc.update("DELETE FROM event_ownership WHERE event_id='evt_QUEUED'");
                assertThrows(DataAccessException.class, () -> store.findTimeline("run_QUEUED"));
                assertThrows(DataAccessException.class, () -> jdbc.update("INSERT INTO event_ownership(event_id,run_id,task_id,tenant_id,project_id) VALUES ('evt_QUEUED','run_SUCCEEDED',NULL,'legacy','legacy')"));
            }
        } finally { admin.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE"); }
    }
}
