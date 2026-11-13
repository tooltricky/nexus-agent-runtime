package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.persistence.RuntimeStore;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import java.util.List;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class PauseMigrationIntegrationTest {
    @Test void v5UpgradePreservesRowsAndHistoryAndEnforcesCheckpointConsistency() throws Exception {
        var source = new DriverManagerDataSource(System.getenv("TEST_DB_URL"),
                System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"), System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"));
        String schema = "b09_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        var admin = new JdbcTemplate(source);
        try {
            Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).target("5").load().migrate();
            try (var c = source.getConnection()) {
                c.setSchema(schema);
                var jdbc = new JdbcTemplate(new SingleConnectionDataSource(c, true));
                jdbc.update("INSERT INTO sessions(id) VALUES ('ses_old')");
                var states = List.of("PENDING", "LEASED", "EXECUTING", "RETRYING", "SUCCEEDED", "FAILED", "CANCELLED", "TIMEOUT");
                for (int i = 0; i < states.size(); i++) {
                    String state = states.get(i);
                    String runState = state.equals("PENDING") ? "QUEUED" : List.of("LEASED", "EXECUTING", "RETRYING").contains(state) ? "RUNNING" : state;
                    String deadline = state.equals("TIMEOUT") ? "2026-11-06T00:00:00Z" : null;
                    jdbc.update("INSERT INTO runs(id,session_id,idempotency_key,request_hash,input,status,output,deadline_at) VALUES(?,'ses_old',?,'hash','old',?,?::jsonb,?::timestamptz)",
                            "run_" + i, "key_" + i, runState, state.equals("SUCCEEDED") ? "{\"summary\":\"old\"}" : null, deadline);
                    jdbc.update("INSERT INTO tasks(id,run_id,name,input,status,deadline_at) VALUES(?,?,'old','{}',?,?::timestamptz)", "tsk_" + i, "run_" + i, state, deadline);
                    jdbc.update("INSERT INTO domain_events(event_id,aggregate_type,aggregate_id,run_id,event_type,payload) VALUES(?,'RUN',?,?,'RunCreated','{}')", "evt_" + i, "run_" + i, "run_" + i);
                }
                var runs = jdbc.queryForList("SELECT to_jsonb(r)::text AS row FROM runs r ORDER BY id");
                var tasks = jdbc.queryForList("SELECT to_jsonb(t)::text AS row FROM tasks t ORDER BY id");
                var events = jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence");
                var flyway = Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).target("6").load();
                assertThat(flyway.migrate().migrationsExecuted).isEqualTo(1); flyway.validate();
                assertThat(jdbc.queryForList("SELECT to_jsonb(r)::text AS row FROM runs r ORDER BY id")).isEqualTo(runs);
                assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text AS row FROM tasks t ORDER BY id")).isEqualTo(tasks);
                assertThat(jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence")).isEqualTo(events);
                assertThat(jdbc.queryForObject("SELECT count(*) FROM task_pause_checkpoints", Integer.class)).isZero();
                for (String sql : List.of("UPDATE runs SET status='PAUSED' WHERE id='run_0'", "UPDATE tasks SET status='PAUSED' WHERE id='tsk_0'")) {
                    assertThrows(DataAccessException.class, () -> jdbc.update(sql));
                }
                Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).load().migrate();
                var store = new RuntimeStore(jdbc, new ObjectMapper());
                c.setAutoCommit(false);
                var task = store.findTasks("run_0").getFirst(); var run = store.findRun("run_0").orElseThrow();
                var checkpoint = store.savePauseCheckpoint(task, run, store.currentTime());
                store.pauseTask(task); store.transitionRun(run, RunStatus.PAUSED, null);
                c.commit(); c.setAutoCommit(true);
                assertThat(store.findPauseCheckpoint(task.id()).orElseThrow()).isEqualTo(checkpoint);
                for (String sql : List.of(
                        "UPDATE tasks SET lease_owner='bad' WHERE id='tsk_0'",
                        "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE id='tsk_0'",
                        "UPDATE tasks SET output='{}' WHERE id='tsk_0'",
                        "UPDATE runs SET output='{}' WHERE id='run_0'",
                        "UPDATE tasks SET version=version+1 WHERE id='tsk_0'",
                        "UPDATE runs SET version=version+1 WHERE id='run_0'",
                        "DELETE FROM task_pause_checkpoints WHERE task_id='tsk_0'",
                        "UPDATE task_pause_checkpoints SET consumed_at=clock_timestamp(),disposition='RESUMED' WHERE task_id='tsk_0'",
                        "UPDATE task_pause_checkpoints SET run_id='run_1' WHERE task_id='tsk_0'",
                        "UPDATE task_pause_checkpoints SET schema_version=2 WHERE task_id='tsk_0'",
                        "UPDATE task_pause_checkpoints SET next_attempt_at=next_attempt_at+INTERVAL '1 second' WHERE task_id='tsk_0'",
                        "UPDATE domain_events SET schema_version='0.4'", "DELETE FROM domain_events")) {
                    assertThrows(DataAccessException.class, () -> jdbc.update(sql), sql);
                }
                c.setAutoCommit(false);
                var paused = store.findTasks("run_0").getFirst();
                store.consumePauseCheckpoint(paused, "RESUMED");
                store.resumeTask(paused, TaskStatus.PENDING, checkpoint.nextAttemptAt());
                store.transitionRun(store.findRun("run_0").orElseThrow(), RunStatus.QUEUED, null);
                c.commit(); c.setAutoCommit(true);
                assertThat(store.findPauseCheckpoint(task.id()).orElseThrow().disposition()).isEqualTo("RESUMED");
                assertThat(store.findTasks("run_0").getFirst().version()).isEqualTo(task.version() + 2);
                assertThat(jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence")).isEqualTo(events);
            }
        } finally { admin.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE"); }
    }
}
