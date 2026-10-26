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

/** Uses unique schemas only inside TEST_DB_URL; never drops the shared public schema. */
@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
class CancellationMigrationIntegrationTest {
    private DriverManagerDataSource dataSource() {
        return new DriverManagerDataSource(System.getenv("TEST_DB_URL"),
                System.getenv().getOrDefault("TEST_DB_USERNAME", "nexus"),
                System.getenv().getOrDefault("TEST_DB_PASSWORD", "nexus"));
    }

    private Flyway migration(DriverManagerDataSource source, String schema, String target) {
        return Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema)
                .locations("classpath:db/migration").target(target).load();
    }

    @Test
    void v3DatabaseUpgradesWithoutChangingAnyExistingRowsOrEvents() throws Exception {
        var source = dataSource();
        String schema = "b04_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        var admin = new JdbcTemplate(source);
        try {
            migration(source, schema, "3").migrate();
            try (var connection = source.getConnection()) {
                connection.setSchema(schema);
                var jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                jdbc.update("INSERT INTO sessions (id) VALUES ('ses_old')");
                // Every old Run/Task status is represented before V4 is applied.
                List<String> runStates = List.of("CREATED", "QUEUED", "RUNNING", "SUCCEEDED", "FAILED");
                List<String> taskStates = List.of("PENDING", "LEASED", "EXECUTING", "RETRYING", "SUCCEEDED", "FAILED");
                int index = 0;
                for (String runState : runStates) {
                    for (String taskState : taskStates) {
                        String run = "run_old_" + index;
                        String task = "tsk_old_" + index++;
                        jdbc.update("""
                                INSERT INTO runs (id, session_id, idempotency_key, request_hash, input, status, output)
                                VALUES (?, 'ses_old', ?, 'hash', 'old input', ?, CAST(? AS jsonb))
                                """, run, run, runState, runState.equals("SUCCEEDED") ? "{\"summary\":\"old result\"}" : null);
                        jdbc.update("""
                                INSERT INTO tasks (id, run_id, name, status, input, attempt, lease_owner, lease_expires_at)
                                VALUES (?, ?, 'old task', ?, '{"input":"old input"}', 2, ?,
                                        CASE WHEN ? THEN clock_timestamp() + INTERVAL '1 hour' ELSE NULL END)
                                """, task, run, taskState,
                                List.of("LEASED", "EXECUTING").contains(taskState) ? "old-owner" : null,
                                List.of("LEASED", "EXECUTING").contains(taskState));
                        jdbc.update("""
                                INSERT INTO domain_events (event_id, aggregate_type, aggregate_id, run_id, event_type, payload)
                                VALUES (?, 'RUN', ?, ?, 'RunCreated', '{"status":"CREATED","input":"old input"}')
                                """, "evt_old_" + index, run, run);
                    }
                }
                var oldRuns = jdbc.queryForList("SELECT * FROM runs ORDER BY id");
                var oldTasks = jdbc.queryForList("SELECT * FROM tasks ORDER BY id");
                var oldEvents = jdbc.queryForList("SELECT to_jsonb(e)::text AS event FROM domain_events e ORDER BY sequence");
                migration(source, schema, "4").migrate();
                assertThat(jdbc.queryForList("SELECT * FROM runs ORDER BY id")).isEqualTo(oldRuns);
                assertThat(jdbc.queryForList("SELECT * FROM tasks ORDER BY id")).isEqualTo(oldTasks);
                assertThat(jdbc.queryForList("SELECT (to_jsonb(e) - 'schema_version')::text AS event FROM domain_events e ORDER BY sequence"))
                        .isEqualTo(oldEvents);
                assertThat(jdbc.queryForObject("SELECT count(*) FROM domain_events WHERE schema_version IS NOT NULL", Integer.class)).isZero();
                var store = new RuntimeStore(jdbc, new ObjectMapper());
                for (int i = 0; i < index; i++) {
                    assertThat(store.findRun("run_old_" + i)).isPresent();
                    assertThat(store.findTasks("run_old_" + i)).hasSize(1);
                    assertThat(store.findTimeline("run_old_" + i).getFirst().schemaVersion()).isNull();
                }
                assertCancellationConstraints(jdbc, store);
                var before = store.findTimeline("run_old_0");
                assertThrows(DataAccessException.class, () -> jdbc.update("UPDATE domain_events SET schema_version = '0.2'"));
                assertThrows(DataAccessException.class, () -> jdbc.update("DELETE FROM domain_events"));
                assertThat(store.findTimeline("run_old_0")).isEqualTo(before);
            }
        } finally {
            admin.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    @Test
    void emptyDatabaseMigratesThroughV4AndRejectsInvalidCancellationFields() throws Exception {
        var source = dataSource();
        String schema = "b04_fresh_" + UUID.randomUUID().toString().replace("-", "");
        try {
            var flyway = migration(source, schema, "4");
            assertThat(flyway.migrate().migrationsExecuted).isEqualTo(4);
            flyway.validate();
            try (var connection = source.getConnection()) {
                connection.setSchema(schema);
                var jdbc = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
                jdbc.update("INSERT INTO sessions (id) VALUES ('ses_old')");
                jdbc.update("INSERT INTO runs (id,session_id,idempotency_key,request_hash,input,status) VALUES ('run_old_0','ses_old','key','hash','test','QUEUED')");
                jdbc.update("INSERT INTO tasks (id,run_id,name,input,status) VALUES ('tsk_old_0','run_old_0','test','{\"input\":\"test\"}','PENDING')");
                assertCancellationConstraints(jdbc, new RuntimeStore(jdbc, new ObjectMapper()));
            }
        } finally {
            new JdbcTemplate(source).execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    private void assertCancellationConstraints(JdbcTemplate jdbc, RuntimeStore store) {
        int attempts = store.findTasks("run_old_0").getFirst().attempt();
        jdbc.update("UPDATE tasks SET status = 'CANCELLED', output = NULL, lease_owner = NULL, lease_expires_at = NULL WHERE id = 'tsk_old_0'");
        jdbc.update("UPDATE runs SET status = 'CANCELLED', output = NULL WHERE id = 'run_old_0'");
        assertThat(store.findRun("run_old_0").orElseThrow().status()).isEqualTo(RunStatus.CANCELLED);
        var task = store.findTasks("run_old_0").getFirst();
        assertThat(task.status()).isEqualTo(TaskStatus.CANCELLED);
        assertThat(task.attempt()).isEqualTo(attempts);
        for (String statement : List.of(
                "UPDATE runs SET status = 'UNKNOWN' WHERE id = 'run_old_0'",
                "UPDATE tasks SET status = 'UNKNOWN' WHERE id = 'tsk_old_0'",
                "UPDATE runs SET output = '{\"summary\":\"late\"}' WHERE id = 'run_old_0'",
                "UPDATE tasks SET output = '{\"summary\":\"late\"}' WHERE id = 'tsk_old_0'",
                "UPDATE tasks SET lease_owner = 'stale-worker' WHERE id = 'tsk_old_0'",
                "UPDATE tasks SET lease_expires_at = clock_timestamp() WHERE id = 'tsk_old_0'")) {
            assertThrows(DataAccessException.class, () -> jdbc.update(statement), statement);
        }
    }
}
