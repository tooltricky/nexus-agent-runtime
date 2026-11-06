package io.nexus.controlplane.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
class DeadlineMigrationIntegrationTest {
    @Test void v4UpgradePreservesEveryRowAndEnforcesTimeoutAndSharedDeadlineConstraints() throws Exception {
        var source = new DriverManagerDataSource(System.getenv("TEST_DB_URL"),
                System.getenv().getOrDefault("TEST_DB_USERNAME","nexus"), System.getenv().getOrDefault("TEST_DB_PASSWORD","nexus"));
        String schema = "b08_upgrade_" + UUID.randomUUID().toString().replace("-", "");
        var admin = new JdbcTemplate(source);
        try {
            Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).target("4").load().migrate();
            try (var c = source.getConnection()) {
                c.setSchema(schema);
                var jdbc = new JdbcTemplate(new SingleConnectionDataSource(c, true));
                jdbc.update("INSERT INTO sessions(id) VALUES('ses_old')");
                List<String> states = List.of("PENDING","LEASED","EXECUTING","RETRYING","SUCCEEDED","FAILED","CANCELLED");
                for (int i=0; i<states.size(); i++) {
                    String state=states.get(i);
                    jdbc.update("INSERT INTO runs(id,session_id,idempotency_key,request_hash,input,status,output) VALUES(?,'ses_old',?,'hash','old',?,?::jsonb)",
                            "run_"+i, "key_"+i, state.equals("PENDING") ? "QUEUED" : List.of("LEASED","EXECUTING","RETRYING").contains(state) ? "RUNNING" : state,
                            state.equals("SUCCEEDED") ? "{\"summary\":\"old\"}" : null);
                    jdbc.update("INSERT INTO tasks(id,run_id,name,input,status) VALUES(?,?,'old','{}',?)", "tsk_"+i,"run_"+i,state);
                    jdbc.update("INSERT INTO domain_events(event_id,aggregate_type,aggregate_id,run_id,event_type,payload) VALUES(?,'RUN',?,?,'RunCreated','{}')",
                            "evt_"+i, "run_"+i,"run_"+i);
                }
                var runs=jdbc.queryForList("SELECT to_jsonb(r)::text AS row FROM runs r ORDER BY id");
                var tasks=jdbc.queryForList("SELECT to_jsonb(t)::text AS row FROM tasks t ORDER BY id");
                var events=jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence");
                var flyway=Flyway.configure().dataSource(source).schemas(schema).defaultSchema(schema).load();
                assertThat(flyway.migrate().migrationsExecuted).isEqualTo(1); flyway.validate();
                assertThat(jdbc.queryForList("SELECT (to_jsonb(r)-'deadline_at')::text AS row FROM runs r ORDER BY id")).isEqualTo(runs);
                assertThat(jdbc.queryForList("SELECT (to_jsonb(t)-'deadline_at')::text AS row FROM tasks t ORDER BY id")).isEqualTo(tasks);
                assertThat(jdbc.queryForList("SELECT to_jsonb(e)::text AS row FROM domain_events e ORDER BY sequence")).isEqualTo(events);
                var store=new RuntimeStore(jdbc,new ObjectMapper());
                assertThat(store.findRun("run_0").orElseThrow().deadlineAt()).isNull();
                assertThat(store.findTasks("run_0").getFirst().deadlineAt()).isNull();
                for (String sql : List.of(
                        "UPDATE tasks SET status='TIMEOUT' WHERE id='tsk_0'",
                        "UPDATE runs SET status='TIMEOUT' WHERE id='run_0'",
                        "UPDATE runs SET deadline_at=clock_timestamp() WHERE id='run_0'",
                        "UPDATE tasks SET deadline_at=clock_timestamp() WHERE id='tsk_0'",
                        "UPDATE runs SET deadline_at='infinity' WHERE id='run_0'")) {
                    assertThrows(DataAccessException.class, () -> jdbc.update(sql));
                }
                c.setAutoCommit(false);
                jdbc.update("UPDATE runs SET deadline_at='2026-11-06T00:00:00Z',status='TIMEOUT' WHERE id='run_0'");
                jdbc.update("UPDATE tasks SET deadline_at='2026-11-06T00:00:00Z',status='TIMEOUT' WHERE id='tsk_0'");
                c.commit(); c.setAutoCommit(true);
                for (String sql : List.of(
                        "UPDATE runs SET output='{}' WHERE id='run_0'",
                        "UPDATE tasks SET output='{}' WHERE id='tsk_0'",
                        "UPDATE tasks SET lease_owner='late' WHERE id='tsk_0'",
                        "UPDATE tasks SET lease_expires_at=clock_timestamp() WHERE id='tsk_0'",
                        "UPDATE domain_events SET schema_version='0.3'", "DELETE FROM domain_events")) {
                    assertThrows(DataAccessException.class, () -> jdbc.update(sql));
                }
            }
        } finally { admin.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE"); }
    }
}
