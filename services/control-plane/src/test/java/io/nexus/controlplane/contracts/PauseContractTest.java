package io.nexus.controlplane.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class PauseContractTest {
    @Test void pausedRequiresAnUnconsumedCheckpointAndNoLeaseOrOutput() {
        ObjectNode run = (ObjectNode) ContractAssertions.resource("examples/run-paused-v0.4.json");
        valid(run);
        assertThat(ContractAssertions.legacyRunErrors(run)).isNotEmpty();
        for (String path : List.of("/v1/runs/{run_id}/pause", "/v1/runs/{run_id}/resume")) {
            assertThat(ContractAssertions.responseErrors(path, "post", 200, run)).isEmpty();
        }
        ObjectNode task = (ObjectNode) run.path("tasks").get(0);
        JsonNode checkpoint = task.remove("checkpoint"); invalid(run); task.set("checkpoint", checkpoint);
        for (String field : List.of("schema_version", "checkpoint_id", "task_id", "run_id", "run_status", "task_status",
                "attempt", "task_version", "run_version", "next_attempt_at", "paused_at")) {
            ObjectNode copy = run.deepCopy();
            ((ObjectNode) copy.path("tasks").get(0).path("checkpoint")).remove(field); invalid(copy);
            copy = run.deepCopy();
            ((ObjectNode) copy.path("tasks").get(0).path("checkpoint")).putArray(field); invalid(copy);
        }
        for (String field : List.of("lease_owner", "lease_expires_at", "output")) {
            task.put(field, "bad"); invalid(run); task.remove(field);
        }
        ObjectNode cp = (ObjectNode) checkpoint;
        cp.put("consumed_at", "2026-11-07T09:00:00Z"); invalid(run);
        cp.remove("consumed_at"); cp.put("disposition", "RESUMED"); invalid(run);
        cp.remove("disposition"); valid(run);
        run.put("status", "RUNNING"); task.put("status", "PENDING"); invalid(run);
        cp.put("consumed_at", "2026-11-07T09:00:00Z").put("disposition", "RESUMED"); valid(run);
        cp.put("disposition", "UNKNOWN"); invalid(run);
    }

    @Test void everyNewEventRequiresItsFieldsAndCannotUseAnOlderOrUnknownVersion() {
        for (JsonNode example : ContractAssertions.resource("examples/events-pause-v0.4.json")) {
            ContractAssertions.event(example);
            for (String version : List.of("0.1", "0.2", "0.3", "9.9")) {
                ObjectNode bad = example.deepCopy(); bad.put("schema_version", version);
                assertThat(ContractAssertions.eventErrors(bad)).as("%s version %s", example.path("event_type"), version).isNotEmpty();
            }
            for (String field : iterableFields(example.path("payload"))) {
                ObjectNode bad = example.deepCopy(); ((ObjectNode) bad.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(bad)).as("missing %s", field).isNotEmpty();
                bad = example.deepCopy(); ((ObjectNode) bad.path("payload")).putArray(field);
                assertThat(ContractAssertions.eventErrors(bad)).as("wrong type %s", field).isNotEmpty();
            }
            ObjectNode bad = example.deepCopy(); ((ObjectNode) bad.path("payload")).put("from", "SUCCEEDED");
            assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            bad = example.deepCopy(); bad.put("event_type", "Unknown");
            assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
        }
    }
    private List<String> iterableFields(JsonNode node) {
        var fields = new java.util.ArrayList<String>(); node.fieldNames().forEachRemaining(fields::add); return fields;
    }
    private void valid(JsonNode run) { assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty(); }
    private void invalid(JsonNode run) { assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isNotEmpty(); }
}
