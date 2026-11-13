package io.nexus.controlplane.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class TenantContractTest {
    @Test void currentResponsesRequireOwnershipWhileFrozenResponseSamplesRemainValid() {
        ObjectNode original = (ObjectNode) ContractAssertions.resource("examples/run-paused-v0.4.json");
        assertThat(ContractAssertions.archivedPauseRunErrors(original)).isEmpty();
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, original)).isNotEmpty();
        ObjectNode current = (ObjectNode) ContractAssertions.resource("examples/run-owned-v0.5.json");
        validRun(current);
        for (String field : List.of("tenant_id", "project_id")) {
            for (boolean task : List.of(false, true)) {
                ObjectNode missing = current.deepCopy();
                (task ? (ObjectNode) missing.path("tasks").get(0) : missing).remove(field);
                assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, missing)).isNotEmpty();
                for (Object value : List.of(3, "", " ", "x".repeat(101))) {
                    ObjectNode bad = current.deepCopy(); ObjectNode node = task ? (ObjectNode) bad.path("tasks").get(0) : bad;
                    if (value instanceof Integer number) node.put(field, number); else node.put(field, (String) value);
                    assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, bad)).isNotEmpty();
                }
            }
        }
    }
    @Test void timelineRequiresOwnershipOverlayWithoutRewritingOriginalPayloadVersion() {
        for (JsonNode event : ContractAssertions.resource("examples/events-owned-v0.5.json")) {
            ContractAssertions.event(event);
            ObjectNode timeline = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode().put("run_id", event.path("run_id").asText());
            timeline.putArray("events").add(event);
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}/timeline", "get", 200, timeline)).isEmpty();
            for (String field : List.of("tenant_id", "project_id")) {
                ObjectNode bad = timeline.deepCopy(); ((ObjectNode) bad.path("events").get(0)).remove(field);
                assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}/timeline", "get", 200, bad)).isNotEmpty();
                bad = timeline.deepCopy(); ((ObjectNode) bad.path("events").get(0)).put(field, 5);
                assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}/timeline", "get", 200, bad)).isNotEmpty();
            }
        }
    }
    private void validRun(JsonNode run) { assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty(); }
}
