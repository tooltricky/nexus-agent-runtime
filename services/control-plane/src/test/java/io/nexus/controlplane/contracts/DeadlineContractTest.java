package io.nexus.controlplane.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class DeadlineContractTest {
    @Test void timeoutResponseRequiresDeadlineAndForbidsOutputAndLease() {
        ObjectNode run = ContractAssertions.withOwnership(ContractAssertions.resource("examples/run-timeout-v0.3.json"));
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}","get",200,run)).isEmpty();
        assertThat(ContractAssertions.legacyRunErrors(run)).isNotEmpty();
        for (ObjectNode node : List.of(run, (ObjectNode)run.path("tasks").get(0))) {
            JsonNode deadline=node.remove("deadline_at");
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}","get",200,run)).isNotEmpty();
            node.set("deadline_at",deadline);
            node.putObject("output").put("summary","late");
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}","get",200,run)).isNotEmpty();
            node.remove("output");
        }
        ((ObjectNode)run.path("tasks").get(0)).put("lease_owner","late");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}","get",200,run)).isNotEmpty();
    }
    @Test void requestSchemaAcceptsOnlyDocumentedTimestampShapes() {
        ObjectNode request = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode()
                .put("session_id", "session").put("input", "test");
        assertThat(ContractAssertions.requestErrors(request)).isEmpty();
        request.putNull("deadline_at");
        assertThat(ContractAssertions.requestErrors(request)).isEmpty();
        for (String value : List.of("2026-12-01T00:00:00Z", "2026-12-01T12:00:00.123456789+08:00",
                "2026-12-01T12:00:00+19:00", "2026-12-01T12:00:00-19:00",
                "2026-12-01T12:00:00+23:59", "2026-12-01t12:00:00.123456789-23:59",
                "9999-12-31T23:59:59-23:59", "0001-01-01T00:00:00+23:59")) {
            request.put("deadline_at", value);
            assertThat(ContractAssertions.requestErrors(request)).isEmpty();
        }
        for (String value : List.of("2026-12-01T12:00Z", "2026-12-01T12:00:00+08:00:01",
                "2026-12-01T00:00:00", "0000-01-01T00:00:00Z", "2026-12-01T12:00:00+24:00",
                "2026-12-01T12:00:00-24:00", "2026-12-01T12:00:00+19:60", "2026-12-01T12:00:00-00:60")) {
            request.put("deadline_at", value);
            assertThat(ContractAssertions.requestErrors(request)).isNotEmpty();
        }
    }

    @Test void newEventsHaveStrictVersionOriginsAndPayloads() {
        for (JsonNode example : ContractAssertions.resource("examples/events-timeout-v0.3.json")) {
            ContractAssertions.event(example);
            assertThat(ContractAssertions.legacyEventErrors(example)).isNotEmpty();
            ObjectNode bad=example.deepCopy(); bad.put("schema_version","0.2");
            assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            bad.put("schema_version","9.9"); assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            bad=example.deepCopy(); ((ObjectNode)bad.path("payload")).put("from","TIMEOUT");
            assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            for (String field : ContractAssertions.requiredEventPayloadFields(example.path("event_type").asText())) {
                bad=example.deepCopy(); ((ObjectNode)bad.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(bad)).isNotEmpty();
            }
        }
    }
}
