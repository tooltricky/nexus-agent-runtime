package io.nexus.controlplane.contracts;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.nexus.controlplane.domain.RunStatus;
import io.nexus.controlplane.domain.TaskStatus;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CancellationContractTest {
    @Test
    void archivedRunAndEveryHistoricalEventRemainReadableUnderBothContracts() {
        JsonNode run = ContractAssertions.resource("examples/run-v0.1.json");
        assertThat(ContractAssertions.legacyRunErrors(run)).isEmpty();
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty();
        for (JsonNode event : ContractAssertions.resource("examples/events-v0.1.json")) {
            assertThat(ContractAssertions.legacyEventErrors(event)).as(event.path("event_type").asText()).isEmpty();
            ContractAssertions.event(event);
            ObjectNode versioned = event.deepCopy();
            versioned.put("schema_version", "0.2");
            ContractAssertions.event(versioned);
        }
    }

    @Test
    void newCancelledRunRequiresTheNewContractAndHasNoLeaseOrOutput() {
        ObjectNode run = (ObjectNode) ContractAssertions.resource("examples/run-cancelled-v0.2.json");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isEmpty();
        assertThat(ContractAssertions.legacyRunErrors(run)).isNotEmpty();
        ObjectNode task = (ObjectNode) run.path("tasks").get(0);
        for (String field : List.of("lease_owner", "lease_expires_at", "output")) {
            task.putNull(field);
            assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).as(field).isNotEmpty();
            task.remove(field);
        }
        run.putObject("output").put("summary", "late result");
        assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}", "get", 200, run)).isNotEmpty();
    }

    @Test
    void cancellationSourcesMatchTheDomainAndTerminalStatesAreRejected() {
        for (JsonNode item : ContractAssertions.resource("examples/events-cancelled-v0.2.json")) {
            ObjectNode event = item.deepCopy();
            String type = event.path("event_type").asText();
            ContractAssertions.event(event);
            assertThat(ContractAssertions.legacyEventErrors(event)).isNotEmpty();
            List<String> states = type.equals("TaskCancelled")
                    ? java.util.Arrays.stream(TaskStatus.values()).map(Enum::name).toList()
                    : java.util.Arrays.stream(RunStatus.values()).map(Enum::name).toList();
            for (String source : states) {
                ((ObjectNode) event.path("payload")).put("from", source);
                boolean terminal = List.of("SUCCEEDED", "FAILED", "CANCELLED").contains(source);
                assertThat(ContractAssertions.eventErrors(event).isEmpty()).as(type + " from " + source).isEqualTo(!terminal);
            }
        }
    }

    @Test
    void cancellationEventsRejectMissingWrongTypeAndInvalidPayloadFields() {
        for (JsonNode event : ContractAssertions.resource("examples/events-cancelled-v0.2.json")) {
            for (String field : ContractAssertions.requiredEventPayloadFields(event.path("event_type").asText())) {
                ObjectNode missing = event.deepCopy();
                ((ObjectNode) missing.path("payload")).remove(field);
                assertThat(ContractAssertions.eventErrors(missing)).as("missing " + field).isNotEmpty();
                ObjectNode wrong = event.deepCopy();
                ((ObjectNode) wrong.path("payload")).putNull(field);
                assertThat(ContractAssertions.eventErrors(wrong)).as("null " + field).isNotEmpty();
            }
            for (String field : List.of("to", "reason")) {
                ObjectNode wrong = event.deepCopy();
                ((ObjectNode) wrong.path("payload")).put(field, "unknown");
                assertThat(ContractAssertions.eventErrors(wrong)).isNotEmpty();
            }
            ObjectNode wrongKind = event.deepCopy();
            wrongKind.put("aggregate_type", event.path("aggregate_type").asText().equals("TASK") ? "RUN" : "TASK");
            assertThat(ContractAssertions.eventErrors(wrongKind)).isNotEmpty();
        }
        ObjectNode task = (ObjectNode) ContractAssertions.resource("examples/events-cancelled-v0.2.json").get(0);
        for (String field : List.of("attempt", "max_attempts")) {
            ObjectNode invalid = task.deepCopy();
            ((ObjectNode) invalid.path("payload")).put(field, -1);
            assertThat(ContractAssertions.eventErrors(invalid)).isNotEmpty();
        }
        task.remove("task_id");
        assertThat(ContractAssertions.eventErrors(task)).isNotEmpty();
    }

    @Test
    void explicitVersionsCannotFallBackToLegacyValidationIncludingInsideTimeline() {
        List<JsonNode> samples = List.of(
                ContractAssertions.resource("examples/events-v0.1.json").get(0),
                ContractAssertions.resource("examples/events-cancelled-v0.2.json").get(0));
        for (JsonNode sample : samples) {
            for (JsonNode version : List.of(
                    com.fasterxml.jackson.databind.node.TextNode.valueOf("unknown"),
                    com.fasterxml.jackson.databind.node.IntNode.valueOf(2),
                    com.fasterxml.jackson.databind.node.NullNode.instance)) {
                ObjectNode event = sample.deepCopy();
                event.set("schema_version", version);
                assertThat(ContractAssertions.eventErrors(event)).isNotEmpty();
                ObjectNode timeline = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
                timeline.put("run_id", event.path("run_id").asText());
                timeline.putArray("events").add(event);
                assertThat(ContractAssertions.responseErrors("/v1/runs/{run_id}/timeline", "get", 200, timeline)).isNotEmpty();
            }
        }
        ObjectNode cancelled = samples.get(1).deepCopy();
        cancelled.remove("schema_version");
        assertThat(ContractAssertions.eventErrors(cancelled)).isNotEmpty();
    }
}
