package io.nexus.controlplane.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SchemaValidatorsConfig;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Validates the operation/status selected from the actual, version-controlled OpenAPI document. */
public final class ContractAssertions {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final JsonSchemaFactory FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    private static final SchemaValidatorsConfig CONFIG = SchemaValidatorsConfig.builder()
            .formatAssertionsEnabled(true).build();
    private static final JsonNode API = readApi("openapi.yaml");
    private static final JsonNode LEGACY_API = readApi("openapi-v0.1.yaml");
    private static final JsonNode EVENT_SCHEMA = resource("events/domain-event.schema.json");
    private static final JsonNode NEW_EVENT_SCHEMA = resource("events/domain-event-v0.2.schema.json");
    private static final JsonSchema LEGACY_EVENT = FACTORY.getSchema(EVENT_SCHEMA, CONFIG);
    private static final JsonSchema EVENT = FACTORY.getSchema(API.at("/components/schemas/DomainEvent"), CONFIG);

    private ContractAssertions() { }

    public static void response(String path, String method, ResponseEntity<JsonNode> response) {
        if (method.equals("get") && response.getStatusCode().is2xxSuccessful()) {
            assertThat(response.getHeaders().getFirst("Nexus-API-Contract")).isEqualTo("0.2");
        }
        assertThat(response.getHeaders().getContentType()).isNotNull()
                .satisfies(type -> assertThat(type.isCompatibleWith(MediaType.APPLICATION_JSON)).isTrue());
        JsonNode schema = responseSchema(path, method, response.getStatusCode().value());
        assertThat(errors(schema, response.getBody())).as("%s %s response %s", method, path, response.getStatusCode()).isEmpty();
    }

    public static void event(JsonNode event) {
        assertThat(eventErrors(event)).as("event %s", event).isEmpty();
    }

    public static Set<ValidationMessage> eventErrors(JsonNode event) {
        return EVENT.validate(event);
    }

    public static Set<ValidationMessage> legacyEventErrors(JsonNode event) {
        return LEGACY_EVENT.validate(event);
    }

    public static Set<ValidationMessage> legacyRunErrors(JsonNode run) {
        return errors(LEGACY_API.at("/components/schemas/Run"), run, LEGACY_API);
    }

    public static JsonNode resource(String name) {
        try (var input = ContractAssertions.class.getResourceAsStream("/contracts/" + name)) {
            if (input == null) throw new IllegalStateException("Missing contract resource " + name);
            return JSON.readTree(input);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read contract " + name, e);
        }
    }

    public static Set<ValidationMessage> responseErrors(String path, String method, int status, JsonNode body) {
        return errors(responseSchema(path, method, status), body);
    }

    public static Set<ValidationMessage> requestErrors(JsonNode body) {
        return errors(API.at("/paths/~1v1~1runs/post/requestBody/content/application~1json/schema"), body);
    }

    public static Set<ValidationMessage> keyErrors(String key) {
        return errors(API.at("/components/parameters/IdempotencyKey/schema"), JSON.valueToTree(key));
    }

    public static List<String> requiredRunFields() {
        return requiredFields(API.at("/components/schemas/Run/required"));
    }

    public static List<String> requiredEventPayloadFields(String eventType) {
        JsonNode required = EVENT_SCHEMA.path("$defs").path(eventType).path("required");
        if (required.isMissingNode()) required = NEW_EVENT_SCHEMA.path("$defs").path(eventType).path("required");
        assertThat(required.isArray()).as("documented event type %s", eventType).isTrue();
        return requiredFields(required);
    }

    private static List<String> requiredFields(JsonNode required) {
        assertThat(required.isArray()).isTrue();
        return java.util.stream.StreamSupport.stream(required.spliterator(), false)
                .map(JsonNode::asText).toList();
    }

    private static JsonNode responseSchema(String path, String method, int status) {
        JsonNode operation = API.path("paths").path(path).path(method);
        assertThat(operation.isMissingNode()).as("documented operation %s %s", method, path).isFalse();
        JsonNode response = operation.path("responses").path(Integer.toString(status));
        if (response.has("$ref")) response = API.at(response.path("$ref").asText().substring(1));
        JsonNode schema = response.at("/content/application~1json/schema");
        assertThat(schema.isMissingNode()).as("documented JSON response %s %s %d", method, path, status).isFalse();
        return schema;
    }

    private static Set<ValidationMessage> errors(JsonNode schema, JsonNode instance) {
        return errors(schema, instance, API);
    }

    private static Set<ValidationMessage> errors(JsonNode schema, JsonNode instance, JsonNode api) {
        // OpenAPI 3.1 Schema Objects use JSON Schema 2020-12; keep component refs in scope.
        ObjectNode root = JSON.createObjectNode();
        root.set("$defs", api.at("/components/schemas"));
        root.setAll((ObjectNode) schema);
        try {
            JsonNode jsonSchema = JSON.readTree(JSON.writeValueAsString(root)
                    .replace("#/components/schemas/", "#/$defs/"));
            return FACTORY.getSchema(jsonSchema, CONFIG).validate(instance);
        } catch (IOException e) {
            throw new IllegalStateException("Cannot resolve OpenAPI schema references", e);
        }
    }

    private static JsonNode readApi(String name) {
        try {
            JsonNode api = new ObjectMapper(new YAMLFactory()).readTree(
                    ContractAssertions.class.getResourceAsStream("/contracts/" + name));
            assertThat(api.path("openapi").asText()).isEqualTo("3.1.0");
            resolveEventReferences(api.at("/components/schemas/DomainEvent"));
            return api;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot load OpenAPI contract", e);
        }
    }

    private static void resolveEventReferences(JsonNode node) {
        if (node.isObject() && node.has("$ref")) {
            String ref = node.path("$ref").asText();
            assertThat(ref).startsWith("./events/");
            try {
                ((ObjectNode) node).put("$ref", ContractAssertions.class
                        .getResource("/contracts/" + ref.substring(2)).toURI().toString());
            } catch (Exception e) {
                throw new IllegalStateException("Cannot resolve event contract " + ref, e);
            }
        }
        node.elements().forEachRemaining(ContractAssertions::resolveEventReferences);
    }
}
