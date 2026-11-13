package io.nexus.controlplane.api;

import com.fasterxml.jackson.databind.JsonNode;
import io.nexus.controlplane.support.ContractAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfEnvironmentVariable(named = "TEST_DB_URL", matches = ".+")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "nexus.identity.development-enabled=false", "nexus.timeout.enabled=false",
        "nexus.worker.enabled=false", "nexus.watchdog.enabled=false",
        "spring.datasource.url=${TEST_DB_URL}", "spring.datasource.username=${TEST_DB_USERNAME:nexus}",
        "spring.datasource.password=${TEST_DB_PASSWORD:nexus}"
})
class TenantIdentityHttpIntegrationTest {
    @Autowired TestRestTemplate http;
    @Test void missingTrustedContextFailsClosedOnEveryResourceOperationDespiteForgedHeaders() {
        HttpHeaders headers = new HttpHeaders(); headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Idempotency-Key", "forged"); headers.set("X-Tenant-Id", "legacy"); headers.set("X-Project-Id", "legacy");
        for (String path : List.of("/v1/sessions", "/v1/runs", "/v1/runs/{run_id}",
                "/v1/runs/{run_id}/timeline", "/v1/runs/{run_id}/cancel", "/v1/runs/{run_id}/pause", "/v1/runs/{run_id}/resume")) {
            HttpMethod method = path.endsWith("{run_id}") || path.endsWith("/timeline") ? HttpMethod.GET : HttpMethod.POST;
            var response = http.exchange(path.replace("{run_id}", "run_missing"), method,
                    new HttpEntity<>(Map.of("session_id", "ses_fake", "input", "test", "tenant_id", "legacy"), headers), JsonNode.class);
            assertThat(response.getStatusCode().value()).as(path).isEqualTo(403);
            assertThat(response.getBody().path("code").asText()).isEqualTo("identity_unavailable");
            ContractAssertions.response(path, method.name().toLowerCase(), response);
        }
    }
}
