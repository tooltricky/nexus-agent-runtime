package io.nexus.controlplane.service;

import io.nexus.controlplane.identity.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

class TenantIdentityConfigurationTest {
    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(TenantIdentityConfiguration.class).withBean(JdbcTemplate.class, () -> jdbc);

    @Test void identityIsDisabledByDefaultEvenIfIdentifiersArePresent() {
        runner.withPropertyValues("nexus.identity.tenant-id=legacy", "nexus.identity.project-id=legacy").run(context -> {
            assertThat(context).hasNotFailed();
            assertThrows(IdentityUnavailableException.class, () -> context.getBean(TenantContextSource.class).requireContext());
            verify(jdbc, never()).queryForObject(anyString(), eq(Boolean.class), anyString(), anyString());
        });
    }
    @Test void enabledIdentityMustBeCompleteAndMustReferenceAnExistingProject() {
        runner.withPropertyValues("nexus.identity.development-enabled=true").run(context -> assertThat(context).hasFailed());
        runner.withPropertyValues("nexus.identity.development-enabled=true", "nexus.identity.tenant-id=tenant",
                "nexus.identity.project-id=project").run(context -> {
            var source = context.getBean(TenantContextSource.class);
            assertThrows(IdentityUnavailableException.class, source::requireContext);
            when(jdbc.queryForObject(anyString(), eq(Boolean.class), eq("tenant"), eq("project"))).thenReturn(true);
            assertThat(source.requireContext()).isEqualTo(new TenantContext("tenant", "project"));
        });
    }
    @Test void contextIdentifiersRejectBlankOrOverlongValues() {
        assertThrows(IllegalArgumentException.class, () -> new TenantContext(" ", "project"));
        assertThrows(IllegalArgumentException.class, () -> new TenantContext("tenant", null));
        assertThrows(IllegalArgumentException.class, () -> new TenantContext("tenant", "x".repeat(101)));
    }
}
