package io.nexus.controlplane.identity;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class TenantIdentityConfiguration {
    @Bean
    @ConditionalOnMissingBean(TenantContextSource.class)
    TenantContextSource tenantContextSource(Environment environment, JdbcTemplate jdbc) {
        boolean enabled = environment.getProperty("nexus.identity.development-enabled", Boolean.class, false);
        TenantContext configured;
        if (enabled) {
            configured = new TenantContext(environment.getProperty("nexus.identity.tenant-id"),
                    environment.getProperty("nexus.identity.project-id"));
        } else {
            configured = null;
        }
        return () -> {
            if (configured == null || !Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT EXISTS(SELECT 1 FROM projects WHERE tenant_id = ? AND id = ?)", Boolean.class,
                    configured.tenantId(), configured.projectId()))) {
                throw new IdentityUnavailableException();
            }
            return configured;
        };
    }
}
