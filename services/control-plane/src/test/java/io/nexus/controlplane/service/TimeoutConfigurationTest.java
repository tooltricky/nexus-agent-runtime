package io.nexus.controlplane.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.assertThat;

class TimeoutConfigurationTest {
    private final ApplicationContextRunner contexts = new ApplicationContextRunner().withUserConfiguration(TimeoutConfiguration.class);
    @Test void validDefaultsAndOverridesStart() {
        contexts.run(c -> { assertThat(c).hasNotFailed(); assertThat(c.getBean(TimeoutConfiguration.class).batchSize()).isEqualTo(50); });
        contexts.withPropertyValues("nexus.timeout.poll-delay-ms=10", "nexus.timeout.batch-size=1")
                .run(c -> assertThat(c).hasNotFailed());
    }
    @ParameterizedTest
    @ValueSource(strings = {"nexus.timeout.poll-delay-ms=0","nexus.timeout.poll-delay-ms=-1",
            "nexus.timeout.batch-size=0","nexus.timeout.batch-size=-1"})
    void invalidConfigurationFailsEvenWhenDisabled(String setting) {
        contexts.withPropertyValues("nexus.timeout.enabled=false", setting).run(c -> {
            assertThat(c).hasFailed();
            assertThat(c.getStartupFailure()).hasRootCauseMessage(setting.split("=")[0] + " must be positive");
        });
    }
}
