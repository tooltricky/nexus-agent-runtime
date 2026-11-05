package io.nexus.controlplane.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerLeaseConfigurationTest {
    private final ApplicationContextRunner contexts = new ApplicationContextRunner()
            .withUserConfiguration(WorkerLeaseConfiguration.class);

    @Test
    void defaultsAndValidOverridesStart() {
        contexts.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(WorkerLeaseConfiguration.class).leaseDurationMs()).isEqualTo(30_000);
            assertThat(context.getBean(WorkerLeaseConfiguration.class).heartbeatIntervalMs()).isEqualTo(10_000);
        });
        contexts.withPropertyValues("nexus.worker.lease-duration-ms=500", "nexus.worker.heartbeat-interval-ms=100")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @ParameterizedTest
    @CsvSource({"0,100,lease-duration-ms", "-1,100,lease-duration-ms",
            "30000,0,heartbeat-interval-ms", "30000,-1,heartbeat-interval-ms",
            "100,100,heartbeat-interval-ms", "100,101,heartbeat-interval-ms"})
    void invalidConfigurationFailsEvenWhenWorkerDisabled(long lease, long heartbeat, String property) {
        contexts.withPropertyValues("nexus.worker.enabled=false",
                        "nexus.worker.lease-duration-ms=" + lease,
                        "nexus.worker.heartbeat-interval-ms=" + heartbeat)
                .run(context -> {
                    assertThat(context).hasFailed();
                    assertThat(context.getStartupFailure()).hasRootCauseMessage(
                            property.equals("lease-duration-ms")
                                    ? "nexus.worker.lease-duration-ms must be positive"
                                    : heartbeat <= 0 ? "nexus.worker.heartbeat-interval-ms must be positive"
                                    : "nexus.worker.heartbeat-interval-ms must be less than nexus.worker.lease-duration-ms");
                });
    }
}
