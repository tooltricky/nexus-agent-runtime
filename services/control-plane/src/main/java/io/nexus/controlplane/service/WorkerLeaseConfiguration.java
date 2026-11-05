package io.nexus.controlplane.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/** Validate lease settings even when the local worker is disabled. */
@Configuration
public class WorkerLeaseConfiguration {
    private final long leaseDurationMs;
    private final long heartbeatIntervalMs;

    public WorkerLeaseConfiguration(
            @Value("${nexus.worker.lease-duration-ms:30000}") long leaseDurationMs,
            @Value("${nexus.worker.heartbeat-interval-ms:10000}") long heartbeatIntervalMs
    ) {
        if (leaseDurationMs <= 0) {
            throw new IllegalArgumentException("nexus.worker.lease-duration-ms must be positive");
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("nexus.worker.heartbeat-interval-ms must be positive");
        }
        if (heartbeatIntervalMs >= leaseDurationMs) {
            throw new IllegalArgumentException(
                    "nexus.worker.heartbeat-interval-ms must be less than nexus.worker.lease-duration-ms");
        }
        this.leaseDurationMs = leaseDurationMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public long leaseDurationMs() { return leaseDurationMs; }
    public long heartbeatIntervalMs() { return heartbeatIntervalMs; }
}
