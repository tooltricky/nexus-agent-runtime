package io.nexus.controlplane.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public record TimeoutConfiguration(long pollDelayMs, int batchSize) {
    public TimeoutConfiguration(@Value("${nexus.timeout.poll-delay-ms:1000}") long pollDelayMs,
                                @Value("${nexus.timeout.batch-size:50}") int batchSize) {
        if (pollDelayMs <= 0) throw new IllegalArgumentException("nexus.timeout.poll-delay-ms must be positive");
        if (batchSize <= 0) throw new IllegalArgumentException("nexus.timeout.batch-size must be positive");
        this.pollDelayMs = pollDelayMs;
        this.batchSize = batchSize;
    }
}
