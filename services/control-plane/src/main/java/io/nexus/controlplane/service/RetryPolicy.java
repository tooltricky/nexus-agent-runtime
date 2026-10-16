package io.nexus.controlplane.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class RetryPolicy {
    private final long initialDelayMs;
    private final long maxDelayMs;

    public RetryPolicy(
            @Value("${nexus.worker.retry-initial-delay-ms:1000}") long initialDelayMs,
            @Value("${nexus.worker.retry-max-delay-ms:30000}") long maxDelayMs
    ) {
        if (initialDelayMs <= 0 || maxDelayMs < initialDelayMs) {
            throw new IllegalArgumentException("Retry delays must satisfy 0 < initial <= max");
        }
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
    }

    /** attempt counts all lease claims, including the initial execution. */
    public Duration delayAfter(int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("A failed attempt must have been claimed");
        }
        long delay = initialDelayMs;
        for (int i = 1; i < attempt && delay < maxDelayMs; i++) {
            // Saturate before multiplying, including for very large configured delays.
            delay = delay > maxDelayMs / 2 ? maxDelayMs : delay * 2;
        }
        return Duration.ofMillis(delay);
    }
}
